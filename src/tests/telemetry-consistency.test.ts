import test from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../app';
import { TwinStore } from '../domain/store';
import { TelemetryService } from '../services/telemetry-service';

const BASE = 1_700_000_000_000;
const METRICS = ['temperature','pressure','flow','vibration'];

type App = ReturnType<typeof buildApp>;

function reading(i:number,overrides:Record<string,unknown>={}):Record<string,unknown>{
  return {
    eventId:`evt-${i}`,
    assetId:`asset-${i%12}`,
    metric:METRICS[i%METRICS.length],
    value:i*1.25,
    unit:'bar',
    observedAt:BASE+(i%60)*1000,
    quality:'good',
    ...overrides,
  };
}

async function ingest(app:App,body:Record<string,unknown>){
  return app.inject({method:'POST',url:'/telemetry',payload:body});
}

async function replayWindow(app:App,from:number,to:number):Promise<any[]>{
  const res=await app.inject({method:'GET',url:`/telemetry/replay?from=${from}&to=${to}`});
  assert.equal(res.statusCode,200,res.body);
  return res.json();
}

async function replayAll(app:App):Promise<any[]>{
  return replayWindow(app,0,Number.MAX_SAFE_INTEGER);
}

async function ingestAudits(app:App):Promise<any[]>{
  const res=await app.inject({method:'GET',url:'/audit'});
  return (res.json() as any[]).filter(a=>a.action==='telemetry.ingest');
}

/** 全链路一致性断言：回放结果、事件索引、审计引用三者互相对应 */
function assertChainConsistent(events:any[],audits:any[],expectedCount:number){
  assert.equal(events.length,expectedCount,'回放必须恰好返回全部已接收事件，一条不多一条不少');
  const ids=events.map(e=>e.id);
  assert.equal(new Set(ids).size,expectedCount,'每个 eventId 恰好保留一次');
  const seqs=events.map(e=>e.sequence);
  assert.equal(new Set(seqs).size,expectedCount,'sequence 在实例内唯一');
  assert.deepEqual([...seqs].sort((a,b)=>a-b),Array.from({length:expectedCount},(_,i)=>i+1),'sequence 为无空洞的 1..N（失败/重试不占号）');
  for(let i=1;i<events.length;i++){
    const prev=events[i-1],cur=events[i];
    assert.ok(prev.timestamp<cur.timestamp||(prev.timestamp===cur.timestamp&&prev.sequence<cur.sequence),'回放按 (timestamp, sequence) 排序');
  }
  assert.equal(audits.length,expectedCount,'每个事件恰好一条接入审计');
  const byId=new Map(events.map(e=>[e.id,e]));
  const audited=new Set<string>();
  for(const a of audits){
    const ev=byId.get(a.entityId);
    assert.ok(ev,`审计 ${a.id} 引用的 eventId ${a.entityId} 必须存在于事件索引`);
    assert.equal(a.metadata.sequence,ev.sequence,'审计记录的 sequence 与事件一致');
    assert.ok(!audited.has(a.entityId),'同一事件不得有重复审计');
    audited.add(a.entityId);
  }
}

test('并发场景：150 条混合资产/指标并发提交后，索引、审计、回放互相对应',async()=>{
  const app=buildApp();
  const N=150;
  const responses=await Promise.all(Array.from({length:N},(_,i)=>ingest(app,reading(i))));
  for(const res of responses)assert.equal(res.statusCode,200,res.body);
  const events=responses.map(r=>r.json());
  assert.equal(new Set(events.map(e=>e.id)).size,N,'每个 eventId 恰好保留一次');
  // 事件索引查询与接入响应一致
  await Promise.all(events.slice(0,25).map(async e=>{
    const got=await app.inject({method:'GET',url:`/telemetry/events/${e.id}`});
    assert.equal(got.statusCode,200);
    assert.deepEqual(got.json(),e,'索引中的事件与接入结果一致');
  }));
  assertChainConsistent(await replayAll(app),await ingestAudits(app),N);
  await app.close();
});

test('重试交错：相同事件重试复用既有结果，与新事件交错提交互不干扰',async()=>{
  const app=buildApp();
  const first=await Promise.all(Array.from({length:60},(_,i)=>ingest(app,reading(i))));
  const original=new Map(first.map(r=>{const e=r.json();return [e.id,e]as const;}));

  // 第二波：60 条重试（其中一半篡改了 payload，首次写入必须胜出）与 40 条新事件交错并发
  const retries=Array.from({length:60},(_,i)=>reading(i,i%2?{value:9999}:{}));
  const fresh=Array.from({length:40},(_,i)=>reading(1000+i));
  const wave:Record<string,unknown>[]=[];
  for(let i=0;i<60;i++){wave.push(retries[i]);if(i<40)wave.push(fresh[i]);}
  const second=await Promise.all(wave.map(b=>ingest(app,b)));
  for(const res of second)assert.equal(res.statusCode,200,res.body);

  for(const res of second){
    const e=res.json();
    const o=original.get(e.id);
    if(o){
      assert.equal(e.sequence,o.sequence,'重试复用既有事件的 sequence');
      assert.equal(e.payload.value,o.payload.value,'首次写入胜出：篡改 payload 的重试不得覆盖原事件');
    }
  }
  // 重试不产生新事件、新审计：总数 = 60 + 40
  assertChainConsistent(await replayAll(app),await ingestAudits(app),100);
  await app.close();
});

test('异常穿插：非法请求干净失败、不占序列号，修复后重试成功且不破坏已成功数据',async()=>{
  const app=buildApp();
  const valids=Array.from({length:40},(_,i)=>reading(i));
  const invalids=[
    reading(100,{metric:''}),
    reading(101,{value:'hot'}),
    reading(102,{quality:'unknown'}),
    reading(103,{assetId:''}),
    reading(104,{observedAt:'yesterday'}),
  ];
  const mixed:Record<string,unknown>[]=[];
  for(let i=0;i<40;i++){mixed.push(valids[i]);if(i<invalids.length)mixed.push(invalids[i]);}
  const responses=await Promise.all(mixed.map(b=>ingest(app,b)));

  const badIds=new Set(invalids.map(b=>b.eventId as string));
  let okCount=0;
  responses.forEach((res,idx)=>{
    const body=mixed[idx];
    if(badIds.has(body.eventId as string)){
      assert.equal(res.statusCode,400,`非法请求 ${body.eventId} 必须被拒绝`);
      assert.equal(res.json().code,'INVALID_TELEMETRY');
    }else{
      assert.equal(res.statusCode,200,`合法请求 ${body.eventId} 必须成功: ${res.body}`);
      okCount++;
    }
  });
  assert.equal(okCount,40);
  // 失败请求不留痕迹：恰好 40 条事件，序列号无空洞
  assertChainConsistent(await replayAll(app),await ingestAudits(app),40);

  // 修复失败请求后以原 eventId 重试，同时重试 10 条已成功事件（模拟网络重发）
  const fixed=invalids.map(b=>reading(200,{eventId:b.eventId}));
  const reRetries=Array.from({length:10},(_,i)=>reading(i));
  const second=await Promise.all([...fixed,...reRetries].map(b=>ingest(app,b)));
  for(const res of second)assert.equal(res.statusCode,200,res.body);

  const events=await replayAll(app);
  assertChainConsistent(events,await ingestAudits(app),45);
  const byId=new Map(events.map(e=>[e.id,e]));
  for(let i=0;i<40;i++)assert.ok((byId.get(`evt-${i}`)!.sequence as number)<=40,'已成功事件的 sequence 不被后续重试改变');
  for(const b of invalids)assert.ok(byId.has(b.eventId as string),'修复后的重试以原 eventId 补入');
  await app.close();
});

test('混合资产：相同时间戳下不同资产与指标互不覆盖，回放顺序确定',async()=>{
  const app=buildApp();
  const metrics10=['temperature','pressure','flow','vibration','current','voltage','rpm','torque','humidity','level'];
  const bodies:Record<string,unknown>[]=[];
  for(let a=0;a<10;a++)for(let m=0;m<10;m++)
    bodies.push({eventId:`evt-a${a}-m${m}`,assetId:`asset-${a}`,metric:metrics10[m],value:a*100+m,unit:'u',observedAt:BASE,quality:'good'});
  const responses=await Promise.all(bodies.map(b=>ingest(app,b)));
  for(const res of responses)assert.equal(res.statusCode,200,res.body);

  const events=await replayAll(app);
  assertChainConsistent(events,await ingestAudits(app),100);
  assert.ok(events.every(e=>e.timestamp===BASE),'全部事件同一时间戳');
  const seqs=events.map(e=>e.sequence);
  assert.deepEqual(seqs,[...seqs].sort((a,b)=>a-b),'相同时间戳下回放严格按 sequence 升序');
  const pairs=new Set(events.map(e=>`${e.assetId}/${e.payload.metric}`));
  assert.equal(pairs.size,100,'每个 资产×指标 组合都独立保留，互不覆盖');
  await app.close();
});

test('完整回放：并发写入后整窗回放还原现场，子窗口拼接与整窗一致',async()=>{
  const app=buildApp();
  const N=240;
  const windowMs=120_000;
  const bodies=Array.from({length:N},(_,i)=>reading(i,{observedAt:BASE+(i*500)%windowMs}));
  const responses=await Promise.all(bodies.map(b=>ingest(app,b)));
  for(const res of responses)assert.equal(res.statusCode,200,res.body);

  const full=await replayWindow(app,BASE,BASE+windowMs);
  assert.equal(full.length,N,'整窗回放恰好还原全部并发写入');
  // 6 个互不相交的子窗口回放拼接后必须与整窗回放完全一致（顺序也一致）
  const slices=await Promise.all([0,1,2,3,4,5].map(k=>
    replayWindow(app,BASE+k*20_000,k===5?BASE+windowMs:BASE+(k+1)*20_000-1)));
  const stitched=slices.flat();
  assert.deepEqual(stitched.map(e=>e.id),full.map(e=>e.id),'子窗口回放拼接等于整窗回放');
  // 回放出的每个事件都能通过事件索引查到，且内容一致
  await Promise.all(full.map(async e=>{
    const got=await app.inject({method:'GET',url:`/telemetry/events/${e.id}`});
    assert.equal(got.statusCode,200);
    assert.equal(got.json().sequence,e.sequence,'索引查询与回放结果一致');
  }));
  assertChainConsistent(full,await ingestAudits(app),N);
  await app.close();
});

test('服务层：幂等接入复用结果对象，失败不耗序列号，自动生成 id 也可索引',()=>{
  const store=new TwinStore();
  const svc=new TelemetryService(store);
  const input={eventId:'evt-1',assetId:'pump-1',metric:'pressure',value:3.2,unit:'bar',observedAt:BASE,quality:'good' as const};

  const first=svc.ingest(input);
  assert.equal(first.created,true);
  assert.equal(first.event.sequence,1);

  const again=svc.ingest(input);
  assert.equal(again.created,false);
  assert.equal(again.event,first.event,'重试复用既有结果对象');
  assert.equal(store.events.length,1);
  assert.equal(store.eventIndex.size,1);
  assert.equal(store.telemetry.length,1);

  // 校验失败：不写事件、不写样本、不占序列号
  assert.throws(()=>svc.ingest({...input,eventId:'evt-2',value:Number.NaN}),(e:any)=>e.code==='INVALID_TELEMETRY');
  assert.equal(store.events.length,1);
  assert.equal(store.eventIndex.size,1);
  assert.equal(store.telemetry.length,1);

  const next=svc.ingest({...input,eventId:'evt-2'});
  assert.equal(next.event.sequence,2,'失败请求未消耗序列号');

  // 未提供 eventId 时服务端生成，同样进入索引、可查询
  const auto=svc.ingest({assetId:'pump-1',metric:'pressure',value:3.3,unit:'bar',observedAt:BASE+1,quality:'good'});
  assert.ok(auto.event.id.length>0);
  assert.equal(svc.getEvent(auto.event.id),auto.event);
  assert.equal(svc.getEvent('missing'),null);

  // 回放窗口校验
  assert.throws(()=>svc.replay(Number.NaN,10),(e:any)=>e.code==='INVALID_REPLAY_WINDOW');
  assert.throws(()=>svc.replay(10,1),(e:any)=>e.code==='INVALID_REPLAY_WINDOW');
});
