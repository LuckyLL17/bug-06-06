import { assertOrThrow } from '../core/errors'; import { newId } from '../core/id'; import { TwinStore } from '../domain/store'; import { DomainEvent, Telemetry } from '../domain/types';

export interface TelemetryIngestInput extends Telemetry { eventId?: string }
export interface IngestOutcome { event:DomainEvent; created:boolean }

const QUALITIES = new Set<Telemetry['quality']>(['good','suspect','bad']);

export class TelemetryService {
 constructor(private store:TwinStore){}

 /**
  * 幂等接入：eventId 是事件的去重键。
  * 顺序为 校验 → 去重 → 分配序列 → 原子提交：
  * - 校验失败的请求不写入任何状态、不占用序列号，修复后可用同一 eventId 安全重试；
  * - 相同 eventId 的重试直接复用既有事件（首次写入胜出），不产生重复事件、不消耗新序列；
  * - 提交是同步的，索引、事件日志、遥测样本在同一事件循环节拍内写入，对并发请求而言是原子的。
  */
 ingest(input:TelemetryIngestInput):IngestOutcome {
  validateTelemetry(input);
  const id = input.eventId ?? newId('evt');
  const existing = this.store.eventIndex.get(id);
  if(existing) return { event:existing, created:false };

  const event:DomainEvent = {
   id,
   type:'telemetry',
   assetId:input.assetId,
   timestamp:input.observedAt,
   sequence:this.store.eventSequences.next(),
   payload:{ metric:input.metric, value:input.value, quality:input.quality },
  };
  this.store.eventIndex.set(id,event);
  this.store.events.push(event);
  this.store.telemetry.push({ assetId:input.assetId, metric:input.metric, value:input.value, unit:input.unit, observedAt:input.observedAt, quality:input.quality });
  return { event, created:true };
 }

 /** 事件索引查询：回放与审计引用核对同一事件的入口 */
 getEvent(id:string){ return this.store.eventIndex.get(id) ?? null; }

 history(assetId:string,metric?:string){return this.store.telemetry.filter(t=>t.assetId===assetId&&(!metric||t.metric===metric)).sort((a,b)=>a.observedAt-b.observedAt)}

 /** 回放：窗口内事件按 (业务时间, 序列号) 确定性排序；序列号唯一，因此相同时间戳下顺序依然稳定 */
 replay(from:number,to:number){
  assertOrThrow(Number.isFinite(from)&&Number.isFinite(to)&&from<=to,'INVALID_REPLAY_WINDOW','replay window must be finite and from <= to');
  return this.store.events.filter(e=>e.timestamp>=from&&e.timestamp<=to).sort((a,b)=>a.timestamp-b.timestamp||a.sequence-b.sequence);
 }
}

function validateTelemetry(input:any):asserts input is TelemetryIngestInput {
 assertOrThrow(!!input&&typeof input==='object','INVALID_TELEMETRY','telemetry body must be an object');
 assertOrThrow(typeof input.assetId==='string'&&input.assetId.length>0,'INVALID_TELEMETRY','assetId must be a non-empty string');
 assertOrThrow(typeof input.metric==='string'&&input.metric.length>0,'INVALID_TELEMETRY','metric must be a non-empty string');
 assertOrThrow(typeof input.value==='number'&&Number.isFinite(input.value),'INVALID_TELEMETRY','value must be a finite number');
 assertOrThrow(typeof input.unit==='string','INVALID_TELEMETRY','unit must be a string');
 assertOrThrow(Number.isFinite(input.observedAt),'INVALID_TELEMETRY','observedAt must be a finite number');
 assertOrThrow(QUALITIES.has(input.quality),'INVALID_TELEMETRY','quality must be one of good|suspect|bad');
 assertOrThrow(input.eventId===undefined||(typeof input.eventId==='string'&&input.eventId.length>0),'INVALID_TELEMETRY','eventId must be a non-empty string when provided');
}
