import { test } from 'node:test';
import assert from 'node:assert/strict';
import { buildApp } from '../app';
import { FastifyInstance } from 'fastify';

function body(eventId: string, over: Record<string, unknown> = {}) {
  return {
    eventId,
    assetId: 'pump-1',
    metric: 'pressure',
    value: 1.5,
    unit: 'MPa',
    observedAt: 5000,
    quality: 'good',
    ...over,
  };
}

function jittered<T>(factory: () => Promise<T>): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    setTimeout(() => {
      try {
        factory().then(resolve, reject);
      } catch (err) {
        reject(err);
      }
    }, Math.floor(Math.random() * 5));
  });
}

test('HTTP 百条并发提交 + 重试交错：200 幂等、409 冲突、400 非法，回放审计互相对应', async () => {
  const app: FastifyInstance = buildApp();
  const N = 130;
  const assets = ['pump-1', 'pump-2', 'valve-3', 'sensor-4', 'reactor-5'];
  const metrics = ['pressure', 'flow', 'temperature'];

  // 每个事件再随机附加 1~2 次相同重试，另穿插冲突重试与非法请求
  type InjectResult = Awaited<ReturnType<FastifyInstance['inject']>>;
  const calls: Array<() => Promise<InjectResult>> = [];
  for (let i = 0; i < N; i++) {
    const b = body(`http_evt_${i}`, {
      assetId: assets[i % assets.length],
      metric: metrics[i % metrics.length],
      value: i,
      observedAt: 5000 + (i % 23),
    });
    calls.push(() => app.inject({ method: 'POST', url: '/telemetry', payload: b }));
    if (i % 3 === 0) {
      calls.push(() => app.inject({ method: 'POST', url: '/telemetry', payload: b }));
    }
    if (i % 7 === 0) {
      calls.push(() =>
        app.inject({
          method: 'POST',
          url: '/telemetry',
          payload: { ...b, metric: 'tampered' },
        }),
      );
    }
    if (i % 11 === 0) {
      calls.push(() =>
        app.inject({ method: 'POST', url: '/telemetry', payload: { ...b, value: 'NaN?' } }),
      );
    }
  }

  type CallResult = { statusCode: number; payload: unknown };
  const responses = await Promise.all(
    calls.map((c) =>
      jittered<CallResult>(() => c().then((r) => ({ statusCode: r.statusCode, payload: r.json() }))),
    ),
  );

  const ok = responses.filter((r) => r.statusCode === 200);
  const conflicts = responses.filter((r) => r.statusCode === 409);
  const bad = responses.filter((r) => r.statusCode === 400);

  // 每个 eventId 恰好一次成功接收：重试返回 200 但属于幂等复用，唯一 id 数恰为 N
  const okIds = new Set(ok.map((r) => (r.payload as { id: string }).id));
  assert.ok(ok.length > N, 'idempotent retries also return 200');
  assert.equal(okIds.size, N);
  assert.ok(conflicts.length > 0);
  assert.ok(bad.length > 0);
  assert.ok(conflicts.every((r) => (r.payload as { code: string }).code === 'EVENT_ID_CONFLICT'));
  assert.ok(bad.every((r) => (r.payload as { code: string }).code.startsWith('INVALID_')));

  // 完整时间段回放：数量与成功接收量一致，eventId 无重复
  const replayRes = await app.inject({
    method: 'GET',
    url: '/telemetry/replay?from=5000&to=5022',
  });
  assert.equal(replayRes.statusCode, 200);
  const replayed = replayRes.json<Array<{ id: string; sequence: number; timestamp: number }>>();
  assert.equal(replayed.length, N);
  assert.equal(new Set(replayed.map((e) => e.id)).size, N);
  const sequences = replayed.map((e) => e.sequence);
  assert.equal(new Set(sequences).size, N);
  for (let i = 1; i < replayed.length; i++) {
    assert.ok(
      replayed[i - 1].timestamp < replayed[i].timestamp ||
        (replayed[i - 1].timestamp === replayed[i].timestamp &&
          replayed[i - 1].sequence < replayed[i].sequence),
    );
  }

  // 回放结果中的事件可通过事件索引入口逐一取回（eventId 不消失）
  for (const e of replayed) {
    const got = await app.inject({ method: 'GET', url: `/events/${e.id}` });
    assert.equal(got.statusCode, 200);
    assert.equal(got.json<{ sequence: number }>().sequence, e.sequence);
  }
  const missing = await app.inject({ method: 'GET', url: '/events/does-not-exist' });
  assert.equal(missing.statusCode, 404);

  // 审计：每个成功 eventId 恰好一条创建记录，且引用的 sequence 与事件/回放一致
  const auditRes = await app.inject({ method: 'GET', url: '/audit' });
  assert.equal(auditRes.statusCode, 200);
  const audits = auditRes.json<
    Array<{ action: string; entityId: string; metadata: { sequence: number } }>
  >();
  const created = audits.filter((a) => a.action === 'telemetry.ingest');
  assert.equal(created.length, N);
  assert.equal(new Set(created.map((a) => a.entityId)).size, N);
  const replayBySequence = new Map(replayed.map((e) => [e.sequence, e.id]));
  for (const entry of created) {
    assert.equal(replayBySequence.get(entry.metadata.sequence), entry.entityId);
  }

  // 按实体过滤审计也能查到
  const one = await app.inject({ method: 'GET', url: '/audit?entityId=http_evt_0' });
  const oneAudits = one.json<Array<{ action: string }>>();
  assert.equal(oneAudits.filter((a) => a.action === 'telemetry.ingest').length, 1);

  // 非法回放参数
  const badRange = await app.inject({
    method: 'GET',
    url: '/telemetry/replay?from=9&to=1',
  });
  assert.equal(badRange.statusCode, 400);
  assert.equal(badRange.json<{ code: string }>().code, 'INVALID_RANGE');

  // 非 JSON / 空体也被规范拒绝而不是 500
  const empty = await app.inject({
    method: 'POST',
    url: '/telemetry',
    headers: { 'content-type': 'application/json' },
    payload: '',
  });
  assert.equal(empty.statusCode, 400);
});
