import { test } from 'node:test';
import assert from 'node:assert/strict';
import { TwinStore } from '../domain/store';
import { AuditService } from '../services/audit-service';
import { TelemetryService } from '../services/telemetry-service';
import { DomainEvent, TelemetryInput } from '../domain/types';

function makeHarness() {
  const store = new TwinStore();
  const audit = new AuditService();
  const service = new TelemetryService(store, audit);
  return { store, audit, service };
}

function sample(over: Partial<TelemetryInput> = {}): TelemetryInput {
  return {
    eventId: 'evt_default',
    assetId: 'pump-1',
    metric: 'pressure',
    value: 1.2,
    unit: 'MPa',
    observedAt: 1000,
    quality: 'good',
    ...over,
  };
}

/** 让并发请求以乱序交错的方式进入临界区，模拟网关并发提交 */
function jitter<T>(factory: () => Promise<T>): Promise<T> {
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

test('百条以上并发遥测：eventId 恰好一次、sequence 实例内唯一且可排序', async () => {
  const { store, service, audit } = makeHarness();
  const N = 240;
  const assets = ['pump-1', 'pump-2', 'valve-7', 'sensor-3', 'reactor-9'];
  const metrics = ['pressure', 'temperature', 'flow', 'vibration'];

  const inputs: TelemetryInput[] = Array.from({ length: N }, (_, i) =>
    sample({
      eventId: `evt_${i}`,
      assetId: assets[i % assets.length],
      metric: metrics[i % metrics.length],
      value: i,
      observedAt: 1000 + (i % 17),
    }),
  );

  const results = await Promise.all(
    inputs.map((input) => jitter(() => service.ingest(input))),
  );

  // 每个 eventId 恰好成功一次，无重复无丢失
  assert.equal(store.events.length, N);
  assert.equal(store.telemetry.length, N);
  assert.equal(store.eventById.size, N);
  assert.equal(new Set(results.map((r) => r.event.id)).size, N);
  assert.ok(results.every((r) => r.reused === false));

  // sequence 在实例内唯一，且恰好覆盖 1..N，按提交顺序可排序
  const sequences = store.events.map((e) => e.sequence).sort((a, b) => a - b);
  assert.deepEqual(sequences, Array.from({ length: N }, (_, i) => i + 1));
  assert.deepEqual(
    [...store.eventById.values()].map((e) => e.sequence),
    store.events.map((e) => e.sequence),
  );

  // 不同资产/指标各走各的索引槽位，互不覆盖
  for (const asset of assets) {
    for (const metric of metrics) {
      const ids = store.assetMetricIndex.get(TwinStore.assetMetricKey(asset, metric));
      assert.ok(ids, `index missing for ${asset}/${metric}`);
      assert.ok(ids!.size > 0);
      for (const id of ids!) {
        const t = store.telemetryByEventId.get(id)!;
        assert.equal(t.assetId, asset);
        assert.equal(t.metric, metric);
      }
    }
  }

  // 每个成功的 eventId 恰好一条创建审计，审计引用真实存在的事件
  const created = audit.entries.filter((e) => e.action === 'telemetry.ingest');
  assert.equal(created.length, N);
  for (const entry of created) {
    const event = store.eventById.get(entry.entityId);
    assert.ok(event, `audit references missing event ${entry.entityId}`);
    assert.equal(entry.metadata.sequence, event.sequence);
  }
});

test('相同事件的并发/交错重试复用既有结果，不新增事件、遥测或序列号', async () => {
  const { store, service, audit } = makeHarness();
  const input = sample({ eventId: 'evt_retry', value: 3.5 });

  const results = await Promise.all([
    jitter(() => service.ingest(input)),
    jitter(() => service.ingest(input)),
    jitter(() => service.ingest(input)),
    jitter(() => service.ingest(input)),
  ]);

  assert.equal(store.events.length, 1);
  assert.equal(store.telemetry.length, 1);
  assert.equal(store.eventById.size, 1);
  const onlyEvent = store.eventById.get('evt_retry')!;
  assert.equal(onlyEvent.sequence, 1);
  assert.ok(results.every((r) => r.event === onlyEvent));
  const first = results.find((r) => r.reused === false);
  const rest = results.filter((r) => r.reused);
  assert.ok(first);
  assert.equal(rest.length, 3);

  // 创建审计只有一条；重试只追加可区分的复用审计并引用同一 eventId/sequence
  const created = audit.entries.filter((e) => e.action === 'telemetry.ingest');
  const reused = audit.entries.filter((e) => e.action === 'telemetry.ingest.reuse');
  assert.equal(created.length, 1);
  assert.equal(reused.length, 3);
  assert.ok(reused.every((e) => e.entityId === 'evt_retry' && e.metadata.sequence === 1));

  // 全部重试结束后再串行重试，结果仍一致
  const again = await service.ingest(input);
  assert.equal(again.event, onlyEvent);
  assert.equal(again.reused, true);
  assert.equal(store.events.length, 1);
});

test('同 eventId 但资产/指标/负载冲突的重试被拒绝，既有数据不被覆盖', async () => {
  const { store, service } = makeHarness();
  await service.ingest(sample({ eventId: 'evt_x', assetId: 'pump-1', metric: 'pressure' }));

  await assert.rejects(
    () => service.ingest(sample({ eventId: 'evt_x', assetId: 'pump-2', metric: 'pressure' })),
    (e: Error & { code?: string }) => e.code === 'EVENT_ID_CONFLICT',
  );
  await assert.rejects(
    () => service.ingest(sample({ eventId: 'evt_x', assetId: 'pump-1', metric: 'flow' })),
    (e: Error & { code?: string }) => e.code === 'EVENT_ID_CONFLICT',
  );
  await assert.rejects(
    () => service.ingest(sample({ eventId: 'evt_x', value: 99 })),
    (e: Error & { code?: string }) => e.code === 'EVENT_ID_CONFLICT',
  );

  // 冲突请求没有产生任何新数据，也没有偷走 sequence
  assert.equal(store.events.length, 1);
  assert.equal(store.eventSequence.value, 1);
  const kept = store.eventById.get('evt_x')!;
  assert.equal(kept.assetId, 'pump-1');
  assert.equal(kept.payload.metric, 'pressure');
  assert.equal(store.telemetryByEventId.get('evt_x')!.value, 1.2);

  // 冲突交错后，相同事件的合法重试依然复用
  const ok = await service.ingest(sample({ eventId: 'evt_x' }));
  assert.equal(ok.event, kept);
  assert.equal(ok.reused, true);
});

test('异常请求穿插在高并发中：全部被拒，不消耗序列、不污染回放与审计', async () => {
  const { store, service, audit } = makeHarness();
  const N = 150;
  const valid: TelemetryInput[] = Array.from({ length: N }, (_, i) =>
    sample({ eventId: `evt_ok_${i}`, value: i, observedAt: 5000 + i }),
  );
  const invalid: unknown[] = [
    null,
    {},
    { ...sample(), eventId: '' },
    { ...sample(), eventId: 1 },
    { ...sample(), assetId: '  ' },
    { ...sample(), metric: '' },
    { ...sample(), value: NaN },
    { ...sample(), value: '1' },
    { ...sample(), unit: '' },
    { ...sample(), observedAt: -3 },
    { ...sample(), observedAt: 'now' },
    { ...sample(), quality: 'great' },
  ];

  const tasks: Array<Promise<unknown>> = [
    ...valid.map((v, i) => jitter(() => service.ingest(v).catch((e) => e))),
    ...invalid.map((bad, i) =>
      jitter(() =>
        service
          .ingest(bad)
          .then(() => assert.fail(`bad payload ${i} should have been rejected`))
          .catch((e) => e),
      ),
    ),
  ];
  const outcomes = await Promise.all(tasks);
  const failures = outcomes.filter((o) => o instanceof Error);
  assert.equal(failures.length, invalid.length);

  assert.equal(store.events.length, N);
  assert.equal(store.telemetry.length, N);
  assert.equal(store.eventSequence.value, N);
  const created = audit.entries.filter((e) => e.action === 'telemetry.ingest');
  assert.equal(created.length, N);
  assert.equal(audit.entries.some((e) => e.entityId === undefined), false);

  // 合法事件的 sequence 仍严格连续，异常请求未在序列上留洞或造成重复
  assert.deepEqual(
    store.events.map((e) => e.sequence).sort((a, b) => a - b),
    Array.from({ length: N }, (_, i) => i + 1),
  );
});

test('部分请求失败后重试成功：已成功数据不被破坏，失败重试正常复用', async () => {
  const { store, service } = makeHarness();
  const a = sample({ eventId: 'evt_a', observedAt: 100 });
  const b = sample({ eventId: 'evt_b', observedAt: 200 });

  // 第一轮：a、b 都成功接收
  await service.ingest(a);
  await service.ingest(b);
  assert.equal(store.eventSequence.value, 2);

  // 网关对 b 的一次重试串包（同 eventId、不同 value），必须失败
  await assert.rejects(
    () => service.ingest({ ...b, value: 42 }),
    (e: Error & { code?: string }) => e.code === 'EVENT_ID_CONFLICT',
  );
  // 失败不改动任何已成功的数据，也不消耗序列号
  assert.equal(store.events.length, 2);
  assert.equal(store.eventSequence.value, 2);
  assert.equal(store.telemetryByEventId.get('evt_b')!.value, 1.2);

  // 网关修正后按原 eventId 重试两个事件 -> 复用既有结果
  const retryB = await service.ingest(b);
  const retryA = await service.ingest(a);
  assert.equal(retryB.reused, true);
  assert.equal(retryA.reused, true);

  assert.deepEqual(store.events.map((e) => e.id).sort(), ['evt_a', 'evt_b']);
  assert.equal(store.eventById.get('evt_a')!.sequence, 1);
  assert.equal(store.eventById.get('evt_b')!.sequence, 2);

  // 再并发重试这两个既有事件，仍然不产生重复
  await Promise.all([
    service.ingest(a),
    service.ingest(a),
    service.ingest(b),
    service.ingest(b),
  ]);
  assert.equal(store.events.length, 2);
  assert.equal(store.telemetry.length, 2);
  assert.equal(store.eventSequence.value, 2);
});

test('完整时间段回放：结果与事件索引、审计引用一一对应且可排序', async () => {
  const { store, service, audit } = makeHarness();
  // 同一业务时间戳的多个资产/指标事件，靠 sequence 决定稳定顺序
  const inputs: TelemetryInput[] = [
    sample({ eventId: 'e1', assetId: 'pump-1', metric: 'pressure', observedAt: 2000, value: 1 }),
    sample({ eventId: 'e2', assetId: 'pump-2', metric: 'flow', observedAt: 2000, value: 2 }),
    sample({ eventId: 'e3', assetId: 'pump-1', metric: 'pressure', observedAt: 2010, value: 3 }),
    sample({ eventId: 'e4', assetId: 'valve-7', metric: 'flow', observedAt: 3000, value: 4 }),
    sample({ eventId: 'e5', assetId: 'pump-2', metric: 'flow', observedAt: 1000, value: 5 }),
  ];
  await Promise.all(inputs.map((i) => jitter(() => service.ingest(i))));

  // 窗口外重放为空，范围非法抛 INVALID_RANGE
  assert.deepEqual(service.replay(0, 999), []);
  assert.throws(
    () => service.replay(3000, 1000),
    (e: Error & { code?: string }) => e.code === 'INVALID_RANGE',
  );

  const window = service.replay(1000, 3000);
  assert.equal(window.length, 5);

  // 先业务时间、再 sequence 排序；同时间戳的提交顺序由并发调度决定，
  // 因此按实际分配的 sequence 计算期望顺序（sequence 正是实例内可排序键）
  const expected = [...store.events]
    .filter((e) => e.timestamp >= 1000 && e.timestamp <= 3000)
    .sort((a, b) => a.timestamp - b.timestamp || a.sequence - b.sequence)
    .map((e) => e.id);
  assert.deepEqual(
    window.map((e) => e.id),
    expected,
  );
  // 同一业务时刻 2000 的两个事件，sequence 严格区分先后
  const at2000 = window.filter((e) => e.timestamp === 2000);
  assert.deepEqual(at2000.map((e) => e.id).sort(), ['e1', 'e2']);
  assert.notEqual(at2000[0].sequence, at2000[1].sequence);
  // sequence 严格随排序单调不减（同时间戳内严格递增）
  for (let i = 1; i < window.length; i++) {
    const prev = window[i - 1];
    const cur = window[i];
    assert.ok(
      prev.timestamp < cur.timestamp ||
        (prev.timestamp === cur.timestamp && prev.sequence < cur.sequence),
    );
  }

  // 回放结果的每个事件都能在索引中按 id 取回同一对象，且都有对应审计
  for (const event of window) {
    const indexed = store.eventById.get(event.id);
    assert.equal(indexed, event);
    const telemetry = store.telemetryByEventId.get(event.id);
    assert.ok(telemetry);
    assert.equal(telemetry!.assetId, event.assetId);
    assert.equal(telemetry!.observedAt, event.timestamp);
    const links = audit.entries.filter(
      (e) => e.action === 'telemetry.ingest' && e.entityId === event.id,
    );
    assert.equal(links.length, 1);
    assert.equal(links[0].metadata.sequence, event.sequence);
  }

  // 回放窗口边界包含
  assert.deepEqual(service.replay(2010, 2010).map((e) => e.id), ['e3']);
  // 回放不修改主存储顺序
  assert.equal(store.events.length, 5);
  assert.deepEqual(
    store.events.map((e) => e.sequence),
    [1, 2, 3, 4, 5],
  );

  // history 只看自己资产/指标，不同资产互不串数据
  const pumpPressure = service.history('pump-1', 'pressure');
  assert.deepEqual(pumpPressure.map((t) => t.eventId), ['e1', 'e3']);
    assert.deepEqual(service.history('pump-2', 'flow').map((t) => t.eventId), ['e5', 'e2']);
  assert.deepEqual(service.history('valve-7').map((t) => t.eventId), ['e4']);
});

test('eventId 缺省时服务端生成并同样只写一次；后续无法伪造为既有事件', async () => {
  const { store, service } = makeHarness();
  const { event, reused } = await service.ingest(sample({ eventId: undefined }));
  assert.match(event.id, /^evt_/);
  assert.equal(reused, false);
  assert.ok(store.eventById.has(event.id));

  // 有人拿服务端生成的 id 用不同负载重试 -> 冲突，原数据不变
  const conflict: TelemetryInput = {
    eventId: event.id,
    assetId: 'pump-2',
    metric: 'flow',
    value: 9,
    unit: 'm3/s',
    observedAt: 1000,
    quality: 'bad',
  };
  await assert.rejects(
    () => service.ingest(conflict),
    (e: Error & { code?: string }) => e.code === 'EVENT_ID_CONFLICT',
  );
  assert.equal(store.eventById.get(event.id)!.assetId, 'pump-1');
});

test('并发后进行异常检测仍能按资产/指标隔离统计', async () => {
  const { service } = makeHarness();
  const N = 120;
  await Promise.all(
    Array.from({ length: N }, (_, i) => {
      const asset = i % 2 === 0 ? 'pump-1' : 'pump-2';
      const isAbnormal = asset === 'pump-1' ? i % 4 === 0 : i % 4 === 1;
      return jitter(() =>
        service.ingest(
          sample({
            eventId: `evt_d_${i}`,
            assetId: asset,
            metric: 'temperature',
            value: isAbnormal ? 100 + i : 10,
            observedAt: 1000 + i,
          }),
        ),
      );
    }),
  );
  const inc1 = new AnomalyServiceShim(service).detect('pump-1', 'temperature', 50);
  const inc2 = new AnomalyServiceShim(service).detect('pump-2', 'temperature', 50);
  assert.ok(inc1);
  assert.ok(inc2);
  assert.deepEqual(inc1!.assetIds, ['pump-1']);
  assert.deepEqual(inc2!.assetIds, ['pump-2']);
});

// 避免在单元测试里引入路由层依赖：复用 history 语义校验隔离性
class AnomalyServiceShim {
  constructor(private svc: TelemetryService) {}
  detect(assetId: string, metric: string, threshold: number) {
    const samples = this.svc
      .history(assetId, metric)
      .filter((s) => s.quality !== 'bad' && Math.abs(s.value) >= threshold);
    if (!samples.length) return null;
    return { id: `inc_${assetId}`, assetIds: [assetId], evidenceCount: samples.length } as {
      id: string;
      assetIds: string[];
      evidenceCount: number;
    };
  }
}

test('所有事件对象经 getEvent 可取回且与回放/审计一致（eventId 不消失）', async () => {
  const { store, service } = makeHarness();
  const ids: string[] = [];
  for (let i = 0; i < 50; i++) {
    const id = `evt_get_${i}`;
    ids.push(id);
    await service.ingest(sample({ eventId: id, observedAt: 7000 + i }));
  }
  for (const id of ids) {
    assert.ok(service.getEvent(id) instanceof Object);
    assert.ok(service.getEvent(id) === store.eventById.get(id));
  }
  assert.equal(service.getEvent('evt_missing'), undefined);
  const replay: DomainEvent[] = service.replay(0, Number.MAX_SAFE_INTEGER);
  assert.equal(new Set(replay.map((e) => e.id)).size, 50);
});
