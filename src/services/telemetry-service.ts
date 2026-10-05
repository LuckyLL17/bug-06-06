import { newId } from '../core/id';
import { TwinStore } from '../domain/store';
import { DomainEvent, Telemetry, TelemetryInput } from '../domain/types';
import { DomainError } from '../core/errors';
import { AsyncMutex } from '../core/mutex';
import { AuditService } from './audit-service';

export interface IngestResult { event: DomainEvent; reused: boolean }

const QUALITIES = ['good', 'suspect', 'bad'] as const;

function isNonEmptyString(v: unknown): v is string {
  return typeof v === 'string' && v.trim().length > 0;
}

function isRequiredField(v: unknown): v is string {
  return isNonEmptyString(v) && v.trim() === v;
}

/** 在进入临界区之前拒绝异常请求：非法请求不消耗序列号、不产生事件和审计 */
export function validateTelemetryInput(input: unknown): TelemetryInput {
  if (typeof input !== 'object' || input === null) {
    throw new DomainError('INVALID_TELEMETRY', 'telemetry body must be an object');
  }
  const b = input as Record<string, unknown>;
  if (b.eventId !== undefined && (!isNonEmptyString(b.eventId) || b.eventId.length > 128)) {
    throw new DomainError('INVALID_EVENT_ID', 'eventId must be a non-empty string of at most 128 chars');
  }
  if (!isRequiredField(b.assetId)) throw new DomainError('INVALID_ASSET_ID', 'assetId is required');
  if (!isRequiredField(b.metric)) throw new DomainError('INVALID_METRIC', 'metric is required');
  if (typeof b.value !== 'number' || !Number.isFinite(b.value)) {
    throw new DomainError('INVALID_VALUE', 'value must be a finite number');
  }
  if (!isRequiredField(b.unit)) throw new DomainError('INVALID_UNIT', 'unit is required');
  if (typeof b.observedAt !== 'number' || !Number.isFinite(b.observedAt) || b.observedAt < 0) {
    throw new DomainError('INVALID_OBSERVED_AT', 'observedAt must be a non-negative finite number');
  }
  if (!QUALITIES.includes(b.quality as (typeof QUALITIES)[number])) {
    throw new DomainError('INVALID_QUALITY', 'quality must be good|suspect|bad');
  }
  return b as unknown as TelemetryInput;
}

export class TelemetryService {
  private writeLock = new AsyncMutex();

  constructor(
    private store: TwinStore,
    private audit: AuditService = new AuditService(),
  ) {}

  /**
   * 并发安全、幂等的遥测接入：
   * 同一 eventId 的重试复用首次提交的结果；同 eventId 但内容冲突显式拒绝，
   * 避免重试串包时不同资产/指标互相覆盖。全部副作用在同一临界区内提交，
   * 部分请求失败或重试都不会改动已成功落库的数据。
   */
  ingest(rawInput: unknown): Promise<IngestResult> {
    return this.writeLock.runExclusive(() => this.commit(validateTelemetryInput(rawInput)));
  }

  private commit(input: TelemetryInput): IngestResult {
    const eventId = input.eventId ?? newId('evt');
    const existing = this.store.eventById.get(eventId);
    if (existing) {
      const accepted = this.store.telemetryByEventId.get(eventId)!;
      if (!this.samePayload(accepted, input)) {
        throw new DomainError(
          'EVENT_ID_CONFLICT',
          `eventId ${eventId} already accepted for another asset/metric/payload`,
        );
      }
      // 相同事件重试：复用既有结果，不再取号、不再写事件，只留一次可追溯的复用审计
      this.audit.record('telemetry.ingest.reuse', eventId, {
        assetId: accepted.assetId,
        sequence: existing.sequence,
      });
      return { event: existing, reused: true };
    }

    const telemetry: Telemetry = {
      eventId,
      assetId: input.assetId,
      metric: input.metric,
      value: input.value,
      unit: input.unit,
      observedAt: input.observedAt,
      quality: input.quality,
    };
    // 共享序列在临界区内单调取号：实例内唯一，且等价于提交顺序、可排序
    const sequence = this.store.eventSequence.next();
    const event: DomainEvent = {
      id: eventId,
      type: 'telemetry',
      assetId: input.assetId,
      timestamp: input.observedAt,
      sequence,
      payload: { metric: input.metric, value: input.value, quality: input.quality },
    };

    this.store.telemetry.push(telemetry);
    this.store.telemetryByEventId.set(eventId, telemetry);
    this.indexAssetMetric(input.assetId, input.metric, eventId);
    this.store.events.push(event);
    this.store.eventById.set(eventId, event);
    // 创建审计与事件在同一临界区产生，每个成功接收的 eventId 恰好一条
    this.audit.record('telemetry.ingest', eventId, {
      assetId: event.assetId,
      metric: input.metric,
      sequence,
    });
    return { event, reused: false };
  }

  private indexAssetMetric(assetId: string, metric: string, eventId: string): void {
    const key = TwinStore.assetMetricKey(assetId, metric);
    let ids = this.store.assetMetricIndex.get(key);
    if (!ids) {
      ids = new Set<string>();
      this.store.assetMetricIndex.set(key, ids);
    }
    ids.add(eventId);
  }

  private samePayload(accepted: Telemetry, input: TelemetryInput): boolean {
    return (
      accepted.assetId === input.assetId &&
      accepted.metric === input.metric &&
      accepted.value === input.value &&
      accepted.unit === input.unit &&
      accepted.observedAt === input.observedAt &&
      accepted.quality === input.quality
    );
  }

  getEvent(eventId: string): DomainEvent | undefined {
    return this.store.eventById.get(eventId);
  }

  history(assetId: string, metric?: string): Telemetry[] {
    const ids = this.store.assetMetricIndex.get(TwinStore.assetMetricKey(assetId, metric ?? ''));
    const stream = ids
      ? [...ids].map((id) => this.store.telemetryByEventId.get(id)!)
      : this.store.telemetry.filter(
          (t) => t.assetId === assetId && (!metric || t.metric === metric),
        );
    return stream.sort((a, b) => a.observedAt - b.observedAt || a.eventId.localeCompare(b.eventId));
  }

  replay(from: number, to: number): DomainEvent[] {
    if (!Number.isFinite(from) || !Number.isFinite(to) || from > to) {
      throw new DomainError('INVALID_RANGE', 'replay requires numeric from/to with from <= to');
    }
    // 始终在副本上排序，绝不改动事件主数组；先按业务时间，再按实例内序列号还原提交顺序
    return this.store.events
      .filter((e) => e.timestamp >= from && e.timestamp <= to)
      .sort((a, b) => a.timestamp - b.timestamp || a.sequence - b.sequence);
  }
}
