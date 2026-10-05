import { SequenceAllocator } from '../core/sequence';
import { Asset, DomainEvent, Incident, Telemetry, TopologyVersion, WorkOrder } from './types';
export class TwinStore {
  assets = new Map<string,Asset>(); telemetry:Telemetry[]=[]; events:DomainEvent[]=[]; incidents=new Map<string,Incident>(); workOrders=new Map<string,WorkOrder>(); topology:TopologyVersion[]=[];
  /** 事件索引：eventId → 事件，是幂等写入、事件查询与审计关联的权威来源；键为全局 eventId，不同资产/指标互不覆盖 */
  readonly eventIndex = new Map<string,DomainEvent>();
  /** 实例内共享的事件序列分配器：只有成功提交的事件才占用序号，保证唯一且随到达顺序单调递增 */
  readonly eventSequences = new SequenceAllocator();
}
