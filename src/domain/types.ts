export type AssetKind = 'pump' | 'valve' | 'sensor' | 'reactor';
export type AssetStatus = 'healthy' | 'degraded' | 'failed';
export type EventType = 'telemetry' | 'alarm' | 'command' | 'ack';
export interface Asset { id:string; name:string; kind:AssetKind; status:AssetStatus; criticality:number; tags:string[] }
export interface Telemetry { eventId:string; assetId:string; metric:string; value:number; unit:string; observedAt:number; quality:'good'|'suspect'|'bad' }
/** 网关注入请求：eventId 用于幂等重试，缺省时由服务端生成（非重试场景） */
export interface TelemetryInput { eventId?:string; assetId:string; metric:string; value:number; unit:string; observedAt:number; quality:'good'|'suspect'|'bad' }
export interface DomainEvent { id:string; type:EventType; assetId:string; timestamp:number; sequence:number; payload:Record<string, unknown> }
export interface Incident { id:string; severity:'low'|'medium'|'high'|'critical'; status:'open'|'investigating'|'contained'|'resolved'; assetIds:string[]; detectedAt:number; evidence:string[] }
export interface TopologyVersion { version:number; effectiveAt:number; edges:Array<{from:string;to:string;weight:number}> }
export interface WorkOrder { id:string; incidentId:string; state:'draft'|'approved'|'executing'|'completed'|'cancelled'; steps:string[]; compensationRequired:boolean }
