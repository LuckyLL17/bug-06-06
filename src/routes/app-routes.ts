import { FastifyInstance, FastifyReply } from 'fastify'; import { DomainError } from '../core/errors'; import { TwinStore } from '../domain/store'; import { TelemetryService } from '../services/telemetry-service'; import { AnomalyService } from '../services/anomaly-service'; import { TopologyService } from '../services/topology-service'; import { RootCauseService } from '../services/root-cause-service'; import { WorkflowService } from '../services/workflow-service'; import { AuditService } from '../services/audit-service';

const badRequest=(res:FastifyReply,e:any)=>{if(e instanceof DomainError)return res.code(400).send({code:e.code,message:e.message});throw e;};

export function registerRoutes(app:FastifyInstance,store:TwinStore){const telemetry=new TelemetryService(store), anomalies=new AnomalyService(store), topology=new TopologyService(store), causes=new RootCauseService(store,topology), workflows=new WorkflowService(store), audit=new AuditService();
 app.get('/health',async()=>({ok:true}));
 app.post('/topology',async(req,res)=>{try{return topology.publish(req.body as any)}catch(e:any){return res.code(400).send({code:e.code,message:e.message})}});
 app.post('/telemetry',async(req,res)=>{try{
  const {event,created}=telemetry.ingest(req.body as any);
  // 只有首次成功接收才记录审计；相同 eventId 的重试复用既有事件及其审计轨迹，保证审计与事件一一对应
  if(created)audit.record('telemetry.ingest',event.id,{assetId:event.assetId,sequence:event.sequence});
  return event;
 }catch(e:any){return badRequest(res,e)}});
 app.get('/telemetry/events/:id',async(req,res)=>{const event=telemetry.getEvent((req.params as any).id);if(!event)return res.code(404).send({code:'EVENT_NOT_FOUND',message:'event not found'});return event});
 app.get('/telemetry/replay',async(req,res)=>{try{const q=req.query as any;return telemetry.replay(Number(q.from),Number(q.to))}catch(e:any){return badRequest(res,e)}});
 app.post('/anomalies/detect',async(req)=>{const b=req.body as any;return anomalies.detect(b.assetId,b.metric,Number(b.threshold))});
 app.get('/incidents/:id/root-cause',async(req,res)=>{try{return causes.analyze((req.params as any).id)}catch(e:any){return res.code(404).send({message:e.message})}});
 app.post('/work-orders',async(req,res)=>{try{return workflows.create((req.body as any).incidentId,(req.body as any).steps||[])}catch(e:any){return res.code(400).send({message:e.message})}});
 app.post('/work-orders/:id/transition',async(req,res)=>{try{return workflows.transition((req.params as any).id,(req.body as any).state)}catch(e:any){return res.code(400).send({message:e.message})}});
 app.get('/audit',async(req)=>audit.list((req.query as any).entityId));}
