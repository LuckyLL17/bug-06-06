import { FastifyInstance } from 'fastify';
import { TwinStore } from '../domain/store';
import { TelemetryService } from '../services/telemetry-service';
import { AnomalyService } from '../services/anomaly-service';
import { TopologyService } from '../services/topology-service';
import { RootCauseService } from '../services/root-cause-service';
import { WorkflowService } from '../services/workflow-service';
import { AuditService } from '../services/audit-service';

function statusFor(code: string | undefined): number {
  if (code === 'EVENT_ID_CONFLICT') return 409;
  if (code === 'NOT_FOUND') return 404;
  return 400;
}

export function registerRoutes(app: FastifyInstance, store: TwinStore) {
  // 审计服务与路由、遥测服务共享同一实例，保证审计引用和事件索引可互查
  const audit = new AuditService();
  const telemetry = new TelemetryService(store, audit);
  const anomalies = new AnomalyService(store);
  const topology = new TopologyService(store);
  const causes = new RootCauseService(store, topology);
  const workflows = new WorkflowService(store);

  app.get('/health', async () => ({ ok: true }));

  app.post('/topology', async (req, res) => {
    try {
      return topology.publish(req.body as never);
    } catch (e: unknown) {
      const d = e as { code?: string; message?: string };
      return res.code(statusFor(d.code)).send({ code: d.code, message: d.message });
    }
  });

  app.post('/telemetry', async (req, res) => {
    try {
      const { event } = await telemetry.ingest(req.body);
      return event;
    } catch (e: unknown) {
      const d = e as { code?: string; message?: string };
      return res.code(statusFor(d.code)).send({ code: d.code, message: d.message });
    }
  });

  app.get('/telemetry/replay', async (req, res) => {
    const q = req.query as { from?: string; to?: string };
    try {
      return telemetry.replay(Number(q.from), Number(q.to));
    } catch (e: unknown) {
      const d = e as { code?: string; message?: string };
      return res.code(400).send({ code: d.code, message: d.message });
    }
  });

  app.get('/events/:id', async (req, res) => {
    const event = telemetry.getEvent((req.params as { id: string }).id);
    if (!event) return res.code(404).send({ code: 'EVENT_NOT_FOUND', message: 'event not found' });
    return event;
  });

  app.post('/anomalies/detect', async (req) => {
    const b = req.body as { assetId: string; metric: string; threshold: number };
    return anomalies.detect(b.assetId, b.metric, Number(b.threshold));
  });

  app.get('/incidents/:id/root-cause', async (req, res) => {
    try {
      return causes.analyze((req.params as { id: string }).id);
    } catch (e: unknown) {
      return res.code(404).send({ message: (e as Error).message });
    }
  });

  app.post('/work-orders', async (req, res) => {
    try {
      const b = req.body as { incidentId: string; steps?: string[] };
      return workflows.create(b.incidentId, b.steps || []);
    } catch (e: unknown) {
      return res.code(400).send({ message: (e as Error).message });
    }
  });

  app.post('/work-orders/:id/transition', async (req, res) => {
    try {
      return workflows.transition(
        (req.params as { id: string }).id,
        (req.body as { state: never }).state,
      );
    } catch (e: unknown) {
      return res.code(400).send({ message: (e as Error).message });
    }
  });

  app.get('/audit', async (req) => audit.list((req.query as { entityId?: string }).entityId));
}
