# 工业过程数字孪生与异常根因推理服务

纯后端 Node.js + TypeScript + Fastify 项目，使用内存状态运行，不依赖 Docker、数据库或消息队列。

## 运行

```bash
npm install
npm test
npm start
```

## 业务闭环

设备拓扑版本 → 遥测接入 → 时间序列重放 → 阈值异常检测 → 拓扑影响分析 → 根因候选 → 工单状态机 → 审计追踪。

关键不变量：拓扑版本连续且按生效时间选择；遥测事件按业务时间和序列号重放；工单只能沿合法状态机迁移；所有重要操作产生审计记录。

## 遥测接入一致性约定（高并发/重试）

- 客户端在 `eventId` 中携带稳定去重键；缺省时服务端生成。相同 `eventId` 的重试**复用首次结果**（HTTP 200 返回同一事件），不重复取号、不重复写事件/遥测/创建审计。
- 相同 `eventId` 但资产、指标或负载不一致的请求返回 `409 EVENT_ID_CONFLICT`，既有数据不被覆盖；非法字段返回 `400 INVALID_*`，且不消耗序列号。
- `sequence` 由实例内共享的单调分配器在写入临界区内发放，实例内唯一、等价于提交顺序，回放时作为同一业务时间戳的排序键。
- 事件主数组 append-only，`eventById` 主键索引、`telemetryByEventId` 与 `(assetId,metric)` 复合索引与主数组同步提交；`GET /events/:id`、`GET /telemetry/replay`、`GET /audit?entityId=` 三者可互相核对。
- 回放：`GET /telemetry/replay?from=&to=`，按 `(timestamp, sequence)` 升序返回，边界包含；结果在副本上排序，不改动事件主数组。
