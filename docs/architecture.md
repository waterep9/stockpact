# 架构说明

本文回答四个问题：**系统由什么组成、一次下单发生了什么、并发与故障下靠什么保证正确、观测到什么程度**。

## 1. 分层与依赖方向

```
api  ──▶  services  ──▶  infra  ──▶  DB
 │            │
 └──▶ domain ◀┘        obs（横切：日志/指标）
```

- `domain/order.py`：状态机、状态迁移白名单、订单行归一化、领域错误（`DomainError` 自带 `code` 与 `http_status`）。
  这一层没有任何 import 自 FastAPI/SQLAlchemy，可纯内存测试。
- `infra/repos.py`：所有 SQL 集中在此。**并发敏感的写操作必须返回 rowcount 判定结果**，不允许"先读后写"。
- `services/*`：用例编排。写操作接受调用方传入的 `Session`（参与外层事务），读操作自开会话。
- `api/routes.py`：协议转换 + 鉴权 + 状态码。业务异常统一由 `DomainError` 处理器映射。
- `container.py`：组合根。所有依赖显式注入，测试可替换数据库 URL 与 TTL。

## 2. 数据模型

| 表 | 职责 | 关键约束 |
| --- | --- | --- |
| `products` / `inventory` | 目录与库存 | `inventory.on_hand >= reserved >= 0`（条件更新维护） |
| `orders` / `order_items` | 订单聚合 | `orders.code` 唯一；`saga_owner + saga_lease_until` 做执行租约 |
| `order_events` | 订单时间线（审计证据链） | `(order_id, seq)` 唯一，seq 单调 |
| `reservations` | 库存预留 | `(order_id, sku)` 唯一；`status ∈ HELD/COMMITTED/RELEASED` |
| `payments` | 支付单 | `order_id` 唯一 → 一单最多一条支付记录 |
| `saga_steps` | 步骤状态机 | `(order_id, step)` 唯一；`attempts/next_attempt_at` 承载重试 |
| `outbox` | 待投递事件 | `(status, next_attempt_at)` 索引；`PENDING/PUBLISHED/DEAD` |
| `processed_events` | 消费幂等 | `(consumer, event_id)` 主键 |
| `idempotency_records` | 下单幂等 | key 主键 + `request_hash` 防"同 key 不同体" |
| `audit_log` | 治理审计 | 记录 actor / action / target / request_id |
| `chaos_config` | 故障注入 | 运行期可改，重启不丢 |
| `order_feed` / `inventory_ledger` / `notifications` | 读模型投影 | 只由 outbox 消费者写入 |

**为什么库存只存 `on_hand` 和 `reserved`**：可售量 `on_hand - reserved` 是派生值。多存一个 `available` 列会引入"三列对不上"的漂移风险，
而条件更新 `WHERE on_hand - reserved >= :qty` 让数据库在计算的同时完成判定，天然无竞态。

## 3. 一次下单的完整时序

```
调用方        API            OrderService        Saga            Inventory      Payment       Outbox
  │  POST /orders │                │                │                 │            │            │
  ├──────────────▶│ 鉴权/限流/校验  │                │                 │            │            │
  │               ├───────────────▶│ 1. 查目录取价   │                 │            │            │
  │               │                │ 2. 幂等键查重   │                 │            │            │
  │               │                │ 3. 单事务：订单+事件+outbox+审计+幂等记录      │            │
  │               │                ├───────────────▶│ 4. advance()    │            │            │
  │               │                │                ├── 租约领取 ────▶│            │            │
  │               │                │                ├── 条件更新预留 ─▶│            │            │
  │               │                │                │  订单 → STOCK_RESERVED       │            │
  │ ◀── 201 ──────┤◀───────────────┤                │                │            │            │
  │               │                │                │   （后台恢复器继续推进）      │            │
  │               │                │                ├── 扣款 ─────────────────────▶│            │
  │               │                │                │  订单 → PAID                 │            │
  │               │                │                ├── 出库(HELD→COMMITTED) ─────▶│            │
  │               │                │                │  订单 → CONFIRMED            │            │
  │               │                │                └── 事件写 outbox ──────────────────────────▶│
  │               │                │                                    中继轮询 → 消费者（去重）
```

关键点：**HTTP 请求只负责"接受订单 + 完成第一步"**，后续步骤由后台恢复器在秒级内推进。
这避免了把第三方支付延迟绑进用户请求，也让"进程崩溃"成为可恢复的正常场景而不是数据事故。

## 4. 并发控制

| 场景 | 机制 | 位置 |
| --- | --- | --- |
| 库存扣减 | 条件 UPDATE + rowcount | `repos.try_reserve` |
| 同一订单只允许一个执行者 | 租约条件 UPDATE（过期可抢占） | `repos.claim_saga` |
| 同一订单只允许一条支付单 | 数据库唯一约束 | `payments.order_id` |
| 同一幂等键只允许一张订单 | 主键冲突 + 回读重放 | `orders._lookup_idempotent` |
| SQLite 写锁冲突 | `run_in_tx` 指数退避重试 | `infra/db.py` |
| Saga 退避不被绕过 | `next_attempt_at` 未到期则释放租约并放弃本轮 | `saga._claim_plan` |

租约的取值：默认 5s（`STOCKPACT_SAGA_LEASE`）。太短会导致执行中被抢（重复扣款由支付幂等兜底），太长会让崩溃恢复变慢。

### 幂等矩阵

| 操作 | 幂等键 | 重复执行的后果 |
| --- | --- | --- |
| 下单 | `Idempotency-Key` + 请求体哈希 | 返回同一订单，`replayed=true` |
| 预留 | `(order_id, sku)` 唯一 | 同一订单不会重复占库存 |
| 扣款 | `payments.order_id` 唯一 + 确定性 `provider_ref` | 不会二次扣款 |
| 出库 | 只处理 `HELD` 预留并按行扣减 | 不会重复出库 |
| 补偿 | 只释放 `HELD`、只退 `CAPTURED` | 重复补偿无副作用 |
| 事件消费 | `(consumer, event_id)` 主键 | 不会重复投影 |

## 5. 故障模型

| 故障 | 检测方式 | 恢复动作 | 用户可见结果 |
| --- | --- | --- | --- |
| 库存不足 | 条件更新 rowcount=0 | 立即补偿（无需重试） | 订单 `CANCELLED / OUT_OF_STOCK` |
| 支付被拒 | 网关返回 DECLINED | 指数退避重试，3 次后补偿 | `CANCELLED / PAYMENT_FAILED` |
| 支付超时 | 网关不返回 | 视为"未确认"，重试；耗尽后补偿 | 不判定成功，最终取消 |
| 用户不付款 | 预留 TTL 到期 | 回收器补偿释放 | `CANCELLED / RESERVATION_EXPIRED` |
| 超出履约时限 | `deadline_at` | 拒绝继续推进并补偿 | `CANCELLED / DEADLINE_EXCEEDED` |
| 执行器崩溃 | 步骤 `RUNNING` + 租约过期 | 恢复器重新领取并重跑该步骤 | 无感（延迟数秒） |
| 消费者故障 | 处理抛异常 | 事件级重试，5 次后进死信；不阻塞其它事件 | 读模型延迟，主流程不受影响 |
| 事件重复投递 | 消费前占位主键 | 跳过已处理 | 无感 |

## 6. 可观测性

**指标**（`/metrics`，Prometheus 文本）
`stockpact_http_requests_total{route,status}`、`stockpact_http_request_duration_seconds`（直方图）、
`stockpact_saga_step_total{step,outcome}`、`stockpact_orders_cancelled_total{reason}`、
`stockpact_inventory_reserve_conflicts_total`、`stockpact_outbox_events_total{outcome,consumer}`、
`stockpact_outbox_pending`、`stockpact_orders_in_flight`、`stockpact_cache_hit_ratio`、`stockpact_rate_limited_total`。

**日志**：单行 JSON，字段 `ts/level/logger/msg/request_id` + 业务字段（order、attempt、duration_ms）。
`request_id` 通过 contextvar 贯穿中间件、服务、审计记录。

**审计**：`/api/ops/audit` 返回 actor、action、target、detail、request_id。

**对账**：`/api/ops/reconcile` 输出 `{healthy, issues[], counters, repaired}`，检查项：
库存负数 / 预留超过在库 / 活跃预留合计与库存列漂移 / 超时未终结订单 / 租约过期仍 RUNNING 的步骤 /
缺失步骤记录 / 死信事件。

## 7. 部署形态

| 形态 | 配置 | 说明 |
| --- | --- | --- |
| 单机（默认） | `STOCKPACT_BACKGROUND=1` | API + 中继 + 恢复器同进程，适合演示与小流量 |
| API / Worker 分离 | API 设 `0`，worker 进程设 `1` | 避免长轮询抢占写锁；两者共享同一数据库 |
| 多实例 | 需 Redis（限流/缓存）+ 独立 worker | 当前进程内实现不支持，见局限 |

## 8. 扩展点

- **换存储**：实现 `repos` 的等价函数即可（注意：Postgres 下可把 `claim_saga` 换成 `FOR UPDATE SKIP LOCKED` 批量领取）。
- **换消息中间件**：`OutboxRelay.dispatch` 是唯一出口，替换为发布到 Kafka 后由消费者写 `processed_events`。
- **加正向步骤**：在 `SagaStepName` 与 `_STEP_FOR_STATE` 注册，补一个 `_exec_*` 与对应补偿。
- **加消费者**：`OutboxRelay.register(name, handler)`，天然获得幂等与重试。
