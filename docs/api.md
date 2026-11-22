# API 文档

Base URL：`http://127.0.0.1:8080`（本地默认）。交互式文档：`/docs`（Swagger）、`/redoc`。

## 约定

- 请求/响应均为 `application/json; charset=utf-8`；金额单位为**分**（`total_cents`）。
- 时间一律 UTC ISO8601（`2026-11-22T03:31:26Z`）。
- 每个响应带 `X-Request-Id`（可用请求头传入自定义值，便于跨系统串联）。
- 管理端接口在设置了 `STOCKPACT_API_KEY` 后必须携带 `X-API-Key`，否则 `401`。
- `/api/*` 默认限流（令牌桶，按 API Key 或客户端 IP 分桶），超限返回 `429` + `Retry-After`。

### 统一错误体

```json
{
  "error": "insufficient_stock",
  "message": "SKU SKU-B 可售不足：需要 99，剩余 3",
  "detail": {"sku": "SKU-B", "requested": 99, "available": 3},
  "request_id": "a1b2c3d4e5f6a7b8"
}
```

| error | HTTP | 触发条件 |
| --- | --- | --- |
| `validation_error` | 422 | 参数缺失/类型错误/超出取值范围 |
| `illegal_argument` | 422 | 业务参数非法（空 SKU、数量为 0、混沌参数越界） |
| `not_found` | 404 | 订单或 SKU 不存在 |
| `insufficient_stock` | 409 | 直接调用库存预留时不足（下单接口不会返回它，见下） |
| `invalid_transition` | 409 | 非法状态迁移（如取消已确认订单） |
| `idempotency_key_reused` | 409 | 同一幂等键用于不同请求体 |
| `unauthorized` | 401 | 管理端缺少/错误 `X-API-Key` |
| `rate_limited` | 429 | 超出限流配额 |

> 下单时库存不足**不会**返回 409：订单会被接受（201），随后由 Saga 补偿为 `CANCELLED`。
> 这是异步履约的语义——"接受订单"和"履约成功"是两件事。

---

## 1. 下单

`POST /api/orders`

| Header | 必填 | 说明 |
| --- | --- | --- |
| `Idempotency-Key` | 否（强烈建议） | 最长 120 字符。同 key 同请求体 → 重放；同 key 不同体 → 409 |
| `Content-Type` | 是 | `application/json` |

```bash
curl -s -X POST http://127.0.0.1:8080/api/orders \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: order-2026-001' \
  -d '{"customer_id":"cust-1024","items":[{"sku":"SKU-BOOK-001","quantity":2}]}'
```

```json
{
  "order_code": "ORD-20261122-LB46GK",
  "state": "STOCK_RESERVED",
  "total_cents": 25800,
  "replayed": false,
  "saga": {
    "order_code": "ORD-20261122-LB46GK",
    "acted": true,
    "state": "STOCK_RESERVED",
    "step": "RESERVE_STOCK",
    "detail": null,
    "skipped_reason": null
  }
}
```

- `201` 新建；`200` 幂等重放（响应头 `X-Idempotent-Replay: true`）。
- 响应头 `Location: /api/orders/{order_code}`。
- 价格由服务端目录决定，请求体里的价格字段会被忽略。
- 同一 SKU 多行会被合并，避免重复扣减。

## 2. 查询

### 列表 `GET /api/orders?state=&limit=50&offset=0`

```json
{"items":[{"order_code":"ORD-...","state":"CONFIRMED","total_cents":12900,"items_count":1,
           "created_at":"2026-11-22T03:31:26Z","updated_at":"...","deadline_at":"..."}],
 "total":112,"limit":50,"offset":0}
```

### 详情 `GET /api/orders/{code}`

返回订单、行项目、时间线（`timeline`）、Saga 步骤（`saga_steps`）、支付、预留。

```json
{
  "order_code": "ORD-20261122-LB46GK",
  "state": "CONFIRMED",
  "cancel_reason": null,
  "total_cents": 25800,
  "timeline": [
    {"seq": 1, "event_type": "ORDER_CREATED", "from_state": null, "to_state": "CREATED", "payload": {}, "at": "..."},
    {"seq": 2, "event_type": "STOCK_RESERVED", "from_state": "CREATED", "to_state": "STOCK_RESERVED",
     "payload": {"skus": [{"sku": "SKU-BOOK-001", "quantity": 2}]}, "at": "..."},
    {"seq": 3, "event_type": "PAYMENT_CAPTURED", "payload": {"provider_ref": "sim_...", "attempt": 1}, "at": "..."}
  ],
  "saga_steps": [
    {"step": "RESERVE_STOCK", "status": "SUCCEEDED", "attempts": 1, "last_error": null, "next_attempt_at": null},
    {"step": "CHARGE_PAYMENT", "status": "SUCCEEDED", "attempts": 1},
    {"step": "CONFIRM_ORDER", "status": "SUCCEEDED", "attempts": 1},
    {"step": "RELEASE_STOCK", "status": "PENDING", "attempts": 0}
  ],
  "payment": {"status": "CAPTURED", "amount_cents": 25800, "provider_ref": "sim_...", "attempts": 1},
  "reservations": [{"sku": "SKU-BOOK-001", "quantity": 2, "status": "COMMITTED", "expires_at": "..."}]
}
```

`RELEASE_STOCK` 是补偿步骤，正常履约时保持 `PENDING`。

## 3. 取消

`POST /api/orders/{code}/cancel`，Header 可选 `X-Actor`（写入审计）。

```bash
curl -s -X POST http://127.0.0.1:8080/api/orders/ORD-20261122-LB46GK/cancel \
  -H 'Content-Type: application/json' -d '{"reason":"CUSTOMER_REQUEST"}'
```

`reason` 取值：`CUSTOMER_REQUEST` / `OUT_OF_STOCK` / `PAYMENT_FAILED` / `RESERVATION_EXPIRED` / `DEADLINE_EXCEEDED` / `MANUAL`。

- 未支付：释放预留 → `CANCELLED`。
- 已支付（`PAID`）：**退款** + 释放预留 → `CANCELLED`。
- 已确认（`CONFIRMED`）：`409 invalid_transition`（需走退货流程）。
- 重复取消：`acted=false`，无副作用。

## 4. 推进与重试

| 接口 | 用途 |
| --- | --- |
| `POST /api/orders/{code}/advance` | 手动推进一次 Saga，用于演示/排障 |
| `POST /api/orders/{code}/retry` | 把待重试步骤的 `next_attempt_at` 提前到当前并立即执行 |

两者都返回 `SagaOutcome`。`acted=false` 且 `skipped_reason="重试退避未到期"` 表示退避窗口未到（正常行为）。

## 5. 目录与库存

```bash
# 建档（管理端）
curl -s -X POST http://127.0.0.1:8080/api/catalog/products \
  -H 'Content-Type: application/json' \
  -d '{"sku":"SKU-BOOK-001","name":"架构书","price_cents":12900,"initial_stock":40}'

# 补货（管理端）
curl -s -X POST http://127.0.0.1:8080/api/inventory/SKU-BOOK-001/restock \
  -H 'Content-Type: application/json' -d '{"quantity":50}'
```

```json
{"sku":"SKU-BOOK-001","name":"架构书","price_cents":12900,"currency":"CNY",
 "on_hand":40,"reserved":2,"available":38,"version":2,"updated_at":"2026-11-22T03:31:26Z"}
```

`available = on_hand - reserved`。`GET /api/inventory` 返回全部 SKU（走 TTL 缓存，写操作会主动失效）。

## 6. 运维接口

### 混沌开关

```bash
curl -s http://127.0.0.1:8080/api/ops/chaos
curl -s -X PUT http://127.0.0.1:8080/api/ops/chaos \
  -H 'Content-Type: application/json' -d '{"payment_failure_rate":1.0,"payment_latency_ms":200}'
```

| 字段 | 范围 | 语义 |
| --- | --- | --- |
| `payment_failure_rate` | 0–1 | 按 `(订单号, 尝试次数)` 确定性伪随机的失败概率 |
| `payment_latency_ms` | 0–5000 | 网关延迟 |
| `payment_stall` | bool | 网关不返回（模拟超时，结果未知） |

### 其他

| 接口 | 返回 |
| --- | --- |
| `GET /api/ops/outbox?status=&limit=` | 事件列表 + `backlog{PENDING/PUBLISHED/DEAD}` |
| `POST /api/ops/outbox/relay` | 手动触发一次投递（管理端） |
| `GET /api/ops/feed?limit=` | `feed` 投影、`notifications`、`ledger` 库存流水 |
| `GET /api/ops/audit?limit=` | 审计日志 |
| `GET /api/ops/sagas` | 状态分布、在途数、outbox 积压、待处理订单、混沌快照 |
| `GET /api/ops/reconcile?repair=true` | 对账报告（`repair` 会先回收/重试） |
| `POST /api/ops/recovery/tick` | 手动跑一轮恢复（管理端） |

### 指标与探针

- `GET /healthz` → `{"status":"ok"}`
- `GET /readyz` → `{"status":"ready","database":"ok","background":true}`，DB 不可用时 503
- `GET /metrics` → Prometheus 文本，示例：

```
# TYPE stockpact_http_request_duration_seconds histogram
stockpact_http_request_duration_seconds_bucket{route="/api/orders",le="0.05"} 12
stockpact_http_request_duration_seconds_bucket{route="/api/orders",le="+Inf"} 40
stockpact_http_request_duration_seconds_sum{route="/api/orders"} 2.71
stockpact_http_request_duration_seconds_count{route="/api/orders"} 40
# TYPE stockpact_outbox_pending gauge
stockpact_outbox_pending 0
```

## 7. 状态与步骤枚举

| 枚举 | 取值 |
| --- | --- |
| 订单状态 | `CREATED` / `STOCK_RESERVED` / `PAID` / `CONFIRMED` / `CANCELLED` |
| 步骤状态 | `PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED` / `COMPENSATED` / `SKIPPED` |
| 支付状态 | `CAPTURED` / `DECLINED` / `REFUNDED` |
| 预留状态 | `HELD` / `COMMITTED` / `RELEASED` |
| Outbox 状态 | `PENDING` / `PUBLISHED` / `DEAD` |
