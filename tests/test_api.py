"""HTTP 契约测试：状态码、错误体、幂等头、指标、鉴权与限流。"""

from __future__ import annotations

from fastapi.testclient import TestClient

from stockpact.config import Settings
from stockpact.main import create_app


def _seed_product(client: TestClient, sku="SKU-A", price=12900, stock=5):
    response = client.post(
        "/api/catalog/products",
        json={"sku": sku, "name": "架构书", "price_cents": price, "initial_stock": stock},
    )
    assert response.status_code == 201, response.text
    return response.json()


def test_console_and_health(client):
    root = client.get("/")
    assert root.status_code == 200
    assert "StockPact" in root.text
    assert client.get("/healthz").json() == {"status": "ok"}
    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["database"] == "ok"


def test_full_lifecycle_over_http(client):
    _seed_product(client, stock=4)
    payload = {"customer_id": "cust-1", "items": [{"sku": "SKU-A", "quantity": 2}]}
    created = client.post("/api/orders", json=payload, headers={"Idempotency-Key": "http-key-1"})
    assert created.status_code == 201
    body = created.json()
    assert body["state"] == "STOCK_RESERVED"
    assert body["total_cents"] == 2 * 12900
    assert created.headers["Location"] == f"/api/orders/{body['order_code']}"
    assert created.headers["X-Idempotent-Replay"] == "false"
    code = body["order_code"]

    replay = client.post("/api/orders", json=payload, headers={"Idempotency-Key": "http-key-1"})
    assert replay.status_code == 200
    assert replay.json()["replayed"] is True
    assert replay.json()["order_code"] == code
    assert replay.headers["X-Idempotent-Replay"] == "true"

    for _ in range(3):
        nudged = client.post(f"/api/orders/{code}/advance")
        assert nudged.status_code == 200
        if nudged.json()["state"] == "CONFIRMED":
            break

    detail = client.get(f"/api/orders/{code}").json()
    assert detail["state"] == "CONFIRMED"
    assert detail["payment"]["status"] == "CAPTURED"
    assert [step["status"] for step in detail["saga_steps"][:3]] == ["SUCCEEDED"] * 3

    inventory = client.get("/api/inventory/SKU-A").json()
    assert inventory["on_hand"] == 2 and inventory["reserved"] == 0


def test_http_error_contract(client):
    _seed_product(client)
    bad_payload = client.post("/api/orders", json={"customer_id": "c", "items": [{"sku": "SKU-A", "quantity": 0}]})
    assert bad_payload.status_code == 422
    assert bad_payload.json()["error"] == "validation_error"

    unknown = client.post("/api/orders", json={"customer_id": "c", "items": [{"sku": "NOPE", "quantity": 1}]})
    assert unknown.status_code == 404
    assert unknown.json()["error"] == "not_found"

    missing = client.get("/api/orders/ORD-NOT-EXIST")
    assert missing.status_code == 404
    assert missing.headers["X-Request-Id"]


def test_insufficient_stock_returns_cancelled_order(client):
    _seed_product(client, stock=1)
    response = client.post("/api/orders", json={"customer_id": "c", "items": [{"sku": "SKU-A", "quantity": 9}]})
    assert response.status_code == 201
    body = response.json()
    assert body["state"] == "CANCELLED"
    assert body["saga"]["detail"] and "可售不足" in body["saga"]["detail"]


def test_cancel_confirmed_order_is_conflict(client):
    _seed_product(client, stock=3)
    code = client.post(
        "/api/orders", json={"customer_id": "c", "items": [{"sku": "SKU-A", "quantity": 1}]}
    ).json()["order_code"]
    for _ in range(3):
        if client.post(f"/api/orders/{code}/advance").json()["state"] == "CONFIRMED":
            break
    conflict = client.post(f"/api/orders/{code}/cancel", json={"reason": "CUSTOMER_REQUEST"})
    assert conflict.status_code == 409
    assert conflict.json()["error"] == "invalid_transition"


def test_ops_endpoints(client):
    _seed_product(client)
    client.post("/api/orders", json={"customer_id": "c", "items": [{"sku": "SKU-A", "quantity": 1}]})
    relay = client.post("/api/ops/outbox/relay")
    assert relay.status_code == 200

    outbox = client.get("/api/ops/outbox").json()
    assert outbox["items"]
    assert "backlog" in outbox

    feed = client.get("/api/ops/feed").json()
    assert feed["feed"]

    audit = client.get("/api/ops/audit").json()
    assert audit["items"] and audit["items"][0]["action"] == "order.create"

    sagas = client.get("/api/ops/sagas").json()
    assert sagas["in_flight"] >= 1
    assert "payment_failure_rate" in sagas["chaos"]

    report = client.get("/api/ops/reconcile").json()
    assert report["healthy"] is True
    assert report["counters"]["skus"] == 1

    tick = client.post("/api/ops/recovery/tick")
    assert tick.status_code == 200


def test_chaos_toggle_persists(client):
    updated = client.put("/api/ops/chaos", json={"payment_failure_rate": 0.5, "payment_stall": False})
    assert updated.status_code == 200
    assert updated.json()["payment_failure_rate"] == 0.5
    assert client.get("/api/ops/chaos").json()["payment_failure_rate"] == 0.5

    invalid = client.put("/api/ops/chaos", json={"payment_failure_rate": 2.0})
    assert invalid.status_code == 422


def test_metrics_exposition(client):
    _seed_product(client)
    client.post("/api/orders", json={"customer_id": "c", "items": [{"sku": "SKU-A", "quantity": 1}]})
    text = client.get("/metrics").text
    assert "stockpact_http_requests_total" in text
    assert "stockpact_http_request_duration_seconds_bucket" in text
    assert "stockpact_outbox_pending" in text
    assert "# TYPE stockpact_orders_in_flight gauge" in text


def _settings_with(**overrides) -> Settings:
    settings = Settings()
    settings.background_enabled = False
    settings.database_url = "sqlite+pysqlite:///:memory:"
    for key, value in overrides.items():
        setattr(settings, key, value)
    return settings


def test_admin_endpoints_require_api_key():
    settings = _settings_with(api_key="top-secret")
    app = create_app(settings, database_url="sqlite+pysqlite:///:memory:", start_background=False)
    with TestClient(app) as client:
        denied = client.post(
            "/api/catalog/products",
            json={"sku": "SKU-X", "name": "x", "price_cents": 100, "initial_stock": 1},
        )
        assert denied.status_code == 401
        assert denied.json()["error"] == "unauthorized"

        allowed = client.post(
            "/api/catalog/products",
            json={"sku": "SKU-X", "name": "x", "price_cents": 100, "initial_stock": 1},
            headers={"X-API-Key": "top-secret"},
        )
        assert allowed.status_code == 201


def test_rate_limit_returns_429_with_retry_after():
    settings = _settings_with(rate_limit_enabled=True, rate_limit_rps=0.01, rate_limit_burst=2)
    app = create_app(settings, database_url="sqlite+pysqlite:///:memory:", start_background=False)
    with TestClient(app) as client:
        statuses = [client.get("/api/meta").status_code for _ in range(4)]
        assert statuses[0] == 200 and statuses[1] == 200
        assert 429 in statuses
        limited = client.get("/api/meta")
        assert limited.status_code == 429
        assert "Retry-After" in limited.headers
        assert limited.json()["error"] == "rate_limited"
