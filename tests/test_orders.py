"""订单履约端到端（服务层）：正常履约、缺货补偿、取消退款、超时兜底。"""

from __future__ import annotations

from sqlalchemy import select

from stockpact.domain.order import CancelReason
from stockpact.infra import repos
from stockpact.infra.schema import Payment


def _inventory(container, sku):
    return container.inventory.snapshot(sku)


def test_happy_path_confirms_and_ships_stock(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 2}])
    assert result.state == "STOCK_RESERVED"

    outcome = container.saga.drive(result.order_code)
    assert outcome.acted is True
    assert outcome.state == "CONFIRMED"

    detail = container.orders.detail(result.order_code)
    assert [event["event_type"] for event in detail["timeline"]] == [
        "ORDER_CREATED",
        "STOCK_RESERVED",
        "PAYMENT_CAPTURED",
        "ORDER_CONFIRMED",
    ]
    statuses = {step["step"]: step["status"] for step in detail["saga_steps"]}
    assert statuses["RESERVE_STOCK"] == "SUCCEEDED"
    assert statuses["CHARGE_PAYMENT"] == "SUCCEEDED"
    assert statuses["CONFIRM_ORDER"] == "SUCCEEDED"
    assert detail["payment"]["status"] == "CAPTURED"
    assert detail["total_cents"] == 2 * 12_900

    inventory = _inventory(container, "SKU-A")
    assert inventory["on_hand"] == 8
    assert inventory["reserved"] == 0


def test_insufficient_stock_cancels_and_releases(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-B", "quantity": 99}])
    assert result.state == "CANCELLED"

    detail = container.orders.detail(result.order_code)
    assert detail["cancel_reason"] == CancelReason.OUT_OF_STOCK.value
    assert all(r["status"] == "RELEASED" or r["status"] == "HELD" for r in detail["reservations"])
    assert _inventory(container, "SKU-B")["available"] == 3
    assert detail["payment"] is None

    # 已取消订单再推进不会重复补偿
    again = container.saga.advance(result.order_code)
    assert again.acted is False


def test_cancel_after_payment_refunds_and_releases(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 4}])
    charged = container.saga.advance(result.order_code)
    assert charged.state == "PAID"

    cancelled = container.saga.cancel(result.order_code, reason=CancelReason.CUSTOMER_REQUEST, actor="test")
    assert cancelled.state == "CANCELLED"

    detail = container.orders.detail(result.order_code)
    assert detail["payment"]["status"] == "REFUNDED"
    after = _inventory(container, "SKU-A")
    assert after["on_hand"] == 10
    assert after["reserved"] == 0
    event_types = [event["event_type"] for event in detail["timeline"]]
    assert "PAYMENT_REFUNDED" in event_types
    assert "STOCK_RELEASED" in event_types


def test_second_cancel_is_idempotent(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    first = container.saga.cancel(result.order_code)
    second = container.saga.cancel(result.order_code)
    assert first.acted is True
    assert second.acted is False
    assert _inventory(container, "SKU-A")["reserved"] == 0


def test_deadline_exceeded_triggers_compensation(seeded):
    container = seeded
    container.settings.order_deadline_seconds = -1  # 已过期
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    detail = container.orders.detail(result.order_code)
    assert detail["state"] == "CANCELLED"
    assert detail["cancel_reason"] == CancelReason.DEADLINE_EXCEEDED.value
    assert _inventory(container, "SKU-A")["available"] == 10


def test_payment_charge_is_idempotent_per_order(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    with container.db.session() as session:
        order = repos.get_order_by_code(session, result.order_code)
        order_id = order.id

    first = container.payments.charge(order_id)
    second = container.payments.charge(order_id)
    assert first.succeeded and second.succeeded
    with container.db.session() as session:
        rows = list(session.scalars(select(Payment)))
    assert len(rows) == 1
    assert rows[0].status == "CAPTURED"


def test_order_list_and_detail_shape(seeded):
    container = seeded
    for _ in range(3):
        container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    page = container.orders.list(limit=2)
    assert page["total"] == 3
    assert len(page["items"]) == 2
    only_cancelled = container.orders.list(state="NOPE")
    assert only_cancelled["total"] == 0
