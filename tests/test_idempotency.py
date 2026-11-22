"""幂等下单：同一 key 一次订单，不同请求体必须冲突。"""

from __future__ import annotations

import threading

import pytest

from stockpact.domain.order import IdempotencyConflict


def test_same_key_same_body_replays_same_order(seeded):
    container = seeded
    first = container.orders.create(
        customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}], idempotency_key="key-1"
    )
    second = container.orders.create(
        customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}], idempotency_key="key-1"
    )
    assert first.replayed is False
    assert second.replayed is True
    assert second.order_code == first.order_code
    assert container.orders.list()["total"] == 1


def test_same_key_different_body_conflicts(seeded):
    container = seeded
    container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}], idempotency_key="key-2")
    with pytest.raises(IdempotencyConflict) as exc:
        container.orders.create(
            customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 2}], idempotency_key="key-2"
        )
    assert exc.value.http_status == 409
    assert container.orders.list()["total"] == 1


def test_concurrent_same_key_yields_one_order(seeded):
    """8 个并发请求带同一个幂等键：数据库主键兜底，只落一张订单。"""
    container = seeded
    codes: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            result = container.orders.create(
                customer_id="cust-9", items=[{"sku": "SKU-A", "quantity": 1}], idempotency_key="key-race"
            )
            with lock:
                codes.append(result.order_code)
        except Exception as exc:  # pragma: no cover
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    assert len(set(codes)) == 1
    assert container.orders.list()["total"] == 1
