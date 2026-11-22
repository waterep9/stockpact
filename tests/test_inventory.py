"""库存并发控制测试：这是"不超卖"这一承诺的证据。"""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest

from stockpact.clock import utcnow
from stockpact.domain.order import InsufficientStock
from stockpact.infra import repos


def _reserve(container, sku: str, qty: int) -> bool:
    return container.db.run_in_tx(lambda s: repos.try_reserve(s, sku, qty))


def _inv(container, sku: str):
    with container.db.session() as session:
        inventory = repos.get_inventory(session, sku)
        session.expunge(inventory)
        return inventory


def test_concurrent_reserve_never_oversells(seeded):
    """库存 3，24 个线程同时抢 1 件：只能成功 3 次，且绝不出现负可售。"""
    container = seeded
    results: list[bool] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            ok = _reserve(container, "SKU-B", 1)
            with lock:
                results.append(ok)
        except Exception as exc:  # pragma: no cover - 失败即测试失败
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(24)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert len(results) == 24
    assert sum(results) == 3
    inventory = _inv(container, "SKU-B")
    assert inventory.reserved == 3
    assert inventory.on_hand - inventory.reserved == 0


def test_multi_sku_reservation_is_all_or_nothing(seeded):
    """B 库存不足时，A 的预留必须一起回滚。"""
    container = seeded
    with container.db.session() as session:
        order = repos.create_order(
            session,
            code="ORD-ATOMIC",
            customer_id="c1",
            total_cents=100,
            currency="CNY",
            lines=[("SKU-A", 2, 100), ("SKU-B", 999, 100)],
            deadline_at=utcnow() + timedelta(seconds=30),
        )
        session.commit()
        order_id = order.id

    def attempt(session):
        order_obj = repos.get_order(session, order_id)
        items = repos.list_order_items(session, order_id)
        container.inventory.reserve_in_tx(session, order_obj, items, utcnow() + timedelta(seconds=30))

    with pytest.raises(InsufficientStock):
        container.db.run_in_tx(attempt)

    assert _inv(container, "SKU-A").reserved == 0
    assert _inv(container, "SKU-B").reserved == 0


def test_release_and_ship_respect_lower_bound(seeded):
    container = seeded
    assert _reserve(container, "SKU-B", 2) is True
    # 不能释放超过已预留的量
    assert container.db.run_in_tx(lambda s: repos.release_reserved(s, "SKU-B", 5)) is False
    assert _inv(container, "SKU-B").reserved == 2

    assert container.db.run_in_tx(lambda s: repos.ship_committed(s, "SKU-B", 2)) is True
    inventory = _inv(container, "SKU-B")
    assert inventory.on_hand == 1
    assert inventory.reserved == 0


def test_cache_is_invalidated_after_restock(seeded):
    container = seeded
    first = container.inventory.snapshot("SKU-A")
    assert first["available"] == 10
    container.inventory.restock("SKU-A", 5)
    second = container.inventory.snapshot("SKU-A")
    assert second["available"] == 15
    assert container.cache.stats()["invalidations"] >= 1
