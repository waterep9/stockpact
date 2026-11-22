"""恢复器与对账：TTL 回收、崩溃接管、不变量校验。"""

from __future__ import annotations

import time

from stockpact.clock import utcnow
from stockpact.domain.order import CancelReason, SagaStepName, StepStatus
from stockpact.infra import repos


def test_expired_reservation_is_compensated(seeded):
    container = seeded
    container.settings.reservation_ttl_seconds = 0.2
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-B", "quantity": 3}])
    assert container.inventory.snapshot("SKU-B")["reserved"] == 3

    time.sleep(0.35)
    ticked = container.recovery.tick()
    assert ticked["expired"] >= 1

    detail = container.orders.detail(result.order_code)
    assert detail["state"] == "CANCELLED"
    assert detail["cancel_reason"] == CancelReason.RESERVATION_EXPIRED.value
    inventory = container.inventory.snapshot("SKU-B")
    assert inventory["reserved"] == 0 and inventory["available"] == 3


def test_crashed_executor_step_is_taken_over(seeded):
    """模拟执行器在 CONFIRM 步骤中途崩溃：租约过期后由恢复器接管。"""
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    paid = container.saga.advance(result.order_code)  # create 已完成预留，这一步是扣款
    assert paid.state == "PAID"

    with container.db.session() as session:
        order = repos.get_order_by_code(session, result.order_code)
        repos.update_step(session, order.id, SagaStepName.CONFIRM_ORDER, status=StepStatus.RUNNING, attempts=1)
        order.saga_owner = "dead-executor:1"
        order.saga_lease_until = utcnow().replace(microsecond=0)
        session.commit()

    time.sleep(0.05)
    container.recovery.tick()
    detail = container.orders.detail(result.order_code)
    assert detail["state"] == "CONFIRMED"


def test_reconcile_reports_healthy_system(seeded):
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    container.saga.drive(result.order_code)
    report = container.recovery.reconcile()
    assert report["healthy"] is True, report
    assert report["issues"] == []


def test_reconcile_detects_inventory_drift(seeded):
    container = seeded
    with container.db.session() as session:
        inventory = repos.get_inventory(session, "SKU-A")
        inventory.reserved = 99
        session.commit()

    report = container.recovery.reconcile()
    assert report["healthy"] is False
    kinds = {issue["type"] for issue in report["issues"]}
    assert "inventory_over_reserved" in kinds
    assert "reservation_drift" in kinds


def test_reconcile_repair_expires_stale_holds(seeded):
    container = seeded
    container.settings.reservation_ttl_seconds = 0.2
    container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-B", "quantity": 1}])
    time.sleep(0.3)
    report = container.recovery.reconcile(repair=True)
    assert report["repaired"]["expired"] >= 1
    inventory = container.inventory.snapshot("SKU-B")
    assert inventory["reserved"] == 0
