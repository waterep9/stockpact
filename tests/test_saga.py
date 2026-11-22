"""Saga 编排测试：并发推进、失败重试、退避、支付超时。"""

from __future__ import annotations

import threading

from sqlalchemy import select

from stockpact.clock import utcnow
from stockpact.domain.order import CancelReason, SagaStepName
from stockpact.infra import repos
from stockpact.infra.schema import Payment


def test_concurrent_advance_charges_exactly_once(seeded):
    """10 个执行器同时推进同一订单：只允许一次扣款、一条支付事件。"""
    container = seeded
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 2}])
    outcomes: list[str] = []
    errors: list[Exception] = []
    lock = threading.Lock()

    def worker() -> None:
        try:
            outcome = container.saga.drive(result.order_code)
        except Exception as exc:  # pragma: no cover - 并发缺陷会在这里暴露
            with lock:
                errors.append(exc)
            return
        with lock:
            outcomes.append(outcome.state)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert errors == []
    detail = container.orders.detail(result.order_code)
    assert detail["state"] == "CONFIRMED"
    with container.db.session() as session:
        payments = list(session.scalars(select(Payment)))
    assert len(payments) == 1
    captured = [e for e in detail["timeline"] if e["event_type"] == "PAYMENT_CAPTURED"]
    assert len(captured) == 1
    inventory = container.inventory.snapshot("SKU-A")
    assert inventory["on_hand"] == 8 and inventory["reserved"] == 0


def test_payment_failure_exhausts_retries_then_compensates(seeded):
    container = seeded
    container.chaos.update({"payment_failure_rate": 1.0})
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-B", "quantity": 2}])

    for _ in range(4):
        container.saga.drive(result.order_code)
        # 清掉退避，模拟时间推进（避免测试里 sleep）
        with container.db.session() as session:
            order = repos.get_order_by_code(session, result.order_code)
            for step in repos.list_steps(session, order.id):
                if step.next_attempt_at is not None:
                    repos.update_step(
                        session, order.id, SagaStepName(step.step), next_attempt_at=utcnow()
                    )
            session.commit()

    detail = container.orders.detail(result.order_code)
    assert detail["state"] == "CANCELLED"
    assert detail["cancel_reason"] == CancelReason.PAYMENT_FAILED.value
    assert detail["payment"]["status"] == "DECLINED"
    assert detail["payment"]["attempts"] == container.settings.payment_max_attempts
    stage = {s["step"]: s for s in detail["saga_steps"]}
    assert stage["CHARGE_PAYMENT"]["attempts"] == container.settings.payment_max_attempts
    # 库存已释放
    inventory = container.inventory.snapshot("SKU-B")
    assert inventory["reserved"] == 0 and inventory["available"] == 3


def test_backoff_is_respected(seeded):
    """失败后立即再推进不应绕过退避窗口。"""
    container = seeded
    container.chaos.update({"payment_failure_rate": 1.0})
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    first = container.saga.advance(result.order_code)
    assert first.state == "STOCK_RESERVED"
    assert first.acted is True

    second = container.saga.advance(result.order_code)
    assert second.acted is False
    assert second.skipped_reason and "退避" in second.skipped_reason


def test_payment_timeout_is_treated_as_unconfirmed(seeded):
    container = seeded
    container.chaos.update({"payment_stall": True})
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    outcome = container.saga.advance(result.order_code)
    assert outcome.state == "STOCK_RESERVED"
    detail = container.orders.detail(result.order_code)
    declined = [e for e in detail["timeline"] if e["event_type"] == "PAYMENT_DECLINED"]
    assert declined and "超时" in declined[-1]["payload"]["error"]
    # 订单没有因为"超时"被判定为已支付
    assert detail["payment"]["status"] == "DECLINED"


def test_retry_now_clears_backoff(seeded):
    container = seeded
    container.chaos.update({"payment_failure_rate": 1.0})
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": 1}])
    container.saga.advance(result.order_code)
    container.chaos.update({"payment_failure_rate": 0.0})
    outcome = container.saga.retry_now(result.order_code)
    assert outcome.acted is True
    detail = container.orders.detail(result.order_code)
    assert detail["state"] in {"PAID", "CONFIRMED"}
