"""仓储层：把 SQL 集中在一处，服务层只表达业务意图。

约定：
- 函数都接收显式 Session，事务边界由调用方（service / run_in_tx）决定；
- 返回 ORM 对象或不可变快照，不做隐式提交；
- 所有"并发敏感"的写入都要求用条件 UPDATE + rowcount 判断结果。
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any, Sequence

from sqlalchemy import delete, func, or_, select, update
from sqlalchemy.orm import Session

from ..clock import utcnow
from ..domain.order import (
    EventType,
    OrderState,
    PaymentStatus,
    ReservationStatus,
    SagaStepName,
    StepStatus,
    TERMINAL_STATES,
)
from .schema import (
    AuditLog,
    ChaosConfig,
    IdempotencyRecord,
    Inventory,
    InventoryLedger,
    Notification,
    Order,
    OrderEvent,
    OrderFeedEntry,
    OrderItem,
    Outbox,
    Payment,
    ProcessedEvent,
    Product,
    Reservation,
    SagaStep,
)

TERMINAL_VALUES = [s.value for s in TERMINAL_STATES]


# ---------------------------------------------------------------- 目录 / 库存


def create_product(session: Session, sku: str, name: str, price_cents: int, currency: str = "CNY") -> Product:
    product = Product(sku=sku, name=name, price_cents=price_cents, currency=currency)
    session.add(product)
    session.flush()
    session.add(Inventory(sku=sku, on_hand=0, reserved=0))
    session.flush()
    return product


def get_product(session: Session, sku: str) -> Product | None:
    return session.get(Product, sku)


def list_products(session: Session) -> list[Product]:
    return list(session.scalars(select(Product).order_by(Product.sku)))


def get_inventory(session: Session, sku: str) -> Inventory | None:
    return session.get(Inventory, sku)


def list_inventory(session: Session) -> list[tuple[Inventory, Product]]:
    stmt = select(Inventory, Product).join(Product, Product.sku == Inventory.sku).order_by(Inventory.sku)
    return [(row[0], row[1]) for row in session.execute(stmt)]


def restock(session: Session, sku: str, quantity: int) -> Inventory:
    session.execute(
        update(Inventory)
        .where(Inventory.sku == sku)
        .values(on_hand=Inventory.on_hand + quantity, version=Inventory.version + 1, updated_at=utcnow())
    )
    session.flush()
    inventory = session.get(Inventory, sku)
    assert inventory is not None
    session.expire(inventory)
    return inventory


def try_reserve(session: Session, sku: str, quantity: int) -> bool:
    """原子预留：只有可售量足够时才会更新成功。

    `on_hand - reserved >= qty` 在数据库内求值，配合行级锁（SQLite 为库级写锁）
    天然消灭"先查后改"的超卖窗口。
    """
    result = session.execute(
        update(Inventory)
        .where(Inventory.sku == sku, Inventory.on_hand - Inventory.reserved >= quantity)
        .values(reserved=Inventory.reserved + quantity, version=Inventory.version + 1, updated_at=utcnow())
    )
    return result.rowcount == 1


def release_reserved(session: Session, sku: str, quantity: int) -> bool:
    """释放预留（补偿）。reserved 不允许被减成负数。"""
    result = session.execute(
        update(Inventory)
        .where(Inventory.sku == sku, Inventory.reserved >= quantity)
        .values(reserved=Inventory.reserved - quantity, version=Inventory.version + 1, updated_at=utcnow())
    )
    return result.rowcount == 1


def ship_committed(session: Session, sku: str, quantity: int) -> bool:
    """确认出库：在库量与被预留量同时扣减。"""
    result = session.execute(
        update(Inventory)
        .where(Inventory.sku == sku, Inventory.reserved >= quantity, Inventory.on_hand >= quantity)
        .values(
            on_hand=Inventory.on_hand - quantity,
            reserved=Inventory.reserved - quantity,
            version=Inventory.version + 1,
            updated_at=utcnow(),
        )
    )
    return result.rowcount == 1


# ---------------------------------------------------------------- 订单


def create_order(
    session: Session,
    *,
    code: str,
    customer_id: str,
    total_cents: int,
    currency: str,
    lines: Sequence[tuple[str, int, int]],
    deadline_at: datetime,
) -> Order:
    order = Order(
        code=code,
        customer_id=customer_id,
        state=OrderState.CREATED.value,
        total_cents=total_cents,
        currency=currency,
        deadline_at=deadline_at,
    )
    session.add(order)
    session.flush()
    for sku, qty, price in lines:
        session.add(OrderItem(order_id=order.id, sku=sku, quantity=qty, unit_price_cents=price))
    session.flush()
    return order


def get_order_by_code(session: Session, code: str) -> Order | None:
    return session.scalar(select(Order).where(Order.code == code))


def get_order(session: Session, order_id: int) -> Order | None:
    return session.get(Order, order_id)


def list_order_items(session: Session, order_id: int) -> list[OrderItem]:
    return list(session.scalars(select(OrderItem).where(OrderItem.order_id == order_id).order_by(OrderItem.id)))


def list_orders(
    session: Session,
    *,
    state: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> tuple[list[Order], int]:
    stmt = select(Order)
    count_stmt = select(func.count()).select_from(Order)
    if state:
        stmt = stmt.where(Order.state == state)
        count_stmt = count_stmt.where(Order.state == state)
    stmt = stmt.order_by(Order.id.desc()).limit(limit).offset(offset)
    total = session.scalar(count_stmt) or 0
    return list(session.scalars(stmt)), total


def set_order_state(
    session: Session,
    order_id: int,
    target: OrderState,
    *,
    cancel_reason: str | None = None,
) -> None:
    values: dict[str, Any] = {"state": target.value, "updated_at": utcnow()}
    if cancel_reason is not None:
        values["cancel_reason"] = cancel_reason
    session.execute(update(Order).where(Order.id == order_id).values(**values))


def claim_saga(session: Session, order_id: int, owner: str, lease_seconds: float) -> bool:
    """领取订单的 Saga 执行租约。

    这是"乐观领取"而非悲观锁：先看租约是否空闲/过期，再用条件 UPDATE 抢，
    rowcount=1 表示抢到。进程崩溃时租约自然过期，其他执行器可接管。
    """
    now = utcnow()
    lease_until = now + timedelta(seconds=lease_seconds)
    result = session.execute(
        update(Order)
        .where(
            Order.id == order_id,
            Order.state.notin_(TERMINAL_VALUES),
            or_(
                Order.saga_owner.is_(None),
                Order.saga_lease_until.is_(None),
                Order.saga_lease_until < now,
                Order.saga_owner == owner,
            ),
        )
        .values(saga_owner=owner, saga_lease_until=lease_until, updated_at=now)
    )
    return result.rowcount == 1


def release_saga(session: Session, order_id: int, owner: str) -> None:
    session.execute(
        update(Order)
        .where(Order.id == order_id, Order.saga_owner == owner)
        .values(saga_owner=None, saga_lease_until=None, updated_at=utcnow())
    )


def touch_order(session: Session, order_id: int) -> None:
    session.execute(update(Order).where(Order.id == order_id).values(updated_at=utcnow()))


# ---------------------------------------------------------------- 时间线 / 事件


def append_order_event(
    session: Session,
    order: Order,
    event_type: EventType,
    *,
    from_state: str | None = None,
    to_state: str | None = None,
    payload: dict[str, Any] | None = None,
) -> OrderEvent:
    seq = (session.scalar(select(func.coalesce(func.max(OrderEvent.seq), 0)).where(OrderEvent.order_id == order.id)) or 0) + 1
    record = OrderEvent(
        order_id=order.id,
        seq=seq,
        event_type=event_type.value,
        from_state=from_state,
        to_state=to_state,
        payload_json=json.dumps(payload or {}, ensure_ascii=False),
    )
    session.add(record)
    session.flush()
    return record


def list_order_events(session: Session, order_id: int) -> list[OrderEvent]:
    return list(session.scalars(select(OrderEvent).where(OrderEvent.order_id == order_id).order_by(OrderEvent.seq)))


# ---------------------------------------------------------------- Saga 步骤


def ensure_steps(session: Session, order_id: int, steps: Sequence[SagaStepName]) -> list[SagaStep]:
    existing = {s.step: s for s in session.scalars(select(SagaStep).where(SagaStep.order_id == order_id))}
    created: list[SagaStep] = []
    for step in steps:
        if step.value not in existing:
            record = SagaStep(order_id=order_id, step=step.value, status=StepStatus.PENDING.value)
            session.add(record)
            created.append(record)
    if created:
        session.flush()
        existing = {s.step: s for s in session.scalars(select(SagaStep).where(SagaStep.order_id == order_id))}
    return [existing[s.value] for s in steps]


def get_step(session: Session, order_id: int, step: SagaStepName) -> SagaStep | None:
    return session.scalar(select(SagaStep).where(SagaStep.order_id == order_id, SagaStep.step == step.value))


def list_steps(session: Session, order_id: int) -> list[SagaStep]:
    return list(session.scalars(select(SagaStep).where(SagaStep.order_id == order_id).order_by(SagaStep.id)))


def update_step(
    session: Session,
    order_id: int,
    step: SagaStepName,
    *,
    status: StepStatus | None = None,
    attempts: int | None = None,
    last_error: str | None = None,
    next_attempt_at: datetime | None = None,
    clear_error: bool = False,
) -> None:
    values: dict[str, Any] = {"updated_at": utcnow()}
    if status is not None:
        values["status"] = status.value
    if attempts is not None:
        values["attempts"] = attempts
    if clear_error:
        values["last_error"] = None
    elif last_error is not None:
        values["last_error"] = last_error[:2000]
    if next_attempt_at is not None:
        values["next_attempt_at"] = next_attempt_at
    session.execute(
        update(SagaStep).where(SagaStep.order_id == order_id, SagaStep.step == step.value).values(**values)
    )


# ---------------------------------------------------------------- 预留 / 支付


def add_reservation(
    session: Session, order_id: int, sku: str, quantity: int, expires_at: datetime
) -> Reservation:
    record = Reservation(
        order_id=order_id,
        sku=sku,
        quantity=quantity,
        status=ReservationStatus.HELD.value,
        expires_at=expires_at,
    )
    session.add(record)
    return record


def list_reservations(session: Session, order_id: int) -> list[Reservation]:
    return list(session.scalars(select(Reservation).where(Reservation.order_id == order_id).order_by(Reservation.id)))


def set_reservations_status(
    session: Session,
    order_id: int,
    *,
    from_status: ReservationStatus,
    to_status: ReservationStatus,
) -> int:
    result = session.execute(
        update(Reservation)
        .where(Reservation.order_id == order_id, Reservation.status == from_status.value)
        .values(status=to_status.value, updated_at=utcnow())
    )
    return result.rowcount


def list_expired_reservations(session: Session, limit: int = 50) -> list[Reservation]:
    stmt = (
        select(Reservation)
        .join(Order, Order.id == Reservation.order_id)
        .where(
            Reservation.status == ReservationStatus.HELD.value,
            Reservation.expires_at <= utcnow(),
            Order.state.notin_(TERMINAL_VALUES),
        )
        .order_by(Reservation.expires_at)
        .limit(limit)
    )
    return list(session.scalars(stmt))


def get_payment(session: Session, order_id: int) -> Payment | None:
    return session.scalar(select(Payment).where(Payment.order_id == order_id))


def add_payment(session: Session, order_id: int, amount_cents: int, status: PaymentStatus, provider_ref: str) -> Payment:
    payment = Payment(
        order_id=order_id,
        amount_cents=amount_cents,
        status=status.value,
        provider_ref=provider_ref,
        attempts=1,
    )
    session.add(payment)
    session.flush()
    return payment


def update_payment(session: Session, payment_id: int, *, status: PaymentStatus, attempts: int) -> None:
    session.execute(
        update(Payment)
        .where(Payment.id == payment_id)
        .values(status=status.value, attempts=attempts, updated_at=utcnow())
    )


# ---------------------------------------------------------------- Outbox


def enqueue_event(
    session: Session,
    *,
    aggregate_type: str,
    aggregate_id: str,
    event_type: EventType,
    payload: dict[str, Any],
    order_code: str | None = None,
) -> Outbox:
    record = Outbox(
        aggregate_type=aggregate_type,
        aggregate_id=str(aggregate_id),
        order_code=order_code,
        event_type=event_type.value,
        payload_json=json.dumps(payload, ensure_ascii=False, default=str),
    )
    session.add(record)
    session.flush()
    return record


def pending_outbox_ids(session: Session, limit: int) -> list[int]:
    stmt = (
        select(Outbox.id)
        .where(Outbox.status == "PENDING", Outbox.next_attempt_at <= utcnow())
        .order_by(Outbox.id)
        .limit(limit)
    )
    return list(session.scalars(stmt))


def get_outbox(session: Session, event_id: int) -> Outbox | None:
    return session.get(Outbox, event_id)


def list_outbox(session: Session, *, status: str | None = None, limit: int = 50) -> list[Outbox]:
    stmt = select(Outbox)
    if status:
        stmt = stmt.where(Outbox.status == status)
    stmt = stmt.order_by(Outbox.id.desc()).limit(limit)
    return list(session.scalars(stmt))


def mark_outbox_published(session: Session, event_id: int) -> None:
    session.execute(
        update(Outbox)
        .where(Outbox.id == event_id)
        .values(status="PUBLISHED", published_at=utcnow(), last_error=None)
    )


def mark_outbox_attempt(
    session: Session,
    event_id: int,
    *,
    error: str,
    next_attempt_at: datetime,
    dead: bool,
) -> None:
    event = session.get(Outbox, event_id)
    if event is None:
        return
    session.execute(
        update(Outbox)
        .where(Outbox.id == event_id)
        .values(
            attempts=event.attempts + 1,
            status="DEAD" if dead else "PENDING",
            last_error=error[:2000],
            next_attempt_at=next_attempt_at,
        )
    )


def claim_consumer_event(session: Session, consumer: str, event_id: int) -> bool:
    """消费侧幂等占位。唯一主键冲突即表示已消费过。"""
    existing = session.get(ProcessedEvent, (consumer, event_id))
    if existing is not None:
        return False
    session.add(ProcessedEvent(consumer=consumer, event_id=event_id))
    try:
        session.flush()
    except Exception:
        session.rollback()
        return False
    return True


def consumer_event_count(session: Session, event_id: int) -> int:
    return session.scalar(select(func.count()).select_from(ProcessedEvent).where(ProcessedEvent.event_id == event_id)) or 0


# ---------------------------------------------------------------- 幂等 / 审计 / 混沌


def get_idempotency(session: Session, key: str) -> IdempotencyRecord | None:
    return session.get(IdempotencyRecord, key)


def save_idempotency(
    session: Session,
    *,
    key: str,
    scope: str,
    request_hash: str,
    status_code: int,
    response_json: str,
    order_code: str | None,
) -> None:
    session.add(
        IdempotencyRecord(
            key=key,
            scope=scope,
            request_hash=request_hash,
            status_code=status_code,
            response_json=response_json,
            order_code=order_code,
        )
    )
    session.flush()


def append_audit(
    session: Session,
    *,
    actor: str,
    action: str,
    target_type: str,
    target_id: str,
    detail: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> AuditLog:
    record = AuditLog(
        actor=actor,
        action=action,
        target_type=target_type,
        target_id=target_id,
        detail_json=json.dumps(detail or {}, ensure_ascii=False, default=str),
        request_id=request_id,
    )
    session.add(record)
    return record


def list_audit(session: Session, limit: int = 100) -> list[AuditLog]:
    return list(session.scalars(select(AuditLog).order_by(AuditLog.id.desc()).limit(limit)))


def get_chaos(session: Session) -> dict[str, str]:
    return {row.key: row.value for row in session.scalars(select(ChaosConfig))}


def set_chaos(session: Session, key: str, value: str) -> None:
    record = session.get(ChaosConfig, key)
    if record is None:
        session.add(ChaosConfig(key=key, value=value))
    else:
        record.value = value
        record.updated_at = utcnow()


# ---------------------------------------------------------------- 读模型


def add_feed_entry(session: Session, order_code: str, event_type: str, summary: str) -> None:
    session.add(OrderFeedEntry(order_code=order_code, event_type=event_type, summary=summary[:255]))


def count_feed(session: Session) -> int:
    return session.scalar(select(func.count()).select_from(OrderFeedEntry)) or 0


def list_feed(session: Session, limit: int = 60) -> list[OrderFeedEntry]:
    return list(session.scalars(select(OrderFeedEntry).order_by(OrderFeedEntry.id.desc()).limit(limit)))


def add_ledger_entry(session: Session, sku: str, movement: str, quantity: int, order_code: str | None) -> None:
    session.add(InventoryLedger(sku=sku, movement=movement, quantity=quantity, order_code=order_code))


def list_ledger(session: Session, sku: str | None = None, limit: int = 60) -> list[InventoryLedger]:
    stmt = select(InventoryLedger)
    if sku:
        stmt = stmt.where(InventoryLedger.sku == sku)
    return list(session.scalars(stmt.order_by(InventoryLedger.id.desc()).limit(limit)))


def add_notification(session: Session, order_code: str, message: str, channel: str = "console") -> None:
    session.add(Notification(order_code=order_code, message=message[:255], channel=channel))


def list_notifications(session: Session, limit: int = 50) -> list[Notification]:
    return list(session.scalars(select(Notification).order_by(Notification.id.desc()).limit(limit)))


# ---------------------------------------------------------------- 运维查询


def stuck_orders(session: Session, *, idle_before: datetime, limit: int = 50) -> list[Order]:
    """非终态且长时间没有变化的订单。"""
    stmt = (
        select(Order)
        .where(Order.state.notin_(TERMINAL_VALUES), Order.updated_at <= idle_before)
        .order_by(Order.updated_at)
        .limit(limit)
    )
    return list(session.scalars(stmt))


def orders_with_due_steps(session: Session, *, limit: int = 50) -> list[Order]:
    """存在"到期待重试"或"租约过期仍 RUNNING"步骤的订单。"""
    now = utcnow()
    stmt = (
        select(Order)
        .join(SagaStep, SagaStep.order_id == Order.id)
        .where(
            Order.state.notin_(TERMINAL_VALUES),
            or_(
                (SagaStep.status == StepStatus.PENDING.value)
                & or_(SagaStep.next_attempt_at.is_(None), SagaStep.next_attempt_at <= now),
                (SagaStep.status == StepStatus.RUNNING.value) & (Order.saga_lease_until < now),
                (SagaStep.status == StepStatus.RUNNING.value) & (Order.saga_lease_until.is_(None)),
            ),
        )
        .order_by(Order.updated_at)
        .limit(limit)
    )
    return list({o.id: o for o in session.scalars(stmt)}.values())


def order_state_counts(session: Session) -> dict[str, int]:
    stmt = select(Order.state, func.count()).group_by(Order.state)
    return {state: int(count) for state, count in session.execute(stmt)}


def reserved_sum_by_sku(session: Session) -> dict[str, int]:
    stmt = (
        select(Reservation.sku, func.coalesce(func.sum(Reservation.quantity), 0))
        .where(Reservation.status == ReservationStatus.HELD.value)
        .group_by(Reservation.sku)
    )
    return {sku: int(total) for sku, total in session.execute(stmt)}


def outbox_backlog(session: Session) -> dict[str, int]:
    stmt = select(Outbox.status, func.count()).group_by(Outbox.status)
    return {status: count for status, count in session.execute(stmt)}


def purge_outbox_published(session: Session, before: datetime) -> int:
    result = session.execute(delete(Outbox).where(Outbox.status == "PUBLISHED", Outbox.published_at < before))
    return result.rowcount

