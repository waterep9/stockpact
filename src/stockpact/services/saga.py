"""Saga 编排器：订单履约的核心。

为什么是编排式（orchestration）而不是协同式（choreography）：
履约只有 3 个参与方，但补偿顺序与"卡住之后谁来接管"必须显式可控。编排器把
流程状态落到 `saga_steps` 表，因此进程崩溃后可以由恢复器接着跑，而不是靠
"某条消息被重复投递"这种偶然性。

三个关键机制：
1. **租约领取**：`orders.saga_owner + saga_lease_until` 条件更新，保证同一订单
   同一时刻只有一个执行者；进程崩溃则租约过期由他人接管。
2. **步骤级幂等**：支付幂等键由订单号派生、库存预留有唯一约束 (order_id, sku)，
   因此"执行了但没记下结果"的模糊状态可以安全重跑。
3. **补偿可重入**：补偿只释放仍为 HELD 的预留、只对 CAPTURED 支付退款，
   重复调用不会二次退款/二次释放。
"""

from __future__ import annotations

import logging
import os
import socket
import uuid
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.orm import Session

from ..clock import utcnow
from ..config import Settings
from ..domain.order import (
    CancelReason,
    DomainError,
    EventType,
    InsufficientStock,
    InvalidTransition,
    NotFound,
    OrderState,
    ReservationStatus,
    SagaStepName,
    StepStatus,
    TERMINAL_STATES,
)
from ..infra import repos
from ..infra.cache import TTLCache
from ..infra.db import Database
from ..infra.schema import Order
from ..obs.metrics import METRICS
from .inventory import InventoryService
from .payments import PaymentService

log = logging.getLogger("stockpact.saga")

ALL_STEPS: tuple[SagaStepName, ...] = (
    SagaStepName.RESERVE_STOCK,
    SagaStepName.CHARGE_PAYMENT,
    SagaStepName.CONFIRM_ORDER,
    SagaStepName.RELEASE_STOCK,
)

_STEP_FOR_STATE = {
    OrderState.CREATED: SagaStepName.RESERVE_STOCK,
    OrderState.STOCK_RESERVED: SagaStepName.CHARGE_PAYMENT,
    OrderState.PAID: SagaStepName.CONFIRM_ORDER,
}


def new_owner() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


@dataclass(slots=True)
class Plan:
    order_id: int
    code: str
    state: OrderState
    step: SagaStepName | None
    deadline_exceeded: bool
    attempts: int
    not_due: bool = False


@dataclass(slots=True)
class SagaOutcome:
    order_code: str
    acted: bool
    state: str
    step: str | None = None
    detail: str | None = None
    skipped_reason: str | None = None

    def as_dict(self) -> dict[str, object]:
        return {
            "order_code": self.order_code,
            "acted": self.acted,
            "state": self.state,
            "step": self.step,
            "detail": self.detail,
            "skipped_reason": self.skipped_reason,
        }


class SagaOrchestrator:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        inventory: InventoryService,
        payments: PaymentService,
        cache: TTLCache,
    ) -> None:
        self.db = db
        self.settings = settings
        self.inventory = inventory
        self.payments = payments
        self.cache = cache

    # ------------------------------------------------------------------ 对外入口

    def advance(self, order_code: str) -> SagaOutcome:
        owner = new_owner()
        plan = self._claim_plan(order_code, owner)
        if plan is None:
            state = self._state_of(order_code)
            if state == "UNKNOWN":
                raise NotFound(f"订单 {order_code} 不存在", order_code=order_code)
            return SagaOutcome(order_code, False, state, skipped_reason="租约被占用或订单已终态")

        if plan.not_due:
            return SagaOutcome(order_code, False, plan.state.value, skipped_reason="重试退避未到期")

        if plan.deadline_exceeded:
            return self._compensate_claimed(plan, CancelReason.DEADLINE_EXCEEDED, "订单超过履约时限", owner)

        if plan.step is SagaStepName.RESERVE_STOCK:
            return self._exec_reserve(plan, owner)
        if plan.step is SagaStepName.CHARGE_PAYMENT:
            return self._exec_charge(plan, owner)
        if plan.step is SagaStepName.CONFIRM_ORDER:
            return self._exec_confirm(plan, owner)
        return self._compensate_claimed(plan, CancelReason.MANUAL, "未知步骤", owner)  # pragma: no cover

    def drive(self, order_code: str, max_steps: int = 4) -> SagaOutcome:
        """连续推进直到无法前进（终态 / 退避未到期 / 租约争用）。

        恢复器用它一次把"刚创建 → 已预留 → 已支付 → 已确认"的短链路跑完，
        既保持异步语义，又不会让订单在两步之间空等一个轮询周期。
        """
        outcome = SagaOutcome(order_code, False, "UNKNOWN")
        for _ in range(max_steps):
            outcome = self.advance(order_code)
            if not outcome.acted:
                break
            if outcome.state in {s.value for s in TERMINAL_STATES}:
                break
        return outcome

    def cancel(self, order_code: str, reason: CancelReason = CancelReason.CUSTOMER_REQUEST, actor: str = "system") -> SagaOutcome:
        """用户/运营取消。已确认订单不允许取消（需要走退货流程）。"""
        owner = new_owner()
        with self.db.session() as session:
            order = repos.get_order_by_code(session, order_code)
            if order is None:
                raise NotFound(f"订单 {order_code} 不存在", order_code=order_code)
            if order.state == OrderState.CONFIRMED.value:
                raise InvalidTransition("已确认订单不能取消，请走退货流程", order_code=order_code)
            if order.state == OrderState.CANCELLED.value:
                return SagaOutcome(order_code, False, order.state, skipped_reason="订单已取消")

        plan = self._claim_plan(order_code, owner)
        if plan is None:
            return SagaOutcome(order_code, False, self._state_of(order_code), skipped_reason="租约被占用")
        return self._compensate_claimed(plan, reason, f"取消请求（{actor}）", owner)

    def retry_now(self, order_code: str) -> SagaOutcome:
        """把下一次重试时间提前到当前，用于人工干预/演练验证。"""
        with self.db.session() as session:
            order = repos.get_order_by_code(session, order_code)
            if order is None:
                raise NotFound(f"订单 {order_code} 不存在", order_code=order_code)
            for step in repos.list_steps(session, order.id):
                if step.status in {StepStatus.PENDING.value, StepStatus.FAILED.value}:
                    repos.update_step(session, order.id, SagaStepName(step.step), status=StepStatus.PENDING, next_attempt_at=utcnow())
            session.commit()
        return self.advance(order_code)

    # ------------------------------------------------------------------ 领取与规划

    def _claim_plan(self, order_code: str, owner: str) -> Plan | None:
        def _tx(session: Session) -> Plan | None:
            order = repos.get_order_by_code(session, order_code)
            if order is None:
                return None
            state = OrderState(order.state)
            if state in TERMINAL_STATES:
                return None
            if not repos.claim_saga(session, order.id, owner, self.settings.saga_lease_seconds):
                return None
            repos.ensure_steps(session, order.id, ALL_STEPS)
            now = utcnow()
            deadline_exceeded = state in {OrderState.CREATED, OrderState.STOCK_RESERVED} and order.deadline_at <= now
            step = None
            attempts = 0
            if not deadline_exceeded:
                step = _STEP_FOR_STATE[state]
                record = repos.get_step(session, order.id, step)
                attempts = (record.attempts if record else 0) + 1
                if record is not None and record.next_attempt_at is not None and record.next_attempt_at > now:
                    # 退避未到期：释放租约，避免"重试循环"把退避策略架空
                    repos.release_saga(session, order.id, owner)
                    return Plan(order.id, order.code, state, None, False, attempts, not_due=True)
                repos.update_step(
                    session,
                    order.id,
                    step,
                    status=StepStatus.RUNNING,
                    attempts=attempts,
                    clear_error=True,
                )
            return Plan(order.id, order.code, state, step, deadline_exceeded, attempts)

        return self.db.run_in_tx(_tx)

    def _state_of(self, order_code: str) -> str:
        with self.db.session() as session:
            order = repos.get_order_by_code(session, order_code)
            return order.state if order else "UNKNOWN"

    # ------------------------------------------------------------------ 各步骤执行

    def _exec_reserve(self, plan: Plan, owner: str) -> SagaOutcome:
        expires_at = utcnow() + timedelta(seconds=self.settings.reservation_ttl_seconds)
        try:

            def _tx(session: Session) -> Order:
                order = repos.get_order(session, plan.order_id)
                assert order is not None
                items = repos.list_order_items(session, plan.order_id)
                self.inventory.reserve_in_tx(session, order, items, expires_at)
                repos.set_order_state(session, order.id, OrderState.STOCK_RESERVED)
                repos.update_step(session, order.id, SagaStepName.RESERVE_STOCK, status=StepStatus.SUCCEEDED, clear_error=True)
                repos.append_order_event(
                    session,
                    order,
                    EventType.STOCK_RESERVED,
                    from_state=OrderState.CREATED.value,
                    to_state=OrderState.STOCK_RESERVED.value,
                    payload={
                        "skus": [{"sku": i.sku, "quantity": i.quantity} for i in items],
                        "expires_at": expires_at.isoformat(),
                    },
                )
                repos.enqueue_event(
                    session,
                    aggregate_type="order",
                    aggregate_id=str(order.id),
                    event_type=EventType.STOCK_RESERVED,
                    order_code=order.code,
                    payload={
                        "order_code": order.code,
                        "customer_id": order.customer_id,
                        "total_cents": order.total_cents,
                        "skus": [{"sku": i.sku, "quantity": i.quantity} for i in items],
                    },
                )
                repos.release_saga(session, order.id, owner)
                return order

            order = self.db.run_in_tx(_tx)
            self.inventory.invalidate()
            METRICS.inc("stockpact_saga_step_total", step=SagaStepName.RESERVE_STOCK.value, outcome="succeeded")
            log.info("stock reserved", extra={"extra_fields": {"order": order.code}})
            return SagaOutcome(order.code, True, OrderState.STOCK_RESERVED.value, SagaStepName.RESERVE_STOCK.value)
        except InsufficientStock as exc:
            METRICS.inc("stockpact_saga_step_total", step=SagaStepName.RESERVE_STOCK.value, outcome="insufficient_stock")
            with self.db.tx() as session:
                repos.update_step(
                    session,
                    plan.order_id,
                    SagaStepName.RESERVE_STOCK,
                    status=StepStatus.FAILED,
                    last_error=exc.message,
                )
            return self._compensate_claimed(plan, CancelReason.OUT_OF_STOCK, exc.message, owner)

    def _exec_charge(self, plan: Plan, owner: str) -> SagaOutcome:
        result = self.payments.charge(plan.order_id)
        if result.succeeded:

            def _tx(session: Session) -> Order:
                order = repos.get_order(session, plan.order_id)
                assert order is not None
                repos.set_order_state(session, order.id, OrderState.PAID)
                repos.update_step(session, order.id, SagaStepName.CHARGE_PAYMENT, status=StepStatus.SUCCEEDED, clear_error=True)
                repos.append_order_event(
                    session,
                    order,
                    EventType.PAYMENT_CAPTURED,
                    from_state=OrderState.STOCK_RESERVED.value,
                    to_state=OrderState.PAID.value,
                    payload={"provider_ref": result.provider_ref, "attempt": result.attempts, "amount_cents": order.total_cents},
                )
                repos.enqueue_event(
                    session,
                    aggregate_type="order",
                    aggregate_id=str(order.id),
                    event_type=EventType.PAYMENT_CAPTURED,
                    order_code=order.code,
                    payload={"order_code": order.code, "amount_cents": order.total_cents, "provider_ref": result.provider_ref},
                )
                repos.release_saga(session, order.id, owner)
                return order

            order = self.db.run_in_tx(_tx)
            METRICS.inc("stockpact_saga_step_total", step=SagaStepName.CHARGE_PAYMENT.value, outcome="succeeded")
            log.info("payment captured", extra={"extra_fields": {"order": order.code, "attempt": result.attempts}})
            return SagaOutcome(order.code, True, OrderState.PAID.value, SagaStepName.CHARGE_PAYMENT.value)

        METRICS.inc("stockpact_saga_step_total", step=SagaStepName.CHARGE_PAYMENT.value, outcome="declined")
        with self.db.tx() as session:
            order = repos.get_order(session, plan.order_id)
            if order is not None:
                repos.append_order_event(
                    session,
                    order,
                    EventType.PAYMENT_DECLINED,
                    payload={"attempt": result.attempts, "error": result.error},
                )

        exhausted = plan.attempts >= self.settings.payment_max_attempts
        if exhausted:
            # 先把步骤定格为 FAILED（保留最后一次错误），再走补偿：
            # 否则时间线上会留下一个永远 RUNNING 的步骤，运维无法判断是"还在试"还是"已放弃"。
            with self.db.tx() as session:
                repos.update_step(
                    session,
                    plan.order_id,
                    SagaStepName.CHARGE_PAYMENT,
                    status=StepStatus.FAILED,
                    last_error=result.error or "支付失败（重试次数耗尽）",
                )
            return self._compensate_claimed(plan, CancelReason.PAYMENT_FAILED, result.error or "支付失败", owner)

        delay = min(self.settings.payment_backoff_base_seconds * (2 ** (plan.attempts - 1)), 5.0)
        next_attempt = utcnow() + timedelta(seconds=delay)

        def _schedule(session: Session) -> None:
            repos.update_step(
                session,
                plan.order_id,
                SagaStepName.CHARGE_PAYMENT,
                status=StepStatus.PENDING,
                last_error=result.error or "支付失败",
                next_attempt_at=next_attempt,
            )
            order = repos.get_order(session, plan.order_id)
            if order is not None:
                repos.append_order_event(
                    session,
                    order,
                    EventType.SAGA_RETRY_SCHEDULED,
                    payload={"step": SagaStepName.CHARGE_PAYMENT.value, "attempt": plan.attempts, "next_attempt_at": next_attempt.isoformat()},
                )
            repos.release_saga(session, plan.order_id, owner)

        self.db.run_in_tx(_schedule)
        log.warning(
            "payment declined, retry scheduled",
            extra={"extra_fields": {"order": plan.code, "attempt": plan.attempts, "delay_s": delay}},
        )
        return SagaOutcome(
            plan.code,
            True,
            OrderState.STOCK_RESERVED.value,
            SagaStepName.CHARGE_PAYMENT.value,
            detail=f"支付失败，第 {plan.attempts} 次尝试，{delay:.1f}s 后重试",
        )

    def _exec_confirm(self, plan: Plan, owner: str) -> SagaOutcome:
        try:

            def _tx(session: Session) -> Order:
                order = repos.get_order(session, plan.order_id)
                assert order is not None
                self.inventory.commit_in_tx(session, order)
                repos.set_order_state(session, order.id, OrderState.CONFIRMED)
                repos.update_step(session, order.id, SagaStepName.CONFIRM_ORDER, status=StepStatus.SUCCEEDED, clear_error=True)
                repos.append_order_event(
                    session,
                    order,
                    EventType.ORDER_CONFIRMED,
                    from_state=OrderState.PAID.value,
                    to_state=OrderState.CONFIRMED.value,
                    payload={"total_cents": order.total_cents},
                )
                repos.enqueue_event(
                    session,
                    aggregate_type="order",
                    aggregate_id=str(order.id),
                    event_type=EventType.ORDER_CONFIRMED,
                    order_code=order.code,
                    payload={
                        "order_code": order.code,
                        "customer_id": order.customer_id,
                        "total_cents": order.total_cents,
                        "skus": [
                            {"sku": item.sku, "quantity": item.quantity}
                            for item in repos.list_order_items(session, order.id)
                        ],
                    },
                )
                repos.release_saga(session, order.id, owner)
                return order

            order = self.db.run_in_tx(_tx)
            self.inventory.invalidate()
            METRICS.inc("stockpact_saga_step_total", step=SagaStepName.CONFIRM_ORDER.value, outcome="succeeded")
            log.info("order confirmed", extra={"extra_fields": {"order": order.code}})
            return SagaOutcome(order.code, True, OrderState.CONFIRMED.value, SagaStepName.CONFIRM_ORDER.value)
        except DomainError as exc:
            METRICS.inc("stockpact_saga_step_total", step=SagaStepName.CONFIRM_ORDER.value, outcome="failed")
            return self._compensate_claimed(plan, CancelReason.MANUAL, f"确认出库失败：{exc.message}", owner)

    # ------------------------------------------------------------------ 补偿

    def _compensate_claimed(self, plan: Plan, reason: CancelReason, error: str | None, owner: str) -> SagaOutcome:
        refunded = self.payments.refund(plan.order_id)
        released = 0

        def _tx(session: Session) -> Order | None:
            order = repos.get_order(session, plan.order_id)
            if order is None:
                return None
            if order.state in {s.value for s in TERMINAL_STATES}:
                repos.release_saga(session, order.id, owner)
                return order
            previous_state = order.state
            nonlocal released
            held = [
                {"sku": r.sku, "quantity": r.quantity}
                for r in repos.list_reservations(session, order.id)
                if r.status == ReservationStatus.HELD.value
            ]
            released = self.inventory.release_in_tx(session, order)

            if refunded:
                repos.append_order_event(
                    session,
                    order,
                    EventType.PAYMENT_REFUNDED,
                    payload={"reason": reason.value, "amount_cents": order.total_cents},
                )
            if released:
                repos.append_order_event(
                    session,
                    order,
                    EventType.STOCK_RELEASED,
                    payload={"reason": reason.value, "skus_released": released},
                )

            repos.set_order_state(session, order.id, OrderState.CANCELLED, cancel_reason=reason.value)
            for step in repos.list_steps(session, order.id):
                name = SagaStepName(step.step)
                if name is SagaStepName.RELEASE_STOCK:
                    repos.update_step(session, order.id, name, status=StepStatus.SUCCEEDED, clear_error=True)
                elif name is SagaStepName.RESERVE_STOCK and step.status == StepStatus.SUCCEEDED.value:
                    repos.update_step(session, order.id, name, status=StepStatus.COMPENSATED)
                elif step.status in {StepStatus.PENDING.value, StepStatus.RUNNING.value}:
                    repos.update_step(session, order.id, name, status=StepStatus.SKIPPED)
                elif name is SagaStepName.CHARGE_PAYMENT and step.status == StepStatus.SUCCEEDED.value and refunded:
                    repos.update_step(session, order.id, name, status=StepStatus.COMPENSATED)

            repos.append_order_event(
                session,
                order,
                EventType.ORDER_CANCELLED,
                from_state=previous_state,
                to_state=OrderState.CANCELLED.value,
                payload={"reason": reason.value, "error": error, "refunded": refunded, "skus_released": released},
            )
            repos.enqueue_event(
                session,
                aggregate_type="order",
                aggregate_id=str(order.id),
                event_type=EventType.ORDER_CANCELLED,
                order_code=order.code,
                payload={
                    "order_code": order.code,
                    "customer_id": order.customer_id,
                    "reason": reason.value,
                    "error": error,
                    "refunded": refunded,
                    "skus": held,
                },
            )
            repos.release_saga(session, order.id, owner)
            return order

        order = self.db.run_in_tx(_tx)
        self.inventory.invalidate()
        METRICS.inc("stockpact_orders_cancelled_total", reason=reason.value)
        METRICS.inc(
            "stockpact_saga_step_total",
            step=SagaStepName.RELEASE_STOCK.value,
            outcome="compensated" if released else "noop",
        )
        if order is None:  # pragma: no cover
            return SagaOutcome(plan.code, False, "UNKNOWN", skipped_reason="订单不存在")
        log.warning(
            "order cancelled",
            extra={"extra_fields": {"order": order.code, "reason": reason.value, "refunded": refunded, "released": released}},
        )
        return SagaOutcome(
            order.code,
            True,
            OrderState.CANCELLED.value,
            SagaStepName.RELEASE_STOCK.value,
            detail="已补偿：" + reason.value + f"；退款={refunded}；释放SKU={released}" + (f"；原因={error}" if error else ""),
        )
