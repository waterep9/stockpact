"""订单应用服务：创建、查询、时间线。

两条硬性约束：
- **价格以服务端目录为准**，客户端传入的单价一律忽略，从源头断掉"改价下单"。
- **创建与幂等记录同事务**：`Idempotency-Key` 的占位与订单落库要么一起成功，
  要么一起回滚；并发重复请求靠主键冲突兜底，不会产生两张订单。
"""

from __future__ import annotations

import hashlib
import json
import logging
import secrets
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy.exc import IntegrityError

from ..clock import iso, utcnow
from ..config import Settings
from ..domain.order import (
    CancelReason,
    EventType,
    IdempotencyConflict,
    IllegalArgument,
    NotFound,
    OrderState,
    normalize_lines,
)
from ..infra import repos
from ..infra.cache import TTLCache
from ..infra.db import Database
from .saga import SagaOrchestrator

log = logging.getLogger("stockpact.orders")

_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"


def generate_order_code() -> str:
    suffix = "".join(secrets.choice(_ALPHABET) for _ in range(6))
    return f"ORD-{utcnow():%Y%m%d}-{suffix}"


@dataclass(slots=True)
class CreateOrderResult:
    order_code: str
    state: str
    total_cents: int
    replayed: bool
    saga: dict[str, object] | None = None


class OrderService:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        saga: SagaOrchestrator,
        cache: TTLCache,
    ) -> None:
        self.db = db
        self.settings = settings
        self.saga = saga
        self.cache = cache

    # ------------------------------------------------------------------ 创建

    def create(
        self,
        *,
        customer_id: str,
        items: list[dict[str, object]],
        idempotency_key: str | None = None,
        request_id: str | None = None,
        actor: str | None = None,
    ) -> CreateOrderResult:
        customer_id = (customer_id or "").strip()
        if not customer_id:
            raise IllegalArgument("customer_id 不能为空")
        if not items:
            raise IllegalArgument("items 不能为空")

        raw: list[tuple[str, int, int]] = []
        with self.db.session() as session:
            for item in items:
                sku = str(item.get("sku", "")).strip()
                quantity = int(item.get("quantity", 0))  # type: ignore[arg-type]
                product = repos.get_product(session, sku)
                if product is None:
                    raise NotFound(f"SKU {sku} 不存在", sku=sku)
                raw.append((sku, quantity, product.price_cents))
        lines = normalize_lines(raw)

        request_hash = self._hash_request(customer_id, lines)
        if idempotency_key:
            replay = self._lookup_idempotent(idempotency_key, request_hash)
            if replay is not None:
                return replay

        code = generate_order_code()
        deadline = utcnow() + timedelta(seconds=self.settings.order_deadline_seconds)
        payload = {
            "order_code": code,
            "state": OrderState.CREATED.value,
            "total_cents": sum(line.subtotal_cents for line in lines),
            "items": [{"sku": l.sku, "quantity": l.quantity, "unit_price_cents": l.unit_price_cents} for l in lines],
        }

        def _tx(session):
            order = repos.create_order(
                session,
                code=code,
                customer_id=customer_id,
                total_cents=payload["total_cents"],
                currency="CNY",
                lines=[(l.sku, l.quantity, l.unit_price_cents) for l in lines],
                deadline_at=deadline,
            )
            repos.append_order_event(
                session,
                order,
                EventType.ORDER_CREATED,
                to_state=OrderState.CREATED.value,
                payload={"items": payload["items"], "total_cents": order.total_cents},
            )
            repos.enqueue_event(
                session,
                aggregate_type="order",
                aggregate_id=str(order.id),
                event_type=EventType.ORDER_CREATED,
                order_code=order.code,
                payload={**payload, "customer_id": customer_id},
            )
            repos.append_audit(
                session,
                actor=actor or customer_id,
                action="order.create",
                target_type="order",
                target_id=order.code,
                detail={"total_cents": order.total_cents, "idempotency_key": idempotency_key},
                request_id=request_id,
            )
            if idempotency_key:
                repos.save_idempotency(
                    session,
                    key=idempotency_key,
                    scope="order.create",
                    request_hash=request_hash,
                    status_code=201,
                    response_json=json.dumps(payload, ensure_ascii=False),
                    order_code=order.code,
                )
            return order

        try:
            order = self.db.run_in_tx(_tx)
        except IntegrityError:
            # 并发同 key 抢先落库：回读后按幂等规则返回
            if idempotency_key:
                replay = self._lookup_idempotent(idempotency_key, request_hash)
                if replay is not None:
                    return replay
            raise

        saga_outcome = self.saga.advance(order.code)
        return CreateOrderResult(
            order_code=order.code,
            state=saga_outcome.state,
            total_cents=order.total_cents,
            replayed=False,
            saga=saga_outcome.as_dict(),
        )

    def _hash_request(self, customer_id: str, lines) -> str:
        canonical = json.dumps(
            {"customer_id": customer_id, "items": sorted([(l.sku, l.quantity) for l in lines])},
            ensure_ascii=False,
            sort_keys=True,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()

    def _lookup_idempotent(self, key: str, request_hash: str) -> CreateOrderResult | None:
        with self.db.session() as session:
            record = repos.get_idempotency(session, key)
            if record is None:
                return None
            if record.request_hash != request_hash:
                raise IdempotencyConflict(
                    "同一 Idempotency-Key 被用于不同的请求体",
                    key=key,
                    order_code=record.order_code,
                )
            body = json.loads(record.response_json)
        state = "UNKNOWN"
        with self.db.session() as session:
            order = repos.get_order_by_code(session, body["order_code"])
            if order is not None:
                state = order.state
        return CreateOrderResult(
            order_code=body["order_code"],
            state=state,
            total_cents=body["total_cents"],
            replayed=True,
        )

    # ------------------------------------------------------------------ 查询

    def list(self, *, state: str | None = None, limit: int = 50, offset: int = 0) -> dict[str, object]:
        with self.db.session() as session:
            orders, total = repos.list_orders(session, state=state, limit=limit, offset=offset)
            items_count = {}
            for order in orders:
                items_count[order.code] = len(repos.list_order_items(session, order.id))
            data = [
                {
                    "order_code": order.code,
                    "customer_id": order.customer_id,
                    "state": order.state,
                    "cancel_reason": order.cancel_reason,
                    "total_cents": order.total_cents,
                    "items_count": items_count[order.code],
                    "created_at": iso(order.created_at),
                    "updated_at": iso(order.updated_at),
                    "deadline_at": iso(order.deadline_at),
                }
                for order in orders
            ]
        return {"items": data, "total": total, "limit": limit, "offset": offset}

    def detail(self, order_code: str) -> dict[str, object]:
        with self.db.session() as session:
            order = repos.get_order_by_code(session, order_code)
            if order is None:
                raise NotFound(f"订单 {order_code} 不存在", order_code=order_code)
            items = repos.list_order_items(session, order.id)
            events = repos.list_order_events(session, order.id)
            steps = repos.list_steps(session, order.id)
            payment = repos.get_payment(session, order.id)
            reservations = repos.list_reservations(session, order.id)

            timeline = [
                {
                    "seq": event.seq,
                    "event_type": event.event_type,
                    "from_state": event.from_state,
                    "to_state": event.to_state,
                    "payload": json.loads(event.payload_json or "{}"),
                    "at": iso(event.created_at),
                }
                for event in events
            ]
            step_view = [
                {
                    "step": step.step,
                    "status": step.status,
                    "attempts": step.attempts,
                    "last_error": step.last_error,
                    "next_attempt_at": iso(step.next_attempt_at),
                    "updated_at": iso(step.updated_at),
                }
                for step in steps
            ]
            data = {
                "order_code": order.code,
                "customer_id": order.customer_id,
                "state": order.state,
                "cancel_reason": order.cancel_reason,
                "total_cents": order.total_cents,
                "currency": order.currency,
                "created_at": iso(order.created_at),
                "updated_at": iso(order.updated_at),
                "deadline_at": iso(order.deadline_at),
                "items": [
                    {"sku": i.sku, "quantity": i.quantity, "unit_price_cents": i.unit_price_cents} for i in items
                ],
                "timeline": timeline,
                "saga_steps": step_view,
                "payment": None
                if payment is None
                else {
                    "status": payment.status,
                    "amount_cents": payment.amount_cents,
                    "provider_ref": payment.provider_ref,
                    "attempts": payment.attempts,
                },
                "reservations": [
                    {
                        "sku": r.sku,
                        "quantity": r.quantity,
                        "status": r.status,
                        "expires_at": iso(r.expires_at),
                    }
                    for r in reservations
                ],
            }
        return data

    # ------------------------------------------------------------------ 取消

    def cancel(self, order_code: str, reason: CancelReason = CancelReason.CUSTOMER_REQUEST, actor: str = "operator") -> dict[str, object]:
        outcome = self.saga.cancel(order_code, reason=reason, actor=actor)
        with self.db.tx() as session:
            repos.append_audit(
                session,
                actor=actor,
                action="order.cancel",
                target_type="order",
                target_id=order_code,
                detail={"reason": reason.value, "acted": outcome.acted},
            )
        return outcome.as_dict()
