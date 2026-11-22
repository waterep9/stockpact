"""事务性 Outbox + 至少一次投递。

写路径：业务状态变更与 outbox 记录在同一事务提交 —— 不存在"订单改了但事件丢了"。
读路径：中继轮询 PENDING 事件，逐个消费者在自己独立事务里处理，并用
`processed_events(consumer, event_id)` 唯一主键做消费侧去重。

结果：语义是 **至少一次 + 消费幂等**（业务上等价于恰好一次），
且单个消费者失败不会阻塞其它事件，也不会重复投递已完成的事件。
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable

from sqlalchemy.orm import Session

from ..clock import utcnow
from ..config import Settings
from ..domain.order import CancelReason
from ..infra import repos
from ..infra.db import Database
from ..infra.schema import Outbox
from ..obs.metrics import METRICS

log = logging.getLogger("stockpact.outbox")


@dataclass(slots=True)
class EventView:
    id: int
    order_code: str | None
    event_type: str
    payload: dict


ConsumerHandler = Callable[[Session, EventView], None]


def _money(cents: int) -> str:
    return f"¥{cents / 100:.2f}"


def read_model_consumer(session: Session, event: EventView) -> None:
    """投影消费者：维护控制台事件流与库存流水。"""
    payload = event.payload
    code = event.order_code or str(payload.get("order_code") or "-")
    event_type = event.event_type

    if event_type == "STOCK_RESERVED":
        skus = payload.get("skus", [])
        detail = "、".join(f"{s['sku']}×{s['quantity']}" for s in skus) or "无明细"
        repos.add_feed_entry(session, code, event_type, f"库存已预留：{detail}")
        for item in skus:
            repos.add_ledger_entry(session, item["sku"], "RESERVE", item["quantity"], code)
    elif event_type == "PAYMENT_CAPTURED":
        repos.add_feed_entry(session, code, event_type, f"支付成功 {_money(payload.get('amount_cents', 0))}")
    elif event_type == "ORDER_CONFIRMED":
        repos.add_feed_entry(session, code, event_type, "订单已确认，库存出库完成")
        for item in payload.get("skus", []):
            repos.add_ledger_entry(session, item["sku"], "SHIP", item["quantity"], code)
    elif event_type == "ORDER_CANCELLED":
        reason = payload.get("reason", "UNKNOWN")
        label = CancelReason(reason).value if reason in {r.value for r in CancelReason} else reason
        repos.add_feed_entry(session, code, event_type, f"订单已取消（{label}）")
        for item in payload.get("skus", []):
            repos.add_ledger_entry(session, item["sku"], "RELEASE", item["quantity"], code)
    else:
        repos.add_feed_entry(session, code, event_type, f"事件 {event_type}")


def notifier_consumer(session: Session, event: EventView) -> None:
    """通知消费者：模拟外发短信/客服工单。故意与读模型独立，用于验证扇出。"""
    payload = event.payload
    code = event.order_code or str(payload.get("order_code") or "-")
    messages = {
        "STOCK_RESERVED": "您的订单已锁定库存，等待支付",
        "PAYMENT_CAPTURED": "支付成功，正在为您出库",
        "ORDER_CONFIRMED": "订单已完成，感谢购买",
        "ORDER_CANCELLED": "订单已取消，库存/款项已回滚",
    }
    message = messages.get(event.event_type)
    if message:
        repos.add_notification(session, code, f"[{code}] {message}")


class OutboxRelay:
    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings
        self.consumers: dict[str, ConsumerHandler] = {}
        self.register("read_model", read_model_consumer)
        self.register("notifier", notifier_consumer)

    def register(self, name: str, handler: ConsumerHandler) -> None:
        self.consumers[name] = handler

    # ------------------------------------------------------------------ 投递

    def relay_once(self, limit: int | None = None) -> dict[str, int]:
        batch = limit or self.settings.outbox_batch_size
        with self.db.session() as session:
            ids = repos.pending_outbox_ids(session, batch)
        stats = {"published": 0, "failed": 0, "partial": 0}
        for event_id in ids:
            outcome = self.dispatch(event_id)
            stats[outcome] += 1
        if ids:
            log.info("outbox relay", extra={"extra_fields": {"batch": len(ids), **stats}})
        return stats

    def dispatch(self, event_id: int) -> str:
        for name, handler in self.consumers.items():
            try:
                with self.db.tx() as session:
                    if not repos.claim_consumer_event(session, name, event_id):
                        continue
                    event = repos.get_outbox(session, event_id)
                    if event is None:
                        continue
                    view = EventView(
                        id=event.id,
                        order_code=event.order_code,
                        event_type=event.event_type,
                        payload=json.loads(event.payload_json or "{}"),
                    )
                    handler(session, view)
            except Exception as exc:  # 毒丸隔离：只影响本条事件
                log.error(
                    "outbox consumer failed",
                    extra={"extra_fields": {"event_id": event_id, "consumer": name, "error": str(exc)}},
                )
                self._mark_failure(event_id, f"{name}: {exc}")
                METRICS.inc("stockpact_outbox_events_total", outcome="failed", consumer=name)
                return "failed"

        with self.db.session() as session:
            processed = repos.consumer_event_count(session, event_id)
            event = repos.get_outbox(session, event_id)
            if event is None:
                return "published"
            if processed >= len(self.consumers):
                with self.db.tx() as tx:
                    repos.mark_outbox_published(tx, event_id)
                METRICS.inc("stockpact_outbox_events_total", outcome="published")
                METRICS.inc("stockpact_outbox_events_total", outcome="delivered", consumer="all")
                return "published"
        METRICS.inc("stockpact_outbox_events_total", outcome="partial")
        return "partial"

    def _mark_failure(self, event_id: int, error: str) -> None:
        def _tx(session: Session) -> None:
            event: Outbox | None = repos.get_outbox(session, event_id)
            if event is None:
                return
            attempts = event.attempts + 1
            dead = attempts >= self.settings.outbox_max_attempts
            delay = min(0.2 * (2 ** (attempts - 1)), 10.0)
            repos.mark_outbox_attempt(
                session,
                event_id,
                error=error,
                next_attempt_at=utcnow() + timedelta(seconds=delay),
                dead=dead,
            )
            log.warning(
                "outbox retry scheduled" if not dead else "outbox dead-lettered",
                extra={"extra_fields": {"event_id": event_id, "attempts": attempts, "error": error}},
            )

        self.db.run_in_tx(_tx)

    # ------------------------------------------------------------------ 观测

    def backlog(self) -> dict[str, int]:
        with self.db.session() as session:
            return repos.outbox_backlog(session)
