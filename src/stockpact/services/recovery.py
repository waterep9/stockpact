"""恢复器与对账器：让系统"自己爬起来"。

三种角色：
1. `expire_reservations` —— 预留到期回收。库存被长期占住是电商最贵的故障之一，
   这里把 TTL 到期当作补偿触发条件，而不是指望用户主动取消。
2. `retry_due` —— 补偿/重试驱动器。扫描"到期待重试"和"步骤 RUNNING 但租约已过期"
   的订单（后者对应执行进程崩溃），重新推进 Saga。
3. `reconcile` —— 只读对账，校验不变量并输出结构化报告，供 /api/ops/reconcile 与
   告警系统消费；`repair=True` 时先自愈再对账。
"""

from __future__ import annotations

import logging
from datetime import timedelta

from ..clock import iso, utcnow
from ..config import Settings
from ..domain.order import CancelReason, InvalidTransition, NotFound, StepStatus, TERMINAL_STATES
from ..infra import repos
from ..infra.db import Database
from .inventory import InventoryService
from .outbox import OutboxRelay
from .saga import SagaOrchestrator

log = logging.getLogger("stockpact.recovery")


class RecoveryService:
    def __init__(
        self,
        db: Database,
        settings: Settings,
        saga: SagaOrchestrator,
        outbox: OutboxRelay,
        inventory: InventoryService,
    ) -> None:
        self.db = db
        self.settings = settings
        self.saga = saga
        self.outbox = outbox
        self.inventory = inventory
        self.last_run: dict[str, object] = {}

    # ------------------------------------------------------------------ 回收

    def expire_reservations(self, limit: int | None = None) -> int:
        with self.db.session() as session:
            expired = repos.list_expired_reservations(session, limit or self.settings.recovery_batch_size)
            codes = sorted({self._order_code(session, r.order_id) for r in expired})
        cancelled = 0
        for code in codes:
            try:
                outcome = self.saga.cancel(code, reason=CancelReason.RESERVATION_EXPIRED, actor="recovery")
                if outcome.acted:
                    cancelled += 1
                    log.warning("expired reservation compensated", extra={"extra_fields": {"order": code}})
            except (InvalidTransition, NotFound):
                continue
        return cancelled

    @staticmethod
    def _order_code(session, order_id: int) -> str:
        order = repos.get_order(session, order_id)
        return order.code if order else ""

    # ------------------------------------------------------------------ 重试

    def retry_due(self, limit: int | None = None) -> int:
        with self.db.session() as session:
            orders = repos.orders_with_due_steps(session, limit=limit or self.settings.recovery_batch_size)
            codes = [o.code for o in orders]
        advanced = 0
        for code in codes:
            try:
                outcome = self.saga.drive(code, max_steps=3)
                if outcome.acted:
                    advanced += 1
            except (NotFound, InvalidTransition):
                continue
        if advanced:
            log.info("saga retried", extra={"extra_fields": {"count": advanced}})
        return advanced

    # ------------------------------------------------------------------ 对账

    def reconcile(self, repair: bool = False) -> dict[str, object]:
        repaired = {"expired": 0, "retried": 0}
        if repair:
            repaired["expired"] = self.expire_reservations()
            repaired["retried"] = self.retry_due()

        now = utcnow()
        issues: list[dict[str, object]] = []
        counters: dict[str, int] = {}

        with self.db.session() as session:
            inventory_rows = repos.list_inventory(session)
            counters["skus"] = len(inventory_rows)
            for inventory, product in inventory_rows:
                if inventory.reserved < 0 or inventory.on_hand < 0:
                    issues.append(
                        {
                            "type": "inventory_negative",
                            "severity": "critical",
                            "detail": f"SKU {inventory.sku} 出现负数：on_hand={inventory.on_hand}, reserved={inventory.reserved}",
                        }
                    )
                elif inventory.reserved > inventory.on_hand:
                    issues.append(
                        {
                            "type": "inventory_over_reserved",
                            "severity": "critical",
                            "detail": f"SKU {inventory.sku} 预留超过在库：reserved={inventory.reserved} > on_hand={inventory.on_hand}",
                        }
                    )

            held_by_sku = repos.reserved_sum_by_sku(session)
            for inventory, _product in inventory_rows:
                held = held_by_sku.get(inventory.sku, 0)
                if held != inventory.reserved:
                    issues.append(
                        {
                            "type": "reservation_drift",
                            "severity": "critical",
                            "detail": f"SKU {inventory.sku} 活跃预留合计 {held} ≠ 库存预留列 {inventory.reserved}",
                        }
                    )

            orders, total = repos.list_orders(session, limit=1000)
            counters["orders_scanned"] = total
            for order in orders:
                if order.state in {s.value for s in TERMINAL_STATES}:
                    continue
                if order.deadline_at <= now:
                    issues.append(
                        {
                            "type": "order_past_deadline",
                            "severity": "warning",
                            "detail": f"订单 {order.code} 处于 {order.state} 且已超过履约时限 {iso(order.deadline_at)}",
                        }
                    )
                steps = repos.list_steps(session, order.id)
                for step in steps:
                    if step.status == StepStatus.RUNNING.value and (
                        order.saga_lease_until is None or order.saga_lease_until < now - timedelta(seconds=1)
                    ):
                        issues.append(
                            {
                                "type": "stuck_step",
                                "severity": "warning",
                                "detail": f"订单 {order.code} 的步骤 {step.step} 仍为 RUNNING 但租约已过期（疑似执行器崩溃）",
                            }
                        )
                if not steps:
                    issues.append(
                        {
                            "type": "missing_saga_steps",
                            "severity": "critical",
                            "detail": f"订单 {order.code} 缺少 Saga 步骤记录",
                        }
                    )

            backlog = repos.outbox_backlog(session)
            counters["outbox_pending"] = backlog.get("PENDING", 0)
            counters["outbox_dead"] = backlog.get("DEAD", 0)
            if counters["outbox_dead"]:
                issues.append(
                    {
                        "type": "outbox_dead_letter",
                        "severity": "critical",
                        "detail": f"{counters['outbox_dead']} 条 outbox 事件进入死信，需要人工排查消费者",
                    }
                )
            counters["feed_entries"] = repos.count_feed(session)
        report = {
            "checked_at": iso(now),
            "healthy": not issues,
            "issues": issues,
            "counters": counters,
            "repaired": repaired,
        }
        self.last_run = {"at": iso(now), "issues": len(issues), "repaired": repaired}
        if issues:
            log.warning("reconcile found issues", extra={"extra_fields": {"count": len(issues)}})
        return report

    # ------------------------------------------------------------------ 单轮

    def tick(self) -> dict[str, object]:
        expired = self.expire_reservations()
        retried = self.retry_due()
        return {"expired": expired, "retried": retried}

    def health(self) -> dict[str, object]:
        with self.db.session() as session:
            orders, _ = repos.list_orders(session, limit=1)
            backlog = repos.outbox_backlog(session)
        return {
            "last_run": self.last_run,
            "outbox": backlog,
            "database": "ok" if self.db.healthy() else "error",
            "orders_probe": len(orders),
        }
