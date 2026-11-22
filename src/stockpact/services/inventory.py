"""库存服务。

核心不变量：
- `0 <= reserved <= on_hand`（数据库条件更新保证，应用层仅作补充校验）
- 一次下单的全部 SKU 要么全预留成功，要么一行都不留（同一事务内，失败即回滚）

这正是把"条件更新"放在仓储层、把"整单原子性"放在事务边界的原因：
前者消灭超卖，后者消灭半成品预留。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Sequence

from sqlalchemy.orm import Session

from ..clock import iso
from ..config import Settings
from ..domain.order import DomainError, IllegalArgument, InsufficientStock, NotFound, ReservationStatus
from ..infra import repos
from ..infra.cache import MISS, TTLCache
from ..infra.db import Database
from ..infra.schema import Inventory, Order, OrderItem, Reservation
from ..obs.metrics import METRICS

log = logging.getLogger("stockpact.inventory")
INVENTORY_CACHE_PREFIX = "inv:"


class InventoryService:
    def __init__(self, db: Database, settings: Settings, cache: TTLCache) -> None:
        self.db = db
        self.settings = settings
        self.cache = cache

    # ------------------------------------------------------------ 写路径（调用方事务）

    def reserve_in_tx(self, session: Session, order: Order, items: Sequence[OrderItem], expires_at: datetime) -> None:
        for item in items:
            reserved = repos.try_reserve(session, item.sku, item.quantity)
            if not reserved:
                inventory = repos.get_inventory(session, item.sku)
                available = (inventory.on_hand - inventory.reserved) if inventory else 0
                METRICS.inc("stockpact_inventory_reserve_conflicts_total", sku=item.sku)
                raise InsufficientStock(
                    f"SKU {item.sku} 可售不足：需要 {item.quantity}，剩余 {available}",
                    sku=item.sku,
                    requested=item.quantity,
                    available=available,
                )
            repos.add_reservation(session, order.id, item.sku, item.quantity, expires_at)
        session.flush()

    def release_in_tx(self, session: Session, order: Order) -> int:
        """释放该订单仍处于 HELD 的预留。返回释放的 SKU 数。"""
        reservations = [r for r in repos.list_reservations(session, order.id)]
        released = 0
        for reservation in reservations:
            if reservation.status != "HELD":
                continue
            if repos.release_reserved(session, reservation.sku, reservation.quantity):
                released += 1
            else:  # pragma: no cover - 说明数据已被破坏，计入对账
                log.error(
                    "release failed",
                    extra={"extra_fields": {"order": order.code, "sku": reservation.sku}},
                )
        repos.set_reservations_status(
            session,
            order.id,
            from_status=ReservationStatus.HELD,
            to_status=ReservationStatus.RELEASED,
        )
        session.flush()
        return released

    def commit_in_tx(self, session: Session, order: Order) -> None:
        """确认出库：HELD -> COMMITTED，同时扣减在库量。"""
        reservations: list[Reservation] = repos.list_reservations(session, order.id)
        for reservation in reservations:
            if reservation.status != "HELD":
                continue
            if not repos.ship_committed(session, reservation.sku, reservation.quantity):
                raise DomainError(
                    f"SKU {reservation.sku} 出库失败：库存不变量被破坏",
                    sku=reservation.sku,
                    order=order.code,
                )
        repos.set_reservations_status(
            session,
            order.id,
            from_status=ReservationStatus.HELD,
            to_status=ReservationStatus.COMMITTED,
        )
        session.flush()

    # ------------------------------------------------------------ 读路径

    def invalidate(self, sku: str | None = None) -> None:
        if sku is None:
            self.cache.invalidate_prefix(INVENTORY_CACHE_PREFIX)
        else:
            self.cache.invalidate(f"{INVENTORY_CACHE_PREFIX}{sku}")
            self.cache.invalidate(f"{INVENTORY_CACHE_PREFIX}list")

    def snapshot(self, sku: str) -> dict[str, object]:
        cache_key = f"{INVENTORY_CACHE_PREFIX}{sku}"
        cached = self.cache.get(cache_key)
        if cached is not None and cached is not MISS:
            return cached  # type: ignore[return-value]

        with self.db.session() as session:
            inventory = repos.get_inventory(session, sku)
            product = repos.get_product(session, sku)
            if inventory is None or product is None:
                raise NotFound(f"SKU {sku} 不存在", sku=sku)
            data = _inventory_view(inventory, product)
        self.cache.set(cache_key, data, ttl=self.settings.cache_ttl_seconds)
        return data

    def list_snapshots(self) -> list[dict[str, object]]:
        cache_key = f"{INVENTORY_CACHE_PREFIX}list"
        cached = self.cache.get(cache_key)
        if cached is not None and cached is not MISS:
            return cached  # type: ignore[return-value]
        with self.db.session() as session:
            data = [_inventory_view(inv, product) for inv, product in repos.list_inventory(session)]
        self.cache.set(cache_key, data, ttl=self.settings.cache_ttl_seconds)
        return data

    def restock(self, sku: str, quantity: int) -> dict[str, object]:
        if quantity <= 0:
            raise IllegalArgument("补货数量必须为正整数")

        def _restock(session):
            if repos.get_product(session, sku) is None:
                raise NotFound(f"SKU {sku} 不存在", sku=sku)
            repos.restock(session, sku, quantity)
            return None

        self.db.run_in_tx(_restock)
        self.invalidate(sku)
        return self.snapshot(sku)


def _inventory_view(inventory: Inventory, product) -> dict[str, object]:
    return {
        "sku": inventory.sku,
        "name": product.name,
        "price_cents": product.price_cents,
        "currency": product.currency,
        "on_hand": inventory.on_hand,
        "reserved": inventory.reserved,
        "available": inventory.on_hand - inventory.reserved,
        "version": inventory.version,
        "updated_at": iso(inventory.updated_at),
    }
