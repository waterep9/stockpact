"""商品目录服务：商品建档 + 期初库存。"""

from __future__ import annotations

from ..domain.order import IllegalArgument, NotFound
from ..infra import repos
from ..infra.db import Database
from .inventory import InventoryService


class CatalogService:
    def __init__(self, db: Database, inventory: InventoryService) -> None:
        self.db = db
        self.inventory = inventory

    def create_product(self, sku: str, name: str, price_cents: int, initial_stock: int = 0) -> dict[str, object]:
        sku = (sku or "").strip().upper()
        if not sku:
            raise IllegalArgument("sku 不能为空")
        if price_cents < 0:
            raise IllegalArgument("price_cents 不能为负")
        if initial_stock < 0:
            raise IllegalArgument("initial_stock 不能为负")

        def _tx(session):
            if repos.get_product(session, sku) is not None:
                raise IllegalArgument(f"SKU {sku} 已存在", sku=sku)
            repos.create_product(session, sku=sku, name=name or sku, price_cents=price_cents)
            if initial_stock:
                repos.restock(session, sku, initial_stock)
            return None

        self.db.run_in_tx(_tx)
        self.inventory.invalidate()
        return self.inventory.snapshot(sku)

    def list_products(self) -> list[dict[str, object]]:
        return self.inventory.list_snapshots()

    def get(self, sku: str) -> dict[str, object]:
        try:
            return self.inventory.snapshot(sku)
        except Exception as exc:  # 统一成 NotFound
            if getattr(exc, "code", "") == "not_found":
                raise
            raise NotFound(f"SKU {sku} 不存在", sku=sku) from exc
