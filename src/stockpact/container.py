"""组合根（composition root）：所有依赖在这里装配一次。

服务之间只通过构造函数拿依赖，没有全局单例、没有隐式导入副作用，
因此单元测试里可以用内存库 + 更小的 TTL 组装出一套独立的实例。
"""

from __future__ import annotations

import threading

from .config import Settings, load_settings
from .infra.cache import TTLCache
from .infra.db import Database
from .infra.ratelimit import RateLimiter
from .obs.metrics import METRICS, Metrics
from .services.catalog import CatalogService
from .services.chaos import ChaosService
from .services.inventory import InventoryService
from .services.orders import OrderService
from .services.outbox import OutboxRelay
from .services.payments import PaymentService
from .services.recovery import RecoveryService
from .services.saga import SagaOrchestrator


class Container:
    def __init__(self, settings: Settings | None = None, *, database_url: str | None = None) -> None:
        self.settings = settings or load_settings()
        self.db = Database(self.settings, url=database_url)
        self.metrics: Metrics = METRICS
        self.cache = TTLCache(ttl_seconds=self.settings.cache_ttl_seconds)

        self.chaos = ChaosService(self.db, self.settings)
        self.inventory = InventoryService(self.db, self.settings, self.cache)
        self.payments = PaymentService(self.db, self.settings, self.chaos)
        self.saga = SagaOrchestrator(self.db, self.settings, self.inventory, self.payments, self.cache)
        self.orders = OrderService(self.db, self.settings, self.saga, self.cache)
        self.catalog = CatalogService(self.db, self.inventory)
        self.outbox = OutboxRelay(self.db, self.settings)
        self.recovery = RecoveryService(self.db, self.settings, self.saga, self.outbox, self.inventory)
        self.rate_limiter = RateLimiter(self.settings.rate_limit_rps, self.settings.rate_limit_burst)

        self._stop = threading.Event()
        self._loops: list[threading.Thread] = []

    # ------------------------------------------------------------------ 生命周期

    def init_schema(self) -> None:
        self.db.create_schema()
        self.chaos.seed_defaults()

    def start_background(self) -> None:
        if not self.settings.background_enabled or self._loops:
            return
        self._spawn("outbox-relay", self.settings.outbox_poll_interval, self._relay_loop)
        self._spawn("recovery", self.settings.recovery_interval, self._recovery_loop)

    def _spawn(self, name: str, interval: float, target) -> None:
        def _run() -> None:
            while not self._stop.wait(interval):
                try:
                    target()
                except Exception:  # 后台循环不允许因为单次异常退出
                    import logging

                    logging.getLogger("stockpact.loop").exception("background loop failed: %s", name)

        thread = threading.Thread(target=_run, name=f"stockpact-{name}", daemon=True)
        thread.start()
        self._loops.append(thread)

    def _relay_loop(self) -> None:
        self.outbox.relay_once()

    def _recovery_loop(self) -> None:
        self.recovery.tick()

    def stop_background(self) -> None:
        self._stop.set()
        for thread in self._loops:
            thread.join(timeout=2)
        self._loops.clear()
        self._stop = threading.Event()

    def shutdown(self) -> None:
        self.stop_background()
        self.db.engine.dispose()
