"""运行期配置。

所有配置走环境变量，默认值面向"本地一条命令跑起来"；生产按注释替换即可。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent.parent


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return float(raw)


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class Settings:
    """进程级配置。"""

    # 数据目录与库地址（SQLite 便于单机复现；生产替换为 postgresql+psycopg://...）
    data_dir: Path = field(default_factory=lambda: Path(os.getenv("STOCKPACT_DATA_DIR", str(PROJECT_ROOT / "var"))))
    database_url: str = field(default_factory=lambda: os.getenv("STOCKPACT_DATABASE_URL", ""))

    # 库存预留在下单后多久过期（秒）；过期由回收器补偿释放
    reservation_ttl_seconds: int = field(default_factory=lambda: _int("STOCKPACT_RESERVATION_TTL", 90))

    # 订单从创建到终态的容忍上限，超时进入补偿
    order_deadline_seconds: int = field(default_factory=lambda: _int("STOCKPACT_ORDER_DEADLINE", 600))

    # 支付步骤最大尝试次数，超过即补偿（释放库存）
    payment_max_attempts: int = field(default_factory=lambda: _int("STOCKPACT_PAYMENT_MAX_ATTEMPTS", 3))
    payment_backoff_base_seconds: float = field(default_factory=lambda: _float("STOCKPACT_PAYMENT_BACKOFF", 0.4))

    # 后台循环：outbox 中继 / 恢复扫描 / 对账
    background_enabled: bool = field(default_factory=lambda: _bool("STOCKPACT_BACKGROUND", True))
    outbox_poll_interval: float = field(default_factory=lambda: _float("STOCKPACT_OUTBOX_INTERVAL", 0.3))
    outbox_batch_size: int = field(default_factory=lambda: _int("STOCKPACT_OUTBOX_BATCH", 50))
    outbox_max_attempts: int = field(default_factory=lambda: _int("STOCKPACT_OUTBOX_MAX_ATTEMPTS", 5))
    recovery_interval: float = field(default_factory=lambda: _float("STOCKPACT_RECOVERY_INTERVAL", 1.0))
    recovery_batch_size: int = field(default_factory=lambda: _int("STOCKPACT_RECOVERY_BATCH", 20))

    # 限流（令牌桶）：每秒补充速率与桶容量，按 API Key / 客户端 IP 分桶
    rate_limit_rps: float = field(default_factory=lambda: _float("STOCKPACT_RATE_LIMIT_RPS", 60.0))
    rate_limit_burst: float = field(default_factory=lambda: _float("STOCKPACT_RATE_LIMIT_BURST", 120.0))
    rate_limit_enabled: bool = field(default_factory=lambda: _bool("STOCKPACT_RATE_LIMIT", True))

    # 读缓存 TTL（秒）
    cache_ttl_seconds: float = field(default_factory=lambda: _float("STOCKPACT_CACHE_TTL", 2.0))

    # Saga 步骤租约（秒），防止同一订单被多个执行器并发推进
    saga_lease_seconds: float = field(default_factory=lambda: _float("STOCKPACT_SAGA_LEASE", 5.0))

    # 模拟支付网关的默认混沌参数（可运行期通过 /api/ops/chaos 调整）
    payment_failure_rate: float = field(default_factory=lambda: _float("STOCKPACT_PAYMENT_FAILURE_RATE", 0.0))
    payment_latency_ms: int = field(default_factory=lambda: _int("STOCKPACT_PAYMENT_LATENCY_MS", 0))
    payment_hang: bool = field(default_factory=lambda: _bool("STOCKPACT_PAYMENT_HANG", False))

    api_key: str = field(default_factory=lambda: os.getenv("STOCKPACT_API_KEY", ""))
    log_level: str = field(default_factory=lambda: os.getenv("STOCKPACT_LOG_LEVEL", "INFO"))

    def resolve_database_url(self) -> str:
        if self.database_url:
            return self.database_url
        self.data_dir.mkdir(parents=True, exist_ok=True)
        db_path = (self.data_dir / "stockpact.db").as_posix()
        return f"sqlite+pysqlite:///{db_path}"

    @property
    def is_sqlite(self) -> bool:
        return self.resolve_database_url().startswith("sqlite")


def load_settings() -> Settings:
    return Settings()
