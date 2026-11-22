"""混沌开关：把"故障注入"做成一等配置项。

故障不是靠改代码/重启注入的——那样演练结论没有说服力。这里把开关持久化到
`chaos_config`，运行期通过 API 调整，支付网关与恢复器实时读取，因此可以在
一次演练中完整观察：失败 → 重试 → 退避 → 补偿 → 库存回滚 的全链路。
"""

from __future__ import annotations

from typing import Any

from ..config import Settings
from ..domain.order import IllegalArgument
from ..infra.db import Database
from ..infra import repos

DEFAULTS: dict[str, Any] = {
    "payment_failure_rate": 0.0,
    "payment_latency_ms": 0,
    "payment_stall": False,
}

_BOOL_KEYS = {"payment_stall"}


class ChaosService:
    def __init__(self, db: Database, settings: Settings) -> None:
        self.db = db
        self.settings = settings

    def seed_defaults(self) -> None:
        """建表后调用：把配置项默认值落库，保证多进程读到的初始值一致。"""
        def _seed(session):
            existing = repos.get_chaos(session)
            if not existing:
                repos.set_chaos(session, "payment_failure_rate", str(self.settings.payment_failure_rate))
                repos.set_chaos(session, "payment_latency_ms", str(self.settings.payment_latency_ms))
                repos.set_chaos(session, "payment_stall", "true" if self.settings.payment_hang else "false")
            return None

        self.db.run_in_tx(_seed)

    def snapshot(self) -> dict[str, Any]:
        with self.db.session() as session:
            raw = repos.get_chaos(session)
        result: dict[str, Any] = {}
        for key, default in DEFAULTS.items():
            value = raw.get(key, default)
            if key in _BOOL_KEYS:
                result[key] = str(value).lower() in {"1", "true", "yes", "on"}
            elif isinstance(default, float):
                result[key] = float(value)
            else:
                result[key] = int(value)
        return result

    def update(self, patch: dict[str, Any]) -> dict[str, Any]:
        cleaned: dict[str, str] = {}
        for key, value in patch.items():
            if key not in DEFAULTS:
                raise IllegalArgument(f"不支持的混沌开关: {key}", allowed=sorted(DEFAULTS))
            if key == "payment_failure_rate":
                rate = float(value)
                if not 0.0 <= rate <= 1.0:
                    raise IllegalArgument("payment_failure_rate 必须在 [0,1] 区间")
                cleaned[key] = str(rate)
            elif key == "payment_latency_ms":
                latency = int(value)
                if not 0 <= latency <= 5000:
                    raise IllegalArgument("payment_latency_ms 必须在 [0,5000] 区间")
                cleaned[key] = str(latency)
            else:
                cleaned[key] = "true" if bool(value) else "false"

        if not cleaned:
            return self.snapshot()

        def _apply(session):
            for key, value in cleaned.items():
                repos.set_chaos(session, key, value)
            return None

        self.db.run_in_tx(_apply)
        return self.snapshot()
