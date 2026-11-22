"""时间工具。

统一使用"朴素 UTC datetime"：SQLite 无时区类型，若混用 aware/naive 会在
比较时抛 TypeError，因此全链路约定 naive UTC，仅在最外层（API 序列化）加 Z。
"""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.replace(microsecond=0).isoformat() + "Z"
