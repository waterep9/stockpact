"""读缓存：进程内 TTL 缓存 + 主动失效。

为什么不是 Redis：本项目单进程部署，库存读路径的 QPS 有限，进程内缓存即可
把"读多写少"的目录/库存查询挡在数据库之前；接口层已经用 CacheBackend 抽象，
换成 Redis 只需实现同样的 get/set/invalidate。代价是失效只覆盖本进程——
多实例部署时必须换成共享缓存或"版本号 + 短 TTL"的方案，这一点写在 README 的取舍里。
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable

MISS = object()


class TTLCache:
    def __init__(self, ttl_seconds: float = 2.0) -> None:
        self.ttl = ttl_seconds
        self._data: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.invalidations = 0

    def get(self, key: str) -> Any:
        now = time.monotonic()
        with self._lock:
            entry = self._data.get(key)
            if entry is None or entry[0] <= now:
                if entry is not None:
                    self._data.pop(key, None)
                self.misses += 1
                return MISS
            self.hits += 1
            return entry[1]

    def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        with self._lock:
            self._data[key] = (time.monotonic() + (ttl if ttl is not None else self.ttl), value)

    def invalidate(self, key: str) -> None:
        with self._lock:
            if self._data.pop(key, None) is not None:
                self.invalidations += 1

    def invalidate_prefix(self, prefix: str) -> None:
        with self._lock:
            for key in [k for k in self._data if k.startswith(prefix)]:
                self._data.pop(key, None)
                self.invalidations += 1

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def get_or_load(self, key: str, loader: Callable[[], Any], ttl: float | None = None) -> Any:
        hit = self.get(key)
        if hit is not MISS:
            return hit
        value = loader()
        if value is not None:
            self.set(key, value, ttl)
        return value

    def stats(self) -> dict[str, float | int]:
        with self._lock:
            total = self.hits + self.misses
            return {
                "entries": len(self._data),
                "hits": self.hits,
                "misses": self.misses,
                "invalidations": self.invalidations,
                "hit_ratio": round(self.hits / total, 4) if total else 0.0,
            }
