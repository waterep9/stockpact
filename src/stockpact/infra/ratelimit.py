"""应用级限流：令牌桶 + 按调用方分桶。

放在 ASGI 中间件层而不是网关层，是为了让"单机自保护"独立生效（网关也可能被打穿）。
真实多实例部署应把桶换成 Redis+Lua 或交给网关/服务网格；此处保留同一套接口语义
（429 + Retry-After），迁移时不改变调用方契约。
"""

from __future__ import annotations

import threading
import time

from ..domain.order import RateLimited


class TokenBucket:
    __slots__ = ("capacity", "rate", "tokens", "updated_at", "last_seen")

    def __init__(self, capacity: float, rate: float, now: float) -> None:
        self.capacity = capacity
        self.rate = rate
        self.tokens = capacity
        self.updated_at = now
        self.last_seen = now

    def take(self, now: float) -> tuple[bool, float]:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
        self.updated_at = now
        self.last_seen = now
        if self.tokens >= 1.0:
            self.tokens -= 1.0
            return True, 0.0
        deficit = 1.0 - self.tokens
        return False, deficit / self.rate if self.rate > 0 else 1.0


class RateLimiter:
    def __init__(self, rate_per_second: float, burst: float) -> None:
        self.rate = rate_per_second
        self.burst = burst
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        self.allowed = 0
        self.rejected = 0

    def check(self, key: str) -> None:
        """不通过则抛 RateLimited，由异常处理器转成 429。"""
        now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                bucket = TokenBucket(self.burst, self.rate, now)
                self._buckets[key] = bucket
            ok, retry_after = bucket.take(now)
            if ok:
                self.allowed += 1
                return
            self.rejected += 1
        raise RateLimited("请求过于频繁，请稍后重试", retry_after=round(retry_after, 3), scope=key)

    def sweep(self, idle_seconds: float = 300.0) -> int:
        now = time.monotonic()
        with self._lock:
            stale = [k for k, b in self._buckets.items() if now - b.last_seen > idle_seconds]
            for key in stale:
                self._buckets.pop(key, None)
            return len(stale)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"buckets": len(self._buckets), "allowed": self.allowed, "rejected": self.rejected}
