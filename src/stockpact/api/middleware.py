"""横切关注点：请求标识、访问日志、指标、限流。

顺序（外层 → 内层）：限流 → 计时/打点 → 业务。限流放在最外层，
被拒绝的请求不占用后续处理资源，也仍然会写入访问日志与 429 指标。
"""

from __future__ import annotations

import logging
import time
import uuid

from fastapi import Request
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from ..domain.order import RateLimited
from ..obs.logging import request_id_var
from ..obs.metrics import METRICS

log = logging.getLogger("stockpact.access")


class ObservabilityMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, container) -> None:
        super().__init__(app)
        self.container = container

    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-Id") or uuid.uuid4().hex[:16]
        token = request_id_var.set(request_id)
        request.state.request_id = request_id
        started = time.perf_counter()

        if request.url.path.startswith("/api") and self.container.settings.rate_limit_enabled:
            key = request.headers.get("X-API-Key") or (request.client.host if request.client else "anonymous")
            try:
                self.container.rate_limiter.check(key)
            except RateLimited as exc:
                METRICS.inc("stockpact_rate_limited_total")
                response = JSONResponse(
                    {"error": exc.code, "message": exc.message, "detail": exc.detail, "request_id": request_id},
                    status_code=429,
                    headers={"Retry-After": str(int(max(1, float(exc.detail.get("retry_after", 1))))), "X-Request-Id": request_id},
                )
                request_id_var.reset(token)
                return response

        try:
            response = await call_next(request)
        except Exception:
            log.exception("unhandled error", extra={"extra_fields": {"path": request.url.path, "method": request.method}})
            request_id_var.reset(token)
            raise

        route = request.scope.get("route")
        route_path = getattr(route, "path", request.url.path)
        elapsed = time.perf_counter() - started
        METRICS.inc(
            "stockpact_http_requests_total",
            method=request.method,
            route=route_path,
            status=str(response.status_code),
        )
        METRICS.observe("stockpact_http_request_duration_seconds", elapsed, route=route_path)
        response.headers["X-Request-Id"] = request_id
        log.info(
            "http",
            extra={
                "extra_fields": {
                    "method": request.method,
                    "path": request.url.path,
                    "status": response.status_code,
                    "duration_ms": round(elapsed * 1000, 2),
                }
            },
        )
        request_id_var.reset(token)
        return response
