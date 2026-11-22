"""应用入口。

单进程部署：uvicorn + 内置后台线程（outbox 中继 / 恢复扫描）。
多实例部署时把 STOCKPACT_BACKGROUND=0 交给独立 worker 进程，
API 实例保持无状态（除了进程内缓存，见 README 的取舍说明）。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from .api.middleware import ObservabilityMiddleware
from .api.routes import _serve_console, metrics_endpoint, router
from .config import Settings, load_settings
from .container import Container
from .domain.order import DomainError
from .obs.logging import configure_logging, request_id_var

log = logging.getLogger("stockpact")


def create_app(
    settings: Settings | None = None,
    *,
    database_url: str | None = None,
    start_background: bool | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    configure_logging(settings.log_level)
    container = Container(settings, database_url=database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        container.init_schema()
        should_start = settings.background_enabled if start_background is None else start_background
        if should_start:
            container.start_background()
        log.info(
            "stockpact started",
            extra={"extra_fields": {"database": container.db.url.split(":")[0], "background": should_start}},
        )
        try:
            yield
        finally:
            container.shutdown()
            log.info("stockpact stopped")

    app = FastAPI(
        title="StockPact",
        version="1.0.0",
        summary="订单履约与库存预留平台（Saga 编排 + 事务性 Outbox）",
        description=(
            "围绕「下单-预留-支付-出库」的履约链路，提供 Saga 编排、库存预留 TTL 回收、"
            "事务性 Outbox、幂等下单、限流、审计与对账能力。"
        ),
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )
    app.state.container = container
    app.add_middleware(ObservabilityMiddleware, container=container)

    # ---------------------------------------------------------------- 错误契约

    @app.exception_handler(DomainError)
    async def _domain_error(request: Request, exc: DomainError) -> JSONResponse:
        return JSONResponse(
            {
                "error": exc.code,
                "message": exc.message,
                "detail": exc.detail,
                "request_id": request_id_var.get(),
            },
            status_code=exc.http_status,
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            {
                "error": "validation_error",
                "message": "请求参数不合法",
                "detail": exc.errors(),
                "request_id": request_id_var.get(),
            },
            status_code=422,
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_error(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        return JSONResponse(
            {"error": "http_error", "message": str(exc.detail), "detail": {}, "request_id": request_id_var.get()},
            status_code=exc.status_code,
        )

    # ---------------------------------------------------------------- 路由

    app.include_router(router)
    app.add_api_route("/", _serve_console, include_in_schema=False)
    app.add_api_route("/metrics", metrics_endpoint, include_in_schema=False, methods=["GET"])

    @app.get("/healthz", tags=["meta"], summary="存活探针")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", tags=["meta"], summary="就绪探针")
    def readyz() -> dict:
        healthy = container.db.healthy()
        return JSONResponse(
            {
                "status": "ready" if healthy else "degraded",
                "database": "ok" if healthy else "unavailable",
                "background": bool(container._loops),
            },
            status_code=200 if healthy else 503,
        )

    return app


# 以工厂模式启动：uvicorn stockpact.main:create_app --factory
