"""HTTP 接口层。

分层意图：路由只做协议转换（鉴权、参数、状态码），不写业务判断；
所有业务规则都在 services 里，可被测试直接调用。
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from ..clock import iso, utcnow
from ..domain.order import CancelReason, DomainError, NotFound, Unauthorized
from ..infra import repos
from ..obs.metrics import METRICS
from ..services.orders import OrderService
from .schemas import CancelIn, ChaosIn, OrderCreateIn, ProductIn, RestockIn

router = APIRouter(prefix="/api")


def get_container(request: Request):
    return request.app.state.container


def require_admin(request: Request, x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> None:
    """管理端鉴权：配置了 STOCKPACT_API_KEY 就强制校验；未配置时视为本地开发模式。

    生产必须配置，否则任何能访问端口的人都能改库存/开关混沌。
    """
    expected = request.app.state.container.settings.api_key
    if not expected:
        return
    if x_api_key != expected:
        raise Unauthorized("缺少或错误的 X-API-Key")


# ---------------------------------------------------------------- 目录 / 库存


@router.get("/catalog/products", tags=["catalog"], summary="商品与库存快照")
def list_products(request: Request) -> dict:
    container = get_container(request)
    return {"items": container.catalog.list_products()}


@router.post("/catalog/products", tags=["catalog"], summary="新建商品（管理端）", status_code=201, dependencies=[Depends(require_admin)])
def create_product(payload: ProductIn, request: Request) -> dict:
    container = get_container(request)
    product = container.catalog.create_product(
        payload.sku, payload.name, payload.price_cents, payload.initial_stock
    )
    return product


@router.get("/inventory", tags=["inventory"], summary="库存列表")
def list_inventory(request: Request) -> dict:
    return {"items": get_container(request).inventory.list_snapshots()}


@router.get("/inventory/{sku}", tags=["inventory"], summary="单 SKU 库存（走缓存）")
def get_inventory(sku: str, request: Request) -> dict:
    return get_container(request).inventory.snapshot(sku)


@router.post(
    "/inventory/{sku}/restock",
    tags=["inventory"],
    summary="补货（管理端）",
    dependencies=[Depends(require_admin)],
)
def restock(sku: str, payload: RestockIn, request: Request) -> dict:
    return get_container(request).inventory.restock(sku, payload.quantity)


# ---------------------------------------------------------------- 订单


@router.post("/orders", tags=["orders"], summary="下单（支持 Idempotency-Key）", status_code=201)
def create_order(
    payload: OrderCreateIn,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
) -> JSONResponse:
    container = get_container(request)
    if idempotency_key and len(idempotency_key) > 120:
        raise DomainError("Idempotency-Key 过长", code="illegal_argument")
    service: OrderService = container.orders
    result = service.create(
        customer_id=payload.customer_id,
        items=[item.model_dump() for item in payload.items],
        idempotency_key=idempotency_key,
        request_id=getattr(request.state, "request_id", None),
        actor=payload.customer_id,
    )
    body = {
        "order_code": result.order_code,
        "state": result.state,
        "total_cents": result.total_cents,
        "replayed": result.replayed,
        "saga": result.saga,
    }
    status_code = 200 if result.replayed else 201
    return JSONResponse(
        body,
        status_code=status_code,
        headers={
            "Location": f"/api/orders/{result.order_code}",
            "X-Idempotent-Replay": "true" if result.replayed else "false",
        },
    )


@router.get("/orders", tags=["orders"], summary="订单列表")
def list_orders(
    request: Request,
    state: str | None = Query(default=None, description="按状态过滤"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict:
    return get_container(request).orders.list(state=state, limit=limit, offset=offset)


@router.get("/orders/{order_code}", tags=["orders"], summary="订单详情（含 Saga 时间线）")
def order_detail(order_code: str, request: Request) -> dict:
    return get_container(request).orders.detail(order_code)


@router.post("/orders/{order_code}/cancel", tags=["orders"], summary="取消订单（触发补偿）")
def cancel_order(order_code: str, payload: CancelIn, request: Request) -> dict:
    container = get_container(request)
    actor = request.headers.get("X-Actor", "operator")
    return container.orders.cancel(order_code, reason=CancelReason(payload.reason), actor=actor)


@router.post("/orders/{order_code}/advance", tags=["orders"], summary="手动推进一次 Saga（演练/补偿）")
def advance_order(order_code: str, request: Request) -> dict:
    return get_container(request).saga.advance(order_code).as_dict()


@router.post("/orders/{order_code}/retry", tags=["orders"], summary="立即重试待重试步骤")
def retry_order(order_code: str, request: Request) -> dict:
    return get_container(request).saga.retry_now(order_code).as_dict()


# ---------------------------------------------------------------- 运维


@router.get("/ops/chaos", tags=["ops"], summary="查看混沌开关")
def get_chaos(request: Request) -> dict:
    return get_container(request).chaos.snapshot()


@router.put("/ops/chaos", tags=["ops"], summary="修改混沌开关", dependencies=[Depends(require_admin)])
def update_chaos(payload: ChaosIn, request: Request) -> dict:
    patch = {k: v for k, v in payload.model_dump().items() if v is not None}
    return get_container(request).chaos.update(patch)


@router.get("/ops/outbox", tags=["ops"], summary="Outbox 事件列表")
def list_outbox(
    request: Request,
    status: str | None = Query(default=None, description="PENDING / PUBLISHED / DEAD"),
    limit: int = Query(default=30, ge=1, le=200),
) -> dict:
    container = get_container(request)
    with container.db.session() as session:
        rows = repos.list_outbox(session, status=status, limit=limit)
        backlog = repos.outbox_backlog(session)
        items = [
            {
                "id": row.id,
                "event_type": row.event_type,
                "order_code": row.order_code,
                "status": row.status,
                "attempts": row.attempts,
                "last_error": row.last_error,
                "created_at": iso(row.created_at),
                "published_at": iso(row.published_at),
            }
            for row in rows
        ]
    return {"items": items, "backlog": backlog}


@router.get("/ops/feed", tags=["ops"], summary="读模型事件流（由 outbox 消费者维护）")
def list_feed(request: Request, limit: int = Query(default=40, ge=1, le=200)) -> dict:
    container = get_container(request)
    with container.db.session() as session:
        feed = repos.list_feed(session, limit=limit)
        notifications = repos.list_notifications(session, limit=15)
        ledger = repos.list_ledger(session, limit=30)
        items = [
            {"order_code": f.order_code, "event_type": f.event_type, "summary": f.summary, "at": iso(f.created_at)}
            for f in feed
        ]
        notes = [{"order_code": n.order_code, "message": n.message, "at": iso(n.created_at)} for n in notifications]
        ledger_items = [
            {"sku": l.sku, "movement": l.movement, "quantity": l.quantity, "order_code": l.order_code, "at": iso(l.created_at)}
            for l in ledger
        ]
    return {"feed": items, "notifications": notes, "ledger": ledger_items}


@router.get("/ops/audit", tags=["ops"], summary="审计日志")
def list_audit(request: Request, limit: int = Query(default=50, ge=1, le=200)) -> dict:
    container = get_container(request)
    with container.db.session() as session:
        rows = repos.list_audit(session, limit=limit)
        items = [
            {
                "actor": r.actor,
                "action": r.action,
                "target": f"{r.target_type}:{r.target_id}",
                "detail": json.loads(r.detail_json or "{}"),
                "request_id": r.request_id,
                "at": iso(r.created_at),
            }
            for r in rows
        ]
    return {"items": items}


@router.get("/ops/sagas", tags=["ops"], summary="Saga 运行态概览")
def saga_overview(request: Request) -> dict:
    container = get_container(request)
    with container.db.session() as session:
        counts = repos.order_state_counts(session)
        stuck = repos.orders_with_due_steps(session, limit=20)
        pending = repos.outbox_backlog(session).get("PENDING", 0)
        stuck_items = [
            {"order_code": o.code, "state": o.state, "updated_at": iso(o.updated_at), "deadline_at": iso(o.deadline_at)}
            for o in stuck
        ]
    return {
        "states": counts,
        "in_flight": sum(counts.get(s, 0) for s in ("CREATED", "STOCK_RESERVED", "PAID")),
        "outbox_pending": pending,
        "due_orders": stuck_items,
        "reservation_ttl_seconds": container.settings.reservation_ttl_seconds,
        "chaos": container.chaos.snapshot(),
    }


@router.post(
    "/ops/recovery/tick",
    tags=["ops"],
    summary="手动触发一轮恢复扫描",
    dependencies=[Depends(require_admin)],
)
def recovery_tick(request: Request) -> dict:
    container = get_container(request)
    result = container.recovery.tick()
    result["outbox"] = container.outbox.relay_once()
    return result


@router.get("/ops/reconcile", tags=["ops"], summary="对账（只读校验不变量）")
def reconcile(request: Request, repair: bool = Query(default=False, description="是否先自愈再对账")) -> dict:
    return get_container(request).recovery.reconcile(repair=repair)


@router.post(
    "/ops/outbox/relay",
    tags=["ops"],
    summary="手动触发一次 outbox 投递",
    dependencies=[Depends(require_admin)],
)
def relay_outbox(request: Request) -> dict:
    return get_container(request).outbox.relay_once()


# ---------------------------------------------------------------- 指标 / 元信息


@router.get("/meta", tags=["meta"], summary="构建与配置元信息")
def meta(request: Request) -> dict:
    container = get_container(request)
    settings = container.settings
    return {
        "service": "stockpact",
        "version": "1.0.0",
        "now": iso(utcnow()),
        "reservation_ttl_seconds": settings.reservation_ttl_seconds,
        "order_deadline_seconds": settings.order_deadline_seconds,
        "payment_max_attempts": settings.payment_max_attempts,
        "outbox_max_attempts": settings.outbox_max_attempts,
        "cache_ttl_seconds": settings.cache_ttl_seconds,
        "rate_limit": {"rps": settings.rate_limit_rps, "burst": settings.rate_limit_burst, "enabled": settings.rate_limit_enabled},
        "admin_auth_required": bool(settings.api_key),
    }


def metrics_endpoint(request: Request) -> PlainTextResponse:
    container = get_container(request)
    with container.db.session() as session:
        counts = repos.order_state_counts(session)
        backlog = repos.outbox_backlog(session)
    METRICS.set_gauge("stockpact_outbox_pending", backlog.get("PENDING", 0))
    METRICS.set_gauge("stockpact_outbox_dead", backlog.get("DEAD", 0))
    METRICS.set_gauge("stockpact_orders_in_flight", sum(counts.get(s, 0) for s in ("CREATED", "STOCK_RESERVED", "PAID")))
    cache_stats = container.cache.stats()
    METRICS.set_gauge("stockpact_cache_hit_ratio", float(cache_stats["hit_ratio"]))
    limiter = container.rate_limiter.stats()
    METRICS.set_gauge("stockpact_rate_limit_rejected_total", limiter["rejected"])
    return PlainTextResponse(METRICS.render(), media_type="text/plain; version=0.0.4; charset=utf-8")


def _serve_console(request: Request):
    from fastapi.responses import FileResponse

    path = Path(__file__).resolve().parent.parent / "web" / "console.html"
    if not path.exists():  # pragma: no cover
        raise NotFound("控制台静态文件缺失")
    return FileResponse(path, media_type="text/html; charset=utf-8")
