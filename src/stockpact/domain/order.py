"""订单履约领域模型。

刻意不依赖 ORM / FastAPI：状态机与不变量是业务的核心资产，应当可以在
纯内存里被单元测试覆盖，也便于未来换存储实现。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class OrderState(str, Enum):
    CREATED = "CREATED"
    STOCK_RESERVED = "STOCK_RESERVED"
    PAID = "PAID"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = frozenset({OrderState.CONFIRMED, OrderState.CANCELLED})

# 状态迁移白名单：只有这里列出的迁移能被 apply_transition 接受。
# PAID -> CANCELLED 保留，用于"已支付但履约失败"的退款路径。
ALLOWED_TRANSITIONS: dict[OrderState, frozenset[OrderState]] = {
    OrderState.CREATED: frozenset({OrderState.STOCK_RESERVED, OrderState.CANCELLED}),
    OrderState.STOCK_RESERVED: frozenset({OrderState.PAID, OrderState.CANCELLED}),
    OrderState.PAID: frozenset({OrderState.CONFIRMED, OrderState.CANCELLED}),
    OrderState.CONFIRMED: frozenset(),
    OrderState.CANCELLED: frozenset(),
}


class SagaStepName(str, Enum):
    RESERVE_STOCK = "RESERVE_STOCK"
    CHARGE_PAYMENT = "CHARGE_PAYMENT"
    CONFIRM_ORDER = "CONFIRM_ORDER"
    RELEASE_STOCK = "RELEASE_STOCK"

    @property
    def is_compensation(self) -> bool:
        return self is SagaStepName.RELEASE_STOCK


# 正向执行的步骤顺序；补偿步骤不在此列，由 compensate() 单独驱动。
FORWARD_STEPS: tuple[SagaStepName, ...] = (
    SagaStepName.RESERVE_STOCK,
    SagaStepName.CHARGE_PAYMENT,
    SagaStepName.CONFIRM_ORDER,
)


class StepStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    COMPENSATED = "COMPENSATED"
    SKIPPED = "SKIPPED"


class ReservationStatus(str, Enum):
    HELD = "HELD"
    COMMITTED = "COMMITTED"
    RELEASED = "RELEASED"


class PaymentStatus(str, Enum):
    CAPTURED = "CAPTURED"
    DECLINED = "DECLINED"
    REFUNDED = "REFUNDED"


class EventType(str, Enum):
    ORDER_CREATED = "ORDER_CREATED"
    STOCK_RESERVED = "STOCK_RESERVED"
    STOCK_RELEASED = "STOCK_RELEASED"
    PAYMENT_CAPTURED = "PAYMENT_CAPTURED"
    PAYMENT_DECLINED = "PAYMENT_DECLINED"
    PAYMENT_REFUNDED = "PAYMENT_REFUNDED"
    ORDER_CONFIRMED = "ORDER_CONFIRMED"
    ORDER_CANCELLED = "ORDER_CANCELLED"
    SAGA_RETRY_SCHEDULED = "SAGA_RETRY_SCHEDULED"
    RESERVATION_EXPIRED = "RESERVATION_EXPIRED"


class CancelReason(str, Enum):
    CUSTOMER_REQUEST = "CUSTOMER_REQUEST"
    OUT_OF_STOCK = "OUT_OF_STOCK"
    PAYMENT_FAILED = "PAYMENT_FAILED"
    RESERVATION_EXPIRED = "RESERVATION_EXPIRED"
    DEADLINE_EXCEEDED = "DEADLINE_EXCEEDED"
    MANUAL = "MANUAL"


class DomainError(Exception):
    """领域错误基类，携带可直接映射为 HTTP 状态码的 code。"""

    code = "domain_error"
    http_status = 400

    def __init__(self, message: str, **detail: object) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail


class InsufficientStock(DomainError):
    code = "insufficient_stock"
    http_status = 409


class InvalidTransition(DomainError):
    code = "invalid_transition"
    http_status = 409


class IllegalArgument(DomainError):
    code = "illegal_argument"
    http_status = 422


class NotFound(DomainError):
    code = "not_found"
    http_status = 404


class IdempotencyConflict(DomainError):
    code = "idempotency_key_reused"
    http_status = 409


class RateLimited(DomainError):
    code = "rate_limited"
    http_status = 429


class Unauthorized(DomainError):
    code = "unauthorized"
    http_status = 401


@dataclass(slots=True)
class OrderLine:
    sku: str
    quantity: int
    unit_price_cents: int

    @property
    def subtotal_cents(self) -> int:
        return self.quantity * self.unit_price_cents


@dataclass(slots=True)
class Order:
    """订单聚合根的内存视图。持久化细节在 infra.repos。"""

    code: str
    customer_id: str
    state: OrderState
    lines: list[OrderLine] = field(default_factory=list)
    cancel_reason: str | None = None

    @property
    def total_cents(self) -> int:
        return sum(line.subtotal_cents for line in self.lines)

    @property
    def is_terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def assert_transition(self, target: OrderState) -> None:
        if target not in ALLOWED_TRANSITIONS[self.state]:
            raise InvalidTransition(
                f"订单 {self.code} 不允许从 {self.state.value} 迁移到 {target.value}",
                code=self.code,
                current=self.state.value,
                target=target.value,
            )


def normalize_lines(raw: list[tuple[str, int, int]]) -> list[OrderLine]:
    """合并同 SKU 行并校验数量，避免同一 SKU 拆行导致重复扣减。"""
    merged: dict[str, OrderLine] = {}
    for sku, qty, price in raw:
        sku = sku.strip()
        if not sku:
            raise IllegalArgument("SKU 不能为空")
        if qty <= 0:
            raise IllegalArgument(f"SKU {sku} 的数量必须为正整数", sku=sku, quantity=qty)
        if price < 0:
            raise IllegalArgument(f"SKU {sku} 的单价不能为负", sku=sku)
        if sku in merged:
            merged[sku].quantity += qty
        else:
            merged[sku] = OrderLine(sku=sku, quantity=qty, unit_price_cents=price)
    if not merged:
        raise IllegalArgument("订单至少需要一个商品行")
    return list(merged.values())
