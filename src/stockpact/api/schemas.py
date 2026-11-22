"""API 契约（Pydantic 模型）。

入参只接受"业务意图"（SKU + 数量），不接受价格——价格永远由服务端目录决定。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

CancelReasonIn = Literal[
    "CUSTOMER_REQUEST",
    "OUT_OF_STOCK",
    "PAYMENT_FAILED",
    "RESERVATION_EXPIRED",
    "DEADLINE_EXCEEDED",
    "MANUAL",
]


class OrderItemIn(BaseModel):
    sku: str = Field(min_length=1, max_length=64, examples=["SKU-BOOK-001"])
    quantity: int = Field(gt=0, le=1000, examples=[1])


class OrderCreateIn(BaseModel):
    customer_id: str = Field(min_length=1, max_length=80, examples=["cust-1024"])
    items: list[OrderItemIn] = Field(min_length=1, max_length=30)


class ProductIn(BaseModel):
    sku: str = Field(min_length=1, max_length=64)
    name: str = Field(min_length=1, max_length=160)
    price_cents: int = Field(ge=0, le=100_000_000)
    initial_stock: int = Field(default=0, ge=0, le=1_000_000)


class RestockIn(BaseModel):
    quantity: int = Field(gt=0, le=1_000_000)


class CancelIn(BaseModel):
    reason: CancelReasonIn = "CUSTOMER_REQUEST"


class ChaosIn(BaseModel):
    payment_failure_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    payment_latency_ms: int | None = Field(default=None, ge=0, le=5000)
    payment_stall: bool | None = None
