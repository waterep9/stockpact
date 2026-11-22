"""支付服务 + 可控的模拟支付网关。

真网关的三个现实问题在这里都有对应设计：
1. **幂等**：`provider_ref` 由订单号确定性派生，重复提交不会重复扣款；
   `payments.order_id` 唯一约束把幂等性下沉到数据库，应用层写错也不会双扣。
2. **结果不确定**：超时并不等于失败。这里用 `payment_stall` 模拟"网关不响应"，
   流程按"未确认"处理并退避重试，而不是直接判定成功。
3. **状态机**：CAPTURED / DECLINED / REFUNDED 三态，退款只能从 CAPTURED 出发。
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass

from ..config import Settings
from ..domain.order import PaymentStatus
from ..infra import repos
from ..infra.db import Database
from ..obs.metrics import METRICS
from sqlalchemy.exc import IntegrityError

from .chaos import ChaosService

log = logging.getLogger("stockpact.payment")


@dataclass(slots=True)
class ChargeResult:
    status: PaymentStatus
    provider_ref: str
    attempts: int
    error: str | None = None

    @property
    def succeeded(self) -> bool:
        return self.status is PaymentStatus.CAPTURED


class PaymentGateway:
    """模拟第三方网关。确定性伪随机：同样的 (订单号, 尝试次数) 结果可复现。"""

    def __init__(self, chaos: ChaosService) -> None:
        self.chaos = chaos

    def charge(self, order_code: str, amount_cents: int, attempt: int) -> tuple[PaymentStatus, str | None]:
        config = self.chaos.snapshot()
        latency_ms = int(config["payment_latency_ms"])
        if latency_ms:
            time.sleep(latency_ms / 1000.0)
        if config["payment_stall"]:
            return PaymentStatus.DECLINED, "支付网关超时（注入故障：不返回结果）"
        rate = float(config["payment_failure_rate"])
        if rate > 0:
            digest = hashlib.sha256(f"{order_code}:{attempt}".encode()).digest()
            bucket = int.from_bytes(digest[:4], "big") / 2**32
            if bucket < rate:
                return PaymentStatus.DECLINED, f"支付被拒绝（注入故障：失败率 {rate:.0%}）"
        return PaymentStatus.CAPTURED, None


class PaymentService:
    def __init__(self, db: Database, settings: Settings, chaos: ChaosService) -> None:
        self.db = db
        self.settings = settings
        self.gateway = PaymentGateway(chaos)

    @staticmethod
    def provider_ref_for(order_code: str) -> str:
        return "sim_" + hashlib.sha1(order_code.encode()).hexdigest()[:16]

    def charge(self, order_id: int) -> ChargeResult:
        with self.db.session() as session:
            order = repos.get_order(session, order_id)
            if order is None:
                raise RuntimeError(f"订单 {order_id} 不存在")
            payment = repos.get_payment(session, order_id)
            previous_status = payment.status if payment else None
            attempt = (payment.attempts if payment else 0) + 1
            order_code = order.code
            amount = order.total_cents

        # 已扣款则直接幂等返回（重试/恢复场景下不会二次扣款）
        if previous_status == PaymentStatus.CAPTURED.value:
            return ChargeResult(PaymentStatus.CAPTURED, self.provider_ref_for(order_code), max(attempt - 1, 1))

        started = time.perf_counter()
        status, error = self.gateway.charge(order_code, amount, attempt)
        elapsed = time.perf_counter() - started
        METRICS.observe("stockpact_payment_duration_seconds", elapsed, outcome=status.value)

        def _persist(session):
            payment = repos.get_payment(session, order_id)
            if payment is None:
                repos.add_payment(
                    session,
                    order_id=order_id,
                    amount_cents=amount,
                    status=status,
                    provider_ref=self.provider_ref_for(order_code),
                )
                return
            if payment.status == PaymentStatus.CAPTURED.value:
                return
            repos.update_payment(session, payment.id, status=status, attempts=attempt)

        try:
            self.db.run_in_tx(_persist)
        except IntegrityError:
            # 并发下另一个执行器已经落库（order_id 唯一约束生效）：回读真实状态返回，
            # 保证"支付单唯一"这一数据库不变量不会被并发写穿。
            with self.db.session() as session:
                existing = repos.get_payment(session, order_id)
            if existing is not None:
                status = PaymentStatus(existing.status)
                attempt = existing.attempts
        log.info(
            "payment charge",
            extra={
                "extra_fields": {
                    "order": order_code,
                    "attempt": attempt,
                    "status": status.value,
                    "error": error,
                    "elapsed_ms": round(elapsed * 1000, 2),
                }
            },
        )
        return ChargeResult(status, self.provider_ref_for(order_code), attempt, error)

    def refund(self, order_id: int) -> bool:
        """退款。只有已扣款才能退；重复退款幂等返回 True。"""

        def _refund(session):
            payment = repos.get_payment(session, order_id)
            if payment is None:
                return False
            if payment.status == PaymentStatus.REFUNDED.value:
                return True
            if payment.status != PaymentStatus.CAPTURED.value:
                return False
            repos.update_payment(session, payment.id, status=PaymentStatus.REFUNDED, attempts=payment.attempts)
            return True

        return bool(self.db.run_in_tx(_refund))

    def snapshot(self, order_id: int) -> dict[str, object] | None:
        with self.db.session() as session:
            payment = repos.get_payment(session, order_id)
            if payment is None:
                return None
            return {
                "status": payment.status,
                "amount_cents": payment.amount_cents,
                "provider_ref": payment.provider_ref,
                "attempts": payment.attempts,
                "updated_at": payment.updated_at,
            }
