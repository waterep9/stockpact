"""故障演练：在真实运行的服务上验证补偿链路。

四个场景（每个都检查"最终状态 + 库存账实 + 支付状态"三件事）：
  A. 正常履约        → CONFIRMED / CAPTURED / 库存减少
  B. 支付持续失败     → CANCELLED(PAYMENT_FAILED) / DECLINED / 库存回滚
  C. 支付网关不响应   → 判为"未确认"，重试到上限后补偿
  D. 库存被抢空       → 部分订单 CANCELLED(OUT_OF_STOCK)，库存不为负

用法：
    python scripts/chaos_drill.py --base-url http://127.0.0.1:8080
"""

from __future__ import annotations

import argparse
import time

import httpx

PASSED: list[str] = []
FAILED: list[str] = []


def check(name: str, condition: bool, detail: str) -> None:
    (PASSED if condition else FAILED).append(name)
    print(f"  [{'PASS' if condition else 'FAIL'}] {name} — {detail}")


def set_chaos(client: httpx.Client, **kwargs) -> None:
    response = client.put("/api/ops/chaos", json=kwargs)
    response.raise_for_status()


def create_order(client: httpx.Client, sku: str, qty: int, customer: str) -> str:
    response = client.post(
        "/api/orders",
        json={"customer_id": customer, "items": [{"sku": sku, "quantity": qty}]},
    )
    response.raise_for_status()
    return response.json()["order_code"]


def wait_for(client: httpx.Client, code: str, targets: set[str], timeout: float = 40.0) -> dict:
    deadline = time.time() + timeout
    detail = {}
    while time.time() < deadline:
        detail = client.get(f"/api/orders/{code}").json()
        if detail["state"] in targets:
            return detail
        time.sleep(0.4)
    return detail


def inventory(client: httpx.Client, sku: str) -> dict:
    return client.get(f"/api/inventory/{sku}").json()


def main() -> int:
    parser = argparse.ArgumentParser(description="StockPact 故障演练")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--sku", default="SKU-BOOK-001")
    parser.add_argument("--api-key", default="")
    args = parser.parse_args()

    headers = {"X-API-Key": args.api_key} if args.api_key else {}
    with httpx.Client(base_url=args.base_url, timeout=40, headers=headers) as client:
        set_chaos(client, payment_failure_rate=0.0, payment_stall=False, payment_latency_ms=0)
        start = inventory(client, args.sku)
        print(f"基线：SKU={args.sku} 在库={start['on_hand']} 预留={start['reserved']} 可售={start['available']}\n")

        # ---------------- A 正常履约 ----------------
        print("场景 A：正常履约")
        code = create_order(client, args.sku, 1, "drill-A")
        detail = wait_for(client, code, {"CONFIRMED", "CANCELLED"})
        inv = inventory(client, args.sku)
        check("A1 订单最终确认", detail["state"] == "CONFIRMED", f"{code} → {detail['state']}")
        check(
            "A2 支付已扣款",
            bool(detail["payment"]) and detail["payment"]["status"] == "CAPTURED",
            f"payment={detail['payment'] and detail['payment']['status']}",
        )
        check(
            "A3 库存已出库",
            inv["on_hand"] == start["on_hand"] - 1 and inv["reserved"] == start["reserved"],
            f"在库 {start['on_hand']} → {inv['on_hand']}，预留={inv['reserved']}",
        )
        print(f"  时间线：{[e['event_type'] for e in detail['timeline']]}\n")

        # ---------------- B 支付持续失败 ----------------
        print("场景 B：支付持续失败（100% 拒绝）")
        set_chaos(client, payment_failure_rate=1.0)
        before = inventory(client, args.sku)
        code = create_order(client, args.sku, 2, "drill-B")
        detail = wait_for(client, code, {"CANCELLED"})
        after = inventory(client, args.sku)
        charge = {s["step"]: s for s in detail["saga_steps"]}.get("CHARGE_PAYMENT", {})
        check(
            "B1 重试耗尽后取消",
            detail["state"] == "CANCELLED" and detail.get("cancel_reason") == "PAYMENT_FAILED",
            f"state={detail['state']} reason={detail.get('cancel_reason')}",
        )
        check(
            "B2 重试次数达到上限",
            charge.get("attempts", 0) == 3 and charge.get("status") == "FAILED",
            f"attempts={charge.get('attempts')} status={charge.get('status')}",
        )
        check(
            "B3 库存已回滚",
            after["reserved"] == before["reserved"] and after["available"] == before["available"],
            f"可售 {before['available']} → {after['available']}，预留={after['reserved']}",
        )
        check(
            "B4 支付状态为拒绝",
            bool(detail["payment"]) and detail["payment"]["status"] == "DECLINED",
            f"payment={detail['payment'] and detail['payment']['status']}",
        )
        set_chaos(client, payment_failure_rate=0.0)
        print()

        # ---------------- C 支付网关不响应 ----------------
        print("场景 C：支付网关不响应（超时）")
        set_chaos(client, payment_stall=True)
        before = inventory(client, args.sku)
        code = create_order(client, args.sku, 1, "drill-C")
        detail = wait_for(client, code, {"CANCELLED"})
        after = inventory(client, args.sku)
        declined = [e for e in detail["timeline"] if e["event_type"] == "PAYMENT_DECLINED"]
        error = declined[-1]["payload"]["error"] if declined else ""
        check("C1 超时不等于成功", detail["payment"] is None or detail["payment"]["status"] != "CAPTURED", f"payment={detail['payment'] and detail['payment']['status']}")
        check("C2 记录为超时故障", "超时" in (error or ""), f"error={error[:40]}")
        check(
            "C3 最终补偿并归还库存",
            detail["state"] == "CANCELLED" and after["available"] == before["available"],
            f"state={detail['state']} 可售 {before['available']} → {after['available']}",
        )
        set_chaos(client, payment_stall=False)
        print()

        # ---------------- D 库存被抢空 ----------------
        print("场景 D：库存耗尽（不超卖）")
        inv = inventory(client, args.sku)
        available = inv["available"]
        if available <= 0:
            check("D0 库存已空，跳过", True, "available=0")
        else:
            codes = [create_order(client, args.sku, available, f"drill-D-{i}") for i in range(1)]
            overs = create_order(client, args.sku, 1, "drill-D-over")
            detail = wait_for(client, overs, {"CANCELLED"})
            after = inventory(client, args.sku)
            check(
                "D1 超出可售的订单被取消",
                detail["state"] == "CANCELLED" and detail.get("cancel_reason") == "OUT_OF_STOCK",
                f"state={detail['state']} reason={detail.get('cancel_reason')}",
            )
            check("D2 可售库存不为负", after["available"] >= 0, f"available={after['available']}")
            # 把刚占满的库存释放，避免污染后续演练
            for code in codes:
                client.post(f"/api/orders/{code}/cancel", json={"reason": "MANUAL"})
        print()

        # ---------------- 综合校验 ----------------
        print("综合校验")
        report = client.get("/api/ops/reconcile", params={"repair": "true"}).json()
        check("E1 对账无不变量违规", report["healthy"], f"counters={report['counters']}")
        outbox = client.get("/api/ops/outbox", params={"status": "DEAD"}).json()
        check("E2 无死信事件", len(outbox["items"]) == 0, f"dead={len(outbox['items'])}")
        metrics = client.get("/metrics").text
        check(
            "E3 指标可用",
            "stockpact_saga_step_total" in metrics and "stockpact_outbox_events_total" in metrics,
            "prometheus 文本已暴露",
        )

    print(f"\n结果：{len(PASSED)} 项通过，{len(FAILED)} 项失败")
    if FAILED:
        print("失败项：" + ", ".join(FAILED))
    return 0 if not FAILED else 1


if __name__ == "__main__":
    raise SystemExit(main())
