"""并发压测：验证"高并发下单不超卖、不丢单、不卡死"。

做的事：
1. N 个线程 × 各自独立 HTTP 连接，用唯一幂等键下单；
2. 统计成功/失败、延迟分位数、状态码分布；
3. 等后台 Saga 收敛后调用 /api/ops/reconcile 校验不变量（库存守恒、预留账实相符）。

用法：
    python scripts/load_test.py --orders 300 --workers 32
    python scripts/load_test.py --orders 200 --workers 16 --qty 2 --sku SKU-CAM-001
"""

from __future__ import annotations

import argparse
import statistics
import threading
import time
import uuid
from collections import Counter

import httpx


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round((p / 100) * (len(ordered) - 1))))
    return ordered[index]


def main() -> int:
    parser = argparse.ArgumentParser(description="StockPact 下单并发压测")
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--orders", type=int, default=200)
    parser.add_argument("--workers", type=int, default=20)
    parser.add_argument("--qty", type=int, default=1)
    parser.add_argument("--sku", default="SKU-BOOK-001")
    parser.add_argument("--api-key", default="")
    parser.add_argument("--converge-timeout", type=float, default=180.0, help="等待后台收敛的最长秒数")
    args = parser.parse_args()

    headers = {"X-API-Key": args.api_key} if args.api_key else {}
    with httpx.Client(base_url=args.base_url, timeout=30) as client:
        inventory = client.get(f"/api/inventory/{args.sku}").json()
        print(f"目标 SKU={args.sku} 在库={inventory['on_hand']} 预留={inventory['reserved']} 可售={inventory['available']}")
        baseline_available = inventory["available"]

    latencies: list[float] = []
    statuses: Counter[str] = Counter()
    states: Counter[str] = Counter()
    failures: list[str] = []
    lock = threading.Lock()
    per_worker = max(1, args.orders // args.workers)
    started = time.perf_counter()

    def worker(worker_id: int) -> None:
        with httpx.Client(base_url=args.base_url, timeout=30, headers=headers) as client:
            for i in range(per_worker):
                key = f"load-{worker_id}-{i}-{uuid.uuid4().hex[:6]}"
                body = {"customer_id": f"load-{worker_id}", "items": [{"sku": args.sku, "quantity": args.qty}]}
                begin = time.perf_counter()
                try:
                    response = client.post("/api/orders", json=body, headers={"Idempotency-Key": key})
                    elapsed = (time.perf_counter() - begin) * 1000
                    with lock:
                        latencies.append(elapsed)
                        statuses[str(response.status_code)] += 1
                        if response.status_code in (200, 201):
                            states[response.json()["state"]] += 1
                        else:
                            failures.append(f"{response.status_code}:{response.text[:120]}")
                except Exception as exc:  # pragma: no cover - 网络异常
                    with lock:
                        statuses["error"] += 1
                        failures.append(str(exc))

    threads = [threading.Thread(target=worker, args=(w,)) for w in range(args.workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    wall = time.perf_counter() - started

    total = sum(statuses.values())
    print("\n=== 压测结果 ===")
    print(f"请求数={total} 并发={args.workers} 墙钟={wall:.2f}s 吞吐={total / wall:.1f} req/s")
    print(f"状态码分布={dict(statuses)}")
    print(f"订单状态分布={dict(states)}")
    if latencies:
        print(
            f"延迟 ms：p50={percentile(latencies, 50):.1f} p95={percentile(latencies, 95):.1f} "
            f"p99={percentile(latencies, 99):.1f} max={max(latencies):.1f} mean={statistics.fmean(latencies):.1f}"
        )
    if failures:
        print(f"失败样本（前 5 条）：{failures[:5]}")

    print("\n等待后台 Saga 收敛（在途订单与 outbox 同时归零）…")
    begin_wait = time.time()
    converged_after = None
    with httpx.Client(base_url=args.base_url, timeout=30, headers=headers) as client:
        while time.time() - begin_wait < args.converge_timeout:
            snapshot = client.get("/api/ops/sagas").json()
            if snapshot["in_flight"] == 0 and snapshot["outbox_pending"] == 0:
                converged_after = time.time() - begin_wait
                break
            time.sleep(1)
    print(f"收敛耗时：{'%.1fs' % converged_after if converged_after is not None else '超时未收敛'}")

    with httpx.Client(base_url=args.base_url, timeout=30, headers=headers) as client:
        after = client.get(f"/api/inventory/{args.sku}").json()
        report = client.get("/api/ops/reconcile", params={"repair": "true"}).json()
        sagas = client.get("/api/ops/sagas").json()

    print(
        f"库存：在库={after['on_hand']} 预留={after['reserved']} 可售={after['available']} "
        f"(起始可售={baseline_available})"
    )
    print(f"在途订单={sagas['in_flight']} outbox_pending={sagas['outbox_pending']}")
    print(f"对账：healthy={report['healthy']} counters={report['counters']}")
    if report["issues"]:
        for issue in report["issues"][:5]:
            print(f"  [{issue['severity']}] {issue['type']}: {issue['detail']}")

    has_5xx = any(code.startswith("5") for code in statuses)
    ok = report["healthy"] and not has_5xx and statuses.get("error", 0) == 0
    print("\n结论：" + ("通过（无不变量违规、无 5xx）" if ok else "未通过，请检查上面的失败样本"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
