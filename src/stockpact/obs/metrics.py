"""轻量指标注册表（Prometheus 文本格式）。

不引入 prometheus_client 依赖：本项目只需要 counter/gauge/histogram 三种原语，
自实现约 100 行可读代码，同时让"埋点即代码"这件事在评审里可见。

局限（明确记录）：进程内指标，多 worker 部署需要 prometheus_client 的
multiprocess 模式或 sidecar 聚合；库层面的 backlog 类指标由 /metrics 时实时查库补齐。
"""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from typing import Iterator

DEFAULT_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.1,
    0.25,
    0.5,
    1.0,
    2.5,
    5.0,
)

_HELP = {
    "stockpact_http_requests_total": "HTTP 请求总数（按路由模板与状态码）",
    "stockpact_http_request_duration_seconds": "HTTP 请求耗时分布",
    "stockpact_orders_created_total": "成功创建的订单数",
    "stockpact_orders_cancelled_total": "进入取消终态的订单数（按原因）",
    "stockpact_saga_step_total": "Saga 步骤执行次数（按步骤与结果）",
    "stockpact_inventory_reserve_conflicts_total": "库存预留失败（可售不足）次数",
    "stockpact_rate_limited_total": "被限流拒绝的请求数",
    "stockpact_outbox_events_total": "outbox 投递尝试次数（按结果）",
    "stockpact_cache_hit_ratio": "读缓存命中率",
    "stockpact_outbox_pending": "当前待投递的 outbox 事件数",
    "stockpact_orders_in_flight": "当前非终态订单数",
}


def _key(name: str, labels: dict[str, str]) -> tuple[str, tuple[tuple[str, str], ...]]:
    return name, tuple(sorted(labels.items()))


def _fmt_labels(labels: tuple[tuple[str, str], ...]) -> str:
    if not labels:
        return ""
    body = ",".join(f'{k}="{v}"' for k, v in labels)
    return "{" + body + "}"


class Metrics:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self._gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        # histogram: key -> [bucket_counts, sum, count]
        self._hist: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}
        self._started_at = time.time()

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        key = _key(name, {k: str(v) for k, v in labels.items()})
        with self._lock:
            self._counters[key] = self._counters.get(key, 0.0) + amount

    def set_gauge(self, name: str, value: float, **labels: str) -> None:
        key = _key(name, {k: str(v) for k, v in labels.items()})
        with self._lock:
            self._gauges[key] = float(value)

    def observe(self, name: str, value: float, **labels: str) -> None:
        key = _key(name, {k: str(v) for k, v in labels.items()})
        with self._lock:
            state = self._hist.get(key)
            if state is None:
                state = [0.0] * len(DEFAULT_BUCKETS) + [0.0, 0.0]
                self._hist[key] = state
            for index, bound in enumerate(DEFAULT_BUCKETS):
                if value <= bound:
                    state[index] += 1
            state[-2] += value
            state[-1] += 1

    @contextmanager
    def timer(self, name: str, **labels: str) -> Iterator[None]:
        started = time.perf_counter()
        try:
            yield
        finally:
            self.observe(name, time.perf_counter() - started, **labels)

    def render(self) -> str:
        lines: list[str] = []
        with self._lock:
            counters = dict(self._counters)
            gauges = dict(self._gauges)
            hists = {k: list(v) for k, v in self._hist.items()}
        emitted: set[str] = set()
        for (name, labels), value in sorted(counters.items()):
            if name not in emitted:
                lines.append(f"# HELP {name} {_HELP.get(name, name)}")
                lines.append(f"# TYPE {name} counter")
                emitted.add(name)
            lines.append(f"{name}{_fmt_labels(labels)} {value:g}")
        for (name, labels), value in sorted(gauges.items()):
            if name not in emitted:
                lines.append(f"# HELP {name} {_HELP.get(name, name)}")
                lines.append(f"# TYPE {name} gauge")
                emitted.add(name)
            lines.append(f"{name}{_fmt_labels(labels)} {value:g}")
        for (name, labels), state in sorted(hists.items()):
            if name not in emitted:
                lines.append(f"# HELP {name} {_HELP.get(name, name)}")
                lines.append(f"# TYPE {name} histogram")
                emitted.add(name)
            cumulative = 0.0
            for index, bound in enumerate(DEFAULT_BUCKETS):
                cumulative = state[index]
                bucket_labels = labels + ((("le", f"{bound:g}")),)
                lines.append(f"{name}_bucket{_fmt_labels(bucket_labels)} {cumulative:g}")
            inf_labels = labels + ((("le", "+Inf")),)
            lines.append(f"{name}_bucket{_fmt_labels(inf_labels)} {state[-1]:g}")
            lines.append(f"{name}_sum{_fmt_labels(labels)} {state[-2]:g}")
            lines.append(f"{name}_count{_fmt_labels(labels)} {state[-1]:g}")
        lines.append("# HELP stockpact_process_uptime_seconds 进程运行时长")
        lines.append("# TYPE stockpact_process_uptime_seconds gauge")
        lines.append(f"stockpact_process_uptime_seconds {time.time() - self._started_at:.0f}")
        return "\n".join(lines) + "\n"


METRICS = Metrics()
