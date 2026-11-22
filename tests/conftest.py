"""测试基线：每个用例一套独立的临时 SQLite 库 + 独立容器。

刻意不用内存库（`:memory:` + StaticPool 会让所有线程共享同一连接，测不出真实并发），
而是用临时文件库 + WAL，让并发用例真的有写竞争。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from stockpact.config import Settings
from stockpact.container import Container
from stockpact.main import create_app


@pytest.fixture
def settings(tmp_path) -> Settings:
    s = Settings()
    s.data_dir = tmp_path
    s.database_url = f"sqlite+pysqlite:///{(tmp_path / 'test.db').as_posix()}"
    s.background_enabled = False
    s.rate_limit_enabled = False
    s.cache_ttl_seconds = 0.5
    s.reservation_ttl_seconds = 3
    s.order_deadline_seconds = 30
    s.payment_max_attempts = 3
    s.payment_backoff_base_seconds = 0.02
    s.outbox_poll_interval = 0.05
    s.recovery_interval = 0.05
    return s


@pytest.fixture
def container(settings) -> Container:
    c = Container(settings)
    c.init_schema()
    try:
        yield c
    finally:
        c.shutdown()


@pytest.fixture
def client(settings) -> TestClient:
    app = create_app(settings, start_background=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def seeded(container):
    """预置两个 SKU：A 库存 10，B 库存 3。"""
    container.catalog.create_product("SKU-A", "架构书", 12_900, 10)
    container.catalog.create_product("SKU-B", "马克杯", 3_500, 3)
    return container


def make_order(container, sku="SKU-A", qty=1, customer="cust-1"):
    return container.orders.create(customer_id=customer, items=[{"sku": sku, "quantity": qty}])
