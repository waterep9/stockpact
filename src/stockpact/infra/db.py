"""数据库接入层。

SQLite 作为默认存储是为了"克隆即可运行"，但设计上不做 SQLite 专属假设：
① 所有并发控制都用条件 UPDATE（rowcount 判定），PostgreSQL 下语义一致；
② 时间统一 naive UTC；
③ 事务边界显式，便于换成 Postgres 的连接池/重试策略。
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import TypeVar

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from ..config import Settings
from .schema import Base

log = logging.getLogger("stockpact.db")

T = TypeVar("T")


class Database:
    def __init__(self, settings: Settings, url: str | None = None) -> None:
        self.settings = settings
        self.url = url or settings.resolve_database_url()
        self.engine = self._build_engine()
        self._sessionmaker = sessionmaker(bind=self.engine, expire_on_commit=False, future=True)

    def _build_engine(self) -> Engine:
        if self.url.startswith("sqlite"):
            kwargs: dict[str, object] = {
                "connect_args": {"check_same_thread": False, "timeout": 30},
                "future": True,
            }
            if ":memory:" in self.url:
                kwargs["poolclass"] = StaticPool
            engine = create_engine(self.url, **kwargs)

            @event.listens_for(engine, "connect")
            def _sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover - 连接级
                cur = dbapi_conn.cursor()
                cur.execute("PRAGMA journal_mode=WAL")
                cur.execute("PRAGMA synchronous=NORMAL")
                cur.execute("PRAGMA foreign_keys=ON")
                cur.execute("PRAGMA busy_timeout=30000")
                cur.close()

            return engine
        return create_engine(self.url, pool_pre_ping=True, pool_size=10, max_overflow=20, future=True)

    def create_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def drop_schema(self) -> None:
        Base.metadata.drop_all(self.engine)

    @contextmanager
    def session(self) -> Iterator[Session]:
        session = self._sessionmaker()
        try:
            yield session
        finally:
            session.close()

    @contextmanager
    def tx(self) -> Iterator[Session]:
        """显式事务。异常时回滚，正常提交。"""
        session = self._sessionmaker()
        try:
            with session.begin():
                yield session
        finally:
            session.close()

    def run_in_tx(self, fn: Callable[[Session], T], *, retries: int = 6) -> T:
        """带锁冲突重试的事务执行器。

        SQLite 在写-写竞争时返回 `database is locked`（Postgres 则是死锁/序列化失败）。
        统一在这一层重试，业务层不必关心存储差异；重试次数有限，避免雪崩。
        """
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                with self.tx() as session:
                    return fn(session)
            except OperationalError as exc:  # pragma: no cover - 依赖真实竞争
                message = str(getattr(exc, "orig", exc)).lower()
                retryable = "locked" in message or "deadlock" in message or "serializ" in message
                if not retryable or attempt == retries - 1:
                    raise
                last_error = exc
                time.sleep(0.01 * (2**attempt) * (0.5 + random.random()))
        raise RuntimeError("事务重试耗尽") from last_error

    def healthy(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:  # pragma: no cover
            return False
