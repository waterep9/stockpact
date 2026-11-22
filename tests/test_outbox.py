"""Outbox 中继测试：投递、消费幂等、毒丸隔离。"""

from __future__ import annotations

import time

from stockpact.infra import repos


def _drive_to_confirmed(container, qty=1):
    result = container.orders.create(customer_id="cust-1", items=[{"sku": "SKU-A", "quantity": qty}])
    container.saga.drive(result.order_code)
    return result


def test_relay_publishes_all_events_and_projects_read_models(seeded):
    container = seeded
    result = _drive_to_confirmed(container)
    stats = container.outbox.relay_once()
    assert stats["published"] == 4  # created / reserved / captured / confirmed
    assert stats["failed"] == 0

    with container.db.session() as session:
        events = repos.list_outbox(session, limit=20)
        feed = repos.list_feed(session, limit=20)
        ledger = repos.list_ledger(session, limit=20)
        notes = repos.list_notifications(session, limit=20)

    assert all(event.status == "PUBLISHED" for event in events)
    summaries = " | ".join(entry.summary for entry in feed)
    assert "库存已预留" in summaries
    assert "支付成功" in summaries
    assert "订单已确认" in summaries
    movements = [entry.movement for entry in ledger]
    assert "RESERVE" in movements and "SHIP" in movements
    # 下单本身不外发通知，其余三个状态各一条
    assert len(notes) == 3
    assert all(note.order_code == result.order_code for note in notes)
    assert any("支付成功" in note.message for note in notes)


def test_duplicate_delivery_does_not_duplicate_projection(seeded):
    """中继至少一次投递：重复投递同一事件，读模型不应重复写入。"""
    container = seeded
    _drive_to_confirmed(container)
    container.outbox.relay_once()
    with container.db.session() as session:
        events = repos.list_outbox(session, limit=20)
        event_id = events[0].id
        before = len(repos.list_feed(session, limit=100))

    outcome = container.outbox.dispatch(event_id)
    assert outcome == "published"
    with container.db.session() as session:
        after = len(repos.list_feed(session, limit=100))
    assert after == before


def test_poison_consumer_is_isolated_per_event(seeded):
    """一个消费者对某类事件持续失败时：只卡住该事件，不阻塞其它事件。

    注意语义：已成功的消费者（read_model）不会回滚——这正是"至少一次 + 消费幂等"
    的代价与保障。重试时已处理的消费者会被 processed_events 跳过，不会重复投影。
    """
    container = seeded
    container.settings.outbox_max_attempts = 3
    good = _drive_to_confirmed(container)
    container.outbox.relay_once()

    bad = container.orders.create(customer_id="cust-2", items=[{"sku": "SKU-A", "quantity": 1}])

    def poison(session, event):
        if event.order_code == bad.order_code:
            raise RuntimeError("消费者故障（注入）")

    container.outbox.register("poison", poison)
    stats = container.outbox.relay_once()
    assert stats["failed"] >= 1

    with container.db.session() as session:
        events = repos.list_outbox(session, status="PENDING", limit=50)
    assert events and all(e.order_code == bad.order_code for e in events)

    # 修复消费者后重试：事件最终投递成功，且读模型没有重复写入
    container.outbox.consumers.pop("poison")
    container.settings.outbox_max_attempts = 5
    time.sleep(0.35)
    container.outbox.relay_once()

    with container.db.session() as session:
        events = repos.list_outbox(session, limit=50)
        bad_feed = [f for f in repos.list_feed(session, limit=50) if f.order_code == bad.order_code]
    assert all(e.status == "PUBLISHED" for e in events)
    # 该订单共 2 条事件（创建 / 预留），重试后仍是 2 条投影：没有重复消费
    assert len(bad_feed) == 2
    assert len({entry.summary for entry in bad_feed}) == 2
