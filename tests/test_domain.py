"""领域层纯单元测试：状态机与入参归一化不依赖数据库。"""

from __future__ import annotations

import pytest

from stockpact.domain.order import (
    ALLOWED_TRANSITIONS,
    InvalidTransition,
    IllegalArgument,
    Order,
    OrderState,
    normalize_lines,
)


def test_terminal_states_have_no_outgoing_transitions():
    assert ALLOWED_TRANSITIONS[OrderState.CONFIRMED] == frozenset()
    assert ALLOWED_TRANSITIONS[OrderState.CANCELLED] == frozenset()


def test_happy_path_transitions_are_allowed():
    order = Order(code="ORD-1", customer_id="c1", state=OrderState.CREATED)
    for target in (OrderState.STOCK_RESERVED, OrderState.PAID, OrderState.CONFIRMED):
        order.assert_transition(target)
        order.state = target


def test_skipping_steps_is_rejected():
    order = Order(code="ORD-2", customer_id="c1", state=OrderState.CREATED)
    with pytest.raises(InvalidTransition) as exc:
        order.assert_transition(OrderState.PAID)
    assert exc.value.http_status == 409
    assert exc.value.detail["current"] == "CREATED"


def test_terminal_order_cannot_move():
    order = Order(code="ORD-3", customer_id="c1", state=OrderState.CANCELLED)
    with pytest.raises(InvalidTransition):
        order.assert_transition(OrderState.CONFIRMED)


def test_normalize_lines_merges_same_sku():
    lines = normalize_lines([("A", 1, 100), ("A", 2, 100), ("B", 1, 50)])
    by_sku = {line.sku: line for line in lines}
    assert by_sku["A"].quantity == 3
    assert by_sku["A"].subtotal_cents == 300
    assert len(lines) == 2


@pytest.mark.parametrize("raw", [[("A", 0, 100)], [("A", -1, 100)], [("  ", 1, 100)], [("A", 1, -1)], []])
def test_normalize_lines_rejects_invalid(raw):
    with pytest.raises(IllegalArgument):
        normalize_lines(raw)
