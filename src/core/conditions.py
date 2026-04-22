"""
Конструктори умов.

Умова — Callable[[Inventories], bool]

Приклади:
    below("comments_written", 7)          # personal.comments_written < 7
    has("is_banned")                      # personal.data["is_banned"]
    alliance_has("shared_items")          # alliance.data["shared_items"]
    all_of(below("comments_written", 7), not_(has("is_banned")))
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:
    from src.core.inventory.model import Inventories

Condition = Callable[["Inventories"], bool]


# ---------------------------------------------------------------------------
# Логічні комбінатори
# ---------------------------------------------------------------------------

def all_of(*conditions: Condition) -> Condition:
    return lambda inv: all(c(inv) for c in conditions)

def any_of(*conditions: Condition) -> Condition:
    return lambda inv: any(c(inv) for c in conditions)

def not_(condition: Condition) -> Condition:
    return lambda inv: not condition(inv)


# ---------------------------------------------------------------------------
# Особистий інвентар
# ---------------------------------------------------------------------------

def reached(stat: str, target: int) -> Condition:
    """personal.{stat} >= target"""
    return lambda inv: getattr(inv.personal, stat, 0) >= target

def below(stat: str, target: int) -> Condition:
    """personal.{stat} < target"""
    return lambda inv: getattr(inv.personal, stat, 0) < target

def has(key: str) -> Condition:
    """personal.data[key] є truthy"""
    return lambda inv: bool(inv.personal.get(key))

def missing(key: str) -> Condition:
    return lambda inv: not inv.personal.get(key)

def data_equals(key: str, value: Any) -> Condition:
    return lambda inv: inv.personal.get(key) == value

def has_pending_trades() -> Condition:
    return lambda inv: len(inv.personal.pending_trades) > 0


# ---------------------------------------------------------------------------
# Альянс
# ---------------------------------------------------------------------------

def alliance_has(key: str) -> Condition:
    """alliance.data[key] є truthy"""
    return lambda inv: bool(inv.alliance.get(key))

def in_alliance() -> Condition:
    return lambda inv: bool(inv.alliance.name)


# ---------------------------------------------------------------------------
# Бібліотека
# ---------------------------------------------------------------------------

def library_has(key: str) -> Condition:
    return lambda inv: bool(inv.library.get(key))