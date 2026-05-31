"""
reader/inventory.py — типізовані інвентарі.

Правило для data в BaseInventory:
  ЗБЕРІГАТИ   — те що бот змінює сам своїми діями
  НЕ ЗБЕРІГАТИ — те що змінює сайт (reputation тощо) → через initial_sync
  ВИНЯТОК      — is_banned: критично зберігати

Зміна архітектури:
  Конкретні інвентарі реєструються через inventory_factory в прикладному
  шарі і кладуться на DynamicInventories динамічно.
  INVENTORY_REGISTRY — проксі до inventory_factory.registry для зворотної
  сумісності.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import (
    TYPE_CHECKING, Any
)

from src.core.inventory.model import BaseInventory

if TYPE_CHECKING:
    from src.mangabuff.farmer.models import ItemReceivedEvent


@dataclass
class PersonalInventory(BaseInventory):

    @property
    def is_banned(self) -> bool:
        return bool(self.data.get("is_banned", False))

    pending_trades: list[dict[str, Any]] = field(default_factory=list)

    def push_trade(self, trade: dict[str, Any]) -> None:
        self.pending_trades.append(trade)

    pending_events: list[dict[str, Any]] = field(default_factory=list)

    def push_event(self, event: dict[str, Any]) -> None:
        self.pending_events.append(event)

    received_items: list[ItemReceivedEvent] = field(default_factory=list)

    def push_item_received(self, event: ItemReceivedEvent) -> None:
        self.received_items.append(event)

    def drain_received_items(self) -> list[ItemReceivedEvent]:
        items, self.received_items = self.received_items, []
        return items

    def __repr__(self) -> str:
        return (
            f"<PersonalInventory "
            f"banned={self.is_banned}>"
        )