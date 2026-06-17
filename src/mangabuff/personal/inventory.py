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
from src.utils.time import today

if TYPE_CHECKING:
    from src.mangabuff.manga_load.models import ItemReceivedEvent


@dataclass
class PersonalInventory(BaseInventory):

    @property
    def is_banned(self) -> bool:
        return bool(self.data.get("is_banned", False))
    
    @is_banned.setter
    def is_banned(self, value: bool) -> None:
        self.data["is_banned"] = value
        
    @property
    def to_day(self) -> str:
        """Дата останньої синхронізації з сайтом. Формат: "YYYY-MM-DD"."""
        value = self.data.get("to_day")
        if value := today() != value:
            self.data["to_day"] = value
        return value

    @to_day.setter
    def to_day(self, value: str | None) -> None:
        if value is None:
            self.data.pop("to_day", None)
        elif value.strip() == "":
            raise ValueError("to_day cannot be an empty string")
        else:
            self.data["to_day"] = value
            
    @property
    def user_name(self) -> str | None:
        """Ім'я користувача на сайті. Зберігається AuthService після авторизації."""
        return self.data.get("user_name")

    @user_name.setter
    def user_name(self, value: str | None) -> None:
        if value is not None:
            self.data["user_name"] = value
        else:
            self.data.pop("user_name", None)
            
    
    @property
    def user_id(self) -> str | None:
        """ID користувача на сайті. Зберігається AuthService після авторизації."""
        return self.data.get("user_id")

    @user_id.setter
    def user_id(self, value: str | None) -> None:
        if value is not None:
            self.data["user_id"] = value
        else:
            self.data.pop("user_id", None)

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
            f"user={self.user_name!r} "
            f"banned={self.is_banned}>"
        )