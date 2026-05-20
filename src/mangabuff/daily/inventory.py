"""
daily/inventory.py — типізовані інвентарі.

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

from dataclasses import dataclass

from src.core.inventory.model import BaseInventory


@dataclass
class DailyInventory(BaseInventory):
    can_claim_calendar: bool = False
    
    @property
    def day(self) -> int:
        """Поточний день стріку (календаря)."""
        return self.data.get("day", 1)
    
    @day.setter
    def day(self, value: int) -> None:
        self.data["day"] = value
    
    @property
    def last_daily_claimed(self) -> str | None:   
        """UTC дата останнього збору звичайного щоденного бонусу."""
        return self.data.get("last_daily_claimed")
    
    @last_daily_claimed.setter
    def last_daily_claimed(self, value: str) -> None:
        self.data["last_daily_claimed"] = value

    @property
    def last_calendar_claimed(self) -> str | None:   
        """UTC дата останнього збору календарного (streak) бонусу."""
        return self.data.get("last_calendar_claimed")
    
    @last_calendar_claimed.setter
    def last_calendar_claimed(self, value: str) -> None:
        self.data["last_calendar_claimed"] = value

    def __repr__(self) -> str:
        return (
            f"<DailyInventory "
            f"day={self.day} "
            f"last_daily={self.last_daily_claimed} "
            f"last_cal={self.last_calendar_claimed}>"
        )