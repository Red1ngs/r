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
    TYPE_CHECKING, Any, Optional, Sequence, cast,
)

from src.core.inventory.model import BaseInventory

if TYPE_CHECKING:
    from src.mangabuff.reader.slot_scheduler import SlotScheduler
    from src.mangabuff.reader.models import ReaderWork


@dataclass
class SlotProgress:
    slot_name:   str
    daily_limit: int
    collected:   int       = 0
    interval:    float     = 0.0
    reward_keys: list[str] = field(default_factory=lambda: [])

    def remaining(self) -> int:
        return max(0, self.daily_limit - self.collected)

    def is_complete(self) -> bool:
        return self.collected >= self.daily_limit

    def matches(self, reward_keys: frozenset[str]) -> bool:
        if not self.reward_keys:
            return True
        return bool(reward_keys & frozenset(self.reward_keys))

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot_name":   self.slot_name,
            "daily_limit": self.daily_limit,
            "collected":   self.collected,
            "interval":    self.interval,
            "reward_keys": self.reward_keys,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "SlotProgress":
        return cls(
            slot_name   = str(d["slot_name"]),
            daily_limit = int(d["daily_limit"]),
            collected   = int(d.get("collected", 0)),
            interval    = float(d.get("interval", 0.0)),
            reward_keys = list(d.get("reward_keys", [])),
        )


# ─────────────────────────────────────────────────────────────────────────────
# ReaderInventory
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ReaderInventory(BaseInventory):
    _work:      Optional[ReaderWork]      = field(default=None, compare=False)
    _scheduler: Optional["SlotScheduler"] = field(default=None, compare=False)

    @property
    def work(self) -> Optional[ReaderWork]:
        return self._work

    @work.setter
    def work(self, value: ReaderWork) -> None:
        self._work = value

    def clear_work(self) -> None:
        self._work = None

    @property
    def slot_scheduler(self) -> "SlotScheduler":
        if self._scheduler is None:
            from src.mangabuff.reader.slot_scheduler import SlotScheduler as _SS
            self._scheduler = _SS(self)
        return self._scheduler

    @property
    def target_slots(self) -> list[str]:
        return cast(list[str], self.data.get("target_slots", []))

    @target_slots.setter
    def target_slots(self, value: list[str]) -> None:
        self.data["target_slots"] = value

    def _raw_slots(self) -> dict[str, Any]:
        return cast(dict[str, Any], self.data.setdefault("slots", {}))

    def _set_slot(self, sp: SlotProgress) -> None:
        self._raw_slots()[sp.slot_name] = sp.to_dict()

    def all_slots(self) -> list[SlotProgress]:
        return sorted(
            (SlotProgress.from_dict(v) for v in self._raw_slots().values()),
            key=lambda s: s.interval,
        )

    def pending_slots(self) -> list[SlotProgress]:
        return [s for s in self.all_slots() if not s.is_complete()]

    def current_slot(self) -> Optional[SlotProgress]:
        pending = self.pending_slots()
        return pending[0] if pending else None

    def init_slots(self, cfgs: Sequence[Any]) -> None:
        targets = set(self.target_slots)
        raw = self._raw_slots()
        for cfg in cfgs:
            if targets and cfg.name not in targets:
                continue
            if cfg.name in raw:
                existing = SlotProgress.from_dict(raw[cfg.name])
                existing.daily_limit = cfg.daily_limit
                existing.interval    = cfg.interval_seconds
                existing.reward_keys = list(cfg.reward_keys)
                raw[cfg.name] = existing.to_dict()
            else:
                raw[cfg.name] = SlotProgress(
                    slot_name   = cfg.name,
                    daily_limit = cfg.daily_limit,
                    interval    = cfg.interval_seconds,
                    reward_keys = list(cfg.reward_keys),
                ).to_dict()

    def record_reward(self, reward_keys: frozenset[str]) -> Optional[str]:
        for slot in self.pending_slots():
            if slot.matches(reward_keys):
                slot.collected += 1
                self._set_slot(slot)
                return slot.slot_name if slot.is_complete() else None
        return None

    def delay_for_current_goal(self) -> float:
        return self.slot_scheduler.delay_until_next()

    def total_reads_for_goals(self) -> int:
        return sum(s.daily_limit for s in self.all_slots())

    def goal_reached(self) -> bool:
        slots = self.all_slots()
        return bool(slots) and all(s.is_complete() for s in slots)

    def __repr__(self) -> str:
        slot    = self.slot_scheduler.current()
        current = slot.slot_name if slot else "—"
        targets = self.target_slots or ["all"]
        delay   = self.slot_scheduler.delay_until_next()
        return (
            f"<ReaderInventory "
            f"targets={targets} "
            f"current={current!r} "
            f"delay={delay:.0f}s "
            f"pending={len(self.pending_slots())}>"
        )

