"""
inventory/model.py — типізовані інвентарі.

Правило для data в BaseInventory:
  ЗБЕРІГАТИ   — те що бот змінює сам своїми діями
  НЕ ЗБЕРІГАТИ — те що змінює сайт (reputation тощо) → через initial_sync
  ВИНЯТОК      — is_banned: критично зберігати

Після перезапуску:
  - персистентні дані завантажуються з БД як є
  - сесійні дані перезаписуються initial_sync
  - in-memory черги (pending_trades, received_items) відновлюються polling-ом
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Optional, Sequence

from src.core.database.repository.manga import MangaRow

if TYPE_CHECKING:
    from src.core.inventory.slot_scheduler import SlotScheduler


@dataclass
class BaseInventory:
    data: dict[str, Any] = field(default_factory=lambda: {})

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        self.data[key] = value

    def delete(self, key: str) -> None:
        self.data.pop(key, None)

    def update(self, patch: dict[str, Any]) -> None:
        self.data.update(patch)


# ─────────────────────────────────────────────────────────────────────────────
# ItemReceivedEvent — подія «акаунт отримав предмет»
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ItemReceivedEvent:
    """
    In-memory подія отримання нагороди.

    Не персистується в БД — стан вже відображений у SlotProgress.collected.
    Використовується ReactiveTask-дренером для:
      - надсилання повідомлень у TG
      - оновлення статистики
      - будь-якої реакції на отримання предмета

    account_id  : якому акаунту надійшла нагорода
    slot_name   : назва слота ('card', 'scroll', тощо)
    reward      : сирий JSON від сайту
    received_at : unix timestamp
    """
    account_id:  str
    slot_name:   str
    reward:      dict[str, Any]
    received_at: float = field(default_factory=time.time)


# ─────────────────────────────────────────────────────────────────────────────
# PersonalInventory
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class PersonalInventory(BaseInventory):
    """
    Особистий стан акаунта.

    Статистика — окремі колонки в БД (accounts.comments_written тощо).

    data зберігає:
      want_list, manga_have     — бот сам змінює при обмінах
      completed_trades          — при кожному підтвердженому обміні
      last_bonus_claimed        — при отриманні бонусу
      is_banned                 — виняток: сайт змінює, але критично зберігати
      synced                    — прапор першого запуску

    data НЕ зберігає (оновлюється initial_sync):
      reputation, trade_limit, is_verified, username
    """
    comments_written: int = 0
    trades_accepted:  int = 0
    trades_declined:  int = 0

    @property
    def want_list(self) -> list[str]:
        return self.data.get("want_list", [])

    @want_list.setter
    def want_list(self, value: list[str]) -> None:
        self.data["want_list"] = value

    @property
    def manga_have(self) -> list[str]:
        return self.data.get("manga_have", [])

    @manga_have.setter
    def manga_have(self, value: list[str]) -> None:
        self.data["manga_have"] = value

    @property
    def is_banned(self) -> bool:
        return bool(self.data.get("is_banned", False))

    # ── Trade черга (in-memory, відновлюється через polling) ──────────────────
    pending_trades: list[dict[str, Any]] = field(default_factory=list)

    def push_trade(self, trade: dict[str, Any]) -> None:
        self.pending_trades.append(trade)

    # ── Generic event черга (legacy, зворотна сумісність) ─────────────────────
    pending_events: list[dict[str, Any]] = field(default_factory=list)

    def push_event(self, event: dict[str, Any]) -> None:
        self.pending_events.append(event)

    # ── Typed event: отримання предмета ───────────────────────────────────────
    received_items: list[ItemReceivedEvent] = field(default_factory=list)

    def push_item_received(self, event: ItemReceivedEvent) -> None:
        """
        Додає подію отримання нагороди у чергу.
        ReactiveTask-дренер обробляє і очищає цю чергу.
        """
        self.received_items.append(event)

    def drain_received_items(self) -> list[ItemReceivedEvent]:
        """Повертає і очищає всі накопичені події."""
        items, self.received_items = self.received_items, []
        return items

    def __repr__(self) -> str:
        return (
            f"<PersonalInventory "
            f"comments={self.comments_written} "
            f"trades=+{self.trades_accepted}/-{self.trades_declined} "
            f"banned={self.is_banned}>"
        )


@dataclass
class AllianceInventory(BaseInventory):
    """Стан альянсу."""

    @property
    def name(self) -> str:
        return self.data.get("name", "")

    @property
    def rank(self) -> int:
        return self.data.get("rank", 0)

    @property
    def shared_items(self) -> list[str]:
        return self.data.get("shared_items", [])

    @property
    def member_count(self) -> int:
        return self.data.get("member_count", 0)

    def __repr__(self) -> str:
        return f"<AllianceInventory name={self.name!r} rank={self.rank}>"


@dataclass
class Manga:
    data_id:       int
    translit_name: str
    name:          str
    rating:        str
    info:          str
    image:         str


@dataclass
class Chapter:
    data_id:     int
    chapter_num: float
    volume:      int
    date:        str
    manga_id:    Optional[int] = None


@dataclass
class ReaderWork:
    mode:             Literal["stale", "catalog"]
    targets:          Optional[list[MangaRow]] = None
    mangas_to_save:   list[Manga]              = field(default_factory=list)
    chapters_to_save: list[Chapter]            = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# SlotProgress
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class SlotProgress:
    """
    Прогрес по одному RewardSlotCfg.

    Зберігається в ReaderInventory.data["slots"][slot_name].
    Слот закривається коли collected >= daily_limit.
    """
    slot_name:   str
    daily_limit: int
    collected:   int       = 0
    interval:    float     = 0.0
    reward_keys: list[str] = field(default_factory=list)

    def remaining(self) -> int:
        return max(0, self.daily_limit - self.collected)

    def is_complete(self) -> bool:
        return self.collected >= self.daily_limit

    def matches(self, reward_keys: frozenset[str]) -> bool:
        """True якщо нагорода належить цьому слоту."""
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
    """
    Стан читача.

    data["target_slots"]  : list[str]        — цілі акаунта
    data["slots"]         : dict[name→dict]  — прогрес по кожному слоту
    data["slot_schedule"] : dict[name→float] — next_fire_at (unix) для кожного слота

    Через .scheduler:
        inv.reader.scheduler.current()          → який слот зараз
        inv.reader.scheduler.delay_until_next() → скільки чекати
        inv.reader.scheduler.mark_done(name)    → після виконання
    """

    _work:      Optional[ReaderWork]     = field(default=None, compare=False)
    _scheduler: Optional["SlotScheduler"] = field(default=None, compare=False)

    # ── work ─────────────────────────────────────────────────────────────────

    @property
    def work(self) -> Optional[ReaderWork]:
        return self._work

    @work.setter
    def work(self, value: ReaderWork) -> None:
        self._work = value

    def clear_work(self) -> None:
        self._work = None

    # ── scheduler ────────────────────────────────────────────────────────────

    @property
    def scheduler(self) -> "SlotScheduler":
        if self._scheduler is None:
            from src.core.inventory.slot_scheduler import SlotScheduler as _SS
            self._scheduler = _SS(self)
        return self._scheduler

    # ── target_slots ─────────────────────────────────────────────────────────

    @property
    def target_slots(self) -> list[str]:
        return self.data.get("target_slots", [])

    @target_slots.setter
    def target_slots(self, value: list[str]) -> None:
        self.data["target_slots"] = value

    # ── slots ─────────────────────────────────────────────────────────────────

    def _raw_slots(self) -> dict[str, Any]:
        return self.data.setdefault("slots", {})

    def _set_slot(self, sp: SlotProgress) -> None:
        self._raw_slots()[sp.slot_name] = sp.to_dict()

    def all_slots(self) -> list[SlotProgress]:
        """Всі ініціалізовані слоти, по interval asc."""
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
        """
        Ініціалізує слоти з RewardSlotCfg, враховуючи target_slots.
        Якщо слот вже існує — оновлює конфіг, зберігаючи collected.
        """
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
        """
        Зараховує нагороду в перший незакритий слот чий matches() = True.
        Повертає ім'я слота якщо він щойно закрився, або None.

        ВАЖЛИВО: reward_keys має бути непорожнім — перевірка на стороні
        викликача (read_chapter).
        """
        for slot in self.pending_slots():
            if slot.matches(reward_keys):
                slot.collected += 1
                self._set_slot(slot)
                return slot.slot_name if slot.is_complete() else None
        return None

    # ── Методи для pipeline ───────────────────────────────────────────────────

    def delay_for_current_goal(self) -> float:
        return self.scheduler.delay_until_next()

    def total_reads_for_goals(self) -> int:
        return sum(s.daily_limit for s in self.all_slots())

    def goal_reached(self) -> bool:
        slots = self.all_slots()
        return bool(slots) and all(s.is_complete() for s in slots)

    def __repr__(self) -> str:
        slot    = self.scheduler.current()
        current = slot.slot_name if slot else "—"
        targets = self.target_slots or ["all"]
        delay   = self.scheduler.delay_until_next()
        return (
            f"<ReaderInventory "
            f"targets={targets} "
            f"current={current!r} "
            f"delay={delay:.0f}s "
            f"pending={len(self.pending_slots())}>"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Inventories + Registry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Inventories:
    personal: PersonalInventory = field(default_factory=PersonalInventory)
    alliance: AllianceInventory = field(default_factory=AllianceInventory)
    reader:   ReaderInventory   = field(default_factory=ReaderInventory)

    def __repr__(self) -> str:
        return f"<Inventories {self.personal} {self.alliance} {self.reader}>"


INVENTORY_REGISTRY: dict[str, tuple[str, type[BaseInventory]]] = {
    "personal": ("personal", PersonalInventory),
    "alliance": ("alliance", AllianceInventory),
    "reader":   ("reader", ReaderInventory),
}