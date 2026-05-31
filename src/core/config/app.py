from __future__ import annotations

import yaml
from pathlib import Path
from dataclasses import dataclass
from typing import Optional, Any


@dataclass(frozen=True)
class RewardSlotCfg:
    name:             str
    daily_limit:      int
    interval_seconds: float
    reward_keys:      tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "RewardSlotCfg":
        return cls(
            name=str(d.get("name", "unknown")),
            daily_limit=int(d.get("daily_limit", 0)),
            interval_seconds=float(d.get("interval_seconds", 0.0)),
            reward_keys=tuple(d.get("reward_keys", [])),
        )

    def matches(self, reward: dict[str, Any]) -> bool:
        if not self.reward_keys:
            return True
        return any(key in reward for key in self.reward_keys)


@dataclass(frozen=True)
class ParsingConfig:
    url_catalog:       str
    url_chapters:      str
    url_chapters_load: str
    reward_selector:   str
    reward_type_attr:  str
    reward_id_attr:    str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ParsingConfig":
        return cls(
            reward_selector=str(d.get("reward_selector", ".reward-item")),
            reward_type_attr=str(d.get("reward_type_attr", "data-reward-type")),
            reward_id_attr=str(d.get("reward_id_attr", "data-reward-id")),
            url_catalog=str(d.get("url_catalog", "/manga")),
            url_chapters=str(d.get("url_chapters", "/manga/{translit_name}")),
            url_chapters_load=str(d.get("url_chapters_load", "/chapters/load")),
        )


@dataclass(frozen=True)
class ReadingModeCfg:
    """
    Конфігурація режиму читання.

    standard:
      - читання кожні read_interval_s секунд (default 5400 = 1.5 год) —
        використовується як fallback коли SlotScheduler повертає 0
        (всі слоти готові одразу, наприклад після рестарту).
      - затримки після нагород визначаються виключно через
        reward_slots[n].interval_seconds — дублювання cooldown тут відсутнє.

    event:
      - читання кожні event_interval_s секунд (default 120 = 2 хв).

    card_slot / scroll_slot:
      Імена слотів з reward_slots що ідентифікують картку / свиток.
      Використовуються тригером для визначення типу нагороди.
    """
    mode:             str
    read_interval_s:  float
    event_interval_s: float
    card_slot:        str
    scroll_slot:      str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReadingModeCfg":
        return cls(
            mode=str(d.get("mode", "standard")),
            read_interval_s=float(d.get("read_interval_s", 5400.0)),
            event_interval_s=float(d.get("event_interval_s", 120.0)),
            card_slot=str(d.get("card_slot", "card")),
            scroll_slot=str(d.get("scroll_slot", "scroll")),
        )

    @property
    def is_event(self) -> bool:
        return self.mode == "event"


@dataclass(frozen=True)
class ReaderAppCfg:
    url_add_history:      str
    update_interval_days: int
    parsing:              ParsingConfig
    reward_slots:         tuple[RewardSlotCfg, ...]
    reading_mode:         ReadingModeCfg

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ReaderAppCfg":
        return cls(
            url_add_history=str(d.get("url_add_history", "/addHistory")),
            update_interval_days=int(d.get("update_interval_days", 1)),
            parsing=ParsingConfig.from_dict(d.get("parsing", {})),
            reward_slots=tuple(
                RewardSlotCfg.from_dict(s) for s in d.get("reward_slots", [])
            ),
            reading_mode=ReadingModeCfg.from_dict(d.get("reading_mode", {})),
        )

    def find_slot(self, reward: dict[str, Any]) -> Optional[RewardSlotCfg]:
        for slot in self.reward_slots:
            if slot.matches(reward):
                return slot
        return None


@dataclass(frozen=True)
class QuizCfg:
    mode:         str
    answer_limit: int
    answer_delay: float

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "QuizCfg":
        return cls(
            mode=str(d.get("mode", "daily")),
            answer_limit=int(d.get("answer_limit", 5)),
            answer_delay=float(d.get("answer_delay", 8.0)),
        )


@dataclass(frozen=True)
class DailyCfg:
    url_balance:        str
    url_ping:           str
    url_calendar_claim: str
    item_selector:      str
    claim_text:         str
    day_attr:           str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DailyCfg":
        return cls(
            url_balance=str(d.get("url_balance", "/balance")),
            url_ping=str(d.get("url_ping", "/visit/ping")),
            url_calendar_claim=str(d.get("url_calendar_claim", "/balance/claim/{}")),
            item_selector=str(d.get("item_selector", ".daily-rewards .daily-rewards-item")),
            claim_text=str(d.get("claim_text", "Забрать")),
            day_attr=str(d.get("day_attr", "data-day")),
        )


@dataclass(frozen=True)
class AppConfig:
    reader: ReaderAppCfg
    daily:  DailyCfg
    quiz:   QuizCfg

    @classmethod
    def from_dict(cls, raw_data: dict[str, Any]) -> "AppConfig":
        return cls(
            reader=ReaderAppCfg.from_dict(raw_data.get("reader", {})),
            daily=DailyCfg.from_dict(raw_data.get("daily", {})),
            quiz=QuizCfg.from_dict(raw_data.get("quiz", {})),
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "AppConfig":
        with open(path, encoding="utf-8") as f:
            return cls.from_dict(yaml.safe_load(f) or {})