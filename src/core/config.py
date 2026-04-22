from __future__ import annotations
import sqlite3

import yaml
from pathlib import Path
from dataclasses import asdict, dataclass
from typing import Optional, Any
from urllib.parse import urlparse

from src.core.database.repository.account import AccountRepository
from src.core.database.repository.manga import MangaRepository, ChapterRepository


# ==========================================
# 1. КОНФІГУРАЦІЯ АКАУНТА (Персональна)
# ==========================================

@dataclass
class BaseHeaders:
    user_agent: str
    sec_ch_ua: str
    sec_ch_ua_platform: str = '"Windows"'
    sec_ch_ua_mobile: str = "?0"
    accept_language: str = "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"
    accept_encoding: str = "gzip, deflate, br, zstd"
    dnt: str = "1"

    def to_dict(self) -> dict[str, str]:
        """Конвертує атрибути класу в HTTP-заголовки"""
        result: dict[str, str] = {}
        for k, v in asdict(self).items():
            key = str(k).replace("_", "-").title().replace("Sec-Ch-Ua", "sec-ch-ua")
            result[key] = str(v)
        return result


@dataclass
class AuthConfig:
    email: str
    password: str


@dataclass
class ClientConfig:
    base_url: str
    auth: Optional[AuthConfig] = None
    cookies: Optional[dict[str, str]] = None
    
    def __post_init__(self) -> None:
        if self.cookies is None and self.auth is None:
            raise ValueError("Помилка: Потрібно передати 'cookies' або 'auth'.")

    @property
    def host(self) -> str:
        return urlparse(self.base_url).netloc


@dataclass
class NetworkConfig:
    proxy: Optional[str] = None
    timeout: int = 15


@dataclass
class BotConfig:
    client: ClientConfig
    browser: BaseHeaders
    network: NetworkConfig


# ==========================================
# 2. КОНФІГУРАЦІЯ ДОДАТКУ (Спільна)
# ==========================================

@dataclass(frozen=True)
class RewardSlotCfg:
    name: str
    daily_limit: int
    interval_seconds: float
    reward_keys: tuple[str, ...] = ()

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> RewardSlotCfg:
        return cls(
            name=str(d.get("name", "unknown")),
            daily_limit=int(d.get("daily_limit", 0)),
            interval_seconds=float(d.get("interval_seconds", 0.0)),
            reward_keys=tuple(d.get("reward_keys", []))
        )

    def matches(self, reward: dict[str, Any]) -> bool:
        if not self.reward_keys:
            return True
        return any(key in reward for key in self.reward_keys)
    

@dataclass(frozen=True)
class ParsingConfig:
    url_catalog: str
    url_chapters: str
    url_chapters_load: str
    reward_selector: str
    reward_type_attr: str
    reward_id_attr: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ParsingConfig:
        return cls(
            reward_selector=str(d.get("reward_selector", ".reward-item")),
            reward_type_attr=str(d.get("reward_type_attr", "data-reward-type")),
            reward_id_attr=str(d.get("reward_id_attr", "data-reward-id")),
            url_catalog=str(d.get("url_catalog", "/manga")),
            url_chapters=str(d.get("url_chapters", "/manga/{translit_name}")),
            url_chapters_load=str(d.get("url_chapters_load", "/chapters/load"))
        )

@dataclass(frozen=True)
class ReaderAppCfg:
    url_add_history: str
    update_interval_days: int
    parsing: ParsingConfig
    reward_slots: tuple[RewardSlotCfg, ...]

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> ReaderAppCfg:
        return cls(
            url_add_history=str(d.get("url_add_history", "/addHistory")),
            update_interval_days=int(d.get("update_interval_days", 1)),
            parsing=ParsingConfig.from_dict(d.get("parsing", {})),
            reward_slots=tuple(
                RewardSlotCfg.from_dict(s) for s in d.get("reward_slots", [])
            )
        )

    def find_slot(self, reward: dict[str, Any]) -> Optional[RewardSlotCfg]:
        for slot in self.reward_slots:
            if slot.matches(reward):
                return slot
        return None


@dataclass(frozen=True)
class DailyStreakCfg:
    url_balance: str
    url_claim: str
    item_selector: str
    claim_text: str
    day_attr: str

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DailyStreakCfg":
        return cls(
            url_balance=str(d.get("url_balance", "/balance")),
            url_claim=str(d.get("url_claim", "/balance/claim/{}")),
            item_selector=str(d.get("item_selector", ".daily-rewards .daily-rewards-item")),
            claim_text=str(d.get("claim_text", "Забрать")),
            day_attr=str(d.get("day_attr", "data-day"))
        )


@dataclass(frozen=True)
class AppConfig:
    reader:    ReaderAppCfg
    daily: DailyStreakCfg
    account_repo: AccountRepository
    manga_repo:   MangaRepository
    chapter_repo: ChapterRepository

    @classmethod
    def from_yaml(
        cls, 
        path: str | Path, 
        conn: sqlite3.Connection,
    ) -> AppConfig:
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Config file not found: {p}")

        with open(p, 'r', encoding='utf-8') as f:
            # Явно вказуємо тип для Pylance
            raw_data: dict[str, Any] = yaml.safe_load(f) or {}

        return cls(
            reader=ReaderAppCfg.from_dict(raw_data.get("reader", {})),
            daily=DailyStreakCfg.from_dict(raw_data.get("daily", {})),
            account_repo=AccountRepository(conn),
            manga_repo=MangaRepository(conn),
            chapter_repo=ChapterRepository(conn)
        )