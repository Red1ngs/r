"""
bot/admin/services/scheduler_service.py

Тонкий фасад між адмін-ботом і Scheduler-сінглтоном.

Зміни:
  - AccountInfo.profession → professions: list[str]
  - assign_profession → add_profession (додає, не замінює)
  - change_profession видалено; натомість remove_profession + add_profession
  - add_account відновлює ВСІ professions з БД при старті
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from src.core.config.app import AppConfig
from src.core.config.bot import AuthConfig, BaseHeaders, BotConfig, ClientConfig, NetworkConfig
from src.core.runtime.scheduler import EventDrivenScheduler
from src.core.account import Account
from src.core.runtime.profession import profession_factory
from src.database.repository.factory import Repositories

_ENV_FILE = Path(".env")


# ─────────────────────────────────────────────────────────────────────────────
# .env helpers
# ─────────────────────────────────────────────────────────────────────────────

def _slug(account_id: str) -> str:
    return account_id.upper().replace("-", "_")

def _pw_key(account_id: str)    -> str: return f"ACCOUNT_PASSWORD_{_slug(account_id)}"
def _proxy_key(account_id: str) -> str: return f"ACCOUNT_PROXY_{_slug(account_id)}"


def _write_env(key: str, value: str) -> None:
    os.environ[key] = value
    line = f'{key}="{value}"\n'
    lines = _ENV_FILE.read_text("utf-8").splitlines(keepends=True) if _ENV_FILE.exists() else []
    for i, ln in enumerate(lines):
        if ln.startswith(f"{key}=") or ln.startswith(f"{key} ="):
            lines[i] = line
            break
    else:
        if lines and lines[-1].strip():
            lines.append("\n")
        lines.append(line)
    _ENV_FILE.write_text("".join(lines), "utf-8")


def _remove_env(key: str) -> None:
    os.environ.pop(key, None)
    if not _ENV_FILE.exists():
        return
    lines = _ENV_FILE.read_text("utf-8").splitlines(keepends=True)
    _ENV_FILE.write_text(
        "".join(ln for ln in lines if not (ln.startswith(f"{key}=") or ln.startswith(f"{key} ="))),
        "utf-8",
    )


def _save_credentials(account_id: str, password: str, proxy: str) -> None:
    _write_env(_pw_key(account_id), password)
    _write_env(_proxy_key(account_id), proxy)


def _load_credentials(account_id: str) -> tuple[str, str]:
    return (
        os.environ.get(_pw_key(account_id), ""),
        os.environ.get(_proxy_key(account_id), ""),
    )


def _erase_credentials(account_id: str) -> None:
    _remove_env(_pw_key(account_id))
    _remove_env(_proxy_key(account_id))


# ─────────────────────────────────────────────────────────────────────────────
# DTO
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AccountInfo:
    account_id:     str
    email:          str
    proxy:          str
    status:         str
    queue_size:     int
    triggers:       list[str]
    next_trigger_s: Optional[float]
    # Список profession у порядку пріоритету (індекс 0 = найвищий)
    professions:    list[str] = field(default_factory=list)

    @property
    def profession(self) -> Optional[str]:
        """Зворотна сумісність: перша profession або None."""
        return self.professions[0] if self.professions else None


@dataclass(frozen=True)
class SchedulerSnapshot:
    total_accounts: int
    accounts:       list[AccountInfo]


_DEFAULT_BROWSER = BaseHeaders(
    user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    sec_ch_ua='"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
    sec_ch_ua_platform='"Windows"',
    sec_ch_ua_mobile="?0",
    accept_language="uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
    accept_encoding="gzip, deflate, br, zstd",
    dnt="1",
)


# ─────────────────────────────────────────────────────────────────────────────
# SchedulerService
# ─────────────────────────────────────────────────────────────────────────────

class SchedulerService:
    def __init__(self, repo: Repositories, app_config: AppConfig) -> None:
        self._repo       = repo
        self._app_config = app_config

    @property
    def _scheduler(self) -> EventDrivenScheduler:
        return EventDrivenScheduler.get_instance()

    # ── Читання стану ─────────────────────────────────────────────────────────

    def snapshot(self) -> SchedulerSnapshot:
        scheduler = self._scheduler
        accounts = [
            info
            for acc_id in scheduler.account_ids()
            if (info := self._build_info(acc_id, scheduler)) is not None
        ]
        return SchedulerSnapshot(total_accounts=len(accounts), accounts=accounts)

    def account_info(self, account_id: str) -> Optional[AccountInfo]:
        return self._build_info(account_id, self._scheduler)

    def _build_info(self, acc_id: str, scheduler: EventDrivenScheduler) -> Optional[AccountInfo]:
        bot    = scheduler.get_bot(acc_id)
        status = scheduler.status(acc_id)
        if bot is None or status is None:
            return None

        # Беремо список professions з runtime (джерело правди для активного стану)
        runtime_profs = scheduler.profession_names(acc_id)

        return AccountInfo(
            account_id     = acc_id,
            email          = bot.bot_config.client.auth.email if bot.bot_config.client.auth else "—",
            proxy          = bot.bot_config.network.proxy or "",
            status         = status.name,
            queue_size     = scheduler.queue_size(acc_id) or 0,
            triggers       = scheduler.trigger_names(acc_id),
            next_trigger_s = scheduler.seconds_until_next(acc_id),
            professions    = runtime_profs,
        )

    def account_ids(self) -> list[str]:
        return self._scheduler.account_ids()

    # ── Створення акаунта ─────────────────────────────────────────────────────

    def add_account(
        self,
        account_id: str,
        email:      str,
        password:   str = "",
        proxy:      str = "",
    ) -> tuple[bool, str]:
        scheduler = self._scheduler
        if scheduler.has_account(account_id):
            return False, f"Акаунт {account_id!r} вже існує"

        if password:
            _save_credentials(account_id, password, proxy)
            # professions=None → не чіпаємо наявний список якщо акаунт вже є в БД
            self._repo.accounts.upsert(account_id, email, professions=None)

        stored_pw, stored_proxy = _load_credentials(account_id)
        if not stored_pw:
            return False, f"Пароль для {account_id!r} не знайдено в .env"

        bot_config = BotConfig(
            client=ClientConfig(
                base_url="https://mangabuff.ru",
                auth=AuthConfig(email=email, password=stored_pw),
            ),
            browser=_DEFAULT_BROWSER,
            network=NetworkConfig(proxy=stored_proxy or None, timeout=15),
        )

        bot = Account(account_id, bot_config, self._app_config, self._repo)

        # Відновлюємо всі professions з БД
        db_acc   = self._repo.accounts.get(account_id)
        prof_names = db_acc.professions if db_acc else []
        professions = []
        for name in prof_names:
            try:
                professions.append(profession_factory.build(name))
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"[{account_id}] Cannot restore profession {name!r}: {e}"
                )

        try:
            scheduler.add_account(account_id, bot, professions=professions)
        except ValueError as e:
            return False, str(e)

        return True, ""

    # ── Управління professions ────────────────────────────────────────────────

    def add_profession(
        self,
        account_id:    str,
        profession_name: str,
        *,
        priority: int = -1,
    ) -> tuple[bool, str]:
        """
        Додає profession до акаунта.

        priority=-1  → найнижчий пріоритет (в кінець списку)
        priority=0   → найвищий (першою)
        priority=N   → вставити на позицію N

        Якщо profession вже призначена — повертає (True, "") без змін (idempotent).
        """
        scheduler = self._scheduler
        if not scheduler.has_account(account_id):
            return False, f"Акаунт {account_id!r} не знайдено"

        if scheduler.has_profession(account_id, profession_name):
            return True, ""  # вже є — без помилки

        try:
            profession = profession_factory.build(profession_name)
        except Exception as e:
            return False, f"Помилка збірки profession {profession_name!r}: {e}"

        try:
            scheduler.add_profession_to_account(account_id, profession)
        except Exception as e:
            return False, f"Не вдалося додати профессію в ядро: {e}"

        # Зберігаємо в БД з урахуванням пріоритету
        self._repo.accounts.add_profession(account_id, profession_name, priority=priority)
        return True, ""

    def remove_profession(
        self,
        account_id:      str,
        profession_name: str,
    ) -> tuple[bool, str]:
        """Видаляє profession з акаунта (idempotent)."""
        scheduler = self._scheduler
        if not scheduler.has_account(account_id):
            return False, f"Акаунт {account_id!r} не знайдено"

        scheduler.remove_profession_from_account(account_id, profession_name)
        self._repo.accounts.remove_profession(account_id, profession_name)
        return True, ""

    def set_professions(
        self,
        account_id:  str,
        profession_names: list[str],
    ) -> tuple[bool, str]:
        """
        Атомарно замінює весь список professions акаунта.
        Видаляє тих що зникли, додає нових, зберігає порядок (= пріоритет).
        """
        scheduler = self._scheduler
        if not scheduler.has_account(account_id):
            return False, f"Акаунт {account_id!r} не знайдено"

        current = set(scheduler.profession_names(account_id))
        target  = list(dict.fromkeys(profession_names))  # dedup зі збереженням порядку

        # Видаляємо яких немає у target
        for name in current - set(target):
            scheduler.remove_profession_from_account(account_id, name)

        # Додаємо нові у порядку target
        for idx, name in enumerate(target):
            if not scheduler.has_profession(account_id, name):
                try:
                    profession = profession_factory.build(name)
                    scheduler.add_profession_to_account(account_id, profession)
                except Exception as e:
                    return False, f"Помилка profession {name!r}: {e}"

        # Синхронізуємо БД
        self._repo.accounts.set_professions(account_id, target)
        return True, ""

    # ── Видалення акаунта ─────────────────────────────────────────────────────

    def remove(self, account_id: str) -> bool:
        ok = self._scheduler.remove_account(account_id)
        if ok:
            _erase_credentials(account_id)
        return ok

    # ── Оновлення налаштувань ─────────────────────────────────────────────────
    def force_parse_mangas(
            self,
            account_id: str,
            *,
            limit:   int = 5,
            targets: Optional[list[str]] = None,
        ) -> tuple[bool, str, dict[str, Any]]:
            """Примусовий парсинг манг. Повертає (ok, reason, data)."""
            res = self._scheduler.ask_sync(
                account_id,
                profession_id="reader",
                intent="force_parse",
                data={"limit": limit, "targets": targets or []},
            )
            if res.approved:
                return True, "", res.data or {}
            return False, res.reason or "невідома помилка", {}

    def mark_mangas_read(
        self,
        account_id: str,
        targets: list[str],
    ) -> tuple[bool, str, dict[str, list[str]]]:
        """Позначає глави вказаних манг як прочитані. Повертає (ok, reason, data)."""
        res = self._scheduler.ask_sync(
            account_id,
            profession_id="reader",
            intent="mark_read",
            data={"targets": targets},
        )
        if res.approved:
            return True, "", res.data or {}
        return False, res.reason or "невідома помилка", {}
    
    def update_reader_slots(self, account_id: str, target_slots: list[str]) -> bool:
        res = self._scheduler.ask_sync(
            account_id,
            profession_id="reader",
            intent="set_targets",
            data={"targets": target_slots},
        )
        if res.approved:
            bot = self._scheduler.get_bot(account_id)
            if bot:
                self._repo.inventory.save(account_id, bot.inventory)
            return True
        return False

    def reschedule_trigger(self, account_id: str, trigger_name: str, run_at: str) -> bool:
        return self._scheduler.reschedule_trigger(account_id, trigger_name, run_at)

    def pause(self, account_id: str)  -> bool: return self._scheduler.pause_account(account_id)
    def resume(self, account_id: str) -> bool: return self._scheduler.resume_account(account_id)
