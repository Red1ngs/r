"""
bot/admin/services/scheduler_service.py

Тонкий фасад між адмін-ботом і Scheduler-сінглтоном.

Відповідальності цього модуля:
  1. .env — зберігання пароля і проксі (ніколи в БД / пам'яті довше ніж потрібно).
  2. DTO  — AccountInfo, SchedulerSnapshot (бот не бачить живих об'єктів ядра).
  3. add_account / remove — повні цикли зі збереженням у .env і БД.

Все інше (pause/resume/list/status/queue) — делегується напряму до Scheduler.
Не дублюємо методи які вже є в Scheduler і нічого не додають.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from src.core.config.app import AppConfig
from src.core.config.bot import AuthConfig, BaseHeaders, BotConfig, ClientConfig, NetworkConfig
from src.core.scheduler import AccountEntry, Scheduler
from src.core.account import Account
from src.core.worker import BotWorker
from src.core.scheduling.profession import profession_factory
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
    _write_env(_proxy_key(account_id), proxy)   # "" теж пишемо — явна відсутність проксі


def _load_credentials(account_id: str) -> tuple[str, str]:
    """Повертає (password, proxy) з os.environ (куди load_dotenv вже завантажив .env)."""
    return (
        os.environ.get(_pw_key(account_id), ""),
        os.environ.get(_proxy_key(account_id), ""),
    )


def _erase_credentials(account_id: str) -> None:
    _remove_env(_pw_key(account_id))
    _remove_env(_proxy_key(account_id))


# ─────────────────────────────────────────────────────────────────────────────
# DTO  (frozen — бот не може їх мутувати)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AccountInfo:
    account_id:     str
    email:          str
    proxy:          str
    status:         str          # AccountStatus.name
    queue_size:     int
    triggers:       list[str]
    next_trigger_s: Optional[float]
    profession:     Optional[str]


@dataclass(frozen=True)
class SchedulerSnapshot:
    total_accounts: int
    accounts:       list[AccountInfo]


# ─────────────────────────────────────────────────────────────────────────────
# Дефолтний браузерний профіль
# ─────────────────────────────────────────────────────────────────────────────

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
    """
    Фасад для адмін-бота.

    Паролі НІКОЛИ не зберігаються в полях цього класу.
    Методи які просто делегують до Scheduler (pause/resume/status/queue/…)
    тут НЕ дублюються — роутери звертаються до Scheduler.get_instance() напряму,
    або через _scheduler property.
    """

    def __init__(self, repo: Repositories, app_config: AppConfig) -> None:
        self._repo       = repo
        self._app_config = app_config

    # ── Зручний доступ до планувальника ──────────────────────────────────────

    @property
    def _scheduler(self) -> Scheduler:
        return Scheduler.get_instance()

    # ── Читання стану (DTO) ───────────────────────────────────────────────────

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

    def _build_info(self, acc_id: str, scheduler: Scheduler) -> Optional[AccountInfo]:
        bot    = scheduler.get_bot(acc_id)
        status = scheduler.status(acc_id)
        if bot is None or status is None:
            return None

        db_acc = self._repo.accounts.get(acc_id)  # для profession і email (можливо, в майбутньому — для інших полів)
        return AccountInfo(
            account_id     = acc_id,
            email          = bot.bot_config.client.auth.email if bot.bot_config.client.auth else "—",
            proxy          = bot.bot_config.network.proxy or "",
            status         = status.name,
            queue_size     = scheduler.queue_size(acc_id) or 0,
            triggers       = scheduler.trigger_names(acc_id),
            next_trigger_s = scheduler.seconds_until_next(acc_id),
            profession     = db_acc.profession if db_acc else None,
        )

    # ── Список id (для перевірки унікальності при додаванні) ─────────────────

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
        """
        Реєструє акаунт у БД і Scheduler БЕЗ profession.
        Profession призначається окремо через assign_profession().

        При новому додаванні (password != ""):
          - пароль і проксі записуються у .env
        При відновленні після рестарту (password == ""):
          - пароль і проксі читаються з .env (завантажених через load_dotenv)
        """
        scheduler = self._scheduler
        if scheduler.has_account(account_id):
            return False, f"Акаунт {account_id!r} вже існує"

        # 1. .env
        if password:
            _save_credentials(account_id, password, proxy)
            self._repo.accounts.upsert(
                account_id, email, profession=None
            )
        
        stored_pw, stored_proxy = _load_credentials(account_id)
        if not stored_pw:
            return False, f"Пароль для {account_id!r} не знайдено в .env"

        # 2. BotConfig
        bot_config = BotConfig(
            client=ClientConfig(
                base_url="https://mangabuff.ru",
                auth=AuthConfig(email=email, password=stored_pw),
            ),
            browser=_DEFAULT_BROWSER,
            network=NetworkConfig(proxy=stored_proxy or None, timeout=15),
        )

        # 4. Account (без inventory-слотів — profession ще не обрана)
        bot   = Account(account_id, bot_config, self._app_config, self._repo)

        # 5. Scheduler — порожній entry (professions=[])
        entry = AccountEntry(worker=BotWorker(bot), professions=[])
        try:
            scheduler.add_account(account_id, entry)
        except ValueError as e:
            return False, str(e)

        return True, ""

    def assign_profession(self, account_id: str, profession_name: str) -> tuple[bool, str]:
        """
        Призначає profession акаунту, що вже існує в Scheduler.
        Записує profession у БД і додає тригери до живого entry.
        """
        scheduler = self._scheduler
        entry = scheduler.get_entry(account_id)
        if entry is None:
            return False, f"Акаунт {account_id!r} не знайдено"

        bot = entry.worker.bot

        # Будуємо profession через factory
        try:
            result = profession_factory.build(profession_name, bot)
        except ValueError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Помилка збірки profession {profession_name!r}: {e}"

        # build може повертати (Profession, Stats) або просто Profession
        profession = result[0]

        # Startup tasks
        tasks = profession.startup_tasks(bot)
        if tasks:
            entry.worker.assign(*tasks)

        # Тригери
        triggers = profession.build_triggers(account_id)
        entry.add_triggers(triggers, profession.name)
        entry.professions.append(profession)

        # БД
        self._repo.accounts.set_profession(account_id, profession_name)

        return True, ""

    # ── Видалення ─────────────────────────────────────────────────────────────

    def remove(self, account_id: str) -> bool:
        ok = self._scheduler.remove_account(account_id)
        if ok:
            _erase_credentials(account_id)
        return ok

    # ── Profession-специфічні налаштування (reader slots) ────────────────────

    def update_reader_slots(self, account_id: str, target_slots: list[str]) -> bool:
        """Оновлює target_slots для reader profession."""
        bot = self._scheduler.get_bot(account_id)
        if bot is None:
            return False
        reader = getattr(bot.inventory, "reader", None)
        if reader is None:
            return False
        reader.target_slots = target_slots
        self._repo.inventory.save(account_id, bot.inventory)
        return True

    # ── Зручний прямий доступ до Scheduler для роутерів ─────────────────────

    def pause(self, account_id: str)  -> bool: return self._scheduler.pause_account(account_id)
    def resume(self, account_id: str) -> bool: return self._scheduler.resume_account(account_id)
