"""
account.py — незмінний. Залишається точно як є.

Account не знає про Scheduler, Profession чи EventBus.
Це правильно — Account є чистою моделлю даних + сесії.

Єдина зміна: видалено старі typing imports якщо вони були пов'язані
з Profession dataclass. Account їх не використовував напряму.
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from src.core.config.app import AppConfig
from src.core.config.bot import BotConfig
from src.core.inventory.model import DynamicInventories as Inventories
from src.core.tasks.stats import stats_factory, DynamicStats as Stats
from src.database.repository.factory import Repositories
from src.core.status import AccountStatus
from src.core.logging.loggers import get_account_logger

if TYPE_CHECKING:
    from src.mangabuff.session import BotSession


class Account:
    def __init__(
        self,
        account_id: str,
        bot_config:  BotConfig,
        app_config:  AppConfig,
        repo:        Repositories,
    ):
        self.account_id  = account_id
        self.status      = AccountStatus.IDLE
        self.error:      Optional[str] = None
        self.bot_config  = bot_config
        self.app_config  = app_config
        self.repo        = repo
        self.inventories: Inventories = self.repo.inventory.load(self.account_id)
        self.recorder:    Stats       = stats_factory.build()
        self._session:    Optional["BotSession"] = None
        self._log = get_account_logger(account_id)

    @property
    def inventory(self) -> Inventories:
        return self.inventories

    @property
    def session(self) -> "BotSession":
        assert self._session is not None, f"[{self.account_id}] Сесія не встановлена"
        return self._session

    def connect(self) -> bool:
        try:
            from src.mangabuff.session import BotSession
            session = BotSession(self.bot_config, self.app_config)

            if self.bot_config.network.proxy:
                if not session.check_proxy():
                    session.close()
                    return self._fail("Проксі недоступне або не працює")

            session.authenticate()
            self._session = session
            self.status   = AccountStatus.IDLE
            self.error    = None
            self._log.info("✅ Підключено")
            return True
        except PermissionError:
            return self._fail("Авторизація провалилась")
        except Exception as e:
            return self._fail(f"Помилка підключення: {e}")

    @property
    def is_connected(self) -> bool:
        return self._session is not None

    def disconnect(self) -> None:
        if self._session:
            self._session.close()
            self._session = None
            self._log.info("🔌 Відключено")

    def mark_working(self) -> None:
        self.status = AccountStatus.WORKING

    def mark_idle(self) -> None:
        self.status = AccountStatus.IDLE

    def mark_dead(self, reason: str) -> None:
        self.status = AccountStatus.DEAD
        self.error  = reason
        self._log.critical(f"💀 {reason}")

    def _fail(self, reason: str) -> bool:
        self.status = AccountStatus.ERROR
        self.error  = reason
        self._log.error(f"❌ {reason}")
        return False

    def __repr__(self) -> str:
        return (
            f"<Account id={self.account_id!r} "
            f"status={self.status.name} | "
            f"{self.inventories.personal}>"
        )