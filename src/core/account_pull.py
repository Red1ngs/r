from __future__ import annotations

from typing import Optional

from src.core.config import BotConfig, AppConfig
from src.core.inventory.model import Inventories
from src.core.inventory.store import InventoryStore
from src.core.status import AccountStatus
from src.core.logging.loggers import get_account_logger


class AccountPull:
    """
    Стан одного акаунта.
    bot.inventories.personal.want_list
    bot.inventories.alliance.shared_items
    bot.inventories.library.reading_progress
    """

    def __init__(self, account_id: str, bot_config: BotConfig, app_config: AppConfig, store: InventoryStore):
        self.account_id  = account_id
        self.bot_config  = bot_config
        self.app_config  = app_config
        self.store       = store
        self.status      = AccountStatus.IDLE
        self.error:      Optional[str] = None

        self.inventories: Inventories = store.load()
        self._session    = None

        # Власний логер акаунта → logs/accounts/{account_id}.log
        self._log = get_account_logger(account_id)

    @property
    def inventory(self) -> "Inventories":
        return self.inventories

    @property
    def session(self):
        assert self._session is not None, f"[{self.account_id}] Сесія не встановлена"
        return self._session

    # ------------------------------------------------------------------

    def connect(self) -> bool:
        try:
            from src.mangabuff.session import BotSession
            self._session = BotSession(self.bot_config, self.app_config)
            self._session.authenticate()
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
            f"<AccountPull id={self.account_id!r} "
            f"status={self.status.name} | "
            f"{self.inventories.personal}>"
        )