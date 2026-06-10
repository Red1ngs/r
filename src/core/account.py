"""
account.py — модель акаунта + сесія.

Account не знає про Scheduler, Profession чи EventBus.

Зміна відносно попередньої версії:
  - session property більше не кидає AssertionError при _session is None.
    Замість цього повертає Optional[BotSession] — відповідальність за
    перевірку наявності сесії перекладена на caller.
  - Додано safe_session property як явний контракт для коду,
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from src.core.config.app import AppConfig
from src.core.config.bot import BotConfig
from src.core.inventory.model import DynamicInventories as Inventories
from src.core.stats import stats_factory, DynamicStats as Stats
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
    def session(self) -> Optional["BotSession"]:
        """
        Повертає активну сесію або None якщо акаунт відключено.

        Callers що очікують сесію завжди повинні перевіряти:
            if not bot.is_connected:
                return  # або логувати попередження

        Для коду де відсутність сесії є справжньою помилкою —
        використовуй safe_session.
        """
        return self._session

    @property
    def safe_session(self) -> "BotSession":
        """
        Повертає сесію або кидає RuntimeError якщо відключено.

        Використовується у місцях де сесія має бути гарантована
        (наприклад, одразу після connect() в startup tasks).
        """
        if self._session is None:
            raise RuntimeError(
                f"[{self.account_id}] Сесія не встановлена. "
                "Переконайся що connect() був викликаний перед використанням сесії."
            )
        return self._session

    @property
    def is_connected(self) -> bool:
        return self._session is not None

    async def connect(self) -> bool:
        try:
            from src.mangabuff.session import BotSession
            from src.utils.logging import set_http_logger

            session = BotSession(self.bot_config, self.app_config)

            if self.bot_config.network.proxy:
                if not await session.check_proxy():
                    await session.close()
                    return self._fail("Проксі недоступне або не працює")

            await session.authenticate()
            self._session = session
            self.status   = AccountStatus.IDLE
            self.error    = None

            # FIX: прив'язуємо HTTP-логер до акаунта одразу після connect().
            # set_http_logger() використовує ContextVar — логи session.py
            # тепер потраплять у logs/accounts/{account_id}.log.
            set_http_logger(get_account_logger(self.account_id))

            self._log.info("✅ Підключено")
            return True
        except PermissionError:
            return self._fail("Авторизація провалилась")
        except Exception as e:
            return self._fail(f"Помилка підключення: {e}")

    async def disconnect(self) -> None:
        if self._session:
            await self._session.close()
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