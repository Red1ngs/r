"""
core_account.py — модель акаунта + сесія.

Account не знає про Scheduler, Profession чи EventBus.

Зміна відносно попередньої версії:
  - session property більше не кидає AssertionError при _session is None.
    Замість цього повертає Optional[BotSession] — відповідальність за
    перевірку наявності сесії перекладена на caller.
  - Додано safe_session property як явний контракт для коду,
"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from browserforge.headers import HeaderGenerator

from src.core.config.app import AppConfig
from src.core.config.bot import AuthConfig, BaseHeaders, BotConfig, ClientConfig, NetworkConfig, NetworkConfig
from src.core.inventory.model import DynamicInventories as Inventories
from src.core.stats import stats_factory, DynamicStats as Stats
from src.database.repository.factory import Repositories
from src.core.status import AccountStatus
from src.core.logging.loggers import get_account_logger
from src.core.runtime.event_bus import EventBus

if TYPE_CHECKING:
    from src.mangabuff.session import BotSession
    from src.core.runtime.core_service import CoreService


def generate_unique_browser() -> BaseHeaders:
    """Генерує консистентний відбиток за допомогою browserforge."""
    headers = HeaderGenerator(
        os=('windows', 'macos'),
        browser=('chrome', 'edge')
    ).generate()

    return BaseHeaders(
        user_agent=headers.get("User-Agent", ""),
        sec_ch_ua=headers.get("sec-ch-ua", ""),
        sec_ch_ua_platform=headers.get("sec-ch-ua-platform", '"Windows"'),
        sec_ch_ua_mobile=headers.get("sec-ch-ua-mobile", "?0"),
        accept_language=headers.get("Accept-Language", "uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7"),
        accept_encoding=headers.get("Accept-Encoding", "gzip, deflate, br, zstd"),
        dnt=headers.get("DNT", "1"),
    )
    

class Account:
    def __init__(
        self,
        account_id: str,
        auth:       AuthConfig,
        network:    NetworkConfig,
        app_config: AppConfig,
        repo:       Repositories,
    ):
        self.account_id:   str           = account_id
        self.status:       AccountStatus = AccountStatus.IDLE
        self.error:        Optional[str] = None
        self.app_config:   AppConfig     = app_config
        self.repo:         Repositories  = repo
        self.inventories:  Inventories   = self.repo.inventory.load(self.account_id)
        self.recorder:     Stats         = stats_factory.build()

        # Персональна шина подій акаунта. CoreService (SocketService тощо)
        # ретранслюють сюди зовнішні події (socket, webhook...), Profession
        # підписується через scheduler.subscribe() — не знаючи джерела події.
        self.event_bus:    EventBus      = EventBus()

        # CoreService-и що автоматично прив'язані до цього акаунта.
        # Заповнюється scheduler при add_account() через bind_core_services().
        self.core_services: list["CoreService"] = []

        self._network:     NetworkConfig = network
        self._auth:        AuthConfig    = auth
        self._session:     Optional["BotSession"] = None
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
    
    @property
    def bot_config(self) -> BotConfig:
        def _browser() -> BaseHeaders:
            if saved_browser := self.repo.sessions.load_browser(self.account_id):
                browser = BaseHeaders.from_dict(saved_browser)
            else:
                browser = generate_unique_browser() 
                self.repo.sessions.save_browser(self.account_id, browser.to_dict())
            return browser
        
        return BotConfig(
            client=ClientConfig(
                base_url=self.app_config.base_url,
                auth=self._auth,
            ),
            browser=_browser(),
            network=self._network,
        )
        
    async def connect(self) -> bool:
        try:
            from src.mangabuff.session import BotSession, AuthSuccessCallback
            from src.mangabuff.personal.auth_service import AuthService
            from src.utils.logging import set_http_logger

            # Шукаємо AuthService серед core_services що вже прив'язані
            # до цього акаунта (bind_core_services() викликається scheduler
            # при add_account(), до connect_account()).
            on_auth_success: Optional[AuthSuccessCallback] = None
            for svc in self.core_services:
                if isinstance(svc, AuthService):
                    on_auth_success = svc.on_auth_success
                    break

            session = BotSession(
                self.bot_config,
                self.app_config,
                self.repo.sessions,
                self.account_id,
                on_auth_success=on_auth_success,
            )

            if self.bot_config.network.proxy:
                if not await session.http.check_proxy():
                    await session.close()
                    self.mark_dead("Проксі недоступне або не працює")
                    return False

            await session.auth.authenticate()
            self._session = session
            self.status   = AccountStatus.IDLE
            self.error    = None

            # CoreService, яким потрібен доступ до сесії (наприклад SocketService
            # підписується на socket-події) — отримують сигнал тут, коли
            # self._session вже встановлено. bind() викликався раніше
            # (при add_account(), коли сесії ще не було) лише для прив'язки bot.
            for svc in self.core_services:
                on_session_ready = getattr(svc, "on_session_ready", None)
                if on_session_ready is not None:
                    await on_session_ready(self)

            set_http_logger(get_account_logger(self.account_id))
            self._log.info("✅ Підключено")
            return True
        except PermissionError:
            return self._fail("Авторизація провалилась")
        except Exception as e:
            return self._fail(f"Помилка підключення: {e}")

    async def disconnect(self) -> None:
        if self._session:
            # Даємо CoreService шанс прибрати свої socket-listeners
            # ДО того як session.close() знищить сам socket.
            for svc in self.core_services:
                on_session_closing = getattr(svc, "on_session_closing", None)
                if on_session_closing is not None:
                    await on_session_closing(self)

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