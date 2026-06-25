"""
src/mangabuff/socket/socket_service.py — SocketService (CoreService).

Роль:
    BotSocket     — транспорт: connect/reconnect, joinRoom, on/off (нічого
                    не знає про Account чи EventBus).
    SocketService — міст: знає про bot.event_bus і bot.session.socket,
                    ретранслює socket-події в bus-події.

Чому НЕ підписуємось у bind():
    bind(bot) викликається scheduler-ом при add_account() — ДО connect().
    На цей момент bot.session ще None (сесія ще не створена), тому
    bot.session.socket недоступний.

    Підписка реально відбувається в on_session_ready(bot), який
    Account.connect() викликає вже ПІСЛЯ того як self._session
    встановлено. Симетрично — on_session_closing(bot) викликається
    в Account.disconnect() ДО session.close().

Підключення (setup.py):
    profession_registry.add_core_service(SocketService)

Як Profession підписується на socket-подію:
    async def setup(self, scheduler, account_id):
        scheduler.subscribe("socket.trade_received", self._on_trade)

    async def _on_trade(self, payload: dict) -> None:
        # payload = {**socket_data, "account_id": "..."}
        ...
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.core.runtime.core_service import CoreService
from src.core.logging.loggers import get_logger

if TYPE_CHECKING:
    from src.core.core_account import Account

log = get_logger("service.socket")


# Мапа: socket-подія → event_bus-подія.
# Додати новий рядок тут — і Profession вже може на неї підписатись.
_SOCKET_TO_BUS: dict[str, str] = {
    "new-notify":              "socket.notify",
    "new-AchievementUnlocked": "socket.achievement",
    "new-sendNewTrade":        "socket.trade_received",
    "auction_bid":             "socket.auction_bid",
    "newLevel":                "socket.level_up",
    "new-sendNewPack":         "socket.pack_received",
    "new-message":             "socket.message",
    "match_found":             "socket.match_found",
}


class SocketService(CoreService):
    """
    Прослуховує BotSocket конкретного акаунта і ретранслює події
    в bot.event_bus.

    Lifecycle:
        bind(bot)              — зберігає посилання, нічого не підписує
        on_session_ready(bot)  — сесія щойно створена → підписуємось
        on_session_closing(bot)— сесія зараз закриється → відписуємось
        unbind()                — повне звільнення (account видаляється)
    """

    def __init__(self) -> None:
        self._account: Account | None = None
        # Зберігаємо самі forwarder-callbacks щоб коректно off() саме їх,
        # а не всі listeners на подію (інші сервіси теж можуть слухати).
        self._forwarders: dict[str, Any] = {}

    @property
    def service_id(self) -> str:
        return "socket"

    # ── CoreService lifecycle ─────────────────────────────────────────────────

    async def bind(self, bot: "Account") -> None:
        """
        Лише прив'язка. Сесії (і socket) ще немає на цьому етапі —
        реальна підписка відбудеться в on_session_ready().
        """
        self._account = bot
        log.debug(f"[{bot.account_id}] SocketService: bound (очікує сесію)")

    async def unbind(self) -> None:
        """Викликається при остаточному видаленні акаунта."""
        self._forwarders.clear()
        self._account = None

    # ── Session lifecycle hooks (викликає Account.connect/disconnect) ───────────

    async def on_session_ready(self, bot: "Account") -> None:
        """
        Сесія щойно встановлена (bot.session тепер не None).
        Підписуємось на всі socket-події з нашої мапи.
        """
        session = bot.session
        if session is None:
            log.warning(f"[{bot.account_id}] on_session_ready викликано без сесії — пропускаємо")
            return

        for socket_event, bus_event in _SOCKET_TO_BUS.items():
            forwarder = self._make_forwarder(bot, bus_event)
            self._forwarders[socket_event] = forwarder
            session.socket.on(socket_event, forwarder)

        log.info(f"[{bot.account_id}] SocketService: підписано на {len(_SOCKET_TO_BUS)} подій")

    async def on_session_closing(self, bot: "Account") -> None:
        """
        Сесія зараз закриється. Знімаємо саме наші forwarder-callbacks
        (off(event, callback) — а не off(event) — щоб не зачепити чужі
        підписки на ту саму подію, якщо такі є).
        """
        session = bot.session
        if session is None:
            return

        for socket_event, forwarder in self._forwarders.items():
            session.socket.off(socket_event, forwarder)

        self._forwarders.clear()
        log.info(f"[{bot.account_id}] SocketService: відписано")

    # ── Private ───────────────────────────────────────────────────────────────

    def _make_forwarder(self, bot: "Account", bus_event: str):
        """
        Повертає async callback що нормалізує socket-дані і пробрасовує
        їх в EventBus конкретного акаунта.
        """
        async def forwarder(data: Any) -> None:
            if isinstance(data, dict):
                payload = data
            elif data is None:
                payload = {}
            else:
                payload = {"raw": data}

            payload = {**payload, "account_id": bot.account_id}

            log.debug(f"[{bot.account_id}] socket → bus [{bus_event}]: {payload}")
            try:
                await bot.event_bus.emit(bus_event, payload, source="socket")
            except Exception as e:
                log.error(f"[{bot.account_id}] event_bus.emit [{bus_event}] failed: {e}")

        return forwarder