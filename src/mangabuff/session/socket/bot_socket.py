"""
src/mangabuff/session/bot_socket.py

WebSocket-клієнт для wss://wss10.mangabuff.ru:443/

Відповідає ТІЛЬКИ за:
  - підтримку кількох WS-з'єднань одночасно ("вкладки", LRU)
  - joinRoom при connect і reconnect кожної вкладки
  - маршрутизацію вхідних подій до зареєстрованих callbacks

НЕ знає про EventBus, Account, Inventory, MessageSocket.
НЕ вирішує яку кімнату коли відкривати — це відповідальність BotAuth/BotSession.

────────────────────────────────────────────────────────────────────────────
МОДЕЛЬ "КІЛЬКА ВКЛАДОК"

Кожна сторінка сайту тримає одну кімнату (room == window.location.pathname).
BotSocket моделює це через LRU OrderedDict:
  - use_room(room): перемикає або відкриває вкладку, evict найстарішу при перевищенні ліміту
  - мережевий реконект — та сама вкладка повторює joinRoom (не навігація)
────────────────────────────────────────────────────────────────────────────
"""
from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Callable, Awaitable, Optional

import socketio  # type: ignore

from src.mangabuff.session.socket.ws_common import STATIC_WS_HEADERS, SocketEventCallback
from src.utils.logging import get_logger as log

HOME_ROOM = "/"


@dataclass
class _Tab:
    """Одна "вкладка" — одне незалежне socket.io-з'єднання, одна кімната."""
    sio:       "socketio.AsyncClient"
    connected: bool = False


def _make_debug_handler(event: str) -> Callable[[Any], Awaitable[None]]:
    async def handler(data: Any) -> None:
        log().debug(f"[socket] ← [{event}] {data}")
    return handler


class BotSocket:
    """
    Socket.IO клієнт для wss://wss10.mangabuff.ru:443/
    Авторизація: Cookie + joinRoom({room, userId}) після кожного connect.

    Публічний контракт (тільки BotAuth / BotSession):
      set_identity(user_id, cookies)  — запам'ятовує ідентичність для joinRoom
      use_room(room) -> bool          — гарантує живу вкладку з кімнатою
      on(event, cb) / off(...)        — підписка на вхідні події
      close()                         — закрити всі вкладки
    """

    WSS_URL          = "wss://wss10.mangabuff.ru:443/"
    DEFAULT_MAX_TABS = 1

    _PASSIVE_EVENTS: tuple[str, ...] = (
        "new-notify", "new-notifyClubNewCard", "new-sendNewReplyComment",
        "new-AchievementUnlocked", "new-NotifyForUsers", "new-sendNewTrade",
        "update-com-like", "new-tip-com", "update-comments", "delete-comm",
        "newLevel", "auction_bid", "new-sendNewPack",
        "new-message", "read-new-message",
        "update-fav",
        "match_found", "queue_update", "search_cancelled",
        "battle-manual-matched", "battle-manual-attack",
    )

    def __init__(self, max_tabs: int = DEFAULT_MAX_TABS) -> None:
        self._max_tabs:      int                                  = max(1, max_tabs)
        self._tabs:          OrderedDict[str, _Tab]               = OrderedDict()
        self._user_id:       Optional[str]                        = None
        self._cookies:       dict[str, str]                       = {}
        self._extra_headers: dict[str, str]                       = {}
        self._listeners:     dict[str, list[SocketEventCallback]] = {}
        self._lock:          asyncio.Lock                         = asyncio.Lock()

    # ── Властивості ───────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return any(t.connected for t in self._tabs.values())

    @property
    def open_rooms(self) -> tuple[str, ...]:
        return tuple(self._tabs.keys())

    # ── Публічний API ─────────────────────────────────────────────────────────

    def set_identity(
        self,
        user_id:       int | str | None,
        cookies:       dict[str, str] = {},
        extra_headers: Optional[dict[str, str]] = None,
    ) -> None:
        """
        Запам'ятовує user_id/cookies для joinRoom і WS-заголовків.
        Якщо user_id змінився — закриває всі старі вкладки (lazy, через ensure_future).
        """
        uid = str(user_id) if user_id is not None else None
        if extra_headers is not None:
            self._extra_headers = extra_headers
        self._cookies = cookies
        if uid != self._user_id and self._tabs:
            log().info(f"[socket] user_id змінився ({self._user_id!r} → {uid!r}), закриваю вкладки")
            asyncio.ensure_future(self._close_all_tabs())
        self._user_id = uid

    async def use_room(self, room: str) -> bool:
        """
        Гарантує живу вкладку з кімнатою `room`.
        Якщо вкладка вже відкрита — перемикається на неї (LRU move_to_end).
        Якщо немає — відкриває нову, evict найдавнішої при перевищенні ліміту.
        """
        async with self._lock:
            tab = self._tabs.get(room)
            if tab is not None and tab.connected:
                self._tabs.move_to_end(room)
                log().debug(f"[socket] перемкнувся на {room!r} (open={list(self._tabs)})")
                return True
            if tab is not None:
                del self._tabs[room]   # мертва вкладка — видаляємо

            if not await self._open_tab(room):
                return False

            while len(self._tabs) > self._max_tabs:
                old_room, old_tab = self._tabs.popitem(last=False)
                log().debug(f"[socket] LRU evict {old_room!r} (ліміт {self._max_tabs})")
                await self._close_tab(old_tab)
            return True

    def on(self, event: str, callback: SocketEventCallback) -> None:
        """Підписка на подію — спільна для всіх вкладок."""
        self._listeners.setdefault(event, []).append(callback)
        for tab in self._tabs.values():
            tab.sio.on(event, self._make_handler(event))  # type: ignore

    def off(self, event: str, callback: Optional[SocketEventCallback] = None) -> None:
        """Відписка. callback=None — видаляє всі listeners на цю подію."""
        if callback is None:
            self._listeners.pop(event, None)
        else:
            self._listeners[event] = [
                cb for cb in self._listeners.get(event, []) if cb is not callback
            ]

    async def close(self) -> None:
        """Закрити всі вкладки і скинути весь стан."""
        async with self._lock:
            await self._close_all_tabs()
            self._listeners.clear()
            self._user_id       = None
            self._cookies       = {}
            self._extra_headers = {}
        log().info("[socket] закрито")

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _open_tab(self, room: str) -> bool:
        sio = socketio.AsyncClient(
            reconnection=True, reconnection_attempts=5,
            reconnection_delay=3, reconnection_delay_max=15,
            logger=False, engineio_logger=False,
        )
        tab = _Tab(sio=sio)
        self._register_handlers(tab, room)

        cookie_str = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
        headers    = {**STATIC_WS_HEADERS, **self._extra_headers, "Cookie": cookie_str}
        try:
            await sio.connect(  # type: ignore
                self.WSS_URL, transports=["websocket"],
                headers=headers, wait_timeout=12,
            )
        except Exception as e:
            log().error(f"[socket] connect({room!r}) failed: {e}")
            return False

        self._tabs[room] = tab
        self._tabs.move_to_end(room)
        return True

    async def _close_tab(self, tab: _Tab) -> None:
        tab.connected = False
        try:
            await tab.sio.disconnect()
        except Exception:
            pass

    async def _close_all_tabs(self) -> None:
        for tab in list(self._tabs.values()):
            await self._close_tab(tab)
        self._tabs.clear()

    def _register_handlers(self, tab: _Tab, room: str) -> None:
        sio = tab.sio

        @sio.event  # type: ignore
        async def connect() -> None:  # type: ignore
            tab.connected = True
            log().info(f"[socket] вкладка підключена (user_id={self._user_id}, room={room!r})")
            # Спрацьовує і після першого підключення, і після мережевого реконекту —
            # в обох випадках сервер бачить новий sid і не пам'ятає стару кімнату.
            await self._safe_emit(tab, "joinRoom", {"room": room, "userId": self._user_id})

        @sio.event  # type: ignore
        async def disconnect() -> None:  # type: ignore
            tab.connected = False
            log().warning(f"[socket] вкладка {room!r} відключена")

        @sio.event  # type: ignore
        async def connect_error(data: Any) -> None:  # type: ignore
            tab.connected = False
            log().error(f"[socket] connect_error [{room!r}]: {data}")

        for event in self._PASSIVE_EVENTS:
            if event not in self._listeners:
                sio.on(event, _make_debug_handler(event))  # type: ignore
        for event in self._listeners:
            sio.on(event, self._make_handler(event))  # type: ignore

    def _make_handler(self, event: str) -> Callable[[Any], Awaitable[None]]:
        async def handler(data: Any) -> None:
            log().debug(f"[socket] ← [{event}] {data}")
            for cb in list(self._listeners.get(event, [])):
                try:
                    await cb(data)
                except Exception as e:
                    log().error(f"[socket] listener [{event}]: {e}")
        return handler

    async def _safe_emit(self, tab: _Tab, event: str, data: Any) -> None:
        if tab.connected:
            try:
                await tab.sio.emit(event, data)  # type: ignore
            except Exception as e:
                log().error(f"[socket] emit [{event}] failed: {e}")