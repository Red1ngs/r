"""
src/mangabuff/session/message_socket.py

WebSocket-клієнт для wss://wss.mangabuff.ru:2087/

Відповідає ТІЛЬКИ за:
  - підключення до сервера особистих повідомлень
  - авторизацію через ?token=<dialog_token> у query-рядку
  - emit send-message / read-message
  - доставку вхідних подій new-message / read-new-message

Принципові відмінності від BotSocket:
  - немає кімнат — одне з'єднання = один відкритий діалог
  - немає LRU вкладок — відкривається/закривається разом з діалогом
  - авторизація через URL, не через Cookie+joinRoom
  - dialog_token береться з data-dialog-token у HTML /messages/<user_id>

НЕ знає про BotSocket, BotAuth, BotSession.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable, Awaitable, Optional

import socketio  # type: ignore

from src.mangabuff.session.socket.ws_common import STATIC_WS_HEADERS, SocketEventCallback
from src.utils.logging import get_logger as log


class MessageSocket:
    """
    Socket.IO клієнт для wss://wss.mangabuff.ru:2087/?token=<dialog_token>

    Публічний контракт (тільки BotSession):
      open(dialog_token, cookies)  — підключитись до діалогу (ідемпотентно по токену)
      close()                      — розірвати з'єднання і скинути підписки
      emit(event, data)            — send-message / read-message
      on(event, cb) / off(...)     — new-message / read-new-message
    """

    WSS_URL = "wss://wss.mangabuff.ru:2087/"

    def __init__(self) -> None:
        self._sio:          Optional[socketio.AsyncClient]       = None
        self._dialog_token: Optional[str]                        = None
        self._cookies:      dict[str, str]                       = {}
        self._connected:    bool                                 = False
        self._listeners:    dict[str, list[SocketEventCallback]] = {}
        self._lock:         asyncio.Lock                         = asyncio.Lock()

    # ── Властивості ───────────────────────────────────────────────────────────

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def current_token(self) -> Optional[str]:
        return self._dialog_token

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def open(self, dialog_token: str, cookies: dict[str, str]) -> bool:
        """
        Підключитись до діалогу з даним токеном.

        Ідемпотентно:
          - той самий токен вже відкритий → нічого не робить, повертає True
          - інший токен → закриває старе з'єднання, відкриває нове
        """
        async with self._lock:
            if self._connected and self._dialog_token == dialog_token:
                log().debug(f"[msg-socket] вже відкрито {dialog_token!r}")
                return True
            if self._sio:
                await self._disconnect()
            self._dialog_token = dialog_token
            self._cookies      = cookies
            return await self._connect()

    async def close(self) -> None:
        """Закрити з'єднання і скинути всі підписки."""
        async with self._lock:
            await self._disconnect()
            self._listeners.clear()
            self._dialog_token = None
        log().info("[msg-socket] закрито")

    # ── Події ─────────────────────────────────────────────────────────────────

    def on(self, event: str, callback: SocketEventCallback) -> None:
        """
        Підписка на подію.
        Якщо вже підключені — реєструємо в sio одразу.
        Якщо ні — реєструємо при наступному _connect().
        """
        self._listeners.setdefault(event, []).append(callback)
        if self._sio:
            self._sio.on(event, self._make_handler(event))  # type: ignore

    def off(self, event: str, callback: Optional[SocketEventCallback] = None) -> None:
        """Відписка. callback=None — видаляє всі listeners на цю подію."""
        if callback is None:
            self._listeners.pop(event, None)
        else:
            self._listeners[event] = [
                cb for cb in self._listeners.get(event, []) if cb is not callback
            ]

    async def emit(self, event: str, data: Any = None) -> None:
        """
        Відправити подію серверу.

        Використовується для:
          emit('send-message', html)    — після успішного POST /messages/<user_id>
          emit('read-message', msg_id)  — після отримання new-message
        """
        if not self._connected or not self._sio:
            log().warning(f"[msg-socket] emit [{event}] пропущено — не підключено")
            return
        try:
            await self._sio.emit(event, data)  # type: ignore
            log().debug(f"[msg-socket] → [{event}]")
        except Exception as e:
            log().error(f"[msg-socket] emit [{event}] failed: {e}")

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _connect(self) -> bool:
        sio = socketio.AsyncClient(
            reconnection=True, reconnection_attempts=5,
            reconnection_delay=3, reconnection_delay_max=15,
            logger=False, engineio_logger=False,
        )
        self._register_handlers(sio)

        cookie_str = "; ".join(f"{k}={v}" for k, v in self._cookies.items())
        # Авторизація через query-рядок — головна відмінність від BotSocket
        url     = f"{self.WSS_URL}?token={self._dialog_token}"
        headers = {**STATIC_WS_HEADERS, "Cookie": cookie_str}

        try:
            await sio.connect(  # type: ignore
                url, transports=["websocket"],
                headers=headers, wait_timeout=12,
            )
        except Exception as e:
            log().error(f"[msg-socket] connect failed (token={self._dialog_token!r}): {e}")
            return False

        self._sio = sio
        return True

    async def _disconnect(self) -> None:
        self._connected = False
        if self._sio:
            try:
                await self._sio.disconnect()  # type: ignore
            except Exception:
                pass
            self._sio = None

    def _register_handlers(self, sio: "socketio.AsyncClient") -> None:

        @sio.event  # type: ignore
        async def connect() -> None:  # type: ignore
            self._connected = True
            log().info(f"[msg-socket] підключено (token={self._dialog_token!r})")

        @sio.event  # type: ignore
        async def disconnect() -> None:  # type: ignore
            self._connected = False
            log().warning("[msg-socket] відключено")

        @sio.event  # type: ignore
        async def connect_error(data: Any) -> None:  # type: ignore
            self._connected = False
            log().error(f"[msg-socket] connect_error: {data}")

        # Підписки зареєстровані до open() — навішуємо одразу
        for event in self._listeners:
            sio.on(event, self._make_handler(event))  # type: ignore

    def _make_handler(self, event: str) -> Callable[[Any], Awaitable[None]]:
        async def handler(data: Any) -> None:
            log().debug(f"[msg-socket] ← [{event}] {data}")
            for cb in list(self._listeners.get(event, [])):
                try:
                    await cb(data)
                except Exception as e:
                    log().error(f"[msg-socket] listener [{event}]: {e}")
        return handlers