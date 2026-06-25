"""
src/mangabuff/session/ws_common.py

Спільні константи і типи для обох WebSocket-клієнтів.
Імпортується з bot_socket.py і message_socket.py.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable

# Браузер завжди шле ці заголовки на WS-handshake (підтверджено HAR).
# Cookie НЕ тут — додається динамічно з http.cookies у кожному клієнті.
STATIC_WS_HEADERS: dict[str, str] = {
    "Origin":        "https://mangabuff.ru",
    "Cache-Control": "no-cache",
    "Pragma":        "no-cache",
}

SocketEventCallback = Callable[[Any], Awaitable[None]]