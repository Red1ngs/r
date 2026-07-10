"""
src/mangabuff/session/__init__.py

Публічний API пакету. Імпортуй звідси — не з окремих модулів.
"""
from src.mangabuff.session.bot_session import BotSession
from src.mangabuff.session.http_result import (
    HttpResult,
    FailReason,
    http_success,
    http_success_none,
    http_fail,
    http_call,
)
from src.mangabuff.session.request_headers import (
    AuthSuccessCallback,
    ReauthCallback,
    ProxyFatalCallback,
)
from src.mangabuff.session.socket.bot_socket import BotSocket, HOME_ROOM
from src.mangabuff.session.socket.message_socket import MessageSocket
from src.core.runtime.proxy_queue import ProxyFatalError

__all__ = [
    "BotSession",
    "HttpResult", "FailReason", "http_success", "http_success_none", "http_fail", "http_call",
    "AuthSuccessCallback", "ReauthCallback", "ProxyFatalCallback", "ProxyFatalError",
    "BotSocket", "HOME_ROOM",
    "MessageSocket",
]