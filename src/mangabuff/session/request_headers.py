"""
src/mangabuff/session/request_headers.py

Формування HTTP-заголовків для навігаційних і AJAX-запитів.
Зберігає поточний стан: referer, xsrf_token, csrf_token.

Не залежить від HTTP-клієнта чи сокета.
"""
from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, Optional

from src.core.config.bot import BotConfig


# ── Типи callback-ів (тут, бо потрібні в http_client і auth) ─────────────────

AuthSuccessCallback = Callable[[dict[str, Any]], Awaitable[Any]]
"""
Викликається BotAuth після кожного успішного check_auth().
Аргумент — dict з user_id, user_name та іншими даними з parse_main_page().
BotAuth нічого не знає про inventory чи Account — він лише сигналізує
«авторизація підтверджена».
"""

ReauthCallback = Callable[[], Awaitable[bool]]
"""
Викликається BotHttpClient коли отримав 419/401 і потребує re-login
перед повторною спробою запиту. Повертає True якщо логін успішний.
"""


class RequestHeaders:
    """
    Формує заголовки для двох типів запитів:
      get_navigation() — для GET сторінок (Sec-Fetch-Mode: navigate)
      get_ajax()       — для XHR/fetch (X-Requested-With, CSRF-токени)

    Стан (referer, xsrf_token, csrf_token) оновлюється ззовні:
      - BotHttpClient оновлює referer після кожного 200
      - BotAuth оновлює csrf_token / xsrf_token після fetch_csrf та check_auth
    """

    def __init__(self, config: BotConfig) -> None:
        self.common      = config.browser
        self.base_url    = config.client.base_url
        self.host        = config.client.host
        self.referer:    Optional[str] = None
        self.xsrf_token: Optional[str] = None
        self.csrf_token: Optional[str] = None

    def reset(self) -> None:
        """Скидає стан токенів і referer — викликається після закриття сесії."""
        self.referer    = None
        self.xsrf_token = None
        self.csrf_token = None

    def get_navigation(self) -> Dict[str, str]:
        """Заголовки для переходу між сторінками (document navigation)."""
        headers: Dict[str, str] = self.common.to_dict()
        headers.update({
            "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Sec-Fetch-Dest":            "document",
            "Sec-Fetch-Mode":            "navigate",
            "Sec-Fetch-Site":            "same-origin" if self.referer else "none",
            "Sec-Fetch-User":            "?1",
            "Upgrade-Insecure-Requests": "1",
        })
        if self.referer:
            headers["Referer"] = self.referer
        return headers

    def get_ajax(self, is_post: bool = True) -> Dict[str, str]:
        """Заголовки для AJAX/fetch-запитів (XHR, JSON, form submit)."""
        headers: Dict[str, str] = self.common.to_dict()
        headers.update({
            "Accept":           "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest":   "empty",
            "Sec-Fetch-Mode":   "cors",
            "Sec-Fetch-Site":   "same-origin",
        })
        if self.xsrf_token:
            headers["X-XSRF-TOKEN"] = self.xsrf_token
        if self.csrf_token:
            headers["X-CSRF-TOKEN"] = self.csrf_token
        if is_post:
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            headers["Origin"]       = self.base_url
        headers["Referer"] = self.referer or f"{self.base_url}/"
        return headers