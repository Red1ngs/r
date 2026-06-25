"""
src/mangabuff/session/http_client.py

Низькорівневий async HTTP-клієнт на curl_cffi.AsyncSession.

Відповідає ТІЛЬКИ за:
  - зберігання cookies-сесії (curl_cffi.AsyncSession)
  - відправку сирих HTTP-запитів (.get, .post, .request)
  - re-login retry при 419/401 через ін'єктований ReauthCallback
  - підняття RateLimitedError при 429

НЕ знає про:
  - csrf/xsrf токени, login flow (це BotAuth)
  - socket (це BotSocket / MessageSocket)
  - бізнес-ендпоінти (це BotSession)

Кожен запит проходить через proxy_queue_manager.enqueue_coro() —
гарантує послідовне виконання per-proxy без 429-колізій.
"""
from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import unquote

from curl_cffi.requests import AsyncSession, Response

from src.core.config.bot import BotConfig
from src.core.runtime.proxy_queue import proxy_queue_manager, RateLimitedError
from src.database.repository.session import SessionRepository
from src.mangabuff.session.http_result import HttpMethodStr
from src.mangabuff.session.request_headers import RequestHeaders, ReauthCallback
from src.utils.logging import log_request_curl, log_response_curl, log_payload_curl
from src.utils.logging import get_logger as log


class BotHttpClient:
    """
    Низькорівневий async HTTP (curl_cffi).
    Нічого не знає про auth-логіку, socket, бізнес-ендпоінти.
    """

    def __init__(
        self,
        bot_config:       BotConfig,
        session_repo:     SessionRepository,
        account_id:       str,
        headers:          RequestHeaders,
        on_reauth_needed: Optional[ReauthCallback] = None,
    ) -> None:
        self.bot_config        = bot_config
        self.session_repo      = session_repo
        self.account_id        = account_id
        self.headers           = headers
        self._client:           Optional[AsyncSession]   = None
        self._on_reauth_needed: Optional[ReauthCallback] = on_reauth_needed
        # Реєстрація воркера відкладена до першого enqueue() — завжди у правильному loop.
        self.recreate_client()

    def set_reauth_callback(self, callback: ReauthCallback) -> None:
        """BotAuth викликає це після свого створення — розриває circular init."""
        self._on_reauth_needed = callback

    # ── Властивості ───────────────────────────────────────────────────────────

    @property
    def client(self) -> Optional[AsyncSession]:
        return self._client

    @property
    def cookies(self) -> Dict[str, str]:
        return dict(self._client.cookies) if self._client else {}

    def get_xsrf_cookie(self) -> Optional[str]:
        if not self._client:
            return None
        if xsrf := self._client.cookies.get("XSRF-TOKEN", domain=self.bot_config.client.host):
            return unquote(xsrf)
        return None

    def update_cookies(self, cookies: Dict[str, str]) -> None:
        if self._client:
            self._client.cookies.update(cookies)
            
    # ── Utils / Proxy ─────────────────────────────────────────────────────────

    async def check_proxy(self) -> bool:
        """
        Перевіряє що проксі реально працює і відрізняється від реального IP.
        Використовує окремі чисті сесії — не торкається cookies основного клієнта.
        """
        if not self.bot_config.network.proxy:
            return True
        try:
            import curl_cffi.requests as _cffi
            proxy = self.bot_config.network.proxy
            async with _cffi.AsyncSession(
                proxies={"https": proxy, "http": proxy}
            ) as proxy_session:
                proxy_r = await proxy_session.get("https://api.ipify.org", timeout=10)
            proxy_ip = proxy_r.text.strip()

            async with _cffi.AsyncSession() as session:
                real_r = await session.get("https://api.ipify.org", timeout=10)
            real_ip = real_r.text.strip()

            if proxy_ip == real_ip:
                log().error(
                    f"  → Proxy IP збігається з реальним ({real_ip}) — проксі не працює"
                )
                return False

            log().info(f"  → Proxy IP: {proxy_ip} (реальний: {real_ip}) ✅")
            return True
        except Exception as e:
            log().error(f"  → Proxy check failed: {e}")
            return False

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def recreate_client(self) -> None:
        """Створює новий AsyncSession, переносячи cookies зі старого або з БД."""
        saved: Dict[str, str] = {}
        if self._client is not None:
            saved        = dict(self._client.cookies)
            self._client = None
        else:
            db = self.session_repo.load(self.account_id)
            if db:
                saved = db
                log().info(f"  → [{self.account_id}] restored {len(db)} cookies from DB")

        proxy = self.bot_config.network.proxy
        self._client = AsyncSession(
            headers=self.bot_config.browser.to_dict(),
            cookies=saved,
            proxies={"https": proxy, "http": proxy} if proxy else None,
            timeout=self.bot_config.network.timeout,
            allow_redirects=True,
            impersonate="chrome120",
        )

    def close(self) -> None:
        self._client = None

    # ── Internal ──────────────────────────────────────────────────────────────

    @staticmethod
    def _json(r: Response) -> dict[str, Any]:
        return r.json()  # type: ignore[no-any-return]

    def _url(self, url: str) -> str:
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"{self.bot_config.client.base_url.rstrip('/')}/{url.lstrip('/')}"

    async def _reauth_retry(
        self, method: HttpMethodStr, url: str, full_url: str, label: str, **kw: Any
    ) -> Response:
        """Повторний запит після успішного re-login — оновлює CSRF у заголовках."""
        if "headers" in kw:
            if self.headers.csrf_token:
                kw["headers"]["X-CSRF-TOKEN"] = self.headers.csrf_token
            if self.headers.xsrf_token:
                kw["headers"]["X-XSRF-TOKEN"] = self.headers.xsrf_token
        log_payload_curl(method, full_url, params=kw.get("params"), data=kw.get("data"), json_body=kw.get("json"))
        t = log_request_curl(method, full_url, kw.get("headers", {}))
        r = await proxy_queue_manager.enqueue_coro(
            proxy=self.bot_config.network.proxy,
            coro=self._client.request(method, full_url, **kw),  # type: ignore[union-attr]
            label=f"{method} {url} ({label})",
        )
        log_response_curl(r.status_code, full_url, r.content, r.headers.get("content-type", ""), t)
        return r

    async def _request(self, method: HttpMethodStr, url: str, **kw: Any) -> Response:
        assert self._client
        full_url = self._url(url)

        if xsrf := self.get_xsrf_cookie():
            self.headers.xsrf_token = xsrf

        log_payload_curl(method, full_url, params=kw.get("params"), data=kw.get("data"), json_body=kw.get("json"))
        t = log_request_curl(method, full_url, kw.get("headers", {}))
        r = await proxy_queue_manager.enqueue_coro(
            proxy=self.bot_config.network.proxy,
            coro=self._client.request(method, full_url, **kw),  # type: ignore[union-attr]
            label=f"{method} {url}",
        )
        log_response_curl(r.status_code, full_url, r.content, r.headers.get("content-type", ""), t)

        if r.status_code == 419:
            log().warning("  → 419 CSRF expired — re-logging in")
            if not await self._reauth():
                self.session_repo.invalidate(self.account_id)
                raise PermissionError("Session expired and re-login failed")
            r = await self._reauth_retry(method, url, full_url, "retry after 419", **kw)

        if r.status_code == 401:
            log().warning("  → 401 Unauthenticated — re-logging in")
            if not await self._reauth():
                self.session_repo.invalidate(self.account_id)
                raise PermissionError("Session expired and re-login failed")
            r = await self._reauth_retry(method, url, full_url, "retry after 401", **kw)
            if r.status_code == 401:
                self.session_repo.invalidate(self.account_id)
                raise PermissionError("Session expired and re-login failed")

        if r.status_code == 429:
            raise RateLimitedError(float(r.headers.get("Retry-After", 15.0)))

        if r.status_code == 200:
            self.headers.referer = full_url

        return r

    async def _reauth(self) -> bool:
        if self._on_reauth_needed is None:
            log().error("  → reauth needed але callback не підключено")
            return False
        return await self._on_reauth_needed()

    # ── Публічний API ─────────────────────────────────────────────────────────

    async def get(self, url: str, **kw: Any) -> Response:
        kw.setdefault("headers", self.headers.get_navigation())
        return await self._request("GET", url, **kw)

    async def post(self, url: str, **kw: Any) -> Response:
        kw.setdefault("headers", self.headers.get_ajax(is_post=True))
        return await self._request("POST", url, **kw)