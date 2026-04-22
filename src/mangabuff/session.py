"""
BotSession — HTTP-транспорт і ендпоінти MangaBuff.

Два рівні:

  ┌─ Transport ────────────────────────────────────────┐
  │  BotSession.get() / .post()                        │
  │  Автоматична CSRF-авторизація через BotAuth        │
  └────────────────────────────────────────────────────┘
           ↓
  ┌─ Endpoint methods ─────────────────────────────────┐
  │  bot.session.fetch_daily_streak()  -> int | None   │
  │  bot.session.claim_daily(day)      -> bool         │
  │  bot.session.fetch_profile()       -> dict         │
  │  ...                                               │
  └────────────────────────────────────────────────────┘

Таски викликають тільки ендпоінт-методи, ніколи .get()/.post() напряму.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Generator, Optional
from urllib.parse import unquote

import httpx
from httpx._types import URLTypes

from src.core.config import AppConfig, BotConfig
from src.core.rate_limiter import RateLimiter
from src.mangabuff.parsers.csrf_token import get_csrf_from_html
from src.mangabuff.parsers.daily import get_claimable_day
from src.utils.log_section import section
from src.utils.logging import log_request, log_response

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Headers helper
# ---------------------------------------------------------------------------

class RequestHeaders:
    def __init__(self, config: BotConfig):
        self.common      = config.browser
        self.base_url    = config.client.base_url
        self.host        = config.client.host
        self.referer:    Optional[str] = None
        self.xsrf_token: Optional[str] = None
        self.csrf_token: Optional[str] = None

    def get_navigation(self) -> Dict[str, str]:
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


# ---------------------------------------------------------------------------
# Auth handler (auto CSRF refresh on 419)
# ---------------------------------------------------------------------------

class BotAuth(httpx.Auth):
    def __init__(self, bot: "BotSession"):
        self.bot = bot
        self._is_relogging = False

    def auth_flow(
        self, request: httpx.Request
    ) -> Generator[httpx.Request, httpx.Response, None]:
        response = yield request

        if response.status_code == 419 and not self._is_relogging:
            log.warning("  → 419 CSRF expired — re-logging in")
            self._is_relogging = True
            success = False
            try:
                success = self.bot.login()
            except Exception as e:
                log.error(f"  → re-login failed: {e}")
            finally:
                self._is_relogging = False

            if success:
                if self.bot.headers.csrf_token:
                    request.headers["X-CSRF-TOKEN"] = self.bot.headers.csrf_token
                if self.bot.headers.xsrf_token:
                    request.headers["X-XSRF-TOKEN"] = self.bot.headers.xsrf_token
                if self.bot.client:
                    cookie_str = "; ".join(
                        f"{k}={v}" for k, v in self.bot.client.cookies.items()
                    )
                    if cookie_str:
                        request.headers["Cookie"] = cookie_str
                yield request
            else:
                log.error("  → re-login failed, request aborted")
                raise PermissionError("Session expired and re-login failed")


# ---------------------------------------------------------------------------
# BotSession
# ---------------------------------------------------------------------------

class BotSession:
    """
    HTTP-клієнт з авто-авторизацією.

    Публічний інтерфейс ділиться на два шари:

      1. Транспорт  — get() / post()  (використовується тільки всередині класу)
      2. Ендпоінти  — named methods   (викликаються тасками через bot.session)
    """

    def __init__(self, bot_config: BotConfig, app_config: AppConfig):
        self.bot_config    = bot_config
        self.app_config    = app_config
        
        self.daily = self.app_config.daily
        self.reader = self.app_config.reader
        
        self.headers       = RequestHeaders(bot_config)
        self.client:       Optional[httpx.Client] = None
        self.saved_cookies = httpx.Cookies()
        self._rate_limiter = RateLimiter(min_interval=1.0)
        self._create_client()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _create_client(self) -> None:
        """Створює новий httpx.Client, зберігаючи накопичені cookies."""
        if self.client is not None:
            self._update_cookies(self.client.cookies)
            self.client.close()

        self.client = httpx.Client(
            base_url=self.bot_config.client.base_url,
            http2=False,
            proxy=self.bot_config.network.proxy,
            timeout=self.bot_config.network.timeout,
            follow_redirects=True,
            cookies=self.saved_cookies,
            auth=BotAuth(self),
            event_hooks={
                "request":  [log_request],
                "response": [log_response],
            },
        )

    def authenticate(self, force: bool = False) -> None:
        """Публічний entry-point для AccountPull.connect()."""
        if not self.login(force=force):
            self.close()
            raise PermissionError("Authentication failed")

    def login(self, force: bool = False) -> bool:
        if not self.client:
            return False

        if self.bot_config.client.cookies:
            self._update_cookies(self.bot_config.client.cookies)

        if not force and self.check_auth():
            return True

        auth = self.bot_config.client.auth
        if not auth:
            log.error("No auth credentials in config")
            return False

        section(f"auth  {auth.email}")
        return self._fetch_csrf() and self._submit_login()

    def _fetch_csrf(self) -> bool:
        assert self.client
        self.headers.referer = self.bot_config.client.base_url
        try:
            r = self.client.get("/login", headers=self.headers.get_navigation())
            r.raise_for_status()
        except Exception as e:
            log.error(f"  → GET /login failed: {e}")
            return False

        token, _ = get_csrf_from_html(r.text)
        if not token:
            log.error("  → CSRF token not found on /login")
            return False

        self.headers.csrf_token = token
        xsrf = self.client.cookies.get("XSRF-TOKEN")
        if xsrf:
            self.headers.xsrf_token = unquote(xsrf)
        return True

    def _submit_login(self) -> bool:
        assert self.client
        assert self.bot_config.client.auth

        self.headers.referer = f"{self.bot_config.client.base_url}/login"
        auth    = self.bot_config.client.auth
        payload = {
            "email":    auth.email,
            "password": auth.password,
            "_token":   self.headers.csrf_token or "",
        }

        try:
            r = self.client.post(
                "/login", data=payload,
                headers=self.headers.get_ajax(is_post=True),
            )
        except Exception as e:
            log.error(f"  → POST /login failed: {e}")
            return False

        if not self._validate_login_response(r):
            return False

        self._update_cookies(self.client.cookies)
        return self.check_auth()

    def _validate_login_response(self, response: httpx.Response) -> bool:
        if "application/json" in response.headers.get("content-type", ""):
            try:
                j = response.json()
                if j.get("errors") or j.get("status") == "error":
                    log.error(f"  → server error: {j}")
                    return False
            except Exception:
                pass
        if response.status_code not in (200, 204, 302):
            log.error(f"  → unexpected HTTP {response.status_code}")
            return False
        return True

    def check_auth(self) -> bool:
        assert self.client
        try:
            r = self.client.get(
                self.bot_config.client.base_url,
                headers=self.headers.get_navigation(),
            )
            r.raise_for_status()
        except httpx.HTTPError as e:
            log.error(f"  → check_auth request failed: {e}")
            return False
        return self._parse_auth_response(r.text)

    def _parse_auth_response(self, html: str) -> bool:
        token, user_name = get_csrf_from_html(html)
        if not token:
            log.debug("  → no CSRF token (not logged in)")
            return False
        if not user_name:
            log.debug("  → no username (guest)")
            return False
        self.headers.csrf_token = token
        log.info(f"  → ✅ {user_name}")
        return True

    def _update_cookies(self, new_cookies: dict[str, str] | httpx.Cookies) -> None:
        self.saved_cookies.update(new_cookies)
        if self.client:
            self.client.cookies.update(new_cookies)

    def close(self) -> None:
        if self.client:
            self.saved_cookies.update(self.client.cookies)
            self.client.close()
            self.client = None

    # ------------------------------------------------------------------
    # Raw transport  (private — use endpoint methods in tasks)
    # ------------------------------------------------------------------

    def _get(self, url: URLTypes, **kwargs: Any) -> httpx.Response:
        assert self.client
        xsrf = self.client.cookies.get("XSRF-TOKEN")
        if xsrf:
            self.headers.xsrf_token = unquote(xsrf)
        kwargs.setdefault("headers", self.headers.get_ajax(is_post=False))
        return self.client.get(url, **kwargs)

    def _post(self, url: URLTypes, **kwargs: Any) -> httpx.Response:
        assert self.client
        xsrf = self.client.cookies.get("XSRF-TOKEN")
        if xsrf:
            self.headers.xsrf_token = unquote(xsrf)
        kwargs.setdefault("headers", self.headers.get_ajax(is_post=True))
        return self.client.post(url, **kwargs)

    # ------------------------------------------------------------------
    # ── Endpoint methods ─────────────────────────────────────────────
    #
    # Naming convention:
    #   fetch_*   — GET,  returns parsed data or raises
    #   submit_*  — POST, returns bool (success) or raises
    #   claim_*   — POST, semantic alias for reward actions
    # ------------------------------------------------------------------

    # ── Daily bonus ──────────────────────────────────────────────────

    def fetch_daily_streak(self) -> Optional[int]:
        """
        GET /balance — повертає номер доступного дня або None.
        None означає що бонус вже отримано або недоступний.
        """
        try:
            url = self.daily.url_balance
            r = self._get(url, headers=self.headers.get_navigation(), timeout=15)
            r.raise_for_status()
            
            day = get_claimable_day(
                r.text, item_selector=self.daily.item_selector, 
                claim_text=self.daily.claim_text,
                day_attr=self.daily.day_attr
            )
            if day is not None:
                log.info(f"  → день {day} доступний")
                return int(day)
            log.info("  → бонус недоступний сьогодні")
            return None
        except Exception as e:
            log.error(f"  → /balance error: {e}")
            return None

    def claim_daily(self, day: int | str) -> bool:
        """POST /balance/claim/{day} — отримати щоденний бонус."""
        try:
            url = self.daily.url_claim.format(day)
            r = self._post(url, headers=self.headers.get_navigation(), timeout=15)
            if r.status_code == 200:
                log.info("  → отримано")
                return True
            msg = ""
            try:
                msg = r.json().get("message", "")
            except Exception:
                pass
            log.warning(f"  → {r.status_code}  {msg}")
            return False
        except Exception as e:
            log.error(f"  → claim error: {e}")
            return False

    # ── Reader ───────────────────────────────────────────────────────

    def submit_add_history(self, items: list[dict[str, Any]]) -> None:
        """POST /addHistory — прочитати глави."""
        try:
            url = self.reader.url_add_history
            
            body = {}
            for i, item in enumerate(items):
                body[f"items[{i}][manga_id]"]   = item["manga_id"]
                body[f"items[{i}][chapter_id]"] = item["chapter_id"]
                
            response = self._post(url, data=body)
            
            if response.status_code != 200:
                return None
            
            if not response.content:
                return None
    
            return response.json()
        except Exception:
            return None
        
    def fetch_manga_catalog(self, page: int = 1) -> Optional[str]:
        """GET /manga?page={page} — отримати HTML каталогу."""
        try:
            url = self.reader.parsing.url_catalog
            r = self._get(url, headers=self.headers.get_navigation(), params={"page": page}, timeout=15)
            r.raise_for_status()
            return r.text
        except Exception as e:
            log.error(f"  → fetch_manga_catalog error: {e}")
            return None
        
    def fetch_manga_chapters(self, translit_name: str, manga_data_id: int) -> Optional[str]:
        """GET + POST — повний HTML глав манги."""
        page_html = self._fetch_manga_page(translit_name)
        if not page_html:
            return None

        more_html = self._fetch_more_chapters(manga_data_id)
        return page_html + more_html


    def _fetch_manga_page(self, translit_name: str) -> Optional[str]:
        """GET /manga/{translit_name} — HTML сторінки манги."""
        try:
            url = self.reader.parsing.url_chapters.format(translit_name=translit_name)
            r = self._get(url, headers=self.headers.get_navigation(), timeout=15)
            r.raise_for_status()
            self.headers.referer = str(r.url)
            return r.text
        except Exception as e:
            log.error(f"  → fetch_manga_page error: {e}")
            return None


    def _fetch_more_chapters(self, manga_data_id: int) -> str:
        """POST /chapters/load — підвантажує приховані глави."""
        try:
            r = self._post(self.reader.parsing.url_chapters_load, data={"manga_id": manga_data_id}, timeout=15)
            r.raise_for_status()
            return r.json().get("content", "")
        except Exception as e:
            log.warning(f"  → fetch_more_chapters error: {e}")
            return ""