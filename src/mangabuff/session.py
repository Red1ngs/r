from __future__ import annotations

import logging
import threading
from typing import Any, Dict, Generator, Optional
from urllib.parse import unquote

import httpx
from httpx._types import URLTypes

from src.core.config.bot import BotConfig
from src.core.config.app import AppConfig
from src.core.utils.rate_limiter import RateLimiter
from src.mangabuff.csrf_token import get_csrf_from_html
from src.mangabuff.daily.parser import get_claimable_day
from src.utils.log_section import section
from src.utils.logging import log_request, log_response

log = logging.getLogger(__name__)


# ===========================================================================
# ДОПОМІЖНІ КЛАСИ (Headers та Auth)
# ===========================================================================

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


class BotAuth(httpx.Auth):
    """
    Автоматичне оновлення CSRF-токену при 419.

    ВИПРАВЛЕННЯ: замість спроби мутувати вже відправлений request,
    при 419 робимо re-login і yield новий request зі свіжими заголовками.
    Це єдиний надійний спосіб — httpx не гарантує, що мутація headers
    старого request об'єкта буде застосована до повторного yield.
    """

    def __init__(self, transport: "BotTransport"):
        self.transport = transport
        # Один lock на транспорт — захищає від паралельного re-login
        # (хоча кожен бот має свій потік, defensively)
        self._relogin_lock = threading.Lock()

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        response = yield request

        if response.status_code != 419:
            return

        # Намагаємось отримати lock без блокування — якщо хтось вже
        # ре-логіниться (не наш кейс, але safe), просто пропускаємо.
        acquired = self._relogin_lock.acquire(blocking=True, timeout=30)
        if not acquired:
            log.error("  → 419: не вдалось отримати relogin lock за 30с")
            return

        try:
            log.warning("  → 419 CSRF expired — re-logging in")
            success = False
            try:
                success = self.transport.login(force=True)
            except Exception as e:
                log.error(f"  → re-login failed: {e}")

            if not success:
                log.error("  → re-login failed, request aborted")
                raise PermissionError("Session expired and re-login failed")

            # Будуємо новий request з актуальними заголовками та cookies.
            # НЕ мутуємо старий request — створюємо новий через httpx.Request.
            new_headers = dict(request.headers)

            # Оновлюємо CSRF-токени
            if self.transport.headers.csrf_token:
                new_headers["X-CSRF-TOKEN"] = self.transport.headers.csrf_token
            if self.transport.headers.xsrf_token:
                new_headers["X-XSRF-TOKEN"] = self.transport.headers.xsrf_token

            # Оновлюємо cookies з клієнта
            if self.transport.client:
                cookie_str = "; ".join(
                    f"{k}={v}" for k, v in self.transport.client.cookies.items()
                )
                if cookie_str:
                    new_headers["Cookie"] = cookie_str

            new_request = httpx.Request(
                method=request.method,
                url=request.url,
                headers=new_headers,
                content=request.content,
            )
            yield new_request

        finally:
            self._relogin_lock.release()


# ===========================================================================
# РІВЕНЬ 1: ТРАНСПОРТ (HTTP, Cookies, Auth, CSRF)
# ===========================================================================

class BotTransport:
    """
    Низькорівневий HTTP-клієнт. Відповідає ТІЛЬКИ за:
    - Зберігання сесії (cookies)
    - Логін і підтримку авторизації (через BotAuth)
    - Відправку сирих HTTP-запитів (.get, .post)
    """

    def __init__(self, bot_config: BotConfig):
        self.bot_config    = bot_config
        self.headers       = RequestHeaders(bot_config)
        self.client:       Optional[httpx.Client] = None
        self.saved_cookies = httpx.Cookies()
        self._rate_limiter = RateLimiter(min_interval=1.0)

        self._create_client()

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
            event_hooks={"request": [log_request], "response": [log_response]},
        )

    def authenticate(self, force: bool = False) -> None:
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
            r = self.get("/login", headers=self.headers.get_navigation())
            r.raise_for_status()
            token, _ = get_csrf_from_html(r.text)
            if not token:
                return False

            self.headers.csrf_token = token
            xsrf = self.client.cookies.get("XSRF-TOKEN")
            if xsrf:
                self.headers.xsrf_token = unquote(xsrf)
            return True
        except Exception as e:
            log.error(f"  → _fetch_csrf error: {e}")
            return False

    def _submit_login(self) -> bool:
        assert self.client and self.bot_config.client.auth
        self.headers.referer = f"{self.bot_config.client.base_url}/login"
        payload = {
            "email":    self.bot_config.client.auth.email,
            "password": self.bot_config.client.auth.password,
            "_token":   self.headers.csrf_token or "",
        }
        try:
            r = self.post("/login", data=payload, headers=self.headers.get_ajax(is_post=True))
            if r.status_code not in (200, 204, 302):
                log.warning(f"  → login POST returned {r.status_code}")
                return False
            self._update_cookies(self.client.cookies)
            ok = self.check_auth()
            if not ok:
                log.warning("  → login POST ok але check_auth провалився")
            return ok
        except Exception as e:
            log.error(f"  → _submit_login error: {e}")
            return False

    def check_auth(self) -> bool:
        assert self.client
        try:
            r = self.get(self.bot_config.client.base_url, headers=self.headers.get_navigation())
            r.raise_for_status()
            token, user_name = get_csrf_from_html(r.text)
            if token and user_name:
                self.headers.csrf_token = token
                # Оновлюємо XSRF з cookies після успішного GET
                xsrf = self.client.cookies.get("XSRF-TOKEN")
                if xsrf:
                    self.headers.xsrf_token = unquote(xsrf)
                log.info(f"  → ✅ {user_name}")
                return True
            return False
        except Exception as e:
            log.error(f"  → check_auth error: {e}")
            return False

    def _update_cookies(self, new_cookies: dict[str, str] | httpx.Cookies) -> None:
        self.saved_cookies.update(new_cookies)
        if self.client:
            self.client.cookies.update(new_cookies)

    def close(self) -> None:
        if self.client:
            self._update_cookies(self.client.cookies)
            self.client.close()
            self.client = None

    # ------------------------------------------------------------------
    # Внутрішні методи HTTP
    # ------------------------------------------------------------------

    def get(self, url: URLTypes, external: bool = False, **kwargs: Any) -> httpx.Response:
        assert self.client

        if not external:
            self._rate_limiter.wait()

        if external:
            kwargs.setdefault("headers", self.bot_config.browser.to_dict())
            kwargs["auth"] = None
        else:
            if xsrf := self.client.cookies.get("XSRF-TOKEN"):
                self.headers.xsrf_token = unquote(xsrf)
            kwargs.setdefault("headers", self.headers.get_navigation())

        return self.client.get(url, **kwargs)

    def post(self, url: URLTypes, external: bool = False, **kwargs: Any) -> httpx.Response:
        assert self.client

        if not external:
            self._rate_limiter.wait()

        if external:
            kwargs.setdefault("headers", self.bot_config.browser.to_dict())
            kwargs["auth"] = None
        else:
            if xsrf := self.client.cookies.get("XSRF-TOKEN"):
                self.headers.xsrf_token = unquote(xsrf)
            kwargs.setdefault("headers", self.headers.get_ajax(is_post=True))

        return self.client.post(url, **kwargs)


# ===========================================================================
# РІВЕНЬ 2: ENDPOINTS (Бізнес-логіка)
# ===========================================================================

class BotSession(BotTransport):
    """
    Високорівневий клас, який містить виключно бізнес-методи (API).
    """

    def __init__(self, bot_config: BotConfig, app_config: AppConfig):
        super().__init__(bot_config)

        self.app_config = app_config
        self.daily  = self.app_config.daily
        self.reader = self.app_config.reader

    # ── Utils / Proxy ────────────────────────────────────────────────

    def check_proxy(self) -> bool:
        try:
            r = self.get("https://api.ipify.org", external=True, timeout=10)
            r.raise_for_status()
            log.info(f"  → Proxy IP: {r.text.strip()}")
            return True
        except Exception as e:
            log.error(f"  → Proxy check failed: {e}")
            return False

    # ── Daily Bonus ──────────────────────────────────────────────────

    def fetch_daily_streak(self) -> Optional[int]:
        try:
            url = self.daily.url_balance
            r = self.get(url, timeout=15)
            r.raise_for_status()

            day = get_claimable_day(
                r.text,
                item_selector=self.daily.item_selector,
                claim_text=self.daily.claim_text,
                day_attr=self.daily.day_attr,
            )
            if day is not None:
                log.info(f"  → день {day} доступний")
                return int(day)

            log.info("  → бонус недоступний сьогодні")
            return None
        except Exception as e:
            log.error(f"  → /balance error: {e}")
            return None

    def claim_calendar(self, day: int | str) -> tuple[bool, dict[str, Any]]:
        try:
            url = self.daily.url_calendar_claim.format(day)
            r = self.post(url, timeout=15)
            if r.status_code == 200:
                log.info("  → отримано")
                return True, r.json()
            log.warning(f"  → {r.status_code} {r.json().get('message', '')}")
            return False, r.json()
        except Exception as e:
            log.error(f"  → claim_calendar error: {e}")
            return False, {}

    def claim_daily(self) -> tuple[bool, dict[str, Any]]:
        try:
            url = self.daily.url_ping
            r = self.post(url, timeout=15)
            if r.status_code == 200:
                log.info("  → отримано")
                return True, r.json()
            log.warning(f"  → {r.status_code} {r.json().get('message', '')}")
            return False, r.json()
        except Exception as e:
            log.error(f"  → claim_daily error: {e}")
            return False, {}

    # ── Reader ───────────────────────────────────────────────────────

    def submit_add_history(self, items: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
        try:
            body = {
                f"items[{i}][{k}]": v
                for i, item in enumerate(items)
                for k, v in item.items()
            }
            r = self.post(self.reader.url_add_history, data=body)
            if r.status_code == 200 and r.content:
                return r.json()
            return None
        except Exception:
            return None

    def fetch_manga_catalog(self, page: int = 1) -> Optional[str]:
        try:
            url = self.reader.parsing.url_catalog
            r = self.get(url, params={"page": page}, timeout=15)
            r.raise_for_status()
            return r.text
        except Exception as e:
            log.error(f"  → fetch_manga_catalog error: {e}")
            return None

    def fetch_manga_chapters(self, translit_name: str, manga_data_id: int) -> Optional[str]:
        page_html = self._fetch_manga_page(translit_name)
        if not page_html:
            return None
        more_html = self._fetch_more_chapters(manga_data_id)
        return page_html + more_html

    def _fetch_manga_page(self, translit_name: str) -> Optional[str]:
        try:
            url = self.reader.parsing.url_chapters.format(translit_name=translit_name)
            r = self.get(url, timeout=15)
            r.raise_for_status()
            self.headers.referer = str(r.url)
            return r.text
        except Exception as e:
            log.error(f"  → fetch_manga_page error: {e}")
            return None

    def _fetch_more_chapters(self, manga_data_id: int) -> str:
        try:
            url = self.reader.parsing.url_chapters_load
            r = self.post(url, data={"manga_id": manga_data_id}, timeout=15)
            r.raise_for_status()
            return r.json().get("content", "")
        except Exception as e:
            log.warning(f"  → fetch_more_chapters error: {e}")
            return ""
        
    # ── Quiz ───────────────────────────────────────────────────────
    
    def quiz_start(self) -> Optional[dict[str, Any]]:
        """
        POST /quiz/start — відкриває нову сесію та повертає перше питання.
        Повертає dict питання або None при помилці.
        """
        try:
            r = self.post("/quiz/start", timeout=15)
            if r.status_code == 200:
                return r.json().get("question")
            log.warning(f"  → quiz_start: {r.status_code}")
            return None
        except Exception as e:
            log.error(f"  → quiz_start error: {e}")
            return None
        
    def quiz_answer(self, answer: str) -> Optional[dict[str, Any]]:
        """
        POST /quiz/answer  body: answer=<текст>
        Повертає повну відповідь сервера або None при мережевій помилці.
        """
        try:
            r = self.post(
                "/quiz/answer",
                data={"answer": answer},
                timeout=15,
            )
            if r.status_code == 200:
                return r.json()
            log.warning(f"  → quiz_answer: {r.status_code}")
            return None
        except Exception as e:
            log.error(f"  → quiz_answer error: {e}")
            return None
 