from __future__ import annotations

import functools
from enum import Enum, auto
from dataclasses import dataclass
from typing import Any, Dict, Generic, Literal, Optional, TypeVar, Callable, Awaitable, ParamSpec, Concatenate
from urllib.parse import unquote

from curl_cffi.requests import AsyncSession, Response
import curl_cffi.requests as _cffi

from src.core.config.bot import BotConfig
from src.core.config.app import AppConfig
from src.core.runtime.proxy_queue import proxy_queue_manager, RateLimitedError
from src.database.repository.session import SessionRepository
from src.mangabuff.csrf_token import parse_main_page
from src.mangabuff.daily.parser import get_claimable_day
from src.utils.log_section import section
from src.utils.logging import log_request_curl, log_response_curl, log_payload_curl  # noqa: F401
from src.utils.logging import get_logger as log


# ===========================================================================
# HttpResult — контракт повернення бізнес-методів
# ===========================================================================

class FailReason(Enum):
    NETWORK   = auto()  # таймаут, з'єднання відхилено, будь-який виняток
    AUTH      = auto()  # 419 після retry, PermissionError
    NOT_FOUND = auto()  # 404
    SERVER    = auto()  # 5xx або неочікуваний статус
    BAD_DATA  = auto()  # 200, але тіло порожнє або не те що очікували
    DENIED    = auto()  # сервер явно відмовив (403, success=false тощо)


T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

# fix #7: Literal замість HttpMethod зі сторонньої бібліотеки — уникає невідповідності типів
HttpMethodStr = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]


@dataclass(frozen=True)
class HttpResult(Generic[T]):
    ok:     bool
    data:   Optional[T]          = None
    reason: Optional[FailReason] = None

    
    def __post_init__(self) -> None:
        if self.ok and self.reason is not None:
            raise ValueError("Успішний результат не повинен мати reason")
        if not self.ok and self.reason is None:
            raise ValueError("Невдалий результат повинен мати reason")

    def __bool__(self) -> bool:
        return self.ok


# Конструктори винесені як функції — єдиний надійний спосіб
# дати Pylance повну інформацію про тип T при Generic dataclass.
def http_success(data: T) -> HttpResult[T]:
    """Створює успішний HttpResult з конкретним значенням."""
    return HttpResult(ok=True, data=data)


def http_success_none() -> HttpResult[None]:
    """Створює успішний HttpResult без даних."""
    return HttpResult(ok=True, data=None)


def http_fail(reason: FailReason) -> HttpResult[Any]:
    """Створює невдалий HttpResult."""
    return HttpResult(ok=False, reason=reason)


# fix #4: R замість T — TypeVar прив'язаний до конкретного виклику декоратора,
#          що дозволяє mypy/pyright коректно виводити тип повернення кожного методу
def http_call(
    func: Callable[Concatenate["BotSession", P], Awaitable[HttpResult[R]]],
) -> Callable[Concatenate["BotSession", P], Awaitable[HttpResult[R]]]:
    @functools.wraps(func)
    async def wrapper(self: "BotSession", *args: P.args, **kwargs: P.kwargs) -> HttpResult[R]:
        try:
            return await func(self, *args, **kwargs)
        except PermissionError:
            log().error(f"  → {func.__name__}: auth error")
            return http_fail(FailReason.AUTH)
        except Exception as e:
            log().error(f"  → {func.__name__}: {e}")
            return http_fail(FailReason.NETWORK)
    return wrapper


# ===========================================================================
# РІВЕНЬ 1: Headers
# ===========================================================================

class RequestHeaders:
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


# ===========================================================================
# РІВЕНЬ 2: ТРАНСПОРТ (HTTP, Cookies, Auth, CSRF)
# ===========================================================================

AuthSuccessCallback = Callable[[str, str], Awaitable[Any]]
"""
Викликається BotTransport після кожного успішного check_auth().
Аргументи — user_name і user_id розпізнані з HTML.

BotTransport нічого не знає про inventory чи Account —
він лише сигналізує «авторизація підтверджена». Хто підписався
(AuthService) — зберігає user_name куди потрібно.
"""


class BotTransport:
    """
    Низькорівневий async HTTP-клієнт на curl_cffi.AsyncSession.

    Відповідає ТІЛЬКИ за:
    - Зберігання сесії (cookies)
    - Логін і підтримку авторизації (419 → re-login)
    - Відправку сирих HTTP-запитів (.get, .post)

    Кожен запит проходить через proxy_queue_manager.enqueue_coro() —
    гарантує послідовне виконання запитів per-proxy без 429-колізій.
    """

    def __init__(
        self,
        bot_config:      BotConfig,
        session_repo:    SessionRepository,
        account_id:      str,
        on_auth_success: Optional[AuthSuccessCallback] = None,
    ) -> None:
        self.bot_config   = bot_config
        self.session_repo = session_repo
        self.account_id   = account_id
        self.headers      = RequestHeaders(bot_config)
        self._client:          Optional[AsyncSession]       = None
        self._on_auth_success: Optional[AuthSuccessCallback] = on_auth_success

        # НЕ викликаємо proxy_queue_manager.ensure() тут.
        # BotTransport.__init__ може виконуватись в aiogram-loop (наприклад,
        # під час hot-add акаунта через бот), тоді як реальні запити йдуть
        # з scheduler-loop. Якщо ensure() зареєструє воркер в aiogram-loop,
        # а перший enqueue() відбудеться вже в scheduler-loop — Future
        # виявиться прив'язаною до іншого loop → RuntimeError.
        # Реєстрація воркера відкладена до першого enqueue() через
        # proxy_queue_manager.enqueue_coro() → worker.enqueue() →
        # _ensure_task_started() — завжди у правильному running loop.
        self._create_client()

    def _create_client(self) -> None:
        """Створює новий AsyncSession, переносячи cookies зі старого або з БД."""
        saved_cookies: Dict[str, str] = {}
        if self._client is not None:
            # Рестарт сесії в межах одного процесу — беремо поточні cookies
            saved_cookies = dict(self._client.cookies)
            self._client  = None
        else:
            # Перший старт — намагаємось відновити сесію з БД
            db_cookies = self.session_repo.load(self.account_id)
            if db_cookies:
                saved_cookies = db_cookies
                log().info(f"  → [{self.account_id}] restored {len(db_cookies)} cookies from DB")

        proxy = self.bot_config.network.proxy
        self._client = AsyncSession(
            headers=self.bot_config.browser.to_dict(),
            cookies=saved_cookies,
            proxies={"https": proxy, "http": proxy} if proxy else None,
            timeout=self.bot_config.network.timeout,
            allow_redirects=True,
            impersonate="chrome120",
        )

    async def authenticate(self, force: bool = False) -> None:
        if not await self.login(force=force):
            self.session_repo.invalidate(self.account_id)
            await self.close()
            raise PermissionError("Authentication failed")

    async def login(self, force: bool = False) -> bool:
        if not self._client:
            return False
        
        if self.bot_config.client.cookies:
            self._client.cookies.update(self.bot_config.client.cookies)

        if not force and await self.check_auth():
            return True

        auth = self.bot_config.client.auth
        if not auth:
            log().error("No auth credentials in config")
            return False

        section(f"auth  {auth.email}")
        return await self._fetch_csrf() and await self._submit_login()

    async def _fetch_csrf(self) -> bool:
        assert self._client
        try:
            r = await self.get("/login", headers=self.headers.get_navigation())
            r.raise_for_status()
            token, _, _ = parse_main_page(r.text)
            if not token:
                return False
            self.headers.csrf_token = token
            if xsrf := self._client.cookies.get("XSRF-TOKEN"):
                self.headers.xsrf_token = unquote(xsrf)
            return True
        except Exception as e:
            log().error(f"  → _fetch_csrf error: {e}")
            return False

    async def _submit_login(self) -> bool:
        assert self._client and self.bot_config.client.auth
        self.headers.referer = f"{self.bot_config.client.base_url}/login"
        payload = {
            "email":    self.bot_config.client.auth.email,
            "password": self.bot_config.client.auth.password,
            "_token":   self.headers.csrf_token or "",
        }
        try:
            r = await self.post("/login", data=payload, headers=self.headers.get_ajax(is_post=True))
            if r.status_code not in (200, 204, 302):
                log().warning(f"  → login POST returned {r.status_code}")
                return False
            ok = await self.check_auth()
            if not ok:
                log().warning("  → login POST ok але check_auth провалився")
            return ok
        except Exception as e:
            log().error(f"  → _submit_login error: {e}")
            return False

    async def check_auth(self) -> bool:
        assert self._client
        try:
            r = await self.get(self.bot_config.client.base_url, headers=self.headers.get_navigation())
            r.raise_for_status()
            token, user_name, user_id = parse_main_page(r.text)
            if token and user_name:
                self.headers.csrf_token = token
                if xsrf := self._client.cookies.get("XSRF-TOKEN"):
                    self.headers.xsrf_token = unquote(xsrf)
                log().info(f"  → ✅ {user_name} ({user_id})")

                # Зберігаємо актуальні cookies у БД після кожного успішного check_auth.
                self.session_repo.save(self.account_id, dict(self._client.cookies))

                # Сигналізуємо підписнику (AuthService) — він сам вирішить
                # що робити з user_name. BotTransport більше нічого не знає.
                if self._on_auth_success is not None:
                    await self._on_auth_success(user_name, user_id)

                return True
            return False
        except Exception as e:
            log().error(f"  → check_auth error: {e}")
            return False

    # fix #5: close() тепер скидає весь стан headers — наступний login стартує чисто
    async def close(self) -> None:
        self._client = None
        self.headers.reset()

    # ── HTTP ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _json(r: Response) -> dict[str, Any]:
        """Повертає тіло відповіді як dict. Єдине місце де придушується Unknown від curl_cffi."""
        return r.json()  # type: ignore[no-any-return]

    def _url(self, url: str) -> str:
        if url.startswith("http://") or url.startswith("https://"):
            return url
        return f"{self.bot_config.client.base_url.rstrip('/')}/{url.lstrip('/')}"

    async def _request(self, method: HttpMethodStr, url: str, external: bool = False, **kwargs: Any) -> Response:
        assert self._client
        full_url = url if external else self._url(url)

        if external:
            kwargs.setdefault("headers", self.bot_config.browser.to_dict())
            kwargs.setdefault("proxies", None)
            log_payload_curl(method, full_url, params=kwargs.get("params"), data=kwargs.get("data"), json_body=kwargs.get("json"))
            t    = log_request_curl(method, full_url, kwargs.get("headers", {}))
            resp = await self._client.request(method, full_url, **kwargs)
            log_response_curl(resp.status_code, full_url, resp.content, resp.headers.get("content-type", ""), t)
            return resp

        if xsrf := self._client.cookies.get("XSRF-TOKEN"):
            self.headers.xsrf_token = unquote(xsrf)

        # Крок 1: payload ДО запиту
        log_payload_curl(method, full_url, params=kwargs.get("params"), data=kwargs.get("data"), json_body=kwargs.get("json"))
        # Крок 2: сам запит
        t        = log_request_curl(method, full_url, kwargs.get("headers", {}))
        response = await proxy_queue_manager.enqueue_coro(
            proxy = self.bot_config.network.proxy,
            coro  = self._client.request(method, full_url, **kwargs),  # type: ignore[union-attr]
            label = f"{method} {url}",
        )
        # Крок 3: відповідь ДО валідації статусу
        log_response_curl(response.status_code, full_url, response.content, response.headers.get("content-type", ""), t)

        if response.status_code == 419:
            log().warning("  → 419 CSRF expired — re-logging in")
            if not await self.login(force=True):
                self.session_repo.invalidate(self.account_id)
                raise PermissionError("Session expired and re-login failed")
            if "headers" in kwargs:
                if self.headers.csrf_token:
                    kwargs["headers"]["X-CSRF-TOKEN"] = self.headers.csrf_token
                if self.headers.xsrf_token:
                    kwargs["headers"]["X-XSRF-TOKEN"] = self.headers.xsrf_token
            log_payload_curl(method, full_url, params=kwargs.get("params"), data=kwargs.get("data"), json_body=kwargs.get("json"))
            t        = log_request_curl(method, full_url, kwargs.get("headers", {}))
            response = await proxy_queue_manager.enqueue_coro(
                proxy = self.bot_config.network.proxy,
                coro  = self._client.request(method, full_url, **kwargs),  # type: ignore[union-attr]
                label = f"{method} {url} (retry after 419)",
            )
            log_response_curl(response.status_code, full_url, response.content, response.headers.get("content-type", ""), t)

        if response.status_code == 429:
            raise RateLimitedError(float(response.headers.get("Retry-After", 15.0)))

        if response.status_code == 200:
            self.headers.referer = full_url
            
        return response

    async def get(self, url: str, external: bool = False, **kwargs: Any) -> Response:
        kwargs.setdefault("headers", self.headers.get_navigation())
        return await self._request("GET", url, external=external, **kwargs)

    async def post(self, url: str, external: bool = False, **kwargs: Any) -> Response:
        kwargs.setdefault("headers", self.headers.get_ajax(is_post=True))
        return await self._request("POST", url, external=external, **kwargs)


# ===========================================================================
# РІВЕНЬ 3: ENDPOINTS (Бізнес-логіка)
# ===========================================================================

class BotSession(BotTransport):
    """
    Високорівневий клас з бізнес-методами API.

    Кожен метод позначений @http_call — транспортні винятки (мережа, auth)
    перехоплює декоратор і повертає http_fail(FailReason.*).
    Метод всередині просто перевіряє статус і повертає success/fail.
    """

    def __init__(
        self,
        bot_config:      BotConfig,
        app_config:      AppConfig,
        session_repo:    SessionRepository,
        account_id:      str,
        on_auth_success: Optional[AuthSuccessCallback] = None,
    ) -> None:
        super().__init__(bot_config, session_repo, account_id, on_auth_success=on_auth_success)
        self.app_config = app_config
        self.daily  = app_config.daily
        self.reader = app_config.reader

    # ── Utils / Proxy ─────────────────────────────────────────────────────────

    async def check_proxy(self) -> bool:
        if not self.bot_config.network.proxy:
            return True
        try:
            r = await self.get("https://api.ipify.org", external=True, timeout=10)
            r.raise_for_status()
            proxy_ip = r.text.strip()

            # fix #2: AsyncSession закривається через async with — усуває витік ресурсів
            async with _cffi.AsyncSession() as session:
                real_r = await session.get("https://api.ipify.org", timeout=10)
            real_ip = real_r.text.strip()

            if proxy_ip == real_ip:
                log().error(f"  → Proxy IP збігається з реальним ({real_ip}) — проксі не працює")
                return False

            log().info(f"  → Proxy IP: {proxy_ip} (реальний: {real_ip}) ✅")
            return True
        except Exception as e:
            log().error(f"  → Proxy check failed: {e}")
            return False

    # ── Daily ─────────────────────────────────────────────────────────────────

    @http_call
    async def fetch_daily_streak(self) -> HttpResult[Optional[int]]:
        r = await self.get(self.daily.url_balance, timeout=15)
        r.raise_for_status()
        day = get_claimable_day(
            r.text,
            item_selector=self.daily.item_selector,
            claim_text=self.daily.claim_text,
            day_attr=self.daily.day_attr,
        )
        if day is not None:
            log().info(f"  → день {day} доступний")
            return http_success(int(day))
        log().info("  → бонус недоступний сьогодні")
        return http_success_none()

    @http_call
    async def claim_calendar(self, day: int | str) -> HttpResult[dict[str, Any]]:
        r = await self.post(self.daily.url_calendar_claim.format(day), timeout=15)
        if r.status_code == 200:
            log().info("  → отримано")
            return http_success(self._json(r))
        log().warning(f"  → claim_calendar: {r.status_code} {self._json(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    @http_call
    async def claim_daily(self) -> HttpResult[dict[str, Any]]:
        r = await self.post(self.daily.url_ping, timeout=15)
        if r.status_code == 200:
            log().info("  → отримано")
            return http_success(self._json(r))
        log().warning(f"  → claim_daily: {r.status_code} {self._json(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    # ── Reader ────────────────────────────────────────────────────────────────

    # fix #8: dict[str, Any] замість голого dict — точніша типізація
    @http_call
    async def submit_add_history(self, items: list[dict[str, Any]]) -> HttpResult[dict[str, Any]]:
        body = {
            f"items[{i}][{k}]": v
            for i, item in enumerate(items)
            for k, v in item.items()
        }
        r = await self.post(self.reader.url_add_history, data=body)
        if r.status_code == 200:
            data = self._json(r) if r.content else {}
            return http_success(data)
        log().warning(f"  → submit_add_history: {r.status_code}")
        return http_fail(FailReason.SERVER)

    @http_call
    async def claim_candy(self, token: str) -> HttpResult[dict[str, Any]]:
        r = await self.post(self.reader.parsing.url_candy_claim, data={"token": token}, timeout=15)
        if r.status_code == 200:
            data: dict[str, Any] = self._json(r) if r.content else {}
            log().info("  → цукерка отримана")
            return http_success(data)
        log().warning(f"  → claim_candy: {r.status_code} {self._json(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    @http_call
    async def fetch_manga_catalog(self, page: int = 1) -> HttpResult[str]:
        r = await self.get(self.reader.parsing.url_catalog, params={"page": page}, timeout=15)
        r.raise_for_status()
        return http_success(r.text)

    # fix #1: доданий @http_call — метод тепер захищений від власних несподіваних винятків
    @http_call
    async def fetch_manga_chapters(self, translit_name: str, manga_data_id: int) -> HttpResult[str]:
        page = await self.fetch_manga_page(translit_name)
        if not page or page.data is None:
            return http_fail(FailReason.NOT_FOUND)
        more = await self._fetch_more_chapters(manga_data_id)
        return http_success(page.data + (more.data or ""))

    @http_call
    async def fetch_manga_page(self, translit_name: str) -> HttpResult[str]:
        r = await self.get(
            self.reader.parsing.url_chapters.format(translit_name=translit_name),
            timeout=15,
        )
        r.raise_for_status()
        self.headers.referer = str(r.url)
        return http_success(r.text)

    @http_call
    async def _fetch_more_chapters(self, manga_data_id: int) -> HttpResult[str]:
        r = await self.post(
            self.reader.parsing.url_chapters_load,
            data={"manga_id": manga_data_id},
            timeout=15,
        )
        r.raise_for_status()
        return http_success(self._json(r).get("content", ""))

    # ── Quiz ──────────────────────────────────────────────────────────────────

    @http_call
    async def quiz_start(self) -> HttpResult[dict[str, Any]]:
        r = await self.post("/quiz/start", timeout=15)
        if r.status_code == 200:
            question = self._json(r).get("question")
            if question is None:
                return http_fail(FailReason.BAD_DATA)
            return http_success(question)
        log().warning(f"  → quiz_start: {r.status_code}")
        return http_fail(FailReason.SERVER)

    @http_call
    async def quiz_answer(self, answer: str) -> HttpResult[dict[str, Any]]:
        r = await self.post("/quiz/answer", data={"answer": answer}, timeout=15)
        if r.status_code == 200:
            return http_success(self._json(r))
        log().warning(f"  → quiz_answer: {r.status_code}")
        return http_fail(FailReason.SERVER)