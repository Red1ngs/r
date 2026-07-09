"""
src/mangabuff/session/bot_auth.py

Відповідає ТІЛЬКИ за:
  - логін (email/password → cookies)
  - підтримку авторизації (check_auth — чи валідна поточна сесія)
  - CSRF/XSRF токени (видобуття, оновлення в RequestHeaders)
  - виклик AuthSuccessCallback після кожного успішного check_auth()
  - кімнати сокета для auth-сторінок ("/login", HOME_ROOM)

Працює ЧЕРЕЗ BotHttpClient.get/post — нічого не знає про curl_cffi,
proxy_queue, деталі retry на 419/401 (це BotHttpClient).

BotHttpClient своєю чергою викликає login() через ReauthCallback при
419/401 — двостороння залежність розірвана callback-ом.

НЕ знає про MessageSocket, inventory, Account, бізнес-методи.

── Priority (core/runtime/proxy_queue.py) ───────────────────────────────────
Повна шкала пріоритетів черги, від найвищого до найнижчого:
    AUTH (0)          ← цей файл (BotAuth) — завжди тут, і ТІЛЬКИ тут.
    TIME_CRITICAL (5)     BotSession.quiz_start/quiz_answer — секундне вікно.
    CRITICAL (10)         BotSession: send_message, claim_daily/calendar.
    NORMAL (20)           BotSession: mining.
    BACKGROUND (30)       BotSession: reader (каталог, історія, цукерки).

BotAuth завжди працює з Priority.AUTH і ніколи не піднімається/опускається
залежно від того, хто спричинив re-login (quiz, mining, reader — не
важливо) — без валідної сесії будь-який запит все одно поверне 401/419,
тож auth має лишатись СТРОГО вище за TIME_CRITICAL, інакше сама причина
для re-login (протухла сесія) заблокує чергу для всіх, включно з квізом.
"""
from __future__ import annotations

from typing import Optional

from src.core.config.bot import BotConfig
from src.core.runtime.proxy_queue import Priority
from src.database.repository.session import SessionRepository
from src.mangabuff.parser import parse_main_page
from src.mangabuff.session.socket.bot_socket import BotSocket, HOME_ROOM
from src.mangabuff.session.http_client import BotHttpClient
from src.mangabuff.session.request_headers import RequestHeaders, AuthSuccessCallback
from src.utils.log_section import section
from src.utils.logging import get_logger as log


class BotAuth:
    """
    Login / check_auth / CSRF-токени.

    Публічний контракт (тільки BotSession):
      authenticate(force)  — авторизуватись, raise PermissionError при невдачі
      login(force)         — авторизуватись, повернути bool
      check_auth()         — перевірити валідність сесії
    """

    def __init__(
        self,
        bot_config:      BotConfig,
        session_repo:    SessionRepository,
        account_id:      str,
        http:            BotHttpClient,
        headers:         RequestHeaders,
        socket:          BotSocket,
        on_auth_success: Optional[AuthSuccessCallback] = None,
    ) -> None:
        self.bot_config       = bot_config
        self.session_repo     = session_repo
        self.account_id       = account_id
        self.headers          = headers
        self._http             = http
        self._socket           = socket
        self._on_auth_success: Optional[AuthSuccessCallback] = on_auth_success
        # Розриваємо circular dependency: HttpClient → reauth → login → http.get/post
        self._http.set_reauth_callback(lambda: self.login(force=True))

    async def authenticate(self, force: bool = False) -> None:
        """Авторизуватись. При невдачі інвалідує сесію і кидає PermissionError."""
        if not await self.login(force=force):
            self.session_repo.invalidate(self.account_id)
            raise PermissionError("Authentication failed")

    async def login(self, force: bool = False) -> bool:
        """
        Авторизуватись.
          force=False → спочатку check_auth(), логінимось тільки якщо сесія мертва
          force=True  → примусовий re-login (викликається при 419/401)
        """
        if not self._http.client:
            return False

        if self.bot_config.client.cookies:
            self._http.update_cookies(self.bot_config.client.cookies)

        if not force and await self.check_auth():
            return True

        auth = self.bot_config.client.auth
        if not auth:
            log().error("No auth credentials in config")
            return False

        section(f"auth  {auth.email}")
        # При логіні user_id=0 (анонімний) — як показує HAR
        self._socket.set_identity(0, self._http.cookies)
        await self._socket.use_room("/login")
        return await self._fetch_csrf() and await self._submit_login()

    async def check_auth(self) -> bool:
        """
        Перевіряє авторизацію через GET /.
        Кімната завжди HOME_ROOM — незалежно від попередньої навігації.
        Оновлює user_id у сокеті, зберігає cookies у БД.
        """
        try:
            self._socket.set_identity(self._socket.user_id, self._http.cookies)
            await self._socket.use_room(HOME_ROOM)

            r = await self._http.get(
                "/", headers=self.headers.get_navigation(), priority=Priority.AUTH
            )
            if r.status_code != 200:
                return False

            data    = parse_main_page(r.text)
            user_id = data.get("user_id")
            if not user_id:
                return False

            # Оновлюємо ідентичність сокета реальним user_id після авторизації
            self._socket.set_identity(user_id, self._http.cookies)

            if token := data.get("csrf_token"):
                self.headers.csrf_token = token
            if xsrf := self._http.get_xsrf_cookie():
                self.headers.xsrf_token = xsrf

            self.session_repo.save(self.account_id, self._http.cookies)

            if self._on_auth_success:
                await self._on_auth_success(data)

            log().info(f"  → check_auth ok (user_id={user_id})")
            return True
        except Exception as e:
            log().error(f"  → check_auth: {e}")
            return False

    # ── Internal ──────────────────────────────────────────────────────────────

    async def _fetch_csrf(self) -> bool:
        """GET /login → витягуємо csrf_token з <meta> і XSRF-TOKEN з cookies."""
        try:
            r = await self._http.get(
                "/login", headers=self.headers.get_navigation(), priority=Priority.AUTH
            )
            r.raise_for_status()
            data  = parse_main_page(r.text, only_token=True)
            token = data.pop("csrf_token")
            if not token:
                return False
            self.headers.csrf_token = token
            if xsrf := self._http.get_xsrf_cookie():
                self.headers.xsrf_token = xsrf
            return True
        except Exception as e:
            log().error(f"  → _fetch_csrf: {e}")
            return False

    async def _submit_login(self) -> bool:
        """POST /login з email/password/_token."""
        assert self.bot_config.client.auth
        self.headers.referer = f"{self.bot_config.client.base_url}/login"
        payload = {
            "email":    self.bot_config.client.auth.email,
            "password": self.bot_config.client.auth.password,
            "_token":   self.headers.csrf_token or "",
        }
        try:
            r = await self._http.post(
                "/login", data=payload,
                headers=self.headers.get_ajax(is_post=True),
                priority=Priority.AUTH,
            )
            if r.status_code not in (200, 204, 302):
                log().warning(f"  → login POST → {r.status_code}")
                return False
            ok = await self.check_auth()
            if not ok:
                log().warning("  → login POST ok але check_auth провалився")
            return ok
        except Exception as e:
            log().error(f"  → _submit_login: {e}")
            return False