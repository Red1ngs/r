"""
src/mangabuff/session/bot_session.py

Публічний фасад — єдина точка входу для бізнес-логіки.

Збирає всі шари разом:

  RequestHeaders   request_headers.py   формування HTTP-заголовків
  BotHttpClient    http_client.py       сирий HTTP, retry, proxy-queue
  BotSocket        bot_socket.py        wss10.mangabuff.ru:443, кімнати
  MessageSocket    message_socket.py    wss.mangabuff.ru:2087, діалоги
  BotAuth          bot_auth.py          login, check_auth, CSRF
  BotSession       (цей файл)           бізнес-методи

┌─────────────────────────────────────────────────────┐
│  wss10 (socket)             wss (msg)               │
│  авт.: Cookie + joinRoom    авт.: ?token= у URL     │
│  кімнати: є (LRU)           кімнат: немає           │
│  глобальні події            new-message             │
└─────────────────────────────────────────────────────┘

Порядок роботи з повідомленнями:
  1. token = await session.open_dialog(user_id)
  2. session.msg.on("new-message", handler)
  3. await session.send_message(user_id, "текст")
  4. await session.close_dialog()
"""
from __future__ import annotations

import re
from typing import Any, Optional

from curl_cffi.requests import Response

from src.core.config.bot import BotConfig
from src.core.config.app import AppConfig, DailyCfg, MiningCfg, PersonalCfg, QuizCfg, ReaderAppCfg
from src.core.runtime.proxy_queue import Priority
from src.database.repository.session import SessionRepository
from src.mangabuff.daily.parser import get_claimable_day
from src.mangabuff.parser import parse_main_page, parse_mining_page
from src.mangabuff.session.bot_auth import BotAuth
from src.mangabuff.session.socket.bot_socket import BotSocket
from src.mangabuff.session.http_client import BotHttpClient
from src.mangabuff.session.http_result import HttpResult, FailReason, http_call, http_success, http_fail, http_success_none
from src.mangabuff.session.socket.message_socket import MessageSocket
from src.mangabuff.session.request_headers import RequestHeaders, AuthSuccessCallback
from src.utils.logging import get_logger as log


class BotSession:
    """
    Єдина точка входу для будь-якої бізнес-логіки.

    Атрибути:
      http    — BotHttpClient   (сирий HTTP)
      auth    — BotAuth         (логін / токени)
      socket  — BotSocket       (wss10, кімнати, глобальні події)
      msg     — MessageSocket   (wss, особисті повідомлення)
    """

    def __init__(
        self,
        bot_config:      BotConfig,
        app_config:      AppConfig,
        session_repo:    SessionRepository,
        account_id:      str,
        on_auth_success: Optional[AuthSuccessCallback] = None,
        max_tabs:        int = BotSocket.DEFAULT_MAX_TABS,
    ) -> None:
        self._headers = RequestHeaders(bot_config)
        self.session_repo = session_repo

        self.http = BotHttpClient(
            bot_config=bot_config,
            session_repo=session_repo,
            account_id=account_id,
            headers=self._headers,
        )
        self.socket = BotSocket(max_tabs=max_tabs)
        self.msg    = MessageSocket()
        self.auth   = BotAuth(
            bot_config=bot_config,
            session_repo=session_repo,
            account_id=account_id,
            http=self.http,
            headers=self._headers,
            socket=self.socket,
            on_auth_success=on_auth_success,
        )
        
        self.config = app_config

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self, force_login: bool = False) -> None:
        """Ініціалізація сесії: авторизація + HOME_ROOM."""
        await self.auth.authenticate(force=force_login)

    async def close(self) -> None:
        """Закрити всі з'єднання і скинути стан."""
        await self.msg.close()
        await self.socket.close()
        self.http.close()
        self._headers.reset()
        log().info("[session] закрито")

    @property
    def user_id(self) -> int | str | None:
        """
        Поточний user_id ідентичності сокета.

        ТИМЧАСОВО читає приватний BotSocket._user_id — сам файл bot_socket.py
        не надавався, тож я не можу додати туди публічну властивість. Це
        єдина крапка доступу в усьому BotSession (замість розкиданих
        self.socket._user_id по бізнес-методах) — коли bot_socket.py
        отримає публічний `user_id`-property, тут достатньо буде замінити
        одну стрічку на `return self.socket.user_id`, і попередження
        reportPrivateUsage зникне остаточно без правок деінде.
        """
        return self.socket._user_id  # pyright: ignore[reportPrivateUsage]

    # ── HTTP-обгортки з автоматичним use_room ─────────────────────────────────
    #
    # priority керує чергою в proxy_queue (core/runtime/proxy_queue.py):
    #   Priority.AUTH          — зарезервовано для BotAuth (логін/re-login),
    #                            сюди НЕ передавати. Auth свідомо не йде через
    #                            BotSession.get/post — BotAuth сам передає
    #                            priority=Priority.AUTH напряму в self.http,
    #                            бо викликається зсередини BotHttpClient як
    #                            reauth-callback на 419/401. Якби auth йшов
    #                            через BotSession, вийшов би цикл залежностей
    #                            BotHttpClient → BotSession → BotHttpClient.
    #   Priority.TIME_CRITICAL — дії з жорстким зовнішнім дедлайном у секундах:
    #                            quiz_start/quiz_answer (вікно відповіді у
    #                            квізі спливає — запізнення означає провал
    #                            дії, а не просто затримку). Обганяє CRITICAL/
    #                            NORMAL/BACKGROUND, але не AUTH: без валідної
    #                            сесії quiz-запит все одно поверне 401/419.
    #   Priority.CRITICAL      — дії, які користувач/сценарій явно чекає
    #                            "зараз" (send_message, claim_daily/
    #                            claim_calendar — обмежені часовим вікном,
    #                            але не секундами).
    #   Priority.NORMAL        — типовий бізнес-цикл монітора (default):
    #                            mining.
    #   Priority.BACKGROUND    — масові/фонові операції (reader: каталог,
    #                            читання історії, цукерки) — не повинні
    #                            відсовувати mining/quiz/daily в черзі.
    #
    # Кожен бізнес-метод нижче явно передає свій priority замість того, щоб
    # покладатись на неявний default кудись у http_client — так пріоритет
    # видно в одному місці (BotSession), поруч з переліком усіх ендпоінтів.

    async def get(
        self, url: str, room: Optional[str] = None,
        priority: Priority = Priority.NORMAL, **kw: Any,
    ) -> Response:
        """
        GET із опціональним перемиканням кімнати.
        room=None — без use_room (API-запити, фонові задачі).
        """
        if room is not None:
            await self.socket.use_room(room)
        return await self.http.get(url, priority=priority, **kw)

    async def post(
        self, url: str, room: Optional[str] = None,
        priority: Priority = Priority.NORMAL, **kw: Any,
    ) -> Response:
        """POST із опціональним use_room."""
        if room is not None:
            await self.socket.use_room(room)
        return await self.http.post(url, priority=priority, **kw)

    # ── Повідомлення ──────────────────────────────────────────────────────────

    async def open_dialog(self, user_id: int | str) -> Optional[str]:
        """
        Відкрити особистий діалог з користувачем:
          1. GET /messages/<user_id>  (кімната /messages у BotSocket)
          2. Витягти data-dialog-token з HTML
          3. Підключити MessageSocket до wss.mangabuff.ru:2087

        Повертає dialog_token або None при помилці.
        Ідемпотентно: якщо той самий токен вже відкритий — повертає його без
        повторного підключення.
        """
        await self.socket.use_room("/messages")
        r = await self.http.get(
            f"/messages/{user_id}",
            headers=self._headers.get_navigation(),
            priority=Priority.CRITICAL,
        )
        if r.status_code != 200:
            log().warning(f"[session] open_dialog({user_id}): HTTP {r.status_code}")
            return None

        m = re.search(r'data-dialog-token=["\']([^"\']+)["\']', r.text)
        if not m:
            log().warning(f"[session] open_dialog({user_id}): data-dialog-token не знайдено")
            return None

        dialog_token = m.group(1)
        if not await self.msg.open(dialog_token, self.http.cookies):
            log().error(f"[session] open_dialog({user_id}): msg socket не підключився")
            return None

        log().info(f"[session] діалог з {user_id} відкрито (token={dialog_token!r})")
        return dialog_token

    async def send_message(
        self,
        to_user_id: int | str,
        text:       str,
        reply_id:   Optional[int | str] = None,
        reply_text: Optional[str]       = None,
    ) -> HttpResult[str]:
        """
        Відправити повідомлення — точно як браузер:
          1. POST /messages/<user_id>  (HTTP)
          2. emit('send-message', html)  (WS)

        MessageSocket має бути відкритий через open_dialog() заздалегідь.
        Якщо WS не підключений — HTTP-частина виконується, emit пропускається
        (повідомлення дійде, але без real-time синхронізації на стороні отримувача).
        """
        payload: dict[str, Any] = {
            "text":       text,
            "reply_id":   reply_id,
            "reply_text": reply_text,
        }
        r = await self.http.post(
            f"/messages/{to_user_id}",
            data=payload,
            headers=self._headers.get_ajax(is_post=True),
            priority=Priority.CRITICAL,
        )
        if r.status_code != 200:
            return http_fail(FailReason.SERVER)

        html = r.text
        if not html.strip():
            return http_fail(FailReason.BAD_DATA)

        await self.msg.emit("send-message", html)
        return http_success(html)
    
    async def mark_messages_read(
        self,
        dialog_token: str,
        last_msg_id:  Optional[str] = None,
    ) -> None:
        """
        Позначити повідомлення прочитаними:
          1. POST /messages/read  (HTTP)
          2. emit('read-message', msg_id)  (WS, якщо є id)
        """
        await self.http.post(
            "/messages/read",
            data={"dialog_token": dialog_token},
            headers=self._headers.get_ajax(is_post=True),
            priority=Priority.CRITICAL,
        )
        if last_msg_id is not None:
            await self.msg.emit("read-message", last_msg_id)

    async def close_dialog(self) -> None:
        """
        Закрити поточний діалог (MessageSocket).
        BotSocket (wss10) продовжує працювати.
        """
        await self.msg.close()
        log().info("[session] діалог закрито")
        
    # ── Daily ─────────────────────────────────────────────────────────────────

    @http_call
    async def fetch_daily_streak(self, daily: DailyCfg) -> HttpResult[Optional[int]]:
        url  = daily.urls.balance
        room = url
        r = await self.get(url, room=room, priority=Priority.CRITICAL, timeout=15)
        r.raise_for_status()
        day = get_claimable_day(
            r.text,
            item_selector=daily.item_selector,
            claim_text=daily.claim_text,
            day_attr=daily.day_attr,
        )
        if day is not None:
            log().info(f"  → день {day} доступний")
            return http_success(int(day))
        log().info("  → бонус недоступний сьогодні")
        return http_success_none()

    @http_call
    async def claim_calendar(self, day: int | str, daily: DailyCfg) -> HttpResult[dict[str, Any]]:
        url  = daily.urls.api_calendar
        room = daily.urls.balance
        try:
            formatted_url = url.format(day=day)
        except (IndexError, KeyError, ValueError):
            formatted_url = url.format(day)

        r = await self.post(formatted_url, room=room, priority=Priority.CRITICAL, timeout=15)
        if r.status_code == 200:
            log().info("  → отримано")
            return http_success(self.http.json_body(r))
        log().warning(f"  → claim_calendar: {r.status_code} {self.http.json_body(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    @http_call
    async def claim_daily(self, daily: DailyCfg, personal: PersonalCfg) -> HttpResult[dict[str, Any]]:
        url  = daily.urls.ping
        user_id = self.user_id
        room = personal.urls.user_page.format(user_id=user_id)
        r = await self.post(url, room, priority=Priority.CRITICAL, timeout=15)
        if r.status_code == 200:
            log().info("  → отримано")
            return http_success(self.http.json_body(r))
        log().warning(f"  → claim_daily: {r.status_code} {self.http.json_body(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    # ── Reader ────────────────────────────────────────────────────────────────

    @http_call
    async def submit_add_history(self, items: list[dict[str, Any]], last_manga_read: str, reader: ReaderAppCfg) -> HttpResult[dict[str, Any]]:
        url = reader.urls.api_history
        room = reader.urls.manga_page.format(translit_name=last_manga_read)
        body = {
            f"items[{i}][{k}]": v
            for i, item in enumerate(items)
            for k, v in item.items()
        }
        r = await self.post(url, room=room, data=body, priority=Priority.BACKGROUND)
        if r.status_code == 200:
            data = self.http.json_body(r) if r.content else {}
            return http_success(data)
        log().warning(f"  → submit_add_history: {r.status_code}")
        return http_fail(FailReason.SERVER)

    @http_call
    async def claim_candy(self, token: str, last_manga_read: str, reader: ReaderAppCfg) -> HttpResult[dict[str, Any]]:
        url  = reader.urls.api_candy
        room = reader.urls.manga_page.format(translit_name=last_manga_read)
        r = await self.post(url, room=room, data={"token": token}, priority=Priority.BACKGROUND, timeout=15)
        if r.status_code == 200:
            data: dict[str, Any] = self.http.json_body(r) if r.content else {}
            log().info("  → цукерка отримана")
            return http_success(data)
        log().warning(f"  → claim_candy: {r.status_code} {self.http.json_body(r).get('message', '') if r.content else ''}")
        return http_fail(FailReason.DENIED)

    @http_call
    async def fetch_manga_catalog(self, reader: ReaderAppCfg, page: int = 1) -> HttpResult[str]:
        url  = reader.urls.catalog
        room = url
        r = await self.get(url, room=room, params={"page": page}, priority=Priority.BACKGROUND, timeout=15)
        r.raise_for_status()
        return http_success(r.text)

    @http_call
    async def fetch_manga_chapters(self, reader: ReaderAppCfg, translit_name: str, manga_data_id: int) -> HttpResult[str]:
        page = await self.fetch_manga_page(reader, translit_name)
        if not page or page.data is None:
            return http_fail(FailReason.NOT_FOUND)
        more = await self._fetch_more_chapters(translit_name, manga_data_id, reader)
        return http_success(page.data + (more.data or ""))

    @http_call
    async def fetch_manga_page(self, reader: ReaderAppCfg, translit_name: str) -> HttpResult[str]:
        url  = reader.urls.manga_page.format(translit_name=translit_name)
        room = url
        r = await self.get(url, room, priority=Priority.BACKGROUND, timeout=15)
        r.raise_for_status()
        return http_success(r.text)

    @http_call
    async def _fetch_more_chapters(self, translit_name: str, manga_data_id: int, reader: ReaderAppCfg) -> HttpResult[str]:
        url  = reader.urls.api_load
        room = reader.urls.manga_page.format(translit_name=translit_name)
        r = await self.post(url, room, data={"manga_id": manga_data_id}, priority=Priority.BACKGROUND, timeout=15,)
        r.raise_for_status()
        return http_success(self.http.json_body(r).get("content", ""))

    # ── Quiz ──────────────────────────────────────────────────────────────────

    @http_call
    async def quiz_start(self, quiz: QuizCfg) -> HttpResult[dict[str, Any]]:
        url  = quiz.urls.start
        room = quiz.urls.quiz_page
        r = await self.post(url, room, priority=Priority.TIME_CRITICAL, timeout=15)
        if r.status_code == 200:
            question = self.http.json_body(r).get("question")
            if question is None:
                return http_fail(FailReason.BAD_DATA)
            return http_success(question)
        log().warning(f"  → quiz_start: {r.status_code}")
        return http_fail(FailReason.SERVER)

    @http_call
    async def quiz_answer(self, answer: str, quiz: QuizCfg) -> HttpResult[dict[str, Any]]:
        url = quiz.urls.answer
        room = quiz.urls.quiz_page
        r = await self.post(url, room, data={"answer": answer}, priority=Priority.TIME_CRITICAL, timeout=15)
        if r.status_code == 200:
            return http_success(self.http.json_body(r))
        log().warning(f"  → quiz_answer: {r.status_code}")
        return http_fail(FailReason.SERVER)

    # ── Mining ──────────────────────────────────────────────────────────────────

    @http_call
    async def mine(self, account_id: str, mining: MiningCfg) -> HttpResult[dict[str, Optional[int]]]:
        url = mining.urls.mining_page
        room = url
        r = await self.get(url, room, priority=Priority.NORMAL, timeout=15)
        if r.status_code == 200:
            data = parse_mining_page(r.text)
            missing = [k for k, v in data.items() if v is None]
            if missing:
                auth_data = parse_main_page(r.text)
                if not auth_data.get("user_id"):
                    log().warning("  → mine: unauthenticated page detected")
                    self.session_repo.invalidate(account_id)
                    return http_fail(FailReason.AUTH)
                log().warning(f"  → mine: missing required mining fields: {', '.join(missing)}")
                return http_fail(FailReason.BAD_DATA)
            return http_success(data)
        log().warning(f"  → mine: {r.status_code}")
        return http_fail(FailReason.SERVER)

    @http_call
    async def mine_hit(self, mining: MiningCfg) -> HttpResult[dict[str, int]]:
        url  = mining.urls.hit
        room = mining.urls.mining_page
        r = await self.post(url, room, priority=Priority.NORMAL, timeout=15)
        if r.status_code == 200:
            return http_success(self.http.json_body(r))
        log().warning(f"  → mine_hit: {r.status_code}")
        return http_fail(FailReason.SERVER)
    
    @http_call
    async def upgrade_pickaxe(self, mining: MiningCfg) -> HttpResult[dict[str, int]]: 
        url  = mining.urls.upgrade
        room = mining.urls.mining_page
        r = await self.post(url, room, priority=Priority.NORMAL, timeout=15)
        if r.status_code == 200:
            return http_success(self.http.json_body(r))
        elif r.status_code == 400:
            log().warning(f"  → upgrade_pickaxe: {r.status_code} (403 Forbidden)")
            return http_fail(FailReason.DENIED)
        log().warning(f"  → upgrade_pickaxe: {r.status_code}")
        return http_fail(FailReason.SERVER)
    
    @http_call
    async def buy_strong_hit(self, mining: MiningCfg) -> HttpResult[dict[str, int]]: 
        url  = mining.urls.buy_strong_hit
        room = mining.urls.mining_page
        r = await self.post(url, room, priority=Priority.NORMAL, timeout=15)
        if r.status_code == 200:
            return http_success(self.http.json_body(r))
        elif r.status_code == 400:
            log().warning(f"  → buy_strong_hit: {r.status_code} (403 Forbidden)")
            return http_fail(FailReason.DENIED)
        log().warning(f"  → buy_strong_hit: {r.status_code}")
        return http_fail(FailReason.SERVER)
    
    @http_call
    async def exchange_ore(self, mining: MiningCfg, diamonds: int) -> HttpResult[dict[str, int]]: 
        url  = mining.urls.exchange
        room = mining.urls.mining_page
        r = await self.post(url, room, payload={"diamonds": diamonds}, priority=Priority.NORMAL, timeout=15)
        if r.status_code == 200:
            return http_success(self.http.json_body(r))
        elif r.status_code == 400:
            log().warning(f"  → exchange_ore: {r.status_code} (403 Forbidden)")
            return http_fail(FailReason.DENIED)
        log().warning(f"  → exchange_ore: {r.status_code}")
        return http_fail(FailReason.SERVER)