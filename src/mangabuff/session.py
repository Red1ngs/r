import json
import logging
from urllib.parse import unquote
from typing import Dict, Generator, Optional, Any
import httpx
from httpx._types import (
    URLTypes
)

from .parsers import get_csrf_from_html
from ..core.config import Config


class RequestHeaders:
    def __init__(self, config: Config):
        self.common = config.browser
        self.base_url = config.client.base_url
        self.host = config.client.host
        self.referer: Optional[str] = None
        
        self.xsrf_token: Optional[str] = None
        self.csrf_token: Optional[str] = None 
        
    def get_navigation(self) -> Dict[str, str]:
        headers: Dict[str, str] = self.common.to_dict()
        headers.update({
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin" if self.referer else "none",
            "Sec-Fetch-User": "?1",
            "Upgrade-Insecure-Requests": "1"
        })
        if self.referer: headers["Referer"] = self.referer
        return headers

    def get_ajax(self, is_post: bool = True) -> Dict[str, str]:
        headers: Dict[str, str] = self.common.to_dict()
        headers.update({
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
        })
        
        if self.xsrf_token:
            headers["X-XSRF-TOKEN"] = self.xsrf_token
            headers["X-CSRF-TOKEN"] = self.xsrf_token
        elif self.csrf_token:
            headers["X-CSRF-TOKEN"] = self.csrf_token
            
        if is_post:
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            headers["Origin"] = self.base_url  # Автоматично підставляє Origin
        headers["Referer"] = self.referer or f"{self.base_url}/"
        return headers
    

class BotAuth(httpx.Auth):
    def __init__(self, bot: 'BotSession'):
        self.bot = bot
        self._is_relodging = False 

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        response = yield request

        # 1. Якщо отримали 419 і ми зараз НЕ в процесі авторизації
        if response.status_code == 419 and not self._is_relodging:
            logging.warning("🔄 [AuthFlow] Отримано 419 (CSRF Expired). Оновлюємо сесію...")
            
            self._is_relodging = True
            success = False
            try:
                # Викликаємо логіку входу бота
                success = self.bot.login()
            except Exception as e:
                logging.error(f"❌ [AuthFlow] Помилка під час релогіну: {e}")
            finally:
                self._is_relodging = False

            if success:
                logging.info("✅ [AuthFlow] Сесію успішно оновлено. Повторюємо запит...")
                
                # 2. Оновлюємо CSRF/XSRF токени у заголовках failed-запиту
                if self.bot.headers.csrf_token:
                    request.headers["X-CSRF-TOKEN"] = self.bot.headers.csrf_token
                
                if self.bot.headers.xsrf_token:
                    request.headers["X-XSRF-TOKEN"] = self.bot.headers.xsrf_token

                # 3. ОНОВЛЕННЯ КУК (Критично важливо!)
                if self.bot.client:
                    cookie_str = "; ".join([f"{k}={v}" for k, v in self.bot.client.cookies.items()])
                    if cookie_str:
                        request.headers["Cookie"] = cookie_str

                yield request
            else:
                logging.error("❌ [AuthFlow] Не вдалося оновити сесію. Запит скасовано.")


class BotSession:
    def __init__(self, config: Config):
        self.config = config
        self.headers = RequestHeaders(config)
        self.client: Optional[httpx.Client] = None
        self.saved_cookies = httpx.Cookies()
        
        self.create_session()
        
    def create_session(self):
        """Створює або перестворює HTTP-клієнт, ін'єктуючи збережені куки та Auth-флоу"""
        if self.client is not None:
            self.update_cookies_manually(self.client.cookies)
            self.client.close()

        self.client = httpx.Client(
            base_url=self.config.client.base_url,
            http2=True,
            proxy=self.config.network.proxy,
            timeout=self.config.network.timeout,
            follow_redirects=True,
            cookies=self.saved_cookies,
            auth=BotAuth(self)
        )
        print("[System] Нову HTTP-сесію створено.")
        
    def authenticate(self, force: bool = False) -> None:
        """Явний метод для входу в систему."""
        logging.info("🚀 Початок процесу автентифікації...")
        if not self.login(force=force):
            self.close()
            raise PermissionError("[Критична Помилка] Бот не зміг авторизуватися. Роботу зупинено.")
    
    def login(self, force: bool = False) -> bool:
        if not self.client: 
            return False

        # 1. Завантажуємо куки з конфігу (якщо вони є)
        if self.config.client.cookies:
            self.update_cookies_manually(self.config.client.cookies)
            logging.info("Підвантажено стартові куки з Config.")
            
        # 2. Якщо НЕ force — перевіряємо, чи ми вже залогінені
        if not force and self.check_auth():
            return True
        
        # 3. Перевіряємо наявність даних для входу ПЕРЕД зверненням до них
        auth = self.config.client.auth
        if not auth:
            logging.error("Неможливо виконати вхід: відсутні дані авторизації (auth).")
            return False

        # 4. Логуємо процес
        log_msg = f"Виконую {'примусовий ' if force else ''}вхід за паролем для {auth.email}..."
        logging.info(log_msg)

        # 5. Сама процедура входу
        if not self._fetch_csrf():
            return False

        return self._submit_login()
        
    def _fetch_csrf(self) -> bool:
        assert self.client
        self.headers.referer = self.config.client.base_url
        try:
            r = self.client.get("/login", headers=self.headers.get_navigation())
            r.raise_for_status()
        except Exception as e:
            logging.error(f"GET /login → {e}")
            return False

        token, _ = get_csrf_from_html(r.text)
        if not token:
            logging.error("❌ CSRF-токен не знайдено на /login")
            return False

        self.headers.csrf_token = token

        xsrf = self.client.cookies.get("XSRF-TOKEN")
        if xsrf:
            self.headers.xsrf_token = unquote(xsrf)

        return True
        
    def _submit_login(self) -> bool:
        assert self.client
        assert self.config.client.auth

        self.headers.referer = f"{self.config.client.base_url}/login"

        auth = self.config.client.auth
        payload: Dict[str, str] = {
            "email": auth.email,
            "password": auth.password,
            "_token": self.headers.csrf_token or "",
        }

        try:
            r = self.client.post("/login", data=payload, headers=self.headers.get_ajax(is_post=True))
        except Exception as e:
            logging.error(f"POST /login → {e}")
            return False

        if not self._validate_login_response(r):
            return False

        self.update_cookies_manually(self.client.cookies)
        return self.check_auth()
    
    def _validate_login_response(self, response: httpx.Response) -> bool:
        if "application/json" in response.headers.get("content-type", ""):
            try:
                j = response.json()
                logging.debug(f"JSON відповідь: {j}")
                if j.get("errors") or j.get("status") == "error":
                    logging.error(f"❌ Сервер повернув помилку: {j}")
                    return False
            except Exception:
                pass
        
        if response.status_code not in (200, 204, 302):
            logging.error(f"❌ HTTP {response.status_code}, body: {json.dumps(response.json(), ensure_ascii=False)}")
            return False

        return True
    
    def check_auth(self) -> bool:
        assert self.client
        try:
            r = self.client.get(self.config.client.base_url, headers=self.headers.get_navigation())
            r.raise_for_status()
        except httpx.HTTPError as e:
            logging.error(f"❌ Не вдалося завантажити сторінку: {e}")
            return False

        return self._parse_auth_response(r.text)
    
    def _parse_auth_response(self, html: str) -> bool:
        """Витягує CSRF і ім'я користувача з HTML."""
        token, user_name = get_csrf_from_html(html)

        if not token:
            logging.warning("⚠️ CSRF-токен не знайдено")
            return False

        if not user_name:
            logging.warning("⚠️ Не вдалося визначити ім'я користувача (Гість)")
            return False

        self.headers.csrf_token = token
        logging.info(f"✅ Вхід підтверджено для: {user_name}")
        return True

    def update_cookies_manually(self, new_cookies: dict[str, str] | httpx.Cookies):
        """Якщо ви отримали куки і хочете їх додати"""
        self.saved_cookies.update(new_cookies)
        if self.client:
            self.client.cookies.update(new_cookies)

    def get(self, url: URLTypes, **kwargs: Any) -> httpx.Response:
        assert self.client
        return self.client.get(url, **kwargs)

    def post(self, url: URLTypes, **kwargs: Any) -> httpx.Response:
        assert self.client
        return self.client.post(url, **kwargs)

    def close(self):
        if self.client:
            self.saved_cookies.update(self.client.cookies)
            self.client.close()
            print("[System] Сесію закрито.")