"""
core/utils/proxy_rate_limiter.py — реєстр лімітерів, розподілених по проксі.

Ідея:
    Якщо два акаунти йдуть через один і той самий проксі — вони поділяють
    один RateLimiter. Сервер бачить запити з однієї IP, тому затримка має
    бути між запитами в цілому, а не «у кожного акаунта своя».

    Якщо акаунт без проксі — він ділить лімітер із усіма іншими no-proxy
    акаунтами (вони теж ідуть з однієї машини).

Використання:
    # При старті (один раз):
    from src.core.utils.proxy_rate_limiter import proxy_limiter_registry
    limiter = proxy_limiter_registry.get_or_create(bot_config.network.proxy)

    # В BotTransport.__init__:
    self._rate_limiter = proxy_limiter_registry.get_or_create(
        bot_config.network.proxy
    )
"""
from __future__ import annotations

import threading
from typing import Optional

from src.core.utils.rate_limiter import RateLimiter

# Ключ для акаунтів без проксі
_NO_PROXY_KEY = "__no_proxy__"


class ProxyRateLimiterRegistry:
    """
    Singleton-реєстр: один RateLimiter на кожну унікальну proxy-адресу.

    Thread-safe: get_or_create можна викликати з будь-якого потоку.
    """

    def __init__(
        self,
        min_delay: float = 1.0,
        max_delay: float = 3.0,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._lock      = threading.Lock()
        self._limiters: dict[str, RateLimiter] = {}

    def get_or_create(self, proxy: Optional[str]) -> RateLimiter:
        """
        Повертає існуючий лімітер для цього проксі або створює новий.

        proxy=None  → спільний лімітер для всіх no-proxy акаунтів.
        proxy="..." → спільний лімітер для всіх акаунтів з цим проксі.
        """
        key = self._normalize(proxy)
        with self._lock:
            if key not in self._limiters:
                self._limiters[key] = RateLimiter(
                    min_delay=self._min_delay,
                    max_delay=self._max_delay,
                )
            return self._limiters[key]

    def get(self, proxy: Optional[str]) -> Optional[RateLimiter]:
        """Повертає лімітер якщо вже існує, інакше None (без створення)."""
        key = self._normalize(proxy)
        with self._lock:
            return self._limiters.get(key)

    def remove(self, proxy: Optional[str]) -> None:
        """Видаляє лімітер (наприклад, коли всі акаунти з цим проксі видалені)."""
        key = self._normalize(proxy)
        with self._lock:
            self._limiters.pop(key, None)

    def all_keys(self) -> list[str]:
        """Список усіх зареєстрованих ключів (для діагностики)."""
        with self._lock:
            return list(self._limiters.keys())

    @staticmethod
    def _normalize(proxy: Optional[str]) -> str:
        """
        Нормалізує proxy-рядок у стабільний ключ.
        http://user:pass@1.2.3.4:8080 → http://1.2.3.4:8080
        (credentials не є частиною «ідентичності» IP)
        """
        if not proxy:
            return _NO_PROXY_KEY
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy)
            # Ключ: scheme + host + port (без credentials)
            host = p.hostname or ""
            port = f":{p.port}" if p.port else ""
            return f"{p.scheme}://{host}{port}"
        except Exception:
            return proxy  # fallback: рядок як є


# Глобальний singleton реєстру.
# min_delay / max_delay можна змінити до першого get_or_create()
# або передати свої значення при старті через proxy_limiter_registry.configure().
proxy_limiter_registry = ProxyRateLimiterRegistry(min_delay=1.0, max_delay=6.0)