"""
src/mangabuff/session/http_result.py

Контракт повернення бізнес-методів.
Не залежить від жодного іншого модуля проєкту.
"""
from __future__ import annotations

import functools
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Awaitable, Callable, Generic, Literal, Optional, TypeVar, ParamSpec, Concatenate

from src.utils.logging import get_logger as log


# ── Типи ─────────────────────────────────────────────────────────────────────

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

HttpMethodStr = Literal["GET", "POST", "PUT", "DELETE", "PATCH", "HEAD"]


class FailReason(Enum):
    NETWORK   = auto()   # таймаут, з'єднання відхилено, будь-який виняток
    AUTH      = auto()   # 419 після retry, PermissionError
    NOT_FOUND = auto()   # 404
    SERVER    = auto()   # 5xx або неочікуваний статус
    BAD_DATA  = auto()   # 200, але тіло порожнє або не те що очікували
    DENIED    = auto()   # сервер явно відмовив (403, success=false тощо)


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


# ── Конструктори ──────────────────────────────────────────────────────────────

def http_success(data: T) -> HttpResult[T]:
    """Створює успішний HttpResult з конкретним значенням."""
    return HttpResult(ok=True, data=data)


def http_success_none() -> HttpResult[None]:
    """Створює успішний HttpResult без даних."""
    return HttpResult(ok=True, data=None)


def http_fail(reason: FailReason) -> HttpResult[Any]:
    """Створює невдалий HttpResult."""
    return HttpResult(ok=False, reason=reason)


# ── Декоратор ────────────────────────────────────────────────────────────────

def http_call(
    func: "Callable[Concatenate[Any, P], Awaitable[HttpResult[R]]]",
) -> "Callable[Concatenate[Any, P], Awaitable[HttpResult[R]]]":
    """
    Обгортає бізнес-метод BotSession:
      - PermissionError → FailReason.AUTH
      - будь-який інший виняток → FailReason.NETWORK
    """
    @functools.wraps(func)
    async def wrapper(self: Any, *args: P.args, **kwargs: P.kwargs) -> HttpResult[R]:
        try:
            return await func(self, *args, **kwargs)
        except PermissionError:
            log().error(f"  → {func.__name__}: auth error")
            return http_fail(FailReason.AUTH)
        except Exception as e:
            log().error(f"  → {func.__name__}: {e}")
            return http_fail(FailReason.NETWORK)
    return wrapper