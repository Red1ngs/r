"""
Task types.

A task's run() receives the full Account — it contains both
  bot.inventory  (Inventories)
  bot.session    (BotSession)

A task's run() may return new tasks — the Worker picks them up and enqueues them.

Returnable from run():
    AnyTask              — single task
    list[AnyTask]        — multiple tasks
    None / anything else — ignored

Scheduling fields (run_at takes priority over delay):
    delay   : float          — relative delay in seconds from assign()
    run_at  : str|int|float  — absolute run time (UTC)

run_at formats:
    "14:30"              — today at 14:30 UTC, tomorrow if already passed
    "2025-06-01 09:00"   — specific date+time UTC
    1_735_689_600        — unix timestamp

source_profession:
    Ідентифікатор profession, що породила задачу.
    Використовується BotWorker.clear_profession() для вибіркового очищення черги.
    None = системна задача (не прив'язана до profession).
"""
from __future__ import annotations

import itertools
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from src.core.account import Account

log = logging.getLogger(__name__)

_seq = itertools.count()


# ─────────────────────────────────────────────────────────────────────────────
# Priority
# ─────────────────────────────────────────────────────────────────────────────

class Priority(IntEnum):
    """
    Пріоритет задачі в черзі BotWorker.
    Менше значення = виконується раніше (min-heap).
    """
    CRITICAL = 0   # системне відновлення, автентифікація
    HIGH     = 1   # time-sensitive (daily bonus, events)
    NORMAL   = 5   # стандартна робота profession
    LOW      = 10  # фонова синхронізація, кеш


class ProfessionPriority(IntEnum):
    """
    Пріоритет profession в рамках одного акаунта.
    Визначає базовий Priority задач, що породжує profession.
    Менше значення = вищий пріоритет.

    Маппінг у task priority:
        profession_priority + base_task_priority → ефективний priority

    Profession з ProfessionPriority.PRIMARY отримує всі задачі з базовим priority.
    Profession з ProfessionPriority.SECONDARY → +10 до кожної задачі.
    Profession з ProfessionPriority.BACKGROUND → +20 до кожної задачі.
    """
    PRIMARY    = 0   # найвища — задачі не зміщуються
    SECONDARY  = 10  # другорядна — +10 до priority задач
    BACKGROUND = 20  # фонова — +20 до priority задач

    @classmethod
    def from_index(cls, index: int) -> "ProfessionPriority":
        """
        Конвертує позицію profession у списку (0-based) у ProfessionPriority.
        Перша у списку → PRIMARY, друга → SECONDARY, решта → BACKGROUND.
        """
        if index == 0:
            return cls.PRIMARY
        if index == 1:
            return cls.SECONDARY
        return cls.BACKGROUND

    def adjust(self, base_priority: int) -> int:
        """Застосовує зміщення profession до базового пріоритету задачі."""
        return base_priority + int(self)


# ─────────────────────────────────────────────────────────────────────────────
# Retry mixin
# ─────────────────────────────────────────────────────────────────────────────

class _RetryMixin:
    name:        str
    max_retries: int
    _retries:    int

    @property
    def can_retry(self) -> bool:
        return self._retries < self.max_retries

    @property
    def retries(self) -> int:
        return self._retries

    def increment_retry(self) -> None:
        self._retries += 1
        log.warning(
            f"[{type(self).__name__}:{self.name}] "
            f"attempt {self._retries}/{self.max_retries}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Task  —  простий одноразовий таск
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(order=True)
class Task(_RetryMixin):
    """fn(bot) -> None | AnyTask | list[AnyTask]"""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:               str                        = field(default="task",         compare=False)
    fn:                 Callable[["Account"], Any]  = field(default=lambda b: None, compare=False)
    max_retries:        int                         = field(default=3,              compare=False)
    _retries:           int                         = field(default=0,              compare=False, init=False)
    meta:               dict[str, Any]              = field(default_factory=dict,   compare=False)
    delay:              float                       = field(default=0.0,            compare=False)
    run_at:             Any                         = field(default=None,           compare=False)
    source_profession:  Optional[str]               = field(default=None,           compare=False)

    def run(self, bot: "Account") -> Any:
        return self.fn(bot)


# ─────────────────────────────────────────────────────────────────────────────
# LoopTask  —  виконує fn() поки condition() == True
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(order=True)
class LoopTask(_RetryMixin):
    """Calls fn(bot) repeatedly while condition(bot) is True. Interruptible via stop_event."""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:               str                              = field(default="loop",          compare=False)
    fn:                 Callable[["Account"], Any]        = field(default=lambda b: None,  compare=False)
    condition:          Callable[["Account"], bool]       = field(default=lambda b: False, compare=False)
    interval:           float                             = field(default=0.0,             compare=False)
    max_retries:        int                               = field(default=3,               compare=False)
    _retries:           int                               = field(default=0,               compare=False, init=False)
    meta:               dict[str, Any]                    = field(default_factory=dict,    compare=False)
    delay:              float                             = field(default=0.0,             compare=False)
    run_at:             Any                               = field(default=None,            compare=False)
    source_profession:  Optional[str]                     = field(default=None,            compare=False)

    def run(self, bot: "Account") -> None:
        import threading
        stop: threading.Event = self.meta.get("stop_event") or threading.Event()
        while not stop.is_set() and self.condition(bot):
            self.fn(bot)
            if self.interval:
                stop.wait(self.interval)


# ─────────────────────────────────────────────────────────────────────────────
# ReactiveTask  —  дренує чергу подій з inventory
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(order=True)
class ReactiveTask(_RetryMixin):
    """
    Drains an event queue. Re-enqueues itself if requeue=True.

    source(bot)          -> list of events
    handler(event, bot)  -> Any
    """

    priority:    int   = field(default=Priority.HIGH,  compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:               str                               = field(default="reactive",       compare=False)
    source:             Callable[["Account"], list[Any]]   = field(default=lambda b: [],    compare=False)
    handler:            Callable[[Any, "Account"], Any]    = field(default=lambda e, b: None, compare=False)
    requeue:            bool                               = field(default=True,             compare=False)
    max_retries:        int                                = field(default=3,                compare=False)
    _retries:           int                                = field(default=0,                compare=False, init=False)
    meta:               dict[str, Any]                     = field(default_factory=dict,     compare=False)
    delay:              float                              = field(default=0.0,              compare=False)
    run_at:             Any                                = field(default=None,             compare=False)
    source_profession:  Optional[str]                      = field(default=None,             compare=False)

    def run(self, bot: "Account") -> list[Any]:
        events = self.source(bot)
        return [self.handler(event, bot) for event in events]


# ─────────────────────────────────────────────────────────────────────────────
# TargetedTask  —  таск з конкретною ціллю (юзер, манга, тощо)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(order=True)
class TargetedTask(_RetryMixin):
    """fn(target, bot) -> Any"""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:               str                              = field(default="targeted",       compare=False)
    target:             Any                               = field(default=None,             compare=False)
    fn:                 Callable[[Any, "Account"], Any]   = field(default=lambda t, b: None, compare=False)
    max_retries:        int                               = field(default=3,                compare=False)
    _retries:           int                               = field(default=0,                compare=False, init=False)
    meta:               dict[str, Any]                    = field(default_factory=dict,     compare=False)
    delay:              float                             = field(default=0.0,              compare=False)
    run_at:             Any                               = field(default=None,             compare=False)
    source_profession:  Optional[str]                     = field(default=None,             compare=False)

    def run(self, bot: "Account") -> Any:
        return self.fn(self.target, bot)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

AnyTask = Task | LoopTask | ReactiveTask | TargetedTask
_TASK_TYPES = (Task, LoopTask, ReactiveTask, TargetedTask)


def is_task(value: Any) -> bool:
    return isinstance(value, _TASK_TYPES)


def extract_spawned(result: Any) -> list[AnyTask]:
    if result is None:
        return []
    if is_task(result):
        return [result]
    if isinstance(result, list):
        return [item for item in result if is_task(item)]
    return []


def tag_profession(tasks: list[AnyTask], profession_id: str) -> list[AnyTask]:
    """
    Проставляє source_profession на кожній задачі списку (in-place).
    Повертає той самий список для зручності чейнингу.

    Використовується в BaseTrigger.producer() та profession.startup_tasks().
    """
    for t in tasks:
        if getattr(t, "source_profession", None) is None:
            t.source_profession = profession_id  # type: ignore[attr-defined]
    return tasks


@dataclass
class TaskResult:
    task:    AnyTask
    success: bool
    value:   Any                 = None
    error:   Optional[Exception] = None

    def __str__(self) -> str:
        if self.success:
            return f"✅ '{self.task.name}'"
        return f"❌ '{self.task.name}' → {self.error}"
