"""
Task types.

A task's run() receives the full AccountPull — it contains both
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
"""
from __future__ import annotations

import itertools
import logging
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Callable, Optional

if TYPE_CHECKING:
    from src.core.account_pull import AccountPull

log = logging.getLogger(__name__)

_seq = itertools.count()


class Priority(IntEnum):
    HIGH   = 1
    NORMAL = 5
    LOW    = 10


class _RetryMixin:
    name:        str
    max_retries: int
    _retries:    int

    @property
    def can_retry(self) -> bool:
        return self._retries < self.max_retries

    def increment_retry(self) -> None:
        self._retries += 1
        log.warning(
            f"[{type(self).__name__}:{self.name}] "
            f"attempt {self._retries}/{self.max_retries}"
        )


# ---------------------------------------------------------------------------
# Task  —  простий одноразовий таск
# ---------------------------------------------------------------------------

@dataclass(order=True)
class Task(_RetryMixin):
    """fn(bot) -> None | AnyTask | list[AnyTask]"""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:        str                            = field(default="task",            compare=False)
    fn:          Callable[["AccountPull"], Any] = field(default=lambda b: None,    compare=False)
    max_retries: int                            = field(default=3,                 compare=False)
    _retries:    int                            = field(default=0,                 compare=False, init=False)
    meta:        dict                           = field(default_factory=dict,      compare=False)
    delay:       float                          = field(default=0.0,               compare=False)
    run_at:      Any                            = field(default=None,              compare=False)

    def run(self, bot: "AccountPull") -> Any:
        return self.fn(bot)


# ---------------------------------------------------------------------------
# LoopTask  —  виконує fn() поки condition() == True
# ---------------------------------------------------------------------------

@dataclass(order=True)
class LoopTask(_RetryMixin):
    """Calls fn(bot) repeatedly while condition(bot) is True. Interruptible via stop_event."""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:        str                                     = field(default="loop",             compare=False)
    fn:          Callable[["AccountPull"], Any]          = field(default=lambda b: None,     compare=False)
    condition:   Callable[["AccountPull"], bool]         = field(default=lambda b: False,    compare=False)
    interval:    float                                   = field(default=0.0,                compare=False)
    max_retries: int                                     = field(default=3,                  compare=False)
    _retries:    int                                     = field(default=0,                  compare=False, init=False)
    meta:        dict                                    = field(default_factory=dict,       compare=False)
    delay:       float                                   = field(default=0.0,                compare=False)
    run_at:      Any                                     = field(default=None,               compare=False)

    def run(self, bot: "AccountPull") -> None:
        import threading
        stop: threading.Event = self.meta.get("stop_event") or threading.Event()
        while not stop.is_set() and self.condition(bot):
            self.fn(bot)
            if self.interval:
                stop.wait(self.interval)


# ---------------------------------------------------------------------------
# ReactiveTask  —  дренує чергу подій з inventory
# ---------------------------------------------------------------------------

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

    name:        str                                          = field(default="reactive",            compare=False)
    source:      Callable[["AccountPull"], list]              = field(default=lambda b: [],          compare=False)
    handler:     Callable[[Any, "AccountPull"], Any]          = field(default=lambda e, b: None,     compare=False)
    requeue:     bool                                         = field(default=True,                  compare=False)
    max_retries: int                                          = field(default=3,                     compare=False)
    _retries:    int                                          = field(default=0,                     compare=False, init=False)
    meta:        dict                                         = field(default_factory=dict,          compare=False)
    delay:       float                                        = field(default=0.0,                   compare=False)
    run_at:      Any                                          = field(default=None,                  compare=False)

    def run(self, bot: "AccountPull") -> list[Any]:
        events = self.source(bot)
        return [self.handler(event, bot) for event in events]


# ---------------------------------------------------------------------------
# TargetedTask  —  таск з конкретною ціллю (юзер, манга, тощо)
# ---------------------------------------------------------------------------

@dataclass(order=True)
class TargetedTask(_RetryMixin):
    """fn(target, bot) -> Any"""

    priority:    int   = field(default=Priority.NORMAL, compare=True)
    created_at:  float = field(default_factory=time.monotonic, compare=True)
    _seq:        int   = field(default_factory=lambda: next(_seq), compare=True, init=False)

    name:        str                                     = field(default="targeted",             compare=False)
    target:      Any                                     = field(default=None,                   compare=False)
    fn:          Callable[[Any, "AccountPull"], Any]     = field(default=lambda t, b: None,      compare=False)
    max_retries: int                                     = field(default=3,                      compare=False)
    _retries:    int                                     = field(default=0,                      compare=False, init=False)
    meta:        dict                                    = field(default_factory=dict,            compare=False)
    delay:       float                                   = field(default=0.0,                    compare=False)
    run_at:      Any                                     = field(default=None,                   compare=False)

    def run(self, bot: "AccountPull") -> Any:
        return self.fn(self.target, bot)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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