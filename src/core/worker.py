"""
worker.py — BotWorker.

Зміни:
  1. set_http_logger() викликається в start() перед connect().
  2. Додано clear_profession(profession_id) — видаляє з черги тільки задачі
     конкретної profession, не чіпаючи задачі інших.
  3. assign() автоматично зберігає source_profession на spawned-задачах
     якщо батьківська задача мала мітку.
"""
from __future__ import annotations

import heapq
import itertools
import threading
import time
from typing import Callable, Optional

from src.core.account import Account
from src.core.logging.loggers import get_account_logger, get_task_logger
from src.core.tasks.base import AnyTask, LoopTask, ReactiveTask, TaskResult, extract_spawned
from src.core.utils.timing import to_monotonic
from src.utils.logging import set_http_logger


class BotWorker:
    HEAL_DELAYS   = [10, 30, 60]
    POLL_INTERVAL = 1.0

    def __init__(
        self,
        bot:      Account,
        on_error: Optional[Callable[[Account], None]] = None,
    ):
        self._bot      = bot
        self._on_error = on_error
        self._waiting: list[tuple[float, int, AnyTask]] = []
        self._ready:   list[tuple[int, int, AnyTask]]   = []
        self._lock     = threading.Lock()
        self._seq      = itertools.count()
        self._stop     = threading.Event()
        self._thread:  Optional[threading.Thread] = None
        self._log      = get_account_logger(bot.account_id)
        self._task_log = get_task_logger(bot.account_id)

    @property
    def bot(self) -> Account:
        return self._bot

    def set_error_callback(self, callback: Callable[[Account], None]) -> None:
        self._on_error = callback

    # ── Queue management ──────────────────────────────────────────────────────

    def assign(self, *tasks: AnyTask) -> None:
        now_mono = time.monotonic()
        with self._lock:
            for task in tasks:
                if isinstance(task, LoopTask):
                    task.meta.setdefault("stop_event", self._stop)
                seq = next(self._seq)
                if task.run_at is not None:
                    run_at_mono = to_monotonic(float(task.run_at))
                    label = f" (run_at={task.run_at!r})"
                elif task.delay:
                    run_at_mono = now_mono + task.delay
                    label = f" (delay={task.delay:.1f}s)"
                else:
                    run_at_mono = now_mono
                    label = ""
                if run_at_mono <= now_mono:
                    heapq.heappush(self._ready, (task.priority, seq, task))
                else:
                    heapq.heappush(self._waiting, (run_at_mono, seq, task))
                self._task_log.debug(
                    f"+ enqueue '{task.name}' p={task.priority}"
                    f" prof={task.source_profession or '—'}{label}"
                )

    def clear(self) -> None:
        """Очищає всю чергу (усі profession)."""
        with self._lock:
            self._waiting.clear()
            self._ready.clear()

    def clear_profession(self, profession_id: str) -> int:
        """
        Видаляє з черги задачі що належать вказаній profession.
        Задачі інших profession та системні (source_profession=None) не чіпає.
        Повертає кількість видалених задач.
        """
        removed = 0
        with self._lock:
            before_ready   = len(self._ready)
            before_waiting = len(self._waiting)

            self._ready = [
                (p, s, t) for p, s, t in self._ready
                if getattr(t, "source_profession", None) != profession_id
            ]
            heapq.heapify(self._ready)

            self._waiting = [
                (rt, s, t) for rt, s, t in self._waiting
                if getattr(t, "source_profession", None) != profession_id
            ]
            heapq.heapify(self._waiting)

            removed = (before_ready - len(self._ready)) + (before_waiting - len(self._waiting))

        if removed:
            self._task_log.info(
                f"clear_profession({profession_id!r}): removed {removed} tasks"
            )
        return removed

    @property
    def queue_size(self) -> int:
        with self._lock:
            return len(self._waiting) + len(self._ready)

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def start(self) -> None:
        set_http_logger(self._task_log)
        if not self._bot.connect():
            self._bot.mark_dead("connect() failed on start")
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            name=f"worker-{self._bot.account_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=30)
        self._bot.disconnect()
        self._bot.repo.inventory.save(self._bot.account_id, self._bot.inventory)

    def run_once(self) -> Optional[TaskResult]:
        """Для тестів — виконує одну задачу синхронно."""
        self._drain_waiting()
        with self._lock:
            if not self._ready:
                return None
            _, _, task = heapq.heappop(self._ready)
        return self._execute(task)

    # ── Internal loop ─────────────────────────────────────────────────────────

    def _loop(self) -> None:
        set_http_logger(self._task_log)
        while not self._stop.is_set():
            self._drain_waiting()
            with self._lock:
                if self._ready:
                    _, _, task = heapq.heappop(self._ready)
                else:
                    task = None
            if task is None:
                self._bot.mark_idle()
                self._stop.wait(self._next_wake())
                continue
            self._execute(task)

    def _drain_waiting(self) -> None:
        now = time.monotonic()
        with self._lock:
            while self._waiting and self._waiting[0][0] <= now:
                _run_at, seq, task = heapq.heappop(self._waiting)
                heapq.heappush(self._ready, (task.priority, seq, task))

    def _next_wake(self) -> float:
        with self._lock:
            if self._waiting:
                remaining = self._waiting[0][0] - time.monotonic()
                return max(0.0, min(remaining, self.POLL_INTERVAL))
        return self.POLL_INTERVAL

    def _execute(self, task: AnyTask) -> TaskResult:
        self._bot.mark_working()
        self._task_log.info(
            f"▶ START  '{task.name}'"
            f"  prof={task.source_profession or '—'}"
            f"  p={task.priority}"
            f"  retry={task.retries}/{task.max_retries}"
        )
        try:
            value  = task.run(self._bot)
            result = TaskResult(task=task, success=True, value=value)
            self._task_log.info(f"✅ DONE   '{task.name}'")

            spawned = extract_spawned(value)
            if spawned:
                # Успадковуємо мітку profession від батьківської задачі
                parent_prof = getattr(task, "source_profession", None)
                if parent_prof:
                    for child in spawned:
                        if getattr(child, "source_profession", None) is None:
                            child.source_profession = parent_prof  # type: ignore[attr-defined]
                self.assign(*spawned)

            self._emit_task_completed(task)

        except Exception as e:
            result = TaskResult(task=task, success=False, error=e)
            self._task_log.error(f"❌ FAIL   '{task.name}': {e}", exc_info=True)
            self._handle_task_error(task, e)
            if self._on_error:
                self._on_error(self._bot)
        finally:
            self._bot.repo.inventory.save(self._bot.account_id, self._bot.inventory)
            self._bot.mark_idle()

        if isinstance(task, ReactiveTask) and task.requeue and result.success:
            self.assign(task)

        return result

    def _emit_task_completed(self, task: AnyTask) -> None:
        try:
            from src.core.runtime.scheduler import EventDrivenScheduler
            scheduler = EventDrivenScheduler.get_instance()
            scheduler.emit_event("task.completed", {
                "account_id":       self._bot.account_id,
                "task_name":        task.name,
                "source_profession": getattr(task, "source_profession", None),
            }, source=self._bot.account_id)
        except RuntimeError:
            pass

    def _handle_task_error(self, task: AnyTask, error: Exception) -> None:
        if task.can_retry:
            task.increment_retry()
            self.assign(task)
        else:
            self._log.error(f"'{task.name}' failed permanently")
            self._bot.mark_dead(f"Task '{task.name}' failed: {error}")
        if self._is_session_error(error):
            self._try_recover()

    def _try_recover(self) -> None:
        self._bot.disconnect()
        for attempt, delay in enumerate(self.HEAL_DELAYS, start=1):
            self._log.warning(f"🔄 Recovery {attempt}/{len(self.HEAL_DELAYS)} — waiting {delay}s")
            self._stop.wait(delay)
            if self._stop.is_set():
                return
            if self._bot.connect():
                self._log.info("✅ Recovered")
                return
        self._bot.mark_dead(f"Failed to recover after {len(self.HEAL_DELAYS)} attempts")
        self._stop.set()

    @staticmethod
    def _is_session_error(error: Exception) -> bool:
        if isinstance(error, (PermissionError, ConnectionError, TimeoutError)):
            return True
        msg = str(error).lower()
        return any(kw in msg for kw in ("419", "401", "unauthorized", "session", "csrf"))

    def __repr__(self) -> str:
        return (
            f"<BotWorker [{self._bot.account_id}] "
            f"status={self._bot.status.name} "
            f"queue={self.queue_size}>"
        )
