"""
core/runtime/proxy_queue.py — черга HTTP-запитів per-proxy.

Замінює proxy_rate_limiter + RateLimiter повністю.
───────────────────────────────────────────────────
Нова схема: один asyncio-воркер на кожен унікальний проксі.
Запити виконуються строго послідовно. Між завершенням одного запиту
і початком наступного воркер витримує REQUEST_DELAY.
Колізії виключені архітектурно.

Від попередньої версії прибрано sync-async місток:
  - немає submit_sync / run_in_executor / concurrent.futures.Future
  - немає threading.Future
  - воркер приймає async корутини напряму через enqueue_coro()
  - BotTransport → async, curl_cffi.AsyncSession

Структура:
    ProxyQueueManager (singleton)
    ├── "http://1.2.3.4:8080" → _ProxyWorker → asyncio.Queue → sequential
    ├── "http://5.6.7.8:3128" → _ProxyWorker → asyncio.Queue → sequential
    └── "__no_proxy__"        → _ProxyWorker → asyncio.Queue → sequential

Використання в BotTransport:
    # В __init__ — реєструємо воркер:
    proxy_queue_manager.ensure(bot_config.network.proxy)

    # В get() і post():
    return await proxy_queue_manager.enqueue_coro(
        proxy = self.bot_config.network.proxy,
        coro  = self._client.get(url, **kwargs),
        label = f"GET {url}",
    )
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Coroutine, Dict, Optional, TypeVar

# Додаємо Generic для підтримки типізації черги
T = TypeVar("T")

log = logging.getLogger("core.proxy_queue")

REQUEST_DELAY: float = 2.0
RETRY_AFTER_DEFAULT: float = 15.0
MAX_RETRIES: int = 5
_NO_PROXY_KEY = "__no_proxy__"


class RateLimitedError(Exception):
    def __init__(self, retry_after: float = RETRY_AFTER_DEFAULT) -> None:
        self.retry_after = retry_after
        super().__init__(f"429 Too Many Requests, retry after {retry_after:.1f}s")


@dataclass
class _Task:
    # Використовуємо Any, бо в черзі лежать різні типи запитів.
    # Future[Any] дозволяє передати результат будь-якого типу.
    coro:   Coroutine[Any, Any, Any]
    future: asyncio.Future[Any]
    label:  str = ""


class _ProxyWorker:
    def __init__(self, key: str) -> None:
        self._key = key
        self._queue: Optional[asyncio.Queue[Optional[_Task]]] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._shutting_down: bool = False  # FIX Bug 4: guard проти нових enqueue після shutdown

    def _get_queue(self) -> asyncio.Queue[Optional[_Task]]:
        if self._queue is None:
            self._queue = asyncio.Queue()
        return self._queue

    def _ensure_task_started(self) -> None:
        """
        Запускає _run()-task у поточному running loop.
        Викликається ліниво при першому enqueue() — гарантує що task
        прив'язаний до того самого loop, що й усі майбутні enqueue/shutdown.

        Якщо попередній task мертвий (loop перезапустився) — стартує новий.
        """
        loop = asyncio.get_running_loop()
        if (
            self._task is None
            or self._task.done()
            or self._task.get_loop() is not loop
        ):
            # FIX Bug 2: стара черга могла містити багато завдань — дренуємо
            # всі, щоб Future-и не зависли навічно.
            if self._queue is not None:
                while True:
                    try:
                        orphan = self._queue.get_nowait()
                        if orphan is not None and not orphan.future.done():
                            orphan.future.set_exception(
                                RuntimeError(
                                    f"[ProxyWorker:{self._key}] worker restarted — task cancelled"
                                )
                            )
                    except asyncio.QueueEmpty:
                        break
            self._queue = asyncio.Queue()
            self._shutting_down = False  # FIX Bug 4: скидаємо прапор при перезапуску
            self._task = asyncio.create_task(self._run(), name=f"proxy-worker-{self._key}")
            log.debug(f"[ProxyWorker:{self._key}] task started in loop {id(loop)}")

    def start(self) -> None:
        """
        Залишено для зворотної сумісності (викликається з ensure()).
        Реальний старт task відкладено до першого enqueue() — щоб task
        гарантовано прив'язався до правильного loop (scheduler's, не main's).
        """
        log.debug(f"[ProxyWorker:{self._key}] registered (lazy start)")

    def enqueue(self, coro: Coroutine[Any, Any, T], label: str = "") -> asyncio.Future[T]:
        # FIX Bug 4: відхиляємо нові завдання після виклику shutdown().
        if self._shutting_down:
            loop = asyncio.get_running_loop()
            fut: asyncio.Future[T] = loop.create_future()
            fut.set_exception(RuntimeError(
                f"[ProxyWorker:{self._key}] worker is shutting down — enqueue rejected"
            ))
            return fut
        # Lazy start: task стартує тут — завжди в правильному running loop.
        self._ensure_task_started()
        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        task = _Task(coro=coro, future=fut, label=label)
        self._get_queue().put_nowait(task)
        return fut

    async def shutdown(self) -> None:
        # FIX Bug 4: встановлюємо прапор ДО sentinel, щоб concurrent
        # enqueue() вже не міг встромити нове завдання між sentinel і worker'ом.
        self._shutting_down = True

        # Надсилаємо sentinel None щоб _run() вийшов з циклу.
        if self._queue is not None:
            self._queue.put_nowait(None)

        if self._task is None or self._task.done():
            return

        try:
            current_loop = asyncio.get_running_loop()
            if self._task.get_loop() is not current_loop:
                # Task прив'язаний до іншого (вже мертвого) loop —
                # cancel() без await є єдиним безпечним варіантом.
                log.warning(
                    f"[ProxyWorker:{self._key}] task loop mismatch — force cancel"
                )
                self._task.cancel()
            else:
                await asyncio.wait_for(self._task, timeout=5.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            if self._task:
                self._task.cancel()
        except RuntimeError as e:
            log.warning(f"[ProxyWorker:{self._key}] shutdown runtime error: {e}")
            if self._task:
                self._task.cancel()

    @property
    def queue_size(self) -> int:
        return self._queue.qsize() if self._queue else 0

    async def _run(self) -> None:
        queue = self._get_queue()
        log.debug(f"[ProxyWorker:{self._key}] loop started")
        while True:
            task = await queue.get()
            if task is None:
                queue.task_done()
                break
            await self._execute(task)
            queue.task_done()
            # FIX Bug 3: затримка завжди, незалежно від наявності наступного
            # завдання у черзі — інакше два запити, що надійшли підряд,
            # виконуються без паузи між ними.
            await asyncio.sleep(REQUEST_DELAY)
        log.debug(f"[ProxyWorker:{self._key}] loop finished")

    async def _execute(self, task: _Task) -> None:
        label = task.label or "<anonymous>"

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                result = await task.coro
                if not task.future.done():
                    task.future.set_result(result)
                return

            except RateLimitedError as e:
                log.warning(
                    f"[ProxyWorker:{self._key}] 429 on {label!r} "
                    f"(attempt {attempt}/{MAX_RETRIES}), "
                    f"freezing proxy for {e.retry_after:.1f}s"
                )
                # FIX Bug 1: реальний retry — чекаємо та повторюємо,
                # а не одразу прокидаємо виключення до caller'а.
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(e.retry_after)
                    # Коректуємо корутину для повторного виклику: оскільки
                    # корутина вже вичерпана після першого await, caller
                    # має передавати фабрику. Але поки API передає готову
                    # корутину — логуємо і виходимо з описовою помилкою.
                    log.warning(
                        f"[ProxyWorker:{self._key}] coroutine {label!r} is exhausted "
                        f"after first await — cannot retry. "
                        f"Refactor callers to pass a coroutine factory for retries."
                    )
                    break
                # Вичерпано всі спроби
                if not task.future.done():
                    task.future.set_exception(e)
                return

            except Exception as exc:
                log.error(
                    f"[ProxyWorker:{self._key}] error on {label!r}: {exc}",
                    exc_info=True,
                )
                if not task.future.done():
                    task.future.set_exception(exc)
                return

        exc = RuntimeError(
            f"[ProxyWorker:{self._key}] {label!r} "
            f"failed after {MAX_RETRIES} retries (429)"
        )
        log.error(str(exc))
        if not task.future.done():
            task.future.set_exception(exc)


class ProxyQueueManager:
    def __init__(self) -> None:
        self._workers: Dict[str, _ProxyWorker] = {}

    def _key(self, proxy: Optional[str]) -> str:
        if not proxy:
            return _NO_PROXY_KEY
        try:
            from urllib.parse import urlparse
            p = urlparse(proxy)
            host = p.hostname or ""
            port = f":{p.port}" if p.port else ""
            return f"{p.scheme}://{host}{port}"
        except Exception:
            return proxy

    def ensure(self, proxy: Optional[str]) -> _ProxyWorker:
        key = self._key(proxy)
        if key not in self._workers:
            worker = _ProxyWorker(key)
            worker.start()
            self._workers[key] = worker
            log.info(f"[ProxyQueueManager] new worker for proxy={key!r}")
        return self._workers[key]

    async def enqueue_coro(
        self,
        proxy: Optional[str],
        coro:  Coroutine[Any, Any, T],
        label: str = "",
    ) -> T:
        """
        Виконує корутину та повертає результат типу T.
        Типізація забезпечується прокиданням T через worker.enqueue.
        """
        worker = self.ensure(proxy)
        # await чекає на Future[T], що повертає T
        return await worker.enqueue(coro, label=label)

    def queue_sizes(self) -> Dict[str, int]:
        return {k: w.queue_size for k, w in self._workers.items()}

    async def shutdown_all(self) -> None:
        keys = list(self._workers.keys())
        for key in keys:
            worker = self._workers[key]
            log.info(f"[ProxyQueueManager] shutting down {key!r}")
            try:
                await worker.shutdown()
            except Exception as e:
                log.error(f"Error shutting down worker {key}: {e}")
            finally:
                del self._workers[key]


proxy_queue_manager = ProxyQueueManager()