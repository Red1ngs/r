"""
startup_manager.py — послідовний запуск акаунтів.

Проблема:
    scheduler.add_account() реєструє акаунт і піднімає профессії,
    але НЕ чіпляє монітори (це зроблено навмисно).
    StartupManager послідовно викликає scheduler.connect_account(),
    який робить bot.connect() і лише після успіху підключає монітори.

    Без паузи між акаунтами — паралельний флуд login-запитів.
    З паузою — плавний старт без банів/throttle.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.core.runtime.scheduler import EventDrivenScheduler

log = logging.getLogger("src.runtime.startup_manager")


@dataclass
class StartupConfig:
    """Параметри плавного запуску. Зчитуються з app.yaml → секція startup:"""
    connect_delay:   float = 5.0   # пауза між connect() двох сусідніх акаунтів (сек)
    connect_timeout: float = 30.0  # таймаут одного connect()
    skip_failed:     bool  = True  # пропустити збійний акаунт і продовжити старт

    @classmethod
    def from_app_config(cls, cfg) -> "StartupConfig":
        s = getattr(cfg, "startup", None)
        if s is None:
            return cls()
        return cls(
            connect_delay=getattr(s, "connect_delay", 5.0),
            connect_timeout=getattr(s, "connect_timeout", 30.0),
            skip_failed=getattr(s, "skip_failed", True),
        )


class StartupManager:
    """
    Послідовний connect для кожного акаунта з паузою між ними.

    scheduler.connect_account() — синхронний (HTTP-запити), тому
    запускається в ThreadPoolExecutor щоб не блокувати event loop.

    Використання в main.py:
        sm = StartupManager(
            scheduler=scheduler,
            cfg=StartupConfig.from_app_config(app_cfg),
        )
        for account_id in registered:
            sm.add(account_id)
        await sm.run()
    """

    def __init__(
        self,
        scheduler: "EventDrivenScheduler",
        cfg: StartupConfig | None = None,
    ) -> None:
        self._scheduler = scheduler
        self._cfg       = cfg or StartupConfig()
        self._queue:  list[str]            = []
        self._ok:     list[str]            = []
        self._failed: list[tuple[str, str]] = []

    def add(self, account_id: str) -> None:
        self._queue.append(account_id)

    @property
    def ok_accounts(self)     -> list[str]:             return list(self._ok)
    @property
    def failed_accounts(self) -> list[tuple[str, str]]: return list(self._failed)

    async def run(self) -> None:
        if not self._queue:
            log.info("[StartupManager] Черга порожня")
            return

        total = len(self._queue)
        log.info(
            f"[StartupManager] Плавний запуск {total} акаунтів "
            f"(затримка між акаунтами: {self._cfg.connect_delay}s)"
        )

        loop = asyncio.get_running_loop()

        for idx, account_id in enumerate(self._queue):
            if idx > 0:
                await asyncio.sleep(self._cfg.connect_delay)

            log.info(f"[StartupManager] [{idx + 1}/{total}] connect '{account_id}' …")
            try:
                # connect_account() синхронний → executor
                ok = await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        self._scheduler.connect_account,
                        account_id,
                    ),
                    timeout=self._cfg.connect_timeout,
                )
                if ok:
                    self._ok.append(account_id)
                    log.info(f"[StartupManager] ✓ '{account_id}' підключено")
                else:
                    bot = self._scheduler.get_bot(account_id)
                    err = getattr(bot, "error", "connect() повернув False") if bot else "акаунт не знайдено"
                    self._failed.append((account_id, str(err)))
                    log.error(f"[StartupManager] ✗ '{account_id}': {err}")
                    if not self._cfg.skip_failed:
                        raise RuntimeError(str(err))

            except asyncio.TimeoutError:
                err = f"timeout ({self._cfg.connect_timeout}s)"
                self._failed.append((account_id, err))
                log.error(f"[StartupManager] ✗ '{account_id}': {err}")
                if not self._cfg.skip_failed:
                    raise

            except Exception as exc:
                self._failed.append((account_id, str(exc)))
                log.error(f"[StartupManager] ✗ '{account_id}': {exc}")
                if not self._cfg.skip_failed:
                    raise

        log.info(
            f"[StartupManager] READY — підключено: {len(self._ok)}/{total}"
            + (f", невдало: {[a for a, _ in self._failed]}" if self._failed else "")
        )