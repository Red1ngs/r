"""
bot/admin/runner.py — запуск адмін-бота в окремому daemon-потоці.

Бот живий весь час поки живий процес.
Зупиняється автоматично при завершенні main (daemon=True).
AdminBotRunner.stop() потрібен тільки для чистого закриття сесії при Ctrl+C.
"""
from __future__ import annotations

import asyncio
import logging
import threading
from typing import Optional

from src.bot.admin.bot import create_admin_bot
from src.bot.admin.config import AdminBotConfig
from src.bot.admin.services.scheduler_service import SchedulerService

log = logging.getLogger(__name__)


class AdminBotRunner:
    def __init__(self, config: AdminBotConfig, service: SchedulerService) -> None:
        self._config  = config
        self._service = service
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._run,
            name="admin-bot",
            daemon=True,   # помирає разом з процесом — явна зупинка не потрібна
        )
        self._thread.start()
        log.info("AdminBot started")

    def stop(self) -> None:
        """
        Викликається тільки при Ctrl+C в main.py.
        Daemon-потік і так помре, але чекаємо трохи для чистого завершення.
        """
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        log.info("AdminBot stopped")

    def _run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._poll())
        except Exception as e:
            log.error(f"AdminBot error: {e}", exc_info=True)
        finally:
            loop.close()

    async def _poll(self) -> None:
        dp, bot = create_admin_bot(self._config, self._service)
        try:
            await dp.start_polling(bot, handle_signals=False)
        finally:
            await bot.session.close()