"""
main.py — точка входу.

При старті завантажує .env (паролі акаунтів, токен бота, admin ids).
Акаунти відновлюються з БД і підключаються послідовно (StartupManager),
щоб уникнути паралельного флуду login-запитів і помилки "Сесія не встановлена".
"""
import asyncio
from pathlib import Path

# ── Завантаження .env ─────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(".env"), override=False)
except ImportError:
    pass

from src.core.logging.setup import setup_logging
setup_logging(log_dir="logs")

from src.core.logging.loggers import get_scheduler_logger
log = get_scheduler_logger()
log.info("=" * 60)
log.info("Application starting")

# ── Реєстрація ────────────────────────────────────────────────────────────────
from src.mangabuff.setup import bootstrap

bootstrap()

# ── БД ────────────────────────────────────────────────────────────────────────
from src.database.setup import init_database
repositories = init_database()

# ── AppConfig ─────────────────────────────────────────────────────────────────
from src.core.config.app import AppConfig
app_cfg = AppConfig.from_yaml("app.yaml")

# ── Часова зона ───────────────────────────────────────────────────────────────
from src.utils.time import set_timezone
set_timezone("Europe/Kiev")

# ── Scheduler ─────────────────────────────────────────────────────────────────
from src.core.core_account import Account
from src.core.runtime.scheduler import EventDrivenScheduler

# ── Services ──────────────────────────────────────────────────────────────────
from src.bot.admin.services.scheduler_service import SchedulerService
from src.database.repository.account import AccountRepository
from src.core.runtime.startup_manager import StartupManager, StartupConfig


async def restore_accounts(
    service:     SchedulerService,
    scheduler:   EventDrivenScheduler,
    repository:  AccountRepository,
    startup_cfg: StartupConfig,
) -> None:
    """
    Крок 1: реєструє всі акаунти в scheduler через add_account()
            (профессії встановлюються, але connect() ще НЕ викликано).
    Крок 2: StartupManager послідовно викликає bot.connect() з паузами —
            лише після цього монітори отримують живу сесію і починають працювати.

    Саме відсутність цього порядку викликала:
        RuntimeError: [My_bot] Сесія не встановлена...
    """
    registered: list[str] = []

    for row in repository.get_all_accounts():
        ok, err = await service.add_account(row.id, row.email)
        if not ok:
            log.warning(f"[restore] '{row.id}' пропущено: {err}")
            continue

        if row.professions:
            log.info(f"[restore] '{row.id}' → professions={row.professions}")
        else:
            log.warning(f"[restore] '{row.id}' без profession — моніторів не буде")

        registered.append(row.id)

    if not registered:
        log.info("[restore] Немає акаунтів для відновлення")
        return

    sm = StartupManager(scheduler=scheduler, cfg=startup_cfg)
    for aid in registered:
        sm.add(aid)

    await sm.run()

    if sm.failed_accounts:
        log.warning(
            "[restore] Не підключились: "
            + ", ".join(f"'{a}' ({e})" for a, e in sm.failed_accounts)
        )


# ── Admin Telegram Bot + main loop ────────────────────────────────────────────
from src.bot.admin.config import AdminBotConfig
from src.bot.admin.runner import AdminBotRunner


async def main() -> None:
    def on_dead(bot: Account) -> None:
        log.critical(f"[DEAD] '{bot.account_id}': {bot.error}")

    scheduler = await EventDrivenScheduler.initialize(on_dead=on_dead)
    log.info("Scheduler initialized (empty)")

    scheduler.start()
    
    startup_cfg = StartupConfig.from_app_config(app_cfg)
    admin_bot = None

    try:
        admin_cfg = AdminBotConfig.from_env()
        svc = SchedulerService(repositories, app_cfg)

        await restore_accounts(svc, scheduler, repositories.accounts, startup_cfg)

        admin_bot = AdminBotRunner(admin_cfg, svc)
        admin_bot.start()
        log.info("AdminBot started — додавай акаунти через /accounts")
    except RuntimeError as e:
        log.error(f"AdminBot не запущено: {e}")

    try:
        while True:
            await asyncio.sleep(30)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutdown requested...")
        try:
            # Обмежуємо час на очищення ресурсів
            await asyncio.wait_for(scheduler.stop(), timeout=20.0)
        except asyncio.TimeoutError:
            log.warning("Shutdown timed out, forcing exit")
        
        if admin_bot:
            admin_bot.stop()
        log.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())