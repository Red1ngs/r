"""
main.py — точка входу.

При старті завантажує .env (паролі акаунтів, токен бота, admin ids).
Акаунти додаються через адмін-панель — main.py їх не знає.
"""
import time
from pathlib import Path

# ── Завантаження .env ─────────────────────────────────────────────────────────
# python-dotenv: pip install python-dotenv
# .env містить: ADMIN_BOT_TOKEN, ADMIN_IDS, ACCOUNT_PASSWORD_ACC_XX=...
try:
    from dotenv import load_dotenv
    load_dotenv(Path(".env"), override=False)  # override=False: os.environ має пріоритет
except ImportError:
    pass  # якщо dotenv не встановлено — змінні мають бути встановлені системою

from src.core.logging.setup import setup_logging
setup_logging(log_dir="logs")

from src.core.logging.loggers import get_scheduler_logger
log = get_scheduler_logger()
log.info("=" * 60)
log.info("Application starting")

# ── Реєстрація інвентарів ─────────────────────────────────────────────────────
from src.mangabuff.setup import register_inventories
register_inventories()

# ── Реєстрація професій ─────────────────────────────────────────────────────
from src.mangabuff.setup import register_professions
register_professions()

# ── Реєстрація репозиторіїв ─────────────────────────────────────────────────────
from src.database.setup import init_database
repositories = init_database()

# ── БД і AppConfig ────────────────────────────────────────────────────────────
from src.core.config.app import AppConfig

app_cfg = AppConfig.from_yaml("app.yaml")

# ── Scheduler (Singleton, порожній) ───────────────────────────────────────────
from src.core.account import Account
from src.core.runtime.scheduler import EventDrivenScheduler

def on_dead(bot: Account) -> None:
    log.critical(f"[DEAD] '{bot.account_id}': {bot.error}")

scheduler = EventDrivenScheduler.initialize(on_dead=on_dead)
scheduler.start()
log.info("Scheduler initialized (empty)")

# ── Відновлення акаунтів з БД ─────────────────────────────────────────────────
from src.bot.admin.services.scheduler_service import SchedulerService
from src.database.repository.account import AccountRepository

def restore_accounts_from_db(
    service: SchedulerService, 
    repository: AccountRepository
) -> int:
    """Завантажує всі активні акаунти з бази в шедулер."""
    restored = 0
    accounts_map = repository.get_all_accounts()

    for acc_id, email in accounts_map.items():
        ok, err = service.add_account(acc_id, email)
        if ok:
            restored += 1
            log.info(f"[restore] '{acc_id}' відновлено")
        else:
            log.warning(f"[restore] '{acc_id}' пропущено: {err}")
            
    return restored

# ── Admin Telegram Bot ────────────────────────────────────────────────────────
from src.bot.admin.config import AdminBotConfig
from src.bot.admin.runner import AdminBotRunner
from src.bot.admin.services.scheduler_service import SchedulerService

try:
    admin_cfg = AdminBotConfig.from_env()
    svc = SchedulerService(repositories, app_cfg)
    restore_accounts_from_db(svc, repositories.accounts)
    
    admin_bot = AdminBotRunner(admin_cfg, svc)
    admin_bot.start()
    log.info("AdminBot started — додавай акаунти через /accounts")
except RuntimeError as e:
    log.error(f"AdminBot не запущено: {e}")
    admin_bot = None  # type: ignore[assignment]

# ── Main loop ─────────────────────────────────────────────────────────────────
try:
    while True:
        time.sleep(30)
except KeyboardInterrupt:
    log.info("Shutdown requested")
    scheduler.stop()
    if admin_bot:
        admin_bot.stop()
    log.info("Shutdown complete")