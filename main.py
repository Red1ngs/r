import time
from typing import List, Dict, Any

# ── 0. Логування ──────────────────────────────────────────────────────────
from src.core.logging.setup import setup_logging
setup_logging(log_dir="logs")

from src.core.logging.loggers import get_scheduler_logger
log = get_scheduler_logger()
log.info("=" * 60)
log.info("Application starting with multiple accounts")

# ── 1. Реєстрація інвентарів ──────────────────────────────────────────────
from src.mangabuff.setup import register_inventories
register_inventories()

from src.core.config.bot import AuthConfig, BaseHeaders, BotConfig, ClientConfig, NetworkConfig
from src.core.config.app import AppConfig
from src.database.ddl import get_db
from src.core.inventory.store import InventoryStore
from src.core.scheduler import AccountEntry, Scheduler
from src.core.worker import BotWorker
from src.core.account import AccountPull
from src.mangabuff.reader.build import build_reader_profession

# ── 2. Конфігурація ────────────────────────────────────────────────────────

# СПИСОК АКАУНТІВ
ACCOUNTS_DATA = [
    {"id": "acc_02", "email": "kun.d.e.rta.rme.l@gmail.com", "password": "", "proxy": ""},
    {"id": "acc_03", "email": "p.r.ic.e00.2.5@gmail.com", "password": "", "proxy": ""},
]

def create_bot_config(email: str, password: str, proxy: str) -> BotConfig:
    return BotConfig(
        client=ClientConfig(
            base_url="https://mangabuff.ru",
            auth=AuthConfig(email=email, password=password),
        ),
        browser=BaseHeaders(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            sec_ch_ua='"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            sec_ch_ua_platform='"Windows"',
            sec_ch_ua_mobile="?0",
            accept_language="uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            accept_encoding="gzip, deflate, br, zstd",
            dnt="1",
        ),
        network=NetworkConfig(proxy=proxy if proxy else None, timeout=15),
    )

conn    = get_db()
app_cfg = AppConfig.from_yaml("app.yaml", conn)

# ── 3. Динамічне створення воркерів ────────────────────────────────────────

# Вказуємо тип словника: ключі - рядки (id), значення - об'єкти AccountEntry
workers_map: Dict[str, AccountEntry] = {}

# Вказуємо тип списку: містить об'єкти статистики (Any, якщо тип не імпортований)
all_reward_stats: List[Any] = [] 

for acc in ACCOUNTS_DATA:
    acc_id = str(acc["id"])
    email = str(acc["email"])
    password = str(acc["password"])
    proxy = str(acc["proxy"])

    # Реєструємо в БД
    app_cfg.account_repo.upsert(
        acc_id, email, "https://mangabuff.ru", profession="reader"
    )

    # Створюємо інстанс бота
    store = InventoryStore(conn, acc_id)
    bot = AccountPull(acc_id, create_bot_config(email, password, proxy), app_cfg, store)
    bot.inventory.reader.target_slots = ["card", "scroll"]

    # Будуємо професію
    reader_profession, reward_stats = build_reader_profession(bot)
    all_reward_stats.append(reward_stats)

    # Додаємо в словник воркерів
    workers_map[acc_id] = AccountEntry(
        worker      = BotWorker(bot),
        professions = [reader_profession],
    )
    
    log.info(f"AccountPull '{acc_id}' initialized")

# ── 4. Scheduler ─────────────────────────────────────────────────────────────

def on_dead(bot: AccountPull) -> None:
    log.critical(f"[DEAD] '{bot.account_id}': {bot.error}")

scheduler = Scheduler(
    workers=workers_map,
    on_dead=on_dead,
)

scheduler.start()
log.info(f"Scheduler started with {len(workers_map)} accounts")

# ── 5. Main loop ─────────────────────────────────────────────────────────────

try:
    while True:
        time.sleep(30)
except KeyboardInterrupt:
    log.info("Shutdown requested")
    scheduler.stop()
    
    # Дамп статистики для всіх акаунтів
    for stats in all_reward_stats:
        stats.dump()
        
    log.info("Shutdown complete")