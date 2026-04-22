# main.py
import time

# ── 0. Логування — ПЕРШИМ рядком ───────────────────────────────────────────
from src.core.logging.setup import setup_logging
setup_logging(log_dir="logs", console=False)

from src.core.logging.loggers import get_scheduler_logger
log = get_scheduler_logger()
log.info("=" * 60)
log.info("Application starting")

# ── Решта імпортів ──────────────────────────────────────────────────────────
from src.core.config import (
    AppConfig, AuthConfig, BaseHeaders,
    BotConfig, ClientConfig, NetworkConfig,
)
from src.core.database.ddl import get_db
from src.core.inventory.store import InventoryStore
from src.core.scheduler import AccountEntry, Scheduler
from src.core.worker import BotWorker
from src.core.account_pull import AccountPull
from src.mangabuff.professions.reader.build import build_reader_profession


# ── 1. Конфіг ───────────────────────────────────────────────────────────────

def create_bot_config(email: str, password: str) -> BotConfig:
    return BotConfig(
        client=ClientConfig(
            base_url="https://mangabuff.ru",
            auth=AuthConfig(email=email, password=password),
        ),
        browser=BaseHeaders(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            sec_ch_ua='"Google Chrome";v="123", "Not:A-Brand";v="8", "Chromium";v="123"',
            sec_ch_ua_platform='"Windows"',
            sec_ch_ua_mobile="?0",
            accept_language="uk-UA,uk;q=0.9,en-US;q=0.8,en;q=0.7",
            accept_encoding="gzip, deflate, br, zstd",
            dnt="1",
        ),
        network=NetworkConfig(proxy=None, timeout=15),
    )


conn    = get_db()
app_cfg = AppConfig.from_yaml("app.yaml", conn)
email, password = "jo.mefar.i.t1.4@gmail.com", "Yuki_char.png"

app_cfg.account_repo.upsert(
    "acc_01", email, "https://mangabuff.ru", profession="reader"
)

store_01 = InventoryStore(conn, "acc_01")
bot_01   = AccountPull("acc_01", create_bot_config(email, password), app_cfg, store_01)
bot_01.inventory.reader.target_slots = ["card"]

log.info("AccountPull 'acc_01' created")


# ── 2. Profession — будується разом з Trigger ────────────────────────────────
#
# build_reader_profession повертає готову Profession з:
#   startup  = [init_reader]   — ініціалізує SlotScheduler одноразово
#   triggers = [SlotTrigger]   — Scheduler викликає reader_pipeline за розкладом
#                                dynamic_next = scheduler.delay_until_next()

reader_profession = build_reader_profession(bot_01)


# ── 3. Scheduler ─────────────────────────────────────────────────────────────

def on_dead(bot: AccountPull) -> None:
    log.critical(f"[DEAD] '{bot.account_id}': {bot.error}")

scheduler = Scheduler(
    workers={
        "acc_01": AccountEntry(
            worker      = BotWorker(bot_01),
            professions = [reader_profession],
        ),
    },
    on_dead=on_dead,
)

scheduler.start()
log.info("Scheduler started")


# ── 4. Main loop ─────────────────────────────────────────────────────────────

try:
    while True:
        for row in scheduler.report():
            log.debug(
                f"[report] {row['id']} | {row['status']} | "
                f"queue={row['queue_size']} | "
                f"next_trigger={row.get('next_trigger_in', '?')}"
            )
        time.sleep(10)
except KeyboardInterrupt:
    log.info("Shutdown requested")
    scheduler.stop()
    log.info("Shutdown complete")