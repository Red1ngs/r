# MangaBuff Reader Bot

Система управління ботами для акаунтів у MangaBuff або подібних сервісах. Підтримує завдання (tasks), професії (professions), інвентарі (inventories) та події (events). Побудована на Python з використанням SQLite для зберігання стану.

## Основні концепції

- **Завдання (Tasks)**: Одноразові, циклічні або реактивні дії для бота.
- **Професії (Professions)**: Ролі акаунтів, що визначають набір завдань.
- **Інвентарі (Inventories)**: Стан акаунта (персональний, альянсовий тощо), зберігається в БД.
- **Події (Events)**: Асинхронні повідомлення, що обробляються реактивними завданнями.
- **Scheduler**: Керує ботами, запускає завдання за розкладом.

## Встановлення

1. Клонувати репозиторій.
2. Встановити залежності: `pip install -r requirements.txt` (якщо є).
3. Налаштувати БД: код автоматично створює `bot_state.db`.
4. Запустити: `python main.py`.

## Приклади використання

### 1. Створення та виконання простого завдання (Task)

Завдання змінює інвентар, наприклад, збільшує кількість коментарів.

```python
from src.core.task import Task, Priority
from src.core.worker import BotWorker
from src.account_pull import Account
from src.core.inventory import Inventories
from unittest.mock import MagicMock  # Для тестів

# Створюємо бота (з конфігом)
config = Config(...)  # Див. bot_configs.py
store = InventoryStore(db, "test_bot")
bot = Account("test_bot", config, store)
bot._session = MagicMock()  # Мок-сесія для тестів
worker = BotWorker(bot)

# Функція завдання: збільшує коментарі
def write_comment(inv: Inventories):
    inv.personal.comments_written += 1
    print(f"Написано коментар #{inv.personal.comments_written}")

# Створюємо завдання з пріоритетом
task = Task(
    name="write_comment",
    fn=write_comment,
    priority=Priority.NORMAL,
    max_retries=2
)

# Додаємо до черги та виконуємо
worker.assign(task)
result = worker.run_once()  # Виконує одразу
print(f"Результат: {result.success}")  # True, якщо успішно
```

### 2. Циклічне завдання (LoopTask) з умовами

Завдання повторюється, поки умова виконується.

```python
from src.core.task import LoopTask
from src.core.conditions import below, all_of, not_, has

def farm_comments(inv: Inventories):
    inv.personal.comments_written += 1
    print(f"Коментар #{inv.personal.comments_written}")

# Умови: менше 5 коментарів І не забанений
condition = all_of(
    below("comments_written", 5),
    not_(has("is_banned"))
)

loop_task = LoopTask(
    name="farm_comments",
    fn=farm_comments,
    condition=condition,
    interval=1.0  # Інтервал між ітераціями (сек)
)

worker.assign(loop_task)
worker.run_once()  # Виконує цикл до зупинки
# Результат: comments_written = 5 (якщо не banned)
```

### 3. Реактивне завдання (ReactiveTask) для подій

Обробляє події з черги, може породжувати нові завдання.

```python
from src.core.task import ReactiveTask, Task

# Джерело подій: pending_trades
def get_trades(inv: Inventories):
    return inv.personal.pending_trades

# Обробник: перевіряє want_list, повертає завдання для прийняття
def process_trade(event: dict, inv: Inventories):
    want_list = inv.personal.get("want_list", [])
    if event["item"] in want_list:
        inv.personal.trades_accepted += 1
        # Породжує нове завдання
        return Task(
            name="accept_trade",
            fn=lambda inv: print(f"Прийнято торгівлю {event['trade_id']}")
        )
    else:
        inv.personal.trades_declined += 1

reactive_task = ReactiveTask(
    name="process_trades",
    source=get_trades,
    handler=process_trade,
    requeue=True  # Залишається активним після обробки
)

# Додаємо подію до інвентаря
bot.inventories.personal.push_trade({
    "item": "Naruto vol.1",
    "trade_id": "t-123"
})

worker.assign(reactive_task)
worker.run_once()  # Обробляє подію, породжує нове завдання
```

### 4. Створення професії (Profession)

Професія — функція, що повертає завдання для бота.

```python
from src.core.task import Task, LoopTask, ReactiveTask
from src.account_pull import Account
from src.core.inventory import Inventories

def my_trader_profession(bot: Account):
    """
    Професія трейдера: синхронізація, ферма коментарів, обробка торгів.
    """
    # Одноразове завдання: синхронізація
    def sync_data(inv: Inventories):
        inv.personal.set("synced", True)
        print("Дані синхронізовано")

    yield Task(name="initial_sync", fn=sync_data, priority=Priority.HIGH)

    # Циклічне: ферма коментарів
    yield LoopTask(
        name="farm_comments",
        fn=lambda inv: inv.personal.comments_written += 1,
        condition=below("comments_written", 10),
        interval=2.0
    )

    # Реактивне: торгівля
    yield ReactiveTask(
        name="handle_trades",
        source=lambda inv: inv.personal.pending_trades,
        handler=lambda e, inv: Task(name="log_trade", fn=lambda inv: print(f"Торгівля: {e}"))
    )

# Використання в scheduler (див. нижче)
```

### 5. Запуск Scheduler з розкладом

Керує ботами, запускає професії за розкладом.

```python
from src.core.database import get_db
from src.core.repository import AccountRepository
from src.core.scheduler import Scheduler
from src.professions.daily_bonus import daily_bonus
from src.professions.trader import trader

# БД та репозиторій
db = get_db("bot_state.db")
repo = AccountRepository(db)
repo.upsert("acc_01", "email@example.com", "https://mangabuff.ru", profession="trader")

# Функція при смерті бота
def on_bot_dead(bot: Account):
    print(f"Бот {bot.account_id} мертвий: {bot.error}")

# Створюємо scheduler
scheduler = Scheduler(
    conn=db,
    repo=repo,
    configs=CONFIGS,  # З bot_configs.py
    professions={
        "acc_01": [trader, daily_bonus]  # Список професій для акаунта
    },
    on_dead=on_bot_dead
)

# Реєструємо розклад: щогодини бонус, зупинка при бані
scheduler.every(
    account_id="acc_01",
    interval=3600,  # Секунди
    producer=daily_bonus,
    until=lambda inv: inv.personal.get("is_banned", False)
)

# Запуск
scheduler.start()

# Моніторинг (в окремому потоці)
import time
try:
    while True:
        time.sleep(30)
        print("\nСтан ботів:")
        for row in scheduler.report():
            bot = scheduler.get_bot(row["id"])
            if bot:
                p = bot.inventories.personal
                print(f"  {row['id']}: статус={row['status']}, черга={row['queue_size']}, "
                      f"коментарі={p.comments_written}, banned={p.get('is_banned')}")
            else:
                print(f"  {row['id']}: OFFLINE")
except KeyboardInterrupt:
    scheduler.stop()
```

### 6. Робота з інвентарем та збереженням

Інвентарі зберігаються автоматично після завдань.

```python
from src.core.inventory_store import InventoryStore

# Зміна даних
bot.inventories.personal.set("want_list", ["Naruto", "One Piece"])
bot.inventories.personal.update({
    "last_sync": time.time(),
    "is_banned": False
})

# Альянсовий інвентар
bot.inventories.alliance.update({
    "name": "Shadow Guild",
    "rank": 5
})

# Збереження вручну
bot.store.save(bot.inventories)

# Завантаження з БД
store = InventoryStore(db, "acc_01")
inventories = store.load()
print(f"Want list: {inventories.personal.get('want_list')}")
print(f"Alliance: {inventories.alliance.name}")
```

### 7. Обробка подій та помилок

```python
# Додавання події
bot.inventories.personal.push_event({
    "type": "notification",
    "text": "Новий коментар"
})

# ReactiveTask обробить її
# При помилці: retry до max_retries, потім heal (reconnect)
# Логування: див. utils/logging.py
```

## Додаткові атомарні приклади

### 8. Використання TargetedTask для конкретних об'єктів

Ситуація: Потрібно виконати дію для конкретного предмета або користувача (наприклад, прийняти торгівлю з певним ID).

```python
from src.core.task import TargetedTask

def accept_trade(target: dict, inv: Inventories):
    trade_id = target["trade_id"]
    print(f"Приймаю торгівлю {trade_id}")
    # session.post(f"/trades/{trade_id}/accept")
    inv.personal.trades_accepted += 1

# Створюємо завдання для конкретної торгівлі
targeted_task = TargetedTask(
    name="accept_specific_trade",
    target={"trade_id": "t-456", "user_id": "user123"},
    fn=accept_trade,
    priority=Priority.HIGH
)

worker.assign(targeted_task)
worker.run_once()  # Виконує для цього target
```

### 9. Створення складних умов для LoopTask

Ситуація: Ферма ресурсів тільки в певний час доби або при виконанні кількох умов.

```python
from src.core.conditions import in_alliance, reached, any_of
import datetime

def is_daytime():
    hour = datetime.datetime.now().hour
    return 8 <= hour <= 20  # День

# Умови: в альянсі, менше 100 коментарів, день І не banned
condition = all_of(
    in_alliance(),  # Перевіряє alliance.name
    below("comments_written", 100),
    lambda inv: is_daytime(),  # Кастомна функція
    not_(has("is_banned"))
)

loop_task = LoopTask(
    name="daytime_farming",
    fn=lambda inv: inv.personal.comments_written += 1,
    condition=condition,
    interval=5.0
)

worker.assign(loop_task)
worker.run_once()
```

### 10. Обробка помилок та retry у завданнях

Ситуація: Завдання може провалитися (наприклад, мережева помилка), потрібно retry або логувати.

```python
def risky_api_call(inv: Inventories):
    # Імітація помилки
    if random.random() < 0.5:
        raise ConnectionError("Network error")
    inv.personal.set("api_called", True)

task = Task(
    name="api_call",
    fn=risky_api_call,
    max_retries=3  # Спробує 3 рази
)

worker.assign(task)
result = worker.run_once()

if not result.success:
    print(f"Помилка: {result.error}")
    # Worker автоматично retry, якщо can_retry
```

### 11. Робота з подіями в пам'яті та БД

Ситуація: Події можуть бути в пам'яті (швидкі) або в БД (стійкі).

```python
# В пам'яті: швидкі події
bot.inventories.personal.push_event({"type": "msg", "text": "Hello"})

# В БД: стійкі події (якщо реалізовані)
from src.core.database import get_db
db = get_db("bot_state.db")
db.execute("INSERT INTO events (account_id, kind, payload) VALUES (?, ?, ?)",
           ("acc_01", "trade", '{"item": "Naruto"}'))

# ReactiveTask для БД-подій
def get_db_events(inv: Inventories):
    # Завантажити з БД
    rows = db.execute("SELECT payload FROM events WHERE account_id = ? AND status = 'pending'",
                      (bot.account_id,)).fetchall()
    return [json.loads(row[0]) for row in rows]

reactive_task = ReactiveTask(
    name="process_db_events",
    source=get_db_events,
    handler=lambda e, inv: print(f"Оброблено: {e}")
)
```

### 12. Міграція даних при зміні полів інвентаря

Ситуація: Додаєте нове поле до data, потрібно оновити існуючі записи в БД.

```python
# Припустимо, додаємо "new_field" до personal data
# Спочатку оновіть код inventory.py, щоб ініціалізувати defaults

# Міграція в коді (при запуску)
def migrate_inventory(db):
    rows = db.execute("SELECT account_id, data FROM inventory WHERE kind = 'personal'").fetchall()
    for account_id, data_str in rows:
        data = json.loads(data_str)
        if "new_field" not in data:
            data["new_field"] = "default_value"
            db.execute("UPDATE inventory SET data = ? WHERE account_id = ? AND kind = 'personal'",
                       (json.dumps(data), account_id))
    db.commit()

# Викликати в main.py перед запуском
migrate_inventory(db)
```

### 13. Тестування окремих компонентів

Ситуація: Тестувати тільки завдання без повного бота.

```python
import unittest
from unittest.mock import MagicMock

class TestMyTask(unittest.TestCase):
    def setUp(self):
        # Мок інвентаря
        self.inv = MagicMock()
        self.inv.personal.comments_written = 0

    def test_task_increases_comments(self):
        def write_comment(inv):
            inv.personal.comments_written += 1

        task = Task(name="test", fn=write_comment)
        result = task.run(self.inv)
        self.assertEqual(self.inv.personal.comments_written, 1)
        self.assertIsNone(result)  # Нічого не повертає

# Запуск: python -m unittest test_my_task.py
```

### 14. Інтеграція з реальними API (з моками)

Ситуація: Тестувати виклики API без реального сервера.

```python
from unittest.mock import patch

def call_api(inv: Inventories):
    # Реальний виклик
    response = bot.session.post("/api/action", json={"data": "test"})
    if response.status_code == 200:
        inv.personal.set("api_success", True)
    else:
        raise ValueError("API error")

# Тест з моками
@patch('src.account_pull.Account.session')
def test_api_call(mock_session):
    mock_session.post.return_value.status_code = 200
    task = Task(name="api", fn=call_api)
    result = task.run(bot.inventories)
    self.assertTrue(bot.inventories.personal.get("api_success"))
```

### 15. Створення нової професії з нуля

Ситуація: Додати професію для модератора, що видаляє спам-коментарі.

```python
def moderator_profession(bot: Account):
    def check_spam(inv: Inventories):
        # Логіка перевірки спаму
        spam_count = inv.personal.get("spam_detected", 0)
        if spam_count > 10:
            inv.personal.set("moderation_needed", True)

    yield Task(name="spam_check", fn=check_spam)

    yield ReactiveTask(
        name="moderate_comments",
        source=lambda inv: inv.personal.pending_events,
        handler=lambda e, inv: Task(name="delete_spam", fn=lambda inv: print("Видалено спам")) if e.get("is_spam") else None
    )

# Додати до scheduler.professions
```

### 16. Моніторинг та логування стану

Ситуація: Логувати зміни інвентаря для дебагу.

```python
import logging

def logged_task(inv: Inventories):
    old_count = inv.personal.comments_written
    inv.personal.comments_written += 1
    logging.info(f"Comments: {old_count} -> {inv.personal.comments_written}")

task = Task(name="logged", fn=logged_task)
# Логування налаштувати в main.py
```

### 17. Паралельне виконання завдань для кількох акаунтів

Ситуація: Запустити кілька ботів одночасно.

```python
accounts = ["acc_01", "acc_02", "acc_03"]
for acc_id in accounts:
    repo.upsert(acc_id, f"email{acc_id}@example.com", "https://site.com")
    scheduler.professions[acc_id] = [trader]

scheduler.start()  # Запускає всіх паралельно
```

### 18. Зупинка завдань за умовою

Ситуація: Зупинити ферму при досягненні ліміту.

```python
scheduler.every(
    "acc_01",
    60,
    lambda bot: LoopTask("limited_farm", lambda inv: inv.personal.comments_written += 1,
                         condition=below("comments_written", 50)),
    until=lambda inv: inv.personal.comments_written >= 50  # Зупинка після 50
)
```

### 19. Використання метаданих у завданнях

Ситуація: Додати контекст до завдання для логування.

```python
task = Task(
    name="custom",
    fn=lambda inv: print("Виконано"),
    meta={"source": "user_input", "priority_reason": "urgent"}
)

# В handler можна використовувати meta
print(f"Завдання з джерела: {task.meta['source']}")
```

### 20. Інтеграція з зовнішніми сервісами (наприклад, Telegram для повідомлень)

Ситуація: Надсилати повідомлення при події.

```python
import requests

def send_telegram_notification(inv: Inventories):
    token = "your_bot_token"
    chat_id = "your_chat_id"
    text = f"Подія: {inv.personal.get('last_event')}"
    requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                  json={"chat_id": chat_id, "text": text})

# Додати як завдання після події
yield Task(name="notify", fn=send_telegram_notification)
```

### Task
- `Task(name, fn, priority=Priority.NORMAL, max_retries=3)`: Одноразова дія.
- `fn(inv: Inventories) -> Any`: Функція, що виконується.

### LoopTask
- `LoopTask(name, fn, condition, interval=0.0)`: Цикл з умовою.

### ReactiveTask
- `ReactiveTask(name, source, handler, requeue=True)`: Обробка подій.
- `source(inv) -> list`: Джерело подій.
- `handler(event, inv) -> Any`: Обробник, може повертати завдання.

### Scheduler
- `every(account_id, interval, producer, until=None)`: Розклад.
- `start()`, `stop()`: Управління.

### Inventories
- `personal`: PersonalInventory (статистика, want_list).
- `alliance`: AllianceInventory (спільні дані).

## Тестування

Запустіть `python -m unittest minimal_test.py` для базових тестів.

## Ліцензія

MIT.</content>
<parameter name="filePath">c:\Users\Huste\OneDrive\Рабочий стол\mangabuff\reader\README.md
