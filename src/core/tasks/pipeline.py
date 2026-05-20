"""
pipeline.py — фабрика для ланцюгових задач.

Концепція
─────────
Pipeline описує ЩО робить один цикл виконання:

    fetch(bot) → дані?
        ├── є  → action(дані, bot)          завершено
        └── ні → parse[0] → parse[1] → ... → fetch знову
                  └── вичерпано parse_retries → завершено

Pipeline НЕ знає КОЛИ його запускати і скільки разів.
Це виключна відповідальність Scheduler + Trigger.

Типове використання:
    # Оголошення — що робить один цикл
    read_one = pipeline(
        name   = "manga_reader",
        fetch  = fetch_next_chapter,
        parse  = [
            Step(find_stale_or_new_mangas, max_retries=1),
            Step(fetch_manga_updates,      max_retries=2),
            Step(save_discovered_mangas,   max_retries=2),
            Step(save_discovered_chapters, max_retries=1),
        ],
        action = read_chapter,
    )

    # У Profession.startup — запускається один раз одразу
    # У Trigger.producer   — запускається за розкладом Scheduler-а

Сигнатури:
    fetch  : (bot: Account) -> Any | None
    parse  : (bot: Account) -> None        (один крок або список)
    action : (data: Any, bot: Account) -> Any

parse може бути:
    Callable              — один крок
    list[Callable | Step] — ланцюг кроків, кожен є окремим Task у черзі

Кожен Step має власний priority і max_retries.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Union, TYPE_CHECKING

from src.core.tasks.base import AnyTask, Priority, Task, extract_spawned

if TYPE_CHECKING:
    from src.core.account import Account

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Step — один крок parse-ланцюга
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Step:
    """
    Один крок у parse-ланцюгу.

    fn          : (bot: Account) -> None
    priority    : пріоритет у черзі воркера
    max_retries : кількість спроб при помилці (0 = без retry)
    delay       : пауза перед виконанням у секундах
    """
    fn:          Callable[["Account"], None]
    priority:    int   = Priority.HIGH
    max_retries: int   = 0
    delay:       float = 0.0


ParseArg = Union[
    Callable[["Account"], None],
    list[Union[Callable[["Account"], None], "Step"]],
]


def _normalize_parse(parse: ParseArg, name: str) -> list[Step]:
    if callable(parse):
        return [Step(fn=parse, priority=Priority.HIGH)]
    steps: list[Step] = []
    for i, item in enumerate(parse):
        if isinstance(item, Step):
            steps.append(item)
        elif callable(item):
            steps.append(Step(fn=item, priority=Priority.HIGH))
        else:
            raise TypeError(
                f"[Pipeline:{name}] parse[{i}] має бути Callable або Step, "
                f"отримано {type(item)}"
            )
    return steps


# ─────────────────────────────────────────────────────────────────────────────
# Ланцюг кроків підготовки
# ─────────────────────────────────────────────────────────────────────────────

def _make_step_chain(
    pipeline_name: str,
    steps:         list[Step],
    after:         AnyTask,
) -> list[AnyTask]:
    """Будує [step_0, ..., step_n-1, after]. Кожен крок породжує наступний."""
    if not steps:
        return [after]

    tasks: list[AnyTask] = [after]
    for step in reversed(steps):
        next_task = tasks[-1]

        def make_fn(s: Step, nxt: AnyTask) -> Callable[["Account"], list[AnyTask]]:
            def _fn(bot: "Account") -> list[AnyTask]:
                s.fn(bot)
                return [nxt]
            return _fn

        tasks.append(Task(
            name        = f"{pipeline_name}:prep:{step.fn.__name__}",
            fn          = make_fn(step, next_task),
            priority    = step.priority,
            max_retries = step.max_retries,
            delay       = step.delay,
        ))

    tasks.reverse()
    return [tasks[0]]


# ─────────────────────────────────────────────────────────────────────────────
# FetchTask
# ─────────────────────────────────────────────────────────────────────────────

def _make_fetch_task(
    name:            str,
    fetch:           Callable[["Account"], Any],
    steps:           list[Step],
    action:          Callable[[Any, "Account"], Any],
    parse_retries:   list[int],   # [залишилось] — мутабельний контейнер
    fetch_priority:  int,
    action_priority: int,
    delay:           float = 0.0,
) -> Task:
    """
    Один FetchTask.
    None  → parse-chain → fetch знову (якщо є retries).
    Дані  → ActionTask → завершено.
    """

    def _run(bot: "Account") -> list[AnyTask]:
        data = fetch(bot)

        if data is None:
            if parse_retries[0] <= 0:
                log.warning(f"[Pipeline:{name}] parse retries вичерпано → стоп")
                return []
            parse_retries[0] -= 1
            log.info(f"[Pipeline:{name}] даних немає → підготовка")
            fetch_again = _make_fetch_task(
                name, fetch, steps, action,
                parse_retries, fetch_priority, action_priority,
                delay=0.0,
            )
            return _make_step_chain(name, steps, after=fetch_again)

        captured = data

        def _action_run(bot: "Account") -> list[AnyTask]:
            result = action(captured, bot)
            log.info(f"[Pipeline:{name}] виконано")
            return extract_spawned(result)

        return [Task(
            name        = f"{name}:action",
            fn          = _action_run,
            priority    = action_priority,
            max_retries = 0,
        )]

    return Task(
        name        = f"{name}:fetch",
        fn          = _run,
        priority    = fetch_priority,
        max_retries = 0,
        delay       = delay,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Публічний API
# ─────────────────────────────────────────────────────────────────────────────

def pipeline(
    name:               str,
    fetch:              Callable[["Account"], Any],
    parse:              ParseArg,
    action:             Callable[[Any, "Account"], Any],
    max_parse_retries:  int = 3,
    fetch_priority:     int = Priority.NORMAL,
    action_priority:    int = Priority.NORMAL,
) -> Callable[["Account"], list[AnyTask]]:
    """
    Повертає producer-функцію: (bot) -> [AnyTask].

    Один виклик = один цикл:
        fetch → parse-chain (якщо немає даних) → action → завершено

    КОЛИ і скільки разів запускати — вирішує Trigger у Scheduler.
    Сумісний з Profession.startup і Trigger.producer.
    """
    steps = _normalize_parse(parse, name)

    def producer(bot: "Account") -> list[AnyTask]:
        log.info(f"[Pipeline:{name}] запуск")
        return [_make_fetch_task(
            name, fetch, steps, action,
            [max_parse_retries],   # новий лічильник на кожен виклик
            fetch_priority, action_priority,
        )]

    producer.__name__ = f"pipeline:{name}"
    return producer