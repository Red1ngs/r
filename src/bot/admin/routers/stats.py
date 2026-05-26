"""
bot/admin/routers/stats.py

/stats        — загальна статистика
/stats <id>   — детально по акаунту
"""
from __future__ import annotations

from aiogram import F, Router
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from src.bot.admin.nav import nav_answer
from src.bot.admin.services.scheduler_service import AccountInfo, SchedulerService

router = Router(name="stats")


def _fmt_seconds(s: float | None) -> str:
    if s is None:
        return "—"
    if s < 60:
        return f"{int(s)}с"
    m = int(s // 60)
    sec = int(s % 60)
    return f"{m}хв {sec}с" if sec else f"{m}хв"


def _account_stats_text(info: AccountInfo) -> str:
    triggers = "\n".join(f"  • {t}" for t in info.triggers) or "  —"
    proxy    = f"\nПроксі: <code>{info.proxy}</code>" if info.proxy else ""

    prof_line = (
        "Професії: <b>" + ", ".join(info.professions) + "</b>"
        if info.professions else
        "Професії: <i>не призначено</i>"
    )

    return (
        f"📊 <b>Статистика: {info.account_id}</b>\n\n"
        f"Email:  <code>{info.email}</code>{proxy}\n"
        f"Статус: <b>{info.status}</b>\n"
        f"Черга:  <b>{info.queue_size}</b> задач\n"
        f"Наступний тригер: <b>{_fmt_seconds(info.next_trigger_s)}</b>\n"
        f"{prof_line}\n\n"
        f"<b>Тригери:</b>\n{triggers}"
    )


@router.message(Command("stats"))
async def cmd_stats(message: Message, state: FSMContext, svc: SchedulerService) -> None:
    parts  = (message.text or "").split(maxsplit=1)
    acc_id = parts[1].strip() if len(parts) > 1 else None

    if acc_id:
        info = svc.account_info(acc_id)
        if info is None:
            await nav_answer(message, state, f"❌ Акаунт <code>{acc_id}</code> не знайдено")
            return
        await nav_answer(
            message, state,
            _account_stats_text(info),
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="↩️ До списку", callback_data="acc:list"),
            ]]),
        )
        return

    snapshot    = svc.snapshot()
    total_queue = sum(a.queue_size for a in snapshot.accounts)
    status_count: dict[str, int] = {}
    for acc in snapshot.accounts:
        status_count[acc.status] = status_count.get(acc.status, 0) + 1

    lines = [f"📊 <b>Загальна статистика</b>\n"]
    lines.append(f"Акаунтів: <b>{snapshot.total_accounts}</b>")
    lines.append(f"Задач у чергах: <b>{total_queue}</b>\n")
    lines.append("<b>За статусами:</b>")
    for status, count in sorted(status_count.items()):
        lines.append(f"  {status}: {count}")
    if snapshot.accounts:
        lines.append("\n<b>Деталі:</b>")
        for acc in snapshot.accounts:
            lines.append(
                f"  <code>{acc.account_id}</code> [{acc.status}] "
                f"q={acc.queue_size} next={_fmt_seconds(acc.next_trigger_s)}"
            )

    await nav_answer(
        message, state,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="📋 Акаунти", callback_data="acc:list"),
        ]]),
    )


# Callback для статистики конкретного акаунта (з меню)
@router.callback_query(F.data.startswith("stats:acc:"))
async def cb_stats_account(call: CallbackQuery, svc: SchedulerService) -> None:
    acc_id = call.data.split(":", 2)[2]  # type: ignore[union-attr]
    info   = svc.account_info(acc_id)
    if info is None:
        await call.answer("❌ Акаунт не знайдено", show_alert=True)
        return
    await call.message.edit_text(  # type: ignore[union-attr]
        _account_stats_text(info),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="↩️ До меню", callback_data=f"acc:menu:{acc_id}"),
        ]]),
    )
    await call.answer()