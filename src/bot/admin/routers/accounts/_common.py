"""
accounts/_common.py

Спільні константи, форматери і клавіатури для пакету accounts.
"""
from __future__ import annotations

from typing import Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.admin.services.scheduler_service import AccountInfo
from src.core.runtime.profession import profession_factory

# ── Статуси → emoji ───────────────────────────────────────────────────────────

STATUS_EMOJI: dict[str, str] = {
    "IDLE":      "💤",
    "WORKING":   "⚙️",
    "COOLDOWN":  "⏳",
    "ERROR":     "⚠️",
    "DEAD":      "💀",
    "SUSPENDED": "⏸️",
}

STATUS_LABEL: dict[str, str] = {
    "IDLE":      "💤",
    "WORKING":   "🟢",
    "COOLDOWN":  "🔵",
    "ERROR":     "🟡",
    "DEAD":      "🔴",
    "SUSPENDED": "⚫",
}

# Emoji для відображення пріоритету profession у списку
_PRIORITY_BADGE = ["①", "②", "③", "④", "⑤"]

# ── Форматування ──────────────────────────────────────────────────────────────

def fmt_seconds(s: Optional[float]) -> str:
    if s is None:
        return "—"
    if s < 60:
        return f"{int(s)}с"
    m, sec = divmod(int(s), 60)
    return f"{m}хв {sec}с" if sec else f"{m}хв"


def account_text(info: AccountInfo) -> str:
    emoji    = STATUS_EMOJI.get(info.status, "❓")
    triggers = ", ".join(info.triggers) if info.triggers else "—"
    proxy_line = f"\nПроксі: <code>{info.proxy}</code>" if info.proxy else ""
    next_line  = f"\nНаступний тригер: <b>{fmt_seconds(info.next_trigger_s)}</b>"

    if info.professions:
        profs_formatted = " ".join(
            f"{_PRIORITY_BADGE[i] if i < len(_PRIORITY_BADGE) else '•'} {name}"
            for i, name in enumerate(info.professions)
        )
        prof_line = f"\nПрофесії: <b>{profs_formatted}</b>"
    else:
        prof_line = "\nПрофесії: <i>не призначено</i>"

    return (
        f"{emoji} <b>{info.account_id}</b>\n"
        f"Email: <code>{info.email}</code>{proxy_line}\n"
        f"Статус: <b>{info.status}</b>  Черга: <b>{info.queue_size}</b>"
        f"{prof_line}{next_line}\n"
        f"Тригери: {triggers}"
    )


# ── Клавіатури ────────────────────────────────────────────────────────────────

def accounts_list_kb(accounts: list[AccountInfo]) -> InlineKeyboardMarkup:
    buttons: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []
    for acc in accounts:
        prefix = STATUS_LABEL.get(acc.status, "❓")
        row.append(InlineKeyboardButton(
            text=f"{prefix} {acc.account_id}",
            callback_data=f"acc:menu:{acc.account_id}",
        ))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="➕ Додати акаунт", callback_data="acc:add")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def account_menu_kb(
    acc_id:       str,
    status:       str,
    has_professions: bool,
) -> InlineKeyboardMarkup:
    is_suspended = status == "SUSPENDED"
    is_dead      = status == "DEAD"
    rows: list[list[InlineKeyboardButton]] = []

    if is_suspended:
        rows.append([InlineKeyboardButton(text="▶️ Відновити", callback_data=f"acc:resume:{acc_id}")])
    elif not is_dead:
        rows.append([InlineKeyboardButton(text="⏸ Пауза", callback_data=f"acc:pause:{acc_id}")])

    if not is_dead:
        rows.append([InlineKeyboardButton(
            text="🎓 Управління професіями",
            callback_data=f"acc:professions:{acc_id}",
        )])

    if not is_dead and has_professions:
        rows.append([InlineKeyboardButton(
            text="🎰 Слоти читача",
            callback_data=f"acc:slots:{acc_id}",
        )])

    # ← додати:
    if not is_dead and has_professions:
        rows.append([
            InlineKeyboardButton(
                text="🔍 Парсинг манг",
                callback_data=f"acc:force_parse:{acc_id}",
            ),
            InlineKeyboardButton(
                text="✅ Прочитані",
                callback_data=f"acc:mark_read:{acc_id}",
            ),
        ])

    rows.append([
        InlineKeyboardButton(text="🔄 Оновити", callback_data=f"acc:refresh:{acc_id}"),
        InlineKeyboardButton(text="🗑 Видалити", callback_data=f"acc:remove:{acc_id}"),
    ])
    rows.append([InlineKeyboardButton(text="↩️ До списку", callback_data="acc:list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def professions_manage_kb(
    acc_id:      str,
    active:      list[str],
) -> InlineKeyboardMarkup:
    """
    Клавіатура управління списком professions акаунта.

    Активні professions відображаються з номером пріоритету і кнопкою видалення.
    Неактивні (доступні для додавання) — окремим рядком.
    """
    all_names = profession_factory.names()
    rows: list[list[InlineKeyboardButton]] = []

    # Поточні professions у порядку пріоритету
    if active:
        rows.append([InlineKeyboardButton(text="── Активні (порядок = пріоритет) ──", callback_data="noop")])
        for i, name in enumerate(active):
            badge = _PRIORITY_BADGE[i] if i < len(_PRIORITY_BADGE) else "•"
            rows.append([
                InlineKeyboardButton(
                    text=f"{badge} {name}",
                    callback_data="noop",
                ),
                InlineKeyboardButton(
                    text="✖ Видалити",
                    callback_data=f"acc:prof_remove:{acc_id}:{name}",
                ),
            ])
    else:
        rows.append([InlineKeyboardButton(text="Професій ще немає", callback_data="noop")])

    # Доступні для додавання
    available = [n for n in all_names if n not in active]
    if available:
        rows.append([InlineKeyboardButton(text="── Додати професію ──", callback_data="noop")])
        for name in available:
            rows.append([InlineKeyboardButton(
                text=f"➕ {name}",
                callback_data=f"acc:prof_add:{acc_id}:{name}",
            )])

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data=f"acc:menu:{acc_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def confirm_remove_kb(acc_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Так, видалити", callback_data=f"acc:remove_confirm:{acc_id}"),
        InlineKeyboardButton(text="❌ Скасувати",     callback_data=f"acc:menu:{acc_id}"),
    ]])


def cancel_add_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ Скасувати", callback_data="acc:add_cancel"),
    ]])


# ── Nav-editor ────────────────────────────────────────────────────────────────

def make_editor(message: Message, data: dict, already_deleted: bool = False):
    nav_id  = data.get("_nav_msg_id")
    bot_obj = message.bot
    chat    = message.chat.id

    async def _edit(text: str, kb: Optional[InlineKeyboardMarkup] = None) -> None:
        if not already_deleted:
            try:
                await message.delete()
            except Exception:
                pass
        if nav_id and bot_obj:
            try:
                await bot_obj.edit_message_text(
                    text, chat_id=chat, message_id=nav_id,
                    reply_markup=kb, parse_mode="HTML",
                )
                return
            except Exception:
                pass
        await message.answer(text, reply_markup=kb)

    return _edit
