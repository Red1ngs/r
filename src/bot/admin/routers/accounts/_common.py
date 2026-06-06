"""
accounts/_common.py

Спільні константи, форматери і клавіатури для пакету accounts.
"""
from __future__ import annotations

from typing import Optional

from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message

from src.bot.admin.services.scheduler_service import AccountInfo
from src.core.runtime.profession import profession_factory
from src.bot.admin.routers.accounts.profession_menu import profession_menu_registry

# ── Статуси ───────────────────────────────────────────────────────────────────

STATUS_EMOJI: dict[str, str] = {
    "IDLE":      "💤",
    "WORKING":   "🟢",
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

STATUS_TEXT: dict[str, str] = {
    "IDLE":      "очікує",
    "WORKING":   "працює",
    "COOLDOWN":  "cooldown",
    "ERROR":     "помилка",
    "DEAD":      "мертвий",
    "SUSPENDED": "призупинено",
}

_PRIORITY_BADGE = ["①", "②", "③", "④", "⑤"]


# ── Форматування ──────────────────────────────────────────────────────────────


def account_text(info: AccountInfo) -> str:
    status_emoji = STATUS_EMOJI.get(info.status, "❓")
    status_text  = STATUS_TEXT.get(info.status, info.status.lower())

    # ── Рядки профессій
    if info.professions:
        prof_parts = []
        for i, name in enumerate(info.professions):
            badge = _PRIORITY_BADGE[i] if i < len(_PRIORITY_BADGE) else "•"
            prof_parts.append(f"{badge} {name}")
        profs_line = "  " + "  ".join(prof_parts)
    else:
        profs_line = "  <i>не призначено</i>"

    # ── Монітори
    if info.monitors:
        monitors_line = "  " + ", ".join(m for m in info.monitors)
    else:
        monitors_line = "  <i>немає</i>"

    # ── Проксі
    proxy_line = f"\n🔗 <code>{info.proxy}</code>" if info.proxy else ""

    # ── Сесія
    session_line = "🟢 сесія активна" if info.is_connected else "🔴 сесія відсутня"

    return (
        f"{status_emoji} <b>{info.account_id}</b>  ·  {status_text}{proxy_line}\n"
        f"📧 <code>{info.email}</code>\n"
        f"{session_line}\n"
        f"\n"
        f"<b>Професії</b>\n{profs_line}\n"
        f"\n"
        f"<b>Монітори</b>\n{monitors_line}"
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
    acc_id:             str,
    status:             str,
    active_professions: list[str],
    is_connected:       bool = False,
) -> InlineKeyboardMarkup:
    is_suspended = status == "SUSPENDED"
    is_dead      = status == "DEAD"
    rows: list[list[InlineKeyboardButton]] = []

    # ── Сесія: підключити / відключити
    if not is_dead:
        if is_connected:
            rows.append([InlineKeyboardButton(
                text="🔌 Відключити сесію",
                callback_data=f"acc:disconnect:{acc_id}",
            )])
        else:
            rows.append([InlineKeyboardButton(
                text="🔗 Підключити сесію",
                callback_data=f"acc:connect:{acc_id}",
            )])

    # ── Пауза / відновлення
    if not is_dead:
        if is_suspended:
            rows.append([InlineKeyboardButton(
                text="▶️ Відновити",
                callback_data=f"acc:resume:{acc_id}",
            )])
        else:
            rows.append([InlineKeyboardButton(
                text="⏸ Пауза",
                callback_data=f"acc:pause:{acc_id}",
            )])

    # ── Управління професіями
    if not is_dead:
        rows.append([InlineKeyboardButton(
            text="🎓 Професії",
            callback_data=f"acc:professions:{acc_id}",
        )])

    # ── Profession-специфічні пункти (автогенерація з реєстру)
    if not is_dead and active_professions:
        prof_items = profession_menu_registry.items_for(active_professions)
        for item in prof_items:
            rows.append([InlineKeyboardButton(
                text=item.label,
                callback_data=item.build_callback(acc_id),
            )])

    # ── Службові кнопки
    rows.append([
        InlineKeyboardButton(text="🔄", callback_data=f"acc:refresh:{acc_id}"),
        InlineKeyboardButton(text="🗑 Видалити", callback_data=f"acc:remove:{acc_id}"),
    ])
    rows.append([InlineKeyboardButton(text="↩️ До списку", callback_data="acc:list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def professions_manage_kb(
    acc_id: str,
    active: list[str],
) -> InlineKeyboardMarkup:
    all_names = profession_factory.names()
    rows: list[list[InlineKeyboardButton]] = []

    if active:
        rows.append([InlineKeyboardButton(
            text="── Активні ──", callback_data="noop",
        )])
        for i, name in enumerate(active):
            badge = _PRIORITY_BADGE[i] if i < len(_PRIORITY_BADGE) else "•"
            rows.append([
                InlineKeyboardButton(
                    text=f"{badge} {name}",
                    callback_data="noop",
                ),
                InlineKeyboardButton(
                    text="✖",
                    callback_data=f"acc:prof_remove:{acc_id}:{name}",
                ),
            ])
    else:
        rows.append([InlineKeyboardButton(
            text="Професій ще немає", callback_data="noop",
        )])

    available = [n for n in all_names if n not in active]
    if available:
        rows.append([InlineKeyboardButton(
            text="── Додати ──", callback_data="noop",
        )])
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