"""
Функциональность управления рассылок для оплативших пользователей в админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class PaidBroadcastsMixin:
    """Миксин для работы с рассылками для оплативших пользователей"""
    
    async def show_paid_broadcast_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления рассылкой для оплативших"""
        messages = self.db.get_all_paid_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url, video_url in messages:
            # Получаем количество кнопок для сообщения
            buttons = self.db.get_paid_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            photo_icon = "🖼" if photo_url else ""
            video_icon = "🎥" if video_url else ""

            # Форматируем отображение времени
            delay_str = self.format_delay_display(delay_hours)

            button_text = f"{photo_icon}{video_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_paid_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_paid_message")])
        keyboard.append([InlineKeyboardButton("📢 Массовая рассылка", callback_data="paid_send_all")])
        keyboard.append([InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="paid_scheduled_broadcasts")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "💰 <b>Рассылки для оплативших</b>\n\n"
            "Управление автоматическими рассылками для пользователей, которые оплатили.\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после оплаты.\n"
            "🖼 - сообщение содержит фото\n"
            "🎥 - сообщение содержит видео\n"
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_paid_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню редактирования конкретного сообщения для оплативших"""
        msg_data = self.db.get_paid_broadcast_message(message_number)
        if not msg_data:
            await update.callback_query.answer("Сообщение не найдено!", show_alert=True)
            return

        text, delay_hours, photo_url, video_url = msg_data
        buttons = self.db.get_paid_message_buttons(message_number)

        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_paid_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_paid_delay_{message_number}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_paid_photo_{message_number}")],
            [InlineKeyboardButton("🎥 Изменить видео", callback_data=f"edit_paid_video_{message_number}")]
        ]

        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_paid_photo_{message_number}")])

        if video_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить видео", callback_data=f"remove_paid_video_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_paid_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_paid_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_paid_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        delay_str = self.format_delay_display_full(delay_hours)
        
        message_text = (
            f"💰 <b>Сообщение для оплативших {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после оплаты\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}\n"
            f"<b>Видео:</b> {'Есть' if video_url else 'Нет'}"
            f"{buttons_info}\n\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )

        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)

    async def show_paid_send_all_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню массовой рассылки для оплативших"""
        user_id = update.effective_user.id
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now(),
                "is_paid_broadcast": True  # Флаг для платных рассылок
            }
        
        # Устанавливаем флаг для платных рассылок
        self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        draft = self.broadcast_drafts[user_id]
        
        # Формируем информацию о текущем состоянии
        text = "💰 <b>Массовая рассылка для оплативших</b>\n\n"
        
        # Текст
        if draft["message_text"]:
            text += f"📝 <b>Текст:</b> {draft['message_text'][:50]}{'...' if len(draft['message_text']) > 50 else ''}\n"
        else:
            text += "📝 <b>Текст:</b> <i>Не задан</i>\n"
        
        # Фото
        if draft["photo_data"]:
            text += "🖼 <b>Фото:</b> Есть\n"
        else:
            text += "🖼 <b>Фото:</b> Нет\n"
        
        # Кнопки
        if draft["buttons"]:
            text += f"🔘 <b>Кнопки:</b> {len(draft['buttons'])}\n"
        else:
            text += "🔘 <b>Кнопки:</b> Нет\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            text += f"⏰ <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
        else:
            text += "⏰ <b>Время отправки:</b> <i>Сразу</i>\n"
        
        # Получаем количество оплативших пользователей
        paid_users = self.db.get_users_with_payment()
        users_count = len(paid_users)
        text += f"\n👥 <b>Получателей:</b> {users_count} оплативших пользователей\n"
        text += "\n💡 <i>Все ссылки автоматически получат UTM метки для отслеживания.</i>\n"
        
        text += "\n<b>Выберите действие:</b>"
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="paid_mass_edit_text")],
            [InlineKeyboardButton("🖼 Добавить фото", callback_data="paid_mass_add_photo")],
            [InlineKeyboardButton("⏰ Время отправки", callback_data="paid_mass_set_time")],
            [InlineKeyboardButton("🔘 Добавить кнопку", callback_data="paid_mass_add_button")],
        ]
        
        # Кнопка удаления фото (если есть)
        if draft["photo_data"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить фото", callback_data="paid_mass_remove_photo")])
        
        # Кнопка удаления последней кнопки (если есть)
        if draft["buttons"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="paid_mass_remove_button")])
        
        keyboard.append([InlineKeyboardButton("📋 Предпросмотр", callback_data="paid_mass_preview")])
        
        # Кнопка отправки (только если есть текст)
        if draft["message_text"]:
            keyboard.append([InlineKeyboardButton("🚀 Отправить сейчас", callback_data="paid_mass_send_now")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_paid_broadcast")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_paid_scheduled_broadcasts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать запланированные рассылки для оплативших"""
        broadcasts = self.db.get_paid_scheduled_broadcasts(include_sent=False)
        
        keyboard = []
        
        if broadcasts:
            for broadcast_id, message_text, photo_url, scheduled_time, is_sent, created_at in broadcasts:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                time_str = scheduled_dt.strftime("%d.%m %H:%M")
                
                # Получаем количество кнопок
                buttons = self.db.get_paid_scheduled_broadcast_buttons(broadcast_id)
                button_icon = f"🔘{len(buttons)}" if buttons else ""
                photo_icon = "🖼" if photo_url else ""
                
                short_text = message_text[:20] + "..." if len(message_text) > 20 else message_text
                button_display = f"{photo_icon}{button_icon} {time_str}: {short_text}"
                keyboard.append([InlineKeyboardButton(button_display, callback_data=f"edit_paid_scheduled_broadcast_{broadcast_id}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_paid_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "💰 <b>Запланированные рассылки для оплативших</b>\n\n"
            f"Активных рассылок: {len(broadcasts)}\n\n"
            "🖼 - сообщение с фото\n"
            "🔘N - количество кнопок\n\n"
            "💡 <i>Все ссылки получат UTM метки для отслеживания.</i>\n\n"
            "Выберите рассылку для редактирования:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # === ОБРАБОТЧИКИ ВВОДА ===
    
    async def handle_add_paid_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления нового сообщения для оплативших"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Сохраняем текст и запрашиваем задержку
            self.waiting_for[user_id]["text"] = text
            self.waiting_for[user_id]["step"] = "delay"
            await update.message.reply_text(
                "⏰ Теперь отправьте задержку после оплаты:\n\n"
                "📝 <b>Форматы ввода:</b>\n"
                "• <code>30м</code> или <code>30 минут</code> - для минут\n"
                "• <code>2ч</code> или <code>2 часа</code> - для часов\n"
                "• <code>1.5</code> - для 1.5 часов\n"
                "• <code>0.05</code> - для 3 минут",
                parse_mode='HTML'
            )
        elif current_step == "delay":
            # Парсим задержку
            delay_hours, delay_display = self.parse_delay_input(text)
            
            if delay_hours is not None and delay_hours >= 0:  # Разрешаем 0 для мгновенной отправки
                # Добавляем сообщение
                message_text = waiting_data["text"]
                new_number = self.db.add_paid_broadcast_message(message_text, delay_hours)
                
                await update.message.reply_text(f"✅ Сообщение для оплативших {new_number} добавлено с задержкой {delay_display}!")
                del self.waiting_for[user_id]
                await self.show_paid_broadcast_menu_from_context(update, context)
            else:
                await update.message.reply_text(
                    "❌ Неверный формат! Примеры правильного ввода:\n\n"
                    "• <code>3м</code> или <code>3 минуты</code>\n"
                    "• <code>2ч</code> или <code>2 часа</code>\n"
                    "• <code>1.5</code> (для 1.5 часов)\n"
                    "• <code>0</code> (для мгновенной отправки)",
                    parse_mode='HTML'
                )
    
    async def show_paid_broadcast_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение меню рассылки для оплативших"""
        user_id = update.effective_user.id
        
        messages = self.db.get_all_paid_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url, video_url in messages:
            buttons = self.db.get_paid_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            photo_icon = "🖼" if photo_url else ""
            video_icon = "🎥" if video_url else ""

            delay_str = self.format_delay_display(delay_hours)

            button_text = f"{photo_icon}{video_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_paid_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_paid_message")])
        keyboard.append([InlineKeyboardButton("📢 Массовая рассылка", callback_data="paid_send_all")])
        keyboard.append([InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="paid_scheduled_broadcasts")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "💰 <b>Рассылки для оплативших</b>\n\n"
            "Управление автоматическими рассылками для пользователей, которые оплатили.\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после оплаты.\n"
            "🖼 - сообщение содержит фото\n"
            "🎥 - сообщение содержит видео\n"
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_paid_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Отправить НОВОЕ сообщение для редактирования платного сообщения"""
        user_id = update.effective_user.id
        
        msg_data = self.db.get_paid_broadcast_message(message_number)
        if not msg_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Сообщение не найдено!")
            return
        
        text, delay_hours, photo_url, video_url = msg_data
        buttons = self.db.get_paid_message_buttons(message_number)

        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_paid_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_paid_delay_{message_number}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_paid_photo_{message_number}")],
            [InlineKeyboardButton("🎥 Изменить видео", callback_data=f"edit_paid_video_{message_number}")]
        ]

        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_paid_photo_{message_number}")])

        if video_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить видео", callback_data=f"remove_paid_video_{message_number}")])

        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_paid_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_paid_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_paid_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"

        delay_str = self.format_delay_display_full(delay_hours)

        message_text = (
            f"💰 <b>Сообщение для оплативших {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после оплаты\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}\n"
            f"<b>Видео:</b> {'Есть' if video_url else 'Нет'}"
            f"{buttons_info}\n\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
