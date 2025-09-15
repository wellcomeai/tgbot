"""
Функциональность управления кнопками платных рассылок для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class PaidButtonsMixin:
    """Миксин для работы с кнопками платных рассылок"""
    
    async def show_paid_message_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню управления кнопками сообщения для оплативших"""
        buttons = self.db.get_paid_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_paid_button_{button_id}")])
        
        if len(buttons) < 3:  # Максимум 3 кнопки
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_paid_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_paid_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"💰 <b>Кнопки сообщения для оплативших {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_paid_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки платного сообщения"""
        # Получаем информацию о кнопке
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT message_number, button_text, button_url 
            FROM paid_message_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        message_number, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_paid_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_paid_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_paid_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_paid_buttons_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"💰 <b>Редактирование кнопки платного сообщения</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def handle_add_paid_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления кнопки к платному сообщению"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Шаг 1: Получаем текст кнопки
            if len(text) > 64:
                await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
                return
            
            # Сохраняем текст и переходим к URL
            self.waiting_for[user_id]["button_text"] = text
            self.waiting_for[user_id]["step"] = "url"
            
            await update.message.reply_text(
                f"✅ Текст кнопки сохранен: <b>{text}</b>\n\n"
                f"🔗 Теперь отправьте URL для кнопки:\n\n"
                f"💡 Пример: https://example.com\n"
                f"🎯 UTM метки будут добавлены автоматически!",
                parse_mode='HTML'
            )
            
        elif current_step == "url":
            # Шаг 2: Получаем URL кнопки
            if not (text.startswith("http://") or text.startswith("https://")):
                await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                return
            
            if len(text) > 256:
                await update.message.reply_text("❌ URL слишком длинный.")
                return
            
            # Добавляем кнопку в базу данных
            message_number = waiting_data["message_number"]
            button_text = waiting_data["button_text"]
            
            # Определяем позицию
            existing_buttons = self.db.get_paid_message_buttons(message_number)
            position = len(existing_buttons) + 1
            
            # Сохраняем кнопку в БД
            self.db.add_paid_message_button(message_number, button_text, text, position)
            
            await update.message.reply_text(
                f"✅ Кнопка для платного сообщения успешно добавлена!\n\n"
                f"📝 <b>Текст:</b> {button_text}\n"
                f"🔗 <b>URL:</b> {text}\n\n"
                f"🎯 <b>UTM метки будут добавлены автоматически при отправке!</b>",
                parse_mode='HTML'
            )
            
            # Очищаем состояние ожидания
            del self.waiting_for[user_id]
            
            # Возвращаемся к меню управления кнопками
            await self.show_paid_message_buttons_from_context(update, context, message_number)
    
    async def show_paid_message_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Отправить НОВОЕ сообщение для управления кнопками платного сообщения"""
        user_id = update.effective_user.id
        
        buttons = self.db.get_paid_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_paid_button_{button_id}")])
        
        if len(buttons) < 3:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_paid_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_paid_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"💰 <b>Кнопки сообщения для оплативших {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
