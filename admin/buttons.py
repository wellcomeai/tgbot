"""
Функциональность управления кнопками для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ButtonsMixin:
    """Миксин для работы с кнопками всех типов"""
    
    # === КНОПКИ СООБЩЕНИЙ РАССЫЛКИ ===
    
    async def show_message_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню управления кнопками сообщения"""
        buttons = self.db.get_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_button_{button_id}")])
        
        if len(buttons) < 3:  # Максимум 3 кнопки
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Кнопки сообщения {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_message_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Отправить НОВОЕ сообщение для управления кнопками"""
        user_id = update.effective_user.id
        
        buttons = self.db.get_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_button_{button_id}")])
        
        if len(buttons) < 3:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"🔘 <b>Кнопки сообщения {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки"""
        # Получаем информацию о кнопке
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT message_number, button_text, button_url 
            FROM message_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        message_number, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_buttons_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Отправить НОВОЕ сообщение для редактирования кнопки"""
        user_id = update.effective_user.id
        
        # Получаем информацию о кнопке
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT message_number, button_text, button_url 
            FROM message_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        message_number, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_buttons_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # === КНОПКИ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ ===
    
    async def show_welcome_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками приветственного сообщения"""
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = []
        
        # Показать существующие кнопки для редактирования
        for button_id, button_text, position in welcome_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_welcome_button_{button_id}")])
        
        # Кнопка добавления (лимит 5 кнопок)
        if len(welcome_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_welcome_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_welcome")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Механические кнопки приветствия</b>\n\n"
            f"Текущие кнопки: {len(welcome_buttons)}/5\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_welcome_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасное отображение управления кнопками приветствия из контекста"""
        user_id = update.effective_user.id
        
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = []
        
        for button_id, button_text, position in welcome_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_welcome_button_{button_id}")])
        
        if len(welcome_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_welcome_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_welcome")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Механические кнопки приветствия</b>\n\n"
            f"Текущие кнопки: {len(welcome_buttons)}/5\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_welcome_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать меню редактирования конкретной кнопки приветствия"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, position FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_id, button_text, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_button_text_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_welcome_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст кнопки:</b> {button_text}\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_welcome_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Безопасное отображение редактирования кнопки приветствия из контекста"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, position FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        button_id, button_text, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_button_text_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_welcome_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст кнопки:</b> {button_text}\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_welcome_button_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать подтверждение удаления кнопки приветствия"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT button_text FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_text = button_data[0]
        
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_welcome_button_{button_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⚠️ <b>Подтверждение удаления</b>\n\n"
            f"Вы уверены, что хотите удалить кнопку:\n"
            f'<b>"{button_text}"</b>?'
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # === КНОПКИ ПРОЩАЛЬНОГО СООБЩЕНИЯ ===
    
    async def show_goodbye_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками прощального сообщения"""
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = []
        
        # Показать существующие кнопки для редактирования
        for button_id, button_text, button_url, position in goodbye_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_goodbye_button_{button_id}")])
        
        # Кнопка добавления (лимит 5 кнопок)
        if len(goodbye_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_goodbye_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_goodbye")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Инлайн кнопки прощания</b>\n\n"
            f"Текущие кнопки: {len(goodbye_buttons)}/5\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_goodbye_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасное отображение управления кнопками прощания из контекста"""
        user_id = update.effective_user.id
        
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = []
        
        for button_id, button_text, button_url, position in goodbye_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_goodbye_button_{button_id}")])
        
        if len(goodbye_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_goodbye_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_goodbye")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Инлайн кнопки прощания</b>\n\n"
            f"Текущие кнопки: {len(goodbye_buttons)}/5\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_goodbye_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать меню редактирования конкретной кнопки прощания"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, button_url, position FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_id, button_text, button_url, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_goodbye_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_goodbye_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_goodbye_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_goodbye_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_goodbye_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Безопасное отображение редактирования кнопки прощания из контекста"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, button_url, position FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        button_id, button_text, button_url, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_goodbye_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_goodbye_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_goodbye_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_goodbye_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_goodbye_button_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать подтверждение удаления кнопки прощания"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT button_text FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_text = button_data[0]
        
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_goodbye_button_{button_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_goodbye_button_{button_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⚠️ <b>Подтверждение удаления</b>\n\n"
            f"Вы уверены, что хотите удалить кнопку:\n"
            f'<b>"{button_text}"</b>?'
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # === ОБРАБОТЧИКИ ВВОДА ===
    
    async def handle_add_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления кнопки к сообщению рассылки"""
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
            existing_buttons = self.db.get_message_buttons(message_number)
            position = len(existing_buttons) + 1
            
            # Сохраняем кнопку в БД
            self.db.add_message_button(message_number, button_text, text, position)
            
            await update.message.reply_text(
                f"✅ Кнопка успешно добавлена!\n\n"
                f"📝 <b>Текст:</b> {button_text}\n"
                f"🔗 <b>URL:</b> {text}\n\n"
                f"🎯 <b>UTM метки будут добавлены автоматически при отправке!</b>",
                parse_mode='HTML'
            )
            
            # Очищаем состояние ожидания
            del self.waiting_for[user_id]
            
            # Возвращаемся к меню управления кнопками
            await self.show_message_buttons_from_context(update, context, message_number)
            
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить кнопку заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    async def handle_add_welcome_button_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления новой кнопки приветствия"""
        user_id = update.effective_user.id
        
        if len(text) > 30:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 30 символов.")
            return
        
        # Проверяем уникальность
        existing_button = self.db.get_welcome_button_by_text(text)
        if existing_button:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Добавляем кнопку
        button_id = self.db.add_welcome_button(text)
        
        await update.message.reply_text(f"✅ Кнопка '{text}' добавлена!")
        del self.waiting_for[user_id]
        
        # Показываем обновленный список
        await self.show_welcome_buttons_management_from_context(update, context)
    
    async def handle_edit_welcome_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения текста кнопки приветствия"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if len(text) > 30:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 30 символов.")
            return
        
        # Проверяем уникальность (исключая текущую кнопку)
        existing_button = self.db.get_welcome_button_by_text(text)
        if existing_button and existing_button[0] != button_id:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Обновляем кнопку
        self.db.update_welcome_button(button_id, button_text=text)
        
        await update.message.reply_text(f"✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_welcome_button_edit_from_context(update, context, button_id)
    
    async def handle_add_goodbye_button_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления новой кнопки прощания (в два этапа)"""
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
            
            button_text = waiting_data["button_text"]
            
            # Проверяем уникальность текста
            existing_button = self.db.get_goodbye_button_by_text(button_text)
            if existing_button:
                await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
                del self.waiting_for[user_id]
                return
            
            # Добавляем кнопку
            button_id = self.db.add_goodbye_button(button_text, text)
            
            await update.message.reply_text(
                f"✅ Кнопка успешно добавлена!\n\n"
                f"📝 <b>Текст:</b> {button_text}\n"
                f"🔗 <b>URL:</b> {text}\n\n"
                f"🎯 <b>UTM метки будут добавлены автоматически при отправке!</b>",
                parse_mode='HTML'
            )
            
            del self.waiting_for[user_id]
            
            # Показываем обновленный список
            await self.show_goodbye_buttons_management_from_context(update, context)
        
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить кнопку заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    async def handle_edit_goodbye_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения текста кнопки прощания"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        # Проверяем уникальность (исключая текущую кнопку)
        existing_button = self.db.get_goodbye_button_by_text(text)
        if existing_button and existing_button[0] != button_id:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Обновляем кнопку
        self.db.update_goodbye_button(button_id, button_text=text)
        
        await update.message.reply_text(f"✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_goodbye_button_edit_from_context(update, context, button_id)
    
    async def handle_edit_goodbye_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения URL кнопки прощания"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        # Обновляем URL кнопки
        self.db.update_goodbye_button(button_id, button_url=text)
        
        await update.message.reply_text("✅ URL кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_goodbye_button_edit_from_context(update, context, button_id)
