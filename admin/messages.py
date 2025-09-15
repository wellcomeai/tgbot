"""
Функциональность управления сообщениями для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MessagesMixin:
    """Миксин для работы с сообщениями бота"""
    
    # === ПРИВЕТСТВЕННОЕ СООБЩЕНИЕ ===
    
    async def show_welcome_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования приветственного сообщения"""
        welcome_data = self.db.get_welcome_message()
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        keyboard.append([InlineKeyboardButton("⌨️ Управление кнопками", callback_data="manage_welcome_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if welcome_buttons:
            buttons_info = f"\n\n<b>⌨️ Кнопки ({len(welcome_buttons)}):</b>\n"
            for i, (button_id, button_text, position) in enumerate(welcome_buttons, 1):
                buttons_info += f"{i}. {button_text}\n"
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_welcome_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение для редактирования приветствия"""
        user_id = update.effective_user.id
        welcome_data = self.db.get_welcome_message()
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        keyboard.append([InlineKeyboardButton("⌨️ Управление кнопками", callback_data="manage_welcome_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if welcome_buttons:
            buttons_info = f"\n\n<b>⌨️ Кнопки ({len(welcome_buttons)}):</b>\n"
            for i, (button_id, button_text, position) in enumerate(welcome_buttons, 1):
                buttons_info += f"{i}. {button_text}\n"
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # === ПРОЩАЛЬНОЕ СООБЩЕНИЕ ===
    
    async def show_goodbye_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования прощального сообщения"""
        goodbye_data = self.db.get_goodbye_message()
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление инлайн кнопками", callback_data="manage_goodbye_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if goodbye_buttons:
            buttons_info = f"\n\n<b>🔘 Инлайн кнопки ({len(goodbye_buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(goodbye_buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_goodbye_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение для редактирования прощания"""
        user_id = update.effective_user.id
        goodbye_data = self.db.get_goodbye_message()
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление инлайн кнопками", callback_data="manage_goodbye_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if goodbye_buttons:
            buttons_info = f"\n\n<b>🔘 Инлайн кнопки ({len(goodbye_buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(goodbye_buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # === СООБЩЕНИЕ ПОДТВЕРЖДЕНИЯ ===
    
    async def show_success_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования сообщения подтверждения"""
        # Получаем текущее сообщение подтверждения
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
        success_msg = cursor.fetchone()
        conn.close()
        
        if success_msg:
            current_message = success_msg[0]
        else:
            current_message = self._get_default_success_message()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_success_message_text")],
            [InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_success_message")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "✅ <b>Сообщение подтверждения</b>\n\n"
            "Это сообщение отправляется пользователям после успешной подписки.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_success_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение для редактирования подтверждения"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение подтверждения
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
        success_msg = cursor.fetchone()
        conn.close()
        
        if success_msg:
            current_message = success_msg[0]
        else:
            current_message = self._get_default_success_message()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_success_message_text")],
            [InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_success_message")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "✅ <b>Сообщение подтверждения</b>\n\n"
            "Это сообщение отправляется пользователям после успешной подписки.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # === СООБЩЕНИЕ ПОСЛЕ ОПЛАТЫ ===
    
    async def show_payment_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования сообщения после оплаты"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение об оплате
        payment_message_data = self.db.get_payment_success_message()
        
        if payment_message_data and payment_message_data['text']:
            current_message = payment_message_data['text']
            current_photo = payment_message_data['photo_url']
        else:
            current_message = self._get_default_payment_message()
            current_photo = None
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_payment_message_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_payment_message_photo")]
        ]
        
        if current_photo:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_payment_message_photo")])
        
        keyboard.append([InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_payment_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "💰 <b>Сообщение после оплаты</b>\n\n"
            "Это сообщение отправляется пользователям после успешной оплаты.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}\n\n"
            f"<b>Фото:</b> {'Есть' if current_photo else 'Нет'}\n\n"
            "💡 <i>В тексте можно использовать переменную {amount} - она будет заменена на сумму платежа.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_payment_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение для редактирования платежного сообщения"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение об оплате
        payment_message_data = self.db.get_payment_success_message()
        
        if payment_message_data and payment_message_data['text']:
            current_message = payment_message_data['text']
            current_photo = payment_message_data['photo_url']
        else:
            current_message = self._get_default_payment_message()
            current_photo = None
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_payment_message_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_payment_message_photo")]
        ]
        
        if current_photo:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_payment_message_photo")])
        
        keyboard.append([InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_payment_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "💰 <b>Сообщение после оплаты</b>\n\n"
            "Это сообщение отправляется пользователям после успешной оплаты.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}\n\n"
            f"<b>Фото:</b> {'Есть' if current_photo else 'Нет'}\n\n"
            "💡 <i>В тексте можно использовать переменную {amount} - она будет заменена на сумму платежа.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # === ОБРАБОТЧИКИ ВВОДА ===
    
    async def handle_payment_message_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                         text: str, input_type: str):
        """Обработка ввода для сообщения после оплаты"""
        user_id = update.effective_user.id
        
        if input_type == "payment_message_text":
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return
            
            # Получаем текущее фото
            current_data = self.db.get_payment_success_message()
            current_photo = current_data['photo_url'] if current_data else None
            
            # Сохраняем новый текст
            self.db.set_payment_success_message(text, current_photo)
            
            await update.message.reply_text("✅ Сообщение после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
            
        elif input_type == "payment_message_photo":
            if not (text.startswith("http://") or text.startswith("https://")):
                await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
                return
            
            # Получаем текущий текст
            current_data = self.db.get_payment_success_message()
            current_text = current_data['text'] if current_data else None
            
            if not current_text:
                current_text = self._get_default_payment_message()
            
            # Сохраняем новое фото
            self.db.set_payment_success_message(current_text, text)
            
            await update.message.reply_text("✅ Фото для сообщения после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
    
    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===
    
    def _get_default_success_message(self):
        """Получить стандартное сообщение подтверждения"""
        return (
            "👋 <b>Добро пожаловать!</b>\n\n"
            "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
            "📚 <b>Вы получите доступ к:</b>\n"
            "• Эксклюзивным материалам\n"
            "• Полезным советам и инструкциям\n"
            "• Актуальным новостям\n"
            "• Поддержке сообщества\n\n"
            "🙏 <b>Спасибо, что подписались!</b>\n\n"
            "💬 Если у вас есть вопросы - не стесняйтесь писать!"
        )
    
    def _get_default_payment_message(self):
        """Получить стандартное сообщение после оплаты"""
        return (
            "🎉 <b>Спасибо за покупку!</b>\n\n"
            "💰 Ваш платеж успешно обработан!\n\n"
            "✅ Вы получили полный доступ ко всем материалам.\n\n"
            "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
            "🙏 Благодарим за доверие!"
        )
