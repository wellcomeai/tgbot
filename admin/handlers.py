"""
Обработчики событий для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging
import asyncio
import utm_utils

logger = logging.getLogger(__name__)


class HandlersMixin:
    """Миксин для обработки событий админ-панели"""
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка всех callback запросов админ-панели"""
        query = update.callback_query
        data = query.data
        user_id = query.from_user.id
        
        try:
            await query.answer()
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.warning(f"⚠️ Ошибка при ответе на callback: {e}")
        
        try:
            # === Основные команды ===
            if data == "admin_back":
                await self.show_main_menu(update, context)
            elif data == "admin_stats":
                await self.show_statistics(update, context)
            elif data == "admin_broadcast":
                await self.show_broadcast_menu(update, context)
            elif data == "admin_broadcast_status":
                await self.show_broadcast_status(update, context)
            elif data == "admin_users":
                await self.show_users_list(update, context)
            elif data == "admin_send_all":
                await self.show_send_all_menu(update, context)
            elif data == "admin_welcome":
                await self.show_welcome_edit(update, context)
            elif data == "admin_goodbye":
                await self.show_goodbye_edit(update, context)
            elif data == "admin_success_message":
                await self.show_success_message_edit(update, context)
            elif data == "admin_scheduled_broadcasts":
                await self.show_scheduled_broadcasts(update, context)
            elif data == "download_csv":
                await self.send_csv_file(update, context)
            elif data == "enable_broadcast":
                self.db.set_broadcast_status(True, None)
                await self.show_broadcast_status(update, context)
            elif data == "disable_broadcast":
                self.db.set_broadcast_status(False, None)
                await self.show_broadcast_status(update, context)
            elif data == "set_broadcast_timer":
                await self.request_text_input(update, context, "broadcast_timer")
            
            # === Платежи ===
            elif data == "admin_payment_message":
                await self.show_payment_message_edit(update, context)
            elif data == "admin_payment_stats":
                await self.show_payment_statistics(update, context)
            elif data == "edit_payment_message_text":
                await self.request_text_input(update, context, "payment_message_text")
            elif data == "edit_payment_message_photo":
                await self.request_text_input(update, context, "payment_message_photo")
            elif data == "remove_payment_message_photo":
                await self._handle_remove_payment_photo(update, context)
            elif data == "reset_payment_message":
                await self._handle_reset_payment_message(update, context)
            
            # === Массовые рассылки ===
            elif data == "mass_edit_text":
                await self.request_text_input(update, context, "mass_text")
            elif data == "mass_add_photo":
                await self.request_text_input(update, context, "mass_photo")
            elif data == "mass_set_time":
                await self.request_text_input(update, context, "mass_time")
            elif data == "mass_add_button":
                await self.request_text_input(update, context, "mass_button_text")
            elif data == "mass_remove_photo":
                await self._handle_mass_remove_photo(update, context)
            elif data == "mass_remove_button":
                await self._handle_mass_remove_button(update, context)
            elif data == "mass_preview":
                await self.show_mass_broadcast_preview(update, context)
            elif data == "mass_send_now":
                await self._handle_mass_send_now(update, context)
            elif data == "mass_confirm_send":
                await self.execute_mass_broadcast(update, context)
            
            # === Остальные обработчики ===
            elif await self.handle_specific_callbacks(update, context):
                pass
            else:
                await self.show_error_message(update, context, "❌ Неизвестная команда.")
                
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при обработке callback {data}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка. Попробуйте еще раз.")
    
    async def handle_specific_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка специфических callback запросов"""
        query = update.callback_query
        data = query.data
        
        # Обработка для основных сообщений рассылки
        if data.startswith("edit_msg_"):
            message_number = int(data.split("_")[2])
            await self.show_message_edit(update, context, message_number)
        elif data.startswith("manage_buttons_"):
            message_number = int(data.split("_")[2])
            await self.show_message_buttons(update, context, message_number)
        elif data.startswith("edit_button_") and not data.startswith("edit_button_text_") and not data.startswith("edit_button_url_"):
            button_id = int(data.split("_")[2])
            await self.show_button_edit(update, context, button_id)
        elif data.startswith("edit_button_text_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_text", button_id=button_id)
        elif data.startswith("edit_button_url_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_url", button_id=button_id)
        elif data.startswith("delete_button_"):
            await self._handle_delete_button(update, context, data)
        
        # Управление приветственными кнопками
        elif data == "manage_welcome_buttons":
            await self.show_welcome_buttons_management(update, context)
        elif data == "add_welcome_button":
            await self.request_text_input(update, context, "add_welcome_button")
        elif data.startswith("edit_welcome_button_") and not data.startswith("edit_welcome_button_text_"):
            button_id = int(data.split("_")[3])
            await self.show_welcome_button_edit(update, context, button_id)
        elif data.startswith("edit_welcome_button_text_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_welcome_button_text", button_id=button_id)
        elif data.startswith("delete_welcome_button_"):
            button_id = int(data.split("_")[3])
            await self.show_welcome_button_delete_confirm(update, context, button_id)
        elif data.startswith("confirm_delete_welcome_button_"):
            await self._handle_confirm_delete_welcome_button(update, context, data)
        
        # Управление прощальными кнопками
        elif data == "manage_goodbye_buttons":
            await self.show_goodbye_buttons_management(update, context)
        elif data == "add_goodbye_button":
            await self.request_text_input(update, context, "add_goodbye_button")
        elif data.startswith("edit_goodbye_button_") and not data.startswith("edit_goodbye_button_text_") and not data.startswith("edit_goodbye_button_url_"):
            button_id = int(data.split("_")[3])
            await self.show_goodbye_button_edit(update, context, button_id)
        elif data.startswith("edit_goodbye_button_text_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_goodbye_button_text", button_id=button_id)
        elif data.startswith("edit_goodbye_button_url_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_goodbye_button_url", button_id=button_id)
        elif data.startswith("delete_goodbye_button_"):
            button_id = int(data.split("_")[3])
            await self.show_goodbye_button_delete_confirm(update, context, button_id)
        elif data.startswith("confirm_delete_goodbye_button_"):
            await self._handle_confirm_delete_goodbye_button(update, context, data)
        
        # Редактирование сообщений
        elif data == "edit_welcome_text":
            await self.request_text_input(update, context, "welcome")
        elif data == "edit_welcome_photo":
            await self.request_text_input(update, context, "welcome_photo")
        elif data == "remove_welcome_photo":
            await self._handle_remove_welcome_photo(update, context)
        elif data == "edit_goodbye_text":
            await self.request_text_input(update, context, "goodbye")
        elif data == "edit_goodbye_photo":
            await self.request_text_input(update, context, "goodbye_photo")
        elif data == "remove_goodbye_photo":
            await self._handle_remove_goodbye_photo(update, context)
        elif data == "edit_success_message_text":
            await self.request_text_input(update, context, "success_message")
        elif data == "reset_success_message":
            await self._handle_reset_success_message(update, context)
        
        # Дополнительные обработчики для основных сообщений
        elif await self.handle_additional_callbacks(update, context):
            pass
        else:
            return False  # Не обработано
        
        return True  # Обработано
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка текстовых сообщений и фото от админа"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            return
        
        waiting_data = self.waiting_for[user_id]
        input_type = waiting_data["type"]
        
        # Проверяем, что состояние валидно
        if not self.validate_waiting_state(waiting_data):
            await self.show_error_message(update, context, "❌ Некорректное состояние. Начните сначала.")
            return
        
        try:
            # Обработка фото
            if update.message.photo:
                await self.handle_photo_input(update, context, waiting_data)
                return
            
            # Обработка текста
            text = update.message.text if update.message.text else update.message.caption
            if not text:
                await self.show_error_message(update, context, "❌ Пустое сообщение. Попробуйте еще раз.")
                return
            
            # Маршрутизация по типам ввода
            await self._route_input_by_type(update, context, text, input_type, waiting_data)
                
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при обработке сообщения от админа {user_id}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка при обработке вашего сообщения.")
    
    async def _route_input_by_type(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                 text: str, input_type: str, waiting_data: dict):
        """Маршрутизация ввода по типам"""
        # Кнопки приветствия
        if input_type == "add_welcome_button":
            await self.handle_add_welcome_button_input(update, context, text)
        elif input_type == "edit_welcome_button_text":
            await self.handle_edit_welcome_button_text_input(update, context, text)
        
        # Кнопки прощания
        elif input_type == "add_goodbye_button":
            await self.handle_add_goodbye_button_input(update, context, text)
        elif input_type == "edit_goodbye_button_text":
            await self.handle_edit_goodbye_button_text_input(update, context, text)
        elif input_type == "edit_goodbye_button_url":
            await self.handle_edit_goodbye_button_url_input(update, context, text)
            
        # Платежи
        elif input_type == "payment_message_text":
            await self.handle_payment_message_input(update, context, text, "payment_message_text")
        elif input_type == "payment_message_photo":
            await self.handle_payment_message_input(update, context, text, "payment_message_photo")
        
        # Массовая рассылка
        elif input_type == "mass_text":
            await self.handle_mass_text_input(update, context, text)
        elif input_type == "mass_photo":
            await self.handle_mass_photo_input(update, context, text)
        elif input_type == "mass_time":
            await self.handle_mass_time_input(update, context, text)
        elif input_type == "mass_button_text":
            await self.handle_mass_button_text_input(update, context, text)
        elif input_type == "mass_button_url":
            await self.handle_mass_button_url_input(update, context, text)
        
        # Остальные типы
        elif input_type == "broadcast_timer":
            await self.handle_broadcast_timer(update, context, text)
        elif input_type == "add_message":
            await self.handle_add_message(update, context, text)
        elif input_type == "add_button":
            await self.handle_add_button(update, context, text)
        
        # Базовые настройки
        elif await self._handle_basic_message_types(update, context, text, input_type, waiting_data):
            pass
        else:
            await self.show_error_message(update, context, "❌ Неизвестный тип ввода.")
    
    async def _handle_basic_message_types(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                        text: str, input_type: str, waiting_data: dict):
        """Обработка базовых типов сообщений"""
        user_id = update.effective_user.id
        
        if input_type == "welcome":
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return True
            self.db.set_welcome_message(text)
            await update.message.reply_text("✅ Приветственное сообщение обновлено!")
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
            
        elif input_type == "goodbye":
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return True
            self.db.set_goodbye_message(text)
            await update.message.reply_text("✅ Прощальное сообщение обновлено!")
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        elif input_type == "success_message":
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return True
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('success_message', text))
            conn.commit()
            conn.close()
            await update.message.reply_text("✅ Сообщение подтверждения обновлено!")
            del self.waiting_for[user_id]
            await self.show_success_message_edit_from_context(update, context)
        
        elif input_type == "broadcast_text":
            message_number = waiting_data["message_number"]
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return True
            self.db.update_broadcast_message(message_number, text=text)
            await update.message.reply_text(f"✅ Текст сообщения {message_number} обновлён!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        
        elif input_type == "broadcast_delay":
            await self._handle_broadcast_delay_input(update, context, text, waiting_data)
        
        elif input_type in ["broadcast_photo", "welcome_photo", "goodbye_photo", "payment_message_photo"]:
            if text.startswith("http://") or text.startswith("https://"):
                await self.handle_photo_url_input(update, context, text, input_type, **waiting_data)
            else:
                await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
        
        elif input_type == "edit_button_text":
            await self._handle_edit_button_text_input(update, context, text, waiting_data)
        
        elif input_type == "edit_button_url":
            await self._handle_edit_button_url_input(update, context, text, waiting_data)
        
        else:
            return False
        
        return True
    
    # === Вспомогательные методы для обработки ===
    
    async def _handle_remove_payment_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление фото из сообщения об оплате"""
        current_data = self.db.get_payment_success_message()
        current_text = current_data['text'] if current_data else None
        if not current_text:
            current_text = (
                "🎉 <b>Спасибо за покупку!</b>\n\n"
                "💰 Ваш платеж успешно обработан!\n\n"
                "✅ Вы получили полный доступ ко всем материалам.\n\n"
                "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                "🙏 Благодарим за доверие!"
            )
        self.db.set_payment_success_message(current_text, "")
        await self.show_payment_message_edit(update, context)
    
    async def _handle_reset_payment_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сброс сообщения об оплате к стандартному"""
        default_payment_message = (
            "🎉 <b>Спасибо за покупку!</b>\n\n"
            "💰 Ваш платеж на сумму {amount} успешно обработан!\n\n"
            "✅ Вы получили полный доступ ко всем материалам.\n\n"
            "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
            "🙏 Благодарим за доверие!"
        )
        self.db.set_payment_success_message(default_payment_message, "")
        await self.show_payment_message_edit(update, context)
    
    async def _handle_mass_remove_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление фото из массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["photo_data"] = None
            await self.show_send_all_menu(update, context)
    
    async def _handle_mass_remove_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление последней кнопки из массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts and self.broadcast_drafts[user_id]["buttons"]:
            self.broadcast_drafts[user_id]["buttons"].pop()
            await self.show_send_all_menu(update, context)
    
    async def _handle_mass_send_now(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Установка немедленной отправки массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["scheduled_hours"] = None
            await self.show_mass_broadcast_preview(update, context)
    
    async def _handle_delete_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Удаление кнопки сообщения"""
        button_id = int(data.split("_")[2])
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT message_number FROM message_buttons WHERE id = ?', (button_id,))
        result = cursor.fetchone()
        conn.close()
        
        if result:
            message_number = result[0]
            self.db.delete_message_button(button_id)
            await self.show_message_buttons(update, context, message_number)
    
    async def _handle_confirm_delete_welcome_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Подтверждение удаления кнопки приветствия"""
        button_id = int(data.split("_")[4])
        self.db.delete_welcome_button(button_id)
        await update.callback_query.answer("✅ Кнопка удалена!")
        await self.show_welcome_buttons_management(update, context)
    
    async def _handle_confirm_delete_goodbye_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, data: str):
        """Подтверждение удаления кнопки прощания"""
        button_id = int(data.split("_")[4])
        self.db.delete_goodbye_button(button_id)
        await update.callback_query.answer("✅ Кнопка удалена!")
        await self.show_goodbye_buttons_management(update, context)
    
    async def _handle_remove_welcome_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление фото приветственного сообщения"""
        welcome_text = self.db.get_welcome_message()['text']
        self.db.set_welcome_message(welcome_text, photo_url="")
        await self.show_welcome_edit(update, context)
    
    async def _handle_remove_goodbye_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление фото прощального сообщения"""
        goodbye_text = self.db.get_goodbye_message()['text']
        self.db.set_goodbye_message(goodbye_text, photo_url="")
        await self.show_goodbye_edit(update, context)
    
    async def _handle_reset_success_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сброс сообщения подтверждения к стандартному"""
        default_success_message = (
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
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', 
                      ('success_message', default_success_message))
        conn.commit()
        conn.close()
        await self.show_success_message_edit(update, context)
    
    async def _handle_broadcast_delay_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                          text: str, waiting_data: dict):
        """Обработка ввода задержки для рассылки"""
        user_id = update.effective_user.id
        message_number = waiting_data["message_number"]
        
        delay_hours, delay_display = self.parse_delay_input(text)
        
        if delay_hours is not None and delay_hours > 0:
            self.db.update_broadcast_message(message_number, delay_hours=delay_hours)
            await update.message.reply_text(f"✅ Задержка для сообщения {message_number} установлена на {delay_display}!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        else:
            await update.message.reply_text(
                "❌ Неверный формат! Примеры правильного ввода:\n\n"
                "• <code>3м</code> или <code>3 минуты</code>\n"
                "• <code>2ч</code> или <code>2 часа</code>\n"
                "• <code>1.5</code> (для 1.5 часов)\n"
                "• <code>0.05</code> (для 3 минут)",
                parse_mode='HTML'
            )
    
    async def _handle_edit_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                           text: str, waiting_data: dict):
        """Обработка изменения текста кнопки"""
        user_id = update.effective_user.id
        button_id = waiting_data["button_id"]
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
            
        self.db.update_message_button(button_id, button_text=text)
        await update.message.reply_text("✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        await self.show_button_edit_from_context(update, context, button_id)
    
    async def _handle_edit_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                          text: str, waiting_data: dict):
        """Обработка изменения URL кнопки"""
        user_id = update.effective_user.id
        button_id = waiting_data["button_id"]
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        self.db.update_message_button(button_id, button_url=text)
        await update.message.reply_text("✅ URL кнопки обновлен!")
        del self.waiting_for[user_id]
        await self.show_button_edit_from_context(update, context, button_id)
    
    async def handle_additional_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дополнительные обработчики callback для рассылок"""
        query = update.callback_query
        data = query.data
        
        # Обработка для основных сообщений рассылки
        if data.startswith("edit_text_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_text", message_number=message_number)
        elif data.startswith("edit_delay_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_delay", message_number=message_number)
        elif data.startswith("edit_photo_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_photo", message_number=message_number)
        elif data.startswith("remove_photo_"):
            message_number = int(data.split("_")[2])
            self.db.update_broadcast_message(message_number, photo_url="")
            await self.show_message_edit(update, context, message_number)
        elif data.startswith("delete_msg_"):
            message_number = int(data.split("_")[2])
            # Подтверждение удаления
            keyboard = [
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{message_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_msg_{message_number}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(
                update, context,
                f"⚠️ Вы уверены, что хотите удалить сообщение {message_number}?\n\nЭто также отменит все запланированные отправки этого сообщения.",
                reply_markup
            )
        elif data.startswith("confirm_delete_"):
            message_number = int(data.split("_")[2])
            self.db.delete_broadcast_message(message_number)
            await self.show_broadcast_menu(update, context)
        elif data == "add_message":
            # Инициализация для добавления сообщения
            user_id = update.callback_query.from_user.id
            self.waiting_for[user_id] = {
                "type": "add_message", 
                "created_at": datetime.now(), 
                "step": "text"
            }
            
            await self.safe_edit_or_send_message(
                update, context,
                "✏️ Отправьте текст нового сообщения:\n\n💡 После этого мы попросим задержку для отправки.",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast")]])
            )
        elif data.startswith("add_button_"):
            message_number = int(data.split("_")[2])
            # Проверяем лимит кнопок
            existing_buttons = self.db.get_message_buttons(message_number)
            if len(existing_buttons) >= 3:
                await query.answer("❌ Максимум 3 кнопки на сообщение!", show_alert=True)
                return False
            await self.request_text_input(update, context, "add_button", message_number=message_number)
        else:
            return False  # Не обработано
        
        return True  # Обработано
