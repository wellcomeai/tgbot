"""
Миксин для обработки пользовательского ввода в админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class InputMixin:
    """Миксин для работы с пользовательским вводом"""
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                input_type, **kwargs):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        
        # Инициализация для кнопок
        if input_type == "add_button":
            self.waiting_for[user_id] = {
                "type": input_type, 
                "created_at": datetime.now(), 
                "step": "text",
                **kwargs
            }
        elif input_type == "add_goodbye_button":
            self.waiting_for[user_id] = {
                "type": input_type, 
                "created_at": datetime.now(), 
                "step": "text",
                **kwargs
            }
        else:
            self.waiting_for[user_id] = {"type": input_type, "created_at": datetime.now(), **kwargs}
        
        # Тексты для различных типов ввода
        texts = {
            "welcome": "✏️ Отправьте новое приветственное сообщение:",
            "goodbye": "✏️ Отправьте новое прощальное сообщение:",
            "success_message": "✏️ Отправьте новое сообщение подтверждения:",
            "broadcast_text": f"✏️ Отправьте новый текст для сообщения {kwargs.get('message_number')}:",
            "broadcast_delay": self._get_delay_text(kwargs.get('message_number')),
            "broadcast_photo": f"🖼 Отправьте фото для сообщения {kwargs.get('message_number')} или ссылку на фото:",
            "welcome_photo": "🖼 Отправьте фото для приветственного сообщения или ссылку на фото:",
            "goodbye_photo": "🖼 Отправьте фото для прощального сообщения или ссылку на фото:",
            "edit_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_button_url": "🔗 Отправьте новый URL для кнопки:",
            "broadcast_timer": "⏰ Отправьте количество часов, через которое возобновить рассылку:",
            "add_message": "✏️ Отправьте текст нового сообщения:",
            "add_button": f"✏️ Отправьте текст для новой кнопки сообщения {kwargs.get('message_number')}:\n\n💡 После этого мы попросим URL для кнопки.",
            
            # Массовая рассылка
            "mass_text": "✏️ Отправьте текст для массовой рассылки:",
            "mass_photo": "🖼 Отправьте фото для массовой рассылки или ссылку на фото:",
            "mass_time": "⏰ Через сколько часов отправить рассылку?\n\nПримеры: 1, 2.5, 24\n\nОставьте пустым для отправки сейчас:",
            "mass_button_text": "✏️ Отправьте текст для кнопки:",
            "mass_button_url": "🔗 Отправьте URL для кнопки:",
            
            # Платежи
            "payment_message_text": "✏️ Отправьте новый текст сообщения после оплаты:\n\n💡 Можно использовать переменную {amount} - она будет заменена на сумму платежа.",
            "payment_message_photo": "🖼 Отправьте фото для сообщения после оплаты или ссылку на фото:",
            
            # Кнопки приветствия
            "add_welcome_button": "⌨️ Отправьте текст для новой кнопки приветствия:",
            "edit_welcome_button_text": "📝 Отправьте новый текст для кнопки:",
            
            # Кнопки прощания
            "add_goodbye_button": "🔘 Отправьте текст для новой кнопки прощания:\n\n💡 После этого мы попросим URL для кнопки.",
            "edit_goodbye_button_text": "📝 Отправьте новый текст для кнопки:",
            "edit_goodbye_button_url": "🔗 Отправьте новый URL для кнопки:",
        }
        
        text = texts.get(input_type, "Отправьте необходимые данные:")
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_send_all")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def handle_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_data: dict):
        """Обработка загруженного фото"""
        user_id = update.effective_user.id
        input_type = waiting_data["type"]
        
        if input_type == "mass_photo":
            # Фото для массовой рассылки
            photo_file_id = update.message.photo[-1].file_id
            
            if user_id not in self.broadcast_drafts:
                self.broadcast_drafts[user_id] = {
                    "message_text": "",
                    "photo_data": None,
                    "buttons": [],
                    "scheduled_hours": None,
                    "created_at": datetime.now()
                }
            
            self.broadcast_drafts[user_id]["photo_data"] = photo_file_id
            
            await update.message.reply_text("✅ Фото добавлено!")
            del self.waiting_for[user_id]
            
            await self.show_send_all_menu_from_context(update, context)
        
        elif input_type == "payment_message_photo":
            # Фото для сообщения об оплате
            photo_file_id = update.message.photo[-1].file_id
            
            # Получаем текущий текст
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
            
            self.db.set_payment_success_message(current_text, photo_file_id)
            await update.message.reply_text("✅ Фото для сообщения после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
        
        elif input_type == "broadcast_photo":
            # Фото для базового сообщения рассылки
            message_number = waiting_data["message_number"]
            photo_file_id = update.message.photo[-1].file_id
            
            self.db.update_broadcast_message(message_number, photo_url=photo_file_id)
            await update.message.reply_text(f"✅ Фото для сообщения {message_number} обновлено!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        
        elif input_type == "welcome_photo":
            # Фото для приветственного сообщения
            photo_file_id = update.message.photo[-1].file_id
            welcome_text = self.db.get_welcome_message()['text']
            self.db.set_welcome_message(welcome_text, photo_url=photo_file_id)
            await update.message.reply_text("✅ Фото для приветственного сообщения обновлено!")
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
        
        elif input_type == "goodbye_photo":
            # Фото для прощального сообщения
            photo_file_id = update.message.photo[-1].file_id
            goodbye_text = self.db.get_goodbye_message()['text']
            self.db.set_goodbye_message(goodbye_text, photo_url=photo_file_id)
            await update.message.reply_text("✅ Фото для прощального сообщения обновлено!")
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        else:
            await self.show_error_message(update, context, "❌ Неожиданное фото.")
    
    async def handle_photo_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                   url: str, input_type: str, **kwargs):
        """Обработка URL-ссылок на фото"""
        user_id = update.effective_user.id
        
        if input_type == "broadcast_photo":
            message_number = kwargs.get("message_number")
            self.db.update_broadcast_message(message_number, photo_url=url)
            await update.message.reply_text(f"✅ Ссылка на фото для сообщения {message_number} сохранена!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        
        elif input_type == "welcome_photo":
            welcome_text = self.db.get_welcome_message()['text']
            self.db.set_welcome_message(welcome_text, photo_url=url)
            await update.message.reply_text("✅ Ссылка на фото для приветственного сообщения сохранена!")
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
        
        elif input_type == "goodbye_photo":
            goodbye_text = self.db.get_goodbye_message()['text']
            self.db.set_goodbye_message(goodbye_text, photo_url=url)
            await update.message.reply_text("✅ Ссылка на фото для прощального сообщения сохранена!")
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        elif input_type == "payment_message_photo":
            # Получаем текущий текст
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
            
            self.db.set_payment_success_message(current_text, url)
            await update.message.reply_text("✅ Ссылка на фото для сообщения после оплаты сохранена!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
        
        elif input_type == "mass_photo":
            if user_id not in self.broadcast_drafts:
                self.broadcast_drafts[user_id] = {
                    "message_text": "",
                    "photo_data": None,
                    "buttons": [],
                    "scheduled_hours": None,
                    "created_at": datetime.now()
                }
            
            self.broadcast_drafts[user_id]["photo_data"] = url
            await update.message.reply_text("✅ Ссылка на фото сохранена!")
            del self.waiting_for[user_id]
            await self.show_send_all_menu_from_context(update, context)
