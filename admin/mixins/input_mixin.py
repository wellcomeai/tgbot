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
    
    async def handle_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_data: dict):
        """Универсальная обработка загрузки фото для ВСЕХ типов"""
        user_id = update.effective_user.id
        input_type = waiting_data["type"]
        
        try:
            # ✅ Получаем file_id напрямую, без конвертации в URL
            photo = update.message.photo[-1]
            photo_file_id = photo.file_id
            
            logger.info(f"📸 Получен file_id фото ({input_type}): {photo_file_id}")
            
            # === БАЗОВЫЕ ТИПЫ ===
            if input_type == "welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, photo_file_id)
                await update.message.reply_text("✅ Фото приветствия обновлено!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)
                
            elif input_type == "goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, photo_file_id)
                await update.message.reply_text("✅ Фото прощания обновлено!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)
                
            elif input_type == "renewal_photo":
                self.db.set_renewal_message(photo_url=photo_file_id)
                await update.message.reply_text("✅ Фото сообщения продления обновлено!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)
            
            # === ТИПЫ ДЛЯ МАССОВЫХ РАССЫЛОК ===
            elif input_type == "mass_photo":
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
            
            elif input_type == "paid_mass_photo":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now(),
                        "is_paid_broadcast": True
                    }
                
                self.broadcast_drafts[user_id]["photo_data"] = photo_file_id
                self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
                await update.message.reply_text("✅ Фото для рассылки оплативших добавлено!")
                del self.waiting_for[user_id]
                await self.show_paid_send_all_menu_from_context(update, context)
            
            # === СООБЩЕНИЕ ОБ ОПЛАТЕ ===
            elif input_type == "payment_message_photo":
                current_data = self.db.get_payment_success_message()
                current_text = current_data['text'] if current_data else self._get_default_payment_message()
                
                self.db.set_payment_success_message(current_text, photo_file_id)
                await update.message.reply_text("✅ Фото для сообщения после оплаты обновлено!")
                del self.waiting_for[user_id]
                await self.show_payment_message_edit_from_context(update, context)
            
            # === ФОТО ДЛЯ ОСНОВНОЙ ВОРОНКИ ===
            elif input_type == "broadcast_photo":
                message_number = waiting_data["message_number"]
                self.db.update_broadcast_message(message_number, photo_url=photo_file_id)
                await update.message.reply_text(f"✅ Фото для сообщения {message_number} обновлено!")
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)
            
            # === ФОТО ДЛЯ ПЛАТНОЙ ВОРОНКИ ===
            elif input_type == "paid_broadcast_photo":
                message_number = waiting_data["message_number"]
                self.db.update_paid_broadcast_message(message_number, photo_url=photo_file_id)
                await update.message.reply_text(f"✅ Фото для платного сообщения {message_number} обновлено!")
                del self.waiting_for[user_id]
                await self.show_paid_message_edit_from_context(update, context, message_number)
            
            else:
                # Неизвестный тип
                logger.error(f"❌ Неизвестный тип фото: {input_type}")
                await update.message.reply_text("❌ Неизвестный тип ввода фото.")
                if user_id in self.waiting_for:
                    del self.waiting_for[user_id]
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке фото: {e}")
            await update.message.reply_text("❌ Ошибка при обработке фото. Попробуйте еще раз.")

    async def handle_video_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_data: dict):
        """Универсальная обработка загрузки видео для ВСЕХ типов"""
        user_id = update.effective_user.id
        input_type = waiting_data["type"]

        try:
            # ✅ Получаем file_id напрямую, без конвертации в URL
            video = update.message.video
            video_file_id = video.file_id

            logger.info(f"🎥 Получен file_id видео ({input_type}): {video_file_id}")

            # === БАЗОВЫЕ ТИПЫ ===
            if input_type == "welcome_video":
                welcome_text = self.db.get_welcome_message()['text']
                welcome_photo = self.db.get_welcome_message()['photo']
                self.db.set_welcome_message(welcome_text, welcome_photo, video_file_id)
                await update.message.reply_text("✅ Видео приветствия обновлено!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)

            elif input_type == "goodbye_video":
                goodbye_text = self.db.get_goodbye_message()['text']
                goodbye_photo = self.db.get_goodbye_message()['photo']
                self.db.set_goodbye_message(goodbye_text, goodbye_photo, video_file_id)
                await update.message.reply_text("✅ Видео прощания обновлено!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)

            elif input_type == "renewal_video":
                self.db.set_renewal_message(video_url=video_file_id)
                await update.message.reply_text("✅ Видео сообщения продления обновлено!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)

            # === ТИПЫ ДЛЯ МАССОВЫХ РАССЫЛОК ===
            elif input_type == "mass_video":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "video_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now()
                    }

                self.broadcast_drafts[user_id]["video_data"] = video_file_id
                await update.message.reply_text("✅ Видео добавлено!")
                del self.waiting_for[user_id]
                await self.show_send_all_menu_from_context(update, context)

            elif input_type == "paid_mass_video":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "video_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now(),
                        "is_paid_broadcast": True
                    }

                self.broadcast_drafts[user_id]["video_data"] = video_file_id
                self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
                await update.message.reply_text("✅ Видео для рассылки оплативших добавлено!")
                del self.waiting_for[user_id]
                await self.show_paid_send_all_menu_from_context(update, context)

            # === СООБЩЕНИЕ ОБ ОПЛАТЕ ===
            elif input_type == "payment_message_video":
                current_data = self.db.get_payment_success_message()
                current_text = current_data['text'] if current_data else self._get_default_payment_message()
                current_photo = current_data['photo_url'] if current_data else None

                self.db.set_payment_success_message(current_text, current_photo, video_file_id)
                await update.message.reply_text("✅ Видео для сообщения после оплаты обновлено!")
                del self.waiting_for[user_id]
                await self.show_payment_message_edit_from_context(update, context)

            # === ВИДЕО ДЛЯ ОСНОВНОЙ ВОРОНКИ ===
            elif input_type == "broadcast_video":
                message_number = waiting_data["message_number"]
                self.db.update_broadcast_message(message_number, video_url=video_file_id)
                await update.message.reply_text(f"✅ Видео для сообщения {message_number} обновлено!")
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)

            # === ВИДЕО ДЛЯ ПЛАТНОЙ ВОРОНКИ ===
            elif input_type == "paid_broadcast_video":
                message_number = waiting_data["message_number"]
                self.db.update_paid_broadcast_message(message_number, video_url=video_file_id)
                await update.message.reply_text(f"✅ Видео для платного сообщения {message_number} обновлено!")
                del self.waiting_for[user_id]
                await self.show_paid_message_edit_from_context(update, context, message_number)

            else:
                # Неизвестный тип
                logger.error(f"❌ Неизвестный тип видео: {input_type}")
                await update.message.reply_text("❌ Неизвестный тип ввода видео.")
                if user_id in self.waiting_for:
                    del self.waiting_for[user_id]

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке видео: {e}")
            await update.message.reply_text("❌ Ошибка при обработке видео. Попробуйте еще раз.")

    async def handle_photo_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                   url: str, input_type: str, **kwargs):
        """Обработка URL-ссылок на фото и видео (для обратной совместимости)"""
        user_id = update.effective_user.id

        try:
            # === БАЗОВЫЕ ТИПЫ ===
            if input_type == "welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, url)
                await update.message.reply_text("✅ Ссылка на фото приветствия сохранена!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)

            elif input_type == "goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, url)
                await update.message.reply_text("✅ Ссылка на фото прощания сохранена!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)

            elif input_type == "renewal_photo":
                self.db.set_renewal_message(photo_url=url)
                await update.message.reply_text("✅ Ссылка на фото сообщения продления сохранена!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)

            # === БАЗОВЫЕ ТИПЫ ВИДЕО ===
            elif input_type == "welcome_video":
                welcome_text = self.db.get_welcome_message()['text']
                welcome_photo = self.db.get_welcome_message()['photo']
                self.db.set_welcome_message(welcome_text, welcome_photo, url)
                await update.message.reply_text("✅ Ссылка на видео приветствия сохранена!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)

            elif input_type == "goodbye_video":
                goodbye_text = self.db.get_goodbye_message()['text']
                goodbye_photo = self.db.get_goodbye_message()['photo']
                self.db.set_goodbye_message(goodbye_text, goodbye_photo, url)
                await update.message.reply_text("✅ Ссылка на видео прощания сохранена!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)

            elif input_type == "renewal_video":
                self.db.set_renewal_message(video_url=url)
                await update.message.reply_text("✅ Ссылка на видео сообщения продления сохранена!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)

            # === ОСНОВНАЯ ВОРОНКА ===
            elif input_type == "broadcast_photo":
                message_number = kwargs.get("message_number")
                self.db.update_broadcast_message(message_number, photo_url=url)
                await update.message.reply_text(f"✅ Ссылка на фото для сообщения {message_number} сохранена!")
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)

            elif input_type == "broadcast_video":
                message_number = kwargs.get("message_number")
                self.db.update_broadcast_message(message_number, video_url=url)
                await update.message.reply_text(f"✅ Ссылка на видео для сообщения {message_number} сохранена!")
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)

            # === ПЛАТНАЯ ВОРОНКА ===
            elif input_type == "paid_broadcast_photo":
                message_number = kwargs.get("message_number")
                self.db.update_paid_broadcast_message(message_number, photo_url=url)
                await update.message.reply_text(f"✅ Ссылка на фото для платного сообщения {message_number} сохранена!")
                del self.waiting_for[user_id]
                await self.show_paid_message_edit_from_context(update, context, message_number)

            elif input_type == "paid_broadcast_video":
                message_number = kwargs.get("message_number")
                self.db.update_paid_broadcast_message(message_number, video_url=url)
                await update.message.reply_text(f"✅ Ссылка на видео для платного сообщения {message_number} сохранена!")
                del self.waiting_for[user_id]
                await self.show_paid_message_edit_from_context(update, context, message_number)

            # === СООБЩЕНИЕ ОБ ОПЛАТЕ ===
            elif input_type == "payment_message_photo":
                current_data = self.db.get_payment_success_message()
                current_text = current_data['text'] if current_data else self._get_default_payment_message()

                self.db.set_payment_success_message(current_text, url)
                await update.message.reply_text("✅ Ссылка на фото для сообщения после оплаты сохранена!")
                del self.waiting_for[user_id]
                await self.show_payment_message_edit_from_context(update, context)

            elif input_type == "payment_message_video":
                current_data = self.db.get_payment_success_message()
                current_text = current_data['text'] if current_data else self._get_default_payment_message()
                current_photo = current_data['photo_url'] if current_data else None

                self.db.set_payment_success_message(current_text, current_photo, url)
                await update.message.reply_text("✅ Ссылка на видео для сообщения после оплаты сохранена!")
                del self.waiting_for[user_id]
                await self.show_payment_message_edit_from_context(update, context)

            # === МАССОВЫЕ РАССЫЛКИ ===
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

            elif input_type == "mass_video":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "video_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now()
                    }

                self.broadcast_drafts[user_id]["video_data"] = url
                await update.message.reply_text("✅ Ссылка на видео сохранена!")
                del self.waiting_for[user_id]
                await self.show_send_all_menu_from_context(update, context)

            elif input_type == "paid_mass_photo":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now(),
                        "is_paid_broadcast": True
                    }

                self.broadcast_drafts[user_id]["photo_data"] = url
                self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
                await update.message.reply_text("✅ Ссылка на фото для рассылки оплативших сохранена!")
                del self.waiting_for[user_id]
                await self.show_paid_send_all_menu_from_context(update, context)

            elif input_type == "paid_mass_video":
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "video_data": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now(),
                        "is_paid_broadcast": True
                    }

                self.broadcast_drafts[user_id]["video_data"] = url
                self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
                await update.message.reply_text("✅ Ссылка на видео для рассылки оплативших сохранена!")
                del self.waiting_for[user_id]
                await self.show_paid_send_all_menu_from_context(update, context)

            else:
                logger.error(f"❌ Неизвестный тип URL: {input_type}")
                await update.message.reply_text("❌ Неизвестный тип ввода URL.")
                if user_id in self.waiting_for:
                    del self.waiting_for[user_id]

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке URL: {e}")
            await update.message.reply_text("❌ Ошибка при сохранении URL. Попробуйте еще раз.")
    
    def _get_default_payment_message(self):
        """Стандартное сообщение об оплате"""
        return (
            "🎉 <b>Спасибо за покупку!</b>\n\n"
            "💰 Ваш платеж успешно обработан!\n\n"
            "✅ Вы получили полный доступ ко всем материалам.\n\n"
            "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
            "🙏 Благодарим за доверие!"
        )
