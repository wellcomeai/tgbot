"""
Обработчики событий для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
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
            elif data == "admin_paid_broadcast":
                await self.show_paid_broadcast_menu(update, context)
            elif data == "paid_send_all":
                await self.show_paid_send_all_menu(update, context)
            elif data == "paid_scheduled_broadcasts":
                await self.show_paid_scheduled_broadcasts(update, context)
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
            
            # === ✅ НОВОЕ: Переключение сообщения подтверждения ===
            elif data == "toggle_success_message":
                # Переключаем статус
                current_status = self.db.is_success_message_enabled()
                new_status = not current_status
                self.db.set_success_message_enabled(new_status)
                
                status_text = "включено" if new_status else "выключено"
                await query.answer(f"✅ Сообщение подтверждения {status_text}!")
                
                # Обновляем меню
                await self.show_success_message_edit(update, context)
            
            # === Продление подписок ===
            elif data == "admin_renewal":
                await self.show_renewal_menu(update, context)
            elif data == "edit_renewal_video":
                await self.request_text_input(update, context, "renewal_video")
            elif data == "remove_renewal_video":
                self.db.set_renewal_message(video_url="")
                await self.show_renewal_edit(update, context)
            
            # === Платежи ===
            elif data == "admin_payment_message":
                await self.show_payment_message_edit(update, context)
            elif data == "admin_payment_stats":
                await self.show_payment_statistics(update, context)
            
            # === 📊 НОВОЕ: Статистика воронки ===
            elif data == "admin_funnel_stats":
                await self.show_funnel_statistics(update, context)
            elif data.startswith("admin_msg_detail_"):
                # Извлекаем номер сообщения из callback данных
                message_number = int(data.split("_")[3])
                await self.show_message_details(update, context, message_number)
            
            elif data == "edit_payment_message_text":
                await self.request_text_input(update, context, "payment_message_text")
            elif data == "edit_payment_message_photo":
                await self.request_text_input(update, context, "payment_message_photo")
            elif data == "remove_payment_message_photo":
                await self._handle_remove_payment_photo(update, context)
            elif data == "edit_payment_message_video":
                await self.request_text_input(update, context, "payment_message_video")
            elif data == "remove_payment_message_video":
                current_data = self.db.get_payment_success_message()
                self.db.set_payment_success_message(current_data['text'], current_data['photo_url'], "")
                await self.show_payment_message_edit(update, context)
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
            elif data == "mass_add_video":
                await self.request_text_input(update, context, "mass_video")
            elif data == "mass_remove_video":
                await self._handle_mass_remove_video(update, context)
            elif data == "mass_remove_button":
                await self._handle_mass_remove_button(update, context)
            elif data == "mass_preview":
                await self.show_mass_broadcast_preview(update, context)
            elif data == "mass_send_now":
                await self._handle_mass_send_now(update, context)
            elif data == "mass_confirm_send":
                await self.execute_mass_broadcast(update, context)
            
            # === 🎬 НОВОЕ: Медиа-альбомы для массовых рассылок ===
            elif data == "mass_create_album":
                await self.show_create_mass_media_album_menu(update, context)
            elif data == "mass_manage_album":
                await self.show_mass_album_management_menu(update, context)
            elif data == "mass_recreate_album":
                await self.show_create_mass_media_album_menu(update, context)
            elif data == "preview_mass_album":
                await self.show_mass_album_preview(update, context)
            elif data == "save_mass_album":
                await self.save_mass_media_album(update, context)
            elif data == "clear_mass_album":
                await self.clear_mass_media_album_draft(update, context)
            elif data == "mass_delete_album":
                await self._handle_mass_delete_album(update, context)
            
            # === Платные массовые рассылки ===
            elif data == "paid_mass_edit_text":
                await self.request_text_input(update, context, "paid_mass_text")
            elif data == "paid_mass_add_photo":
                await self.request_text_input(update, context, "paid_mass_photo")
            elif data == "paid_mass_set_time":
                await self.request_text_input(update, context, "paid_mass_time")
            elif data == "paid_mass_add_button":
                await self.request_text_input(update, context, "paid_mass_button_text")
            elif data == "paid_mass_remove_photo":
                await self._handle_paid_mass_remove_photo(update, context)
            elif data == "paid_mass_add_video":
                await self.request_text_input(update, context, "paid_mass_video")
            elif data == "paid_mass_remove_video":
                await self._handle_paid_mass_remove_video(update, context)
            elif data == "paid_mass_remove_button":
                await self._handle_paid_mass_remove_button(update, context)
            elif data == "paid_mass_preview":
                await self.show_paid_mass_broadcast_preview(update, context)
            elif data == "paid_mass_send_now":
                await self._handle_paid_mass_send_now(update, context)
            elif data == "paid_mass_confirm_send":
                await self.execute_paid_mass_broadcast(update, context)
            
            # === 🎬 НОВОЕ: Медиа-альбомы для платных рассылок ===
            elif data == "paid_mass_create_album":
                # Переключаем на платный режим
                user_id = update.callback_query.from_user.id
                if user_id not in self.broadcast_drafts:
                    self.broadcast_drafts[user_id] = {
                        "message_text": "",
                        "photo_data": None,
                        "video_data": None,
                        "media_album": None,
                        "buttons": [],
                        "scheduled_hours": None,
                        "created_at": datetime.now(),
                        "is_paid_broadcast": True
                    }
                self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
                await self.show_create_mass_media_album_menu(update, context, is_paid=True)
            elif data == "paid_mass_manage_album":
                await self.show_paid_mass_album_management_menu(update, context)
            elif data == "paid_mass_recreate_album":
                await self.show_create_mass_media_album_menu(update, context, is_paid=True)
            elif data == "preview_paid_mass_album":
                await self.show_paid_mass_album_preview(update, context)
            elif data == "save_paid_mass_album":
                await self.save_paid_mass_media_album(update, context)
            elif data == "clear_paid_mass_album":
                await self.clear_paid_mass_media_album_draft(update, context)
            elif data == "paid_mass_delete_album":
                await self._handle_paid_mass_delete_album(update, context)
            
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
        
        # === Обработчики для продления подписок ===
        if await self.handle_renewal_callbacks(update, context):
            return True
        
        # Обработка для основных сообщений рассылки
        elif data.startswith("edit_msg_"):
            message_number = int(data.split("_")[2])
            await self.show_message_edit(update, context, message_number)
        
        # === 🎬 НОВОЕ: Медиа-альбом для воронки ===
        elif data.startswith("create_album_"):
            message_number = int(data.split("_")[2])
            await self.show_create_media_album_menu(update, context, message_number)
        elif data.startswith("manage_album_"):
            message_number = int(data.split("_")[2])
            await self.show_manage_media_album_menu(update, context, message_number)
        elif data.startswith("preview_album_"):
            message_number = int(data.split("_")[2])
            await self.show_media_album_preview(update, context, message_number)
        elif data.startswith("save_album_"):
            message_number = int(data.split("_")[2])
            await self.save_media_album(update, context, message_number)
        elif data.startswith("clear_album_"):
            message_number = int(data.split("_")[2])
            await self.clear_media_album_draft(update, context, message_number)
        elif data.startswith("delete_album_"):
            message_number = int(data.split("_")[2])
            await self.delete_saved_media_album(update, context, message_number)
        
        elif data.startswith("manage_buttons_"):
            message_number = int(data.split("_")[2])
            await self.show_message_buttons(update, context, message_number)
        elif data.startswith("edit_button_") and not data.startswith("edit_button_text_") and not data.startswith("edit_button_url_") and not data.startswith("edit_button_count_"):
            button_id = int(data.split("_")[2])
            await self.show_button_edit(update, context, button_id)
        elif data.startswith("edit_button_text_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_text", button_id=button_id)
        elif data.startswith("edit_button_url_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_url", button_id=button_id)
        elif data.startswith("edit_button_count_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_count", button_id=button_id)
        elif data.startswith("delete_button_"):
            await self._handle_delete_button(update, context, data)
        
        # Обработка для платных сообщений рассылки
        elif data.startswith("edit_paid_msg_"):
            message_number = int(data.split("_")[3])
            await self.show_paid_message_edit(update, context, message_number)
        elif data.startswith("manage_paid_buttons_"):
            message_number = int(data.split("_")[3])
            await self.show_paid_message_buttons(update, context, message_number)
        elif data.startswith("edit_paid_button_") and not data.startswith("edit_paid_button_text_") and not data.startswith("edit_paid_button_url_"):
            button_id = int(data.split("_")[3])
            await self.show_paid_button_edit(update, context, button_id)
        elif data.startswith("add_paid_button_"):
            message_number = int(data.split("_")[3])
            await self.request_text_input(update, context, "add_paid_button", message_number=message_number)
        elif data == "add_paid_message":
            user_id = update.callback_query.from_user.id
            self.waiting_for[user_id] = {
                "type": "add_paid_message", 
                "created_at": datetime.now(), 
                "step": "text"
            }
            await self.safe_edit_or_send_message(
                update, context,
                "💰 ✏️ Отправьте текст нового сообщения для оплативших:\n\n💡 После этого мы попросим задержку для отправки.",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_paid_broadcast")]])
            )
        # Редактирование платных сообщений
        elif data.startswith("edit_paid_text_"):
            message_number = int(data.split("_")[3])
            await self.request_text_input(update, context, "paid_broadcast_text", message_number=message_number)
        elif data.startswith("edit_paid_delay_"):
            message_number = int(data.split("_")[3])
            await self.request_text_input(update, context, "paid_broadcast_delay", message_number=message_number)
        elif data.startswith("edit_paid_photo_"):
            message_number = int(data.split("_")[3])
            await self.request_text_input(update, context, "paid_broadcast_photo", message_number=message_number)
        elif data.startswith("remove_paid_photo_"):
            message_number = int(data.split("_")[3])
            self.db.update_paid_broadcast_message(message_number, photo_url="")
            await self.show_paid_message_edit(update, context, message_number)

        # Видео для платных сообщений
        elif data.startswith("edit_paid_video_"):
            message_number = int(data.split("_")[3])
            await self.request_text_input(update, context, "paid_broadcast_video", message_number=message_number)
        elif data.startswith("remove_paid_video_"):
            message_number = int(data.split("_")[3])
            self.db.update_paid_broadcast_message(message_number, video_url="")
            await self.show_paid_message_edit(update, context, message_number)

        elif data.startswith("delete_paid_msg_"):
            message_number = int(data.split("_")[3])
            # Подтверждение удаления
            keyboard = [
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_paid_{message_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_paid_msg_{message_number}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(
                update, context,
                f"⚠️ Вы уверены, что хотите удалить сообщение для оплативших {message_number}?\n\nЭто также отменит все запланированные отправки этого сообщения.",
                reply_markup
            )
        elif data.startswith("confirm_delete_paid_"):
            message_number = int(data.split("_")[3])
            self.db.delete_paid_broadcast_message(message_number)
            await self.show_paid_broadcast_menu(update, context)
        
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
        elif data == "edit_welcome_video":
            await self.request_text_input(update, context, "welcome_video")
        elif data == "remove_welcome_video":
            welcome_text = self.db.get_welcome_message()['text']
            welcome_photo = self.db.get_welcome_message()['photo']
            self.db.set_welcome_message(welcome_text, welcome_photo, "")
            await self.show_welcome_edit(update, context)

        elif data == "edit_goodbye_text":
            await self.request_text_input(update, context, "goodbye")
        elif data == "edit_goodbye_photo":
            await self.request_text_input(update, context, "goodbye_photo")
        elif data == "remove_goodbye_photo":
            await self._handle_remove_goodbye_photo(update, context)
        elif data == "edit_goodbye_video":
            await self.request_text_input(update, context, "goodbye_video")
        elif data == "remove_goodbye_video":
            goodbye_text = self.db.get_goodbye_message()['text']
            goodbye_photo = self.db.get_goodbye_message()['photo']
            self.db.set_goodbye_message(goodbye_text, goodbye_photo, "")
            await self.show_goodbye_edit(update, context)

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
            # Проверяем, ожидаем ли мы медиа для альбома
            if user_id in self.media_album_drafts:
                await self.handle_media_album_input(update, context)
                return
            elif user_id in self.mass_media_album_drafts:
                await self.handle_mass_media_album_input(update, context)
                return
            else:
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

            # Обработка видео
            if update.message.video:
                await self.handle_video_input(update, context, waiting_data)
                return

            # ✅ ОБНОВЛЕНО: Обработка текста с HTML форматированием из Telegram
            # Используем text_html и caption_html для сохранения форматирования
            if update.message.text:
                text = update.message.text_html  # ← Автоматически конвертирует entities в HTML
            elif update.message.caption:
                text = update.message.caption_html  # ← Для подписей к фото
            else:
                text = None
            
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
        # === Продление подписок ===
        if input_type == "renewal_text":
            await self.handle_renewal_text_input(update, context, text)
        elif input_type == "renewal_button_text":
            await self.handle_renewal_button_text_input(update, context, text)
        elif input_type == "renewal_button_url":
            await self.handle_renewal_button_url_input(update, context, text)
        
        # Кнопки приветствия
        elif input_type == "add_welcome_button":
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
        
        # Платные рассылки
        elif input_type == "add_paid_message":
            await self.handle_add_paid_message(update, context, text)
        elif input_type == "add_paid_button":
            await self.handle_add_paid_button(update, context, text)
        elif input_type == "paid_mass_text":
            await self.handle_paid_mass_text_input(update, context, text)
        elif input_type == "paid_mass_photo":
            await self.handle_paid_mass_photo_input(update, context, text)
        elif input_type == "paid_mass_time":
            await self.handle_paid_mass_time_input(update, context, text)
        elif input_type == "paid_mass_button_text":
            await self.handle_paid_mass_button_text_input(update, context, text)
        elif input_type == "paid_mass_button_url":
            await self.handle_paid_mass_button_url_input(update, context, text)
        
        # Платные сообщения рассылки
        elif input_type == "paid_broadcast_text":
            await self.handle_paid_broadcast_text_input(update, context, text)
        elif input_type == "paid_broadcast_delay":
            await self.handle_paid_broadcast_delay_input(update, context, text)
        elif input_type == "paid_broadcast_photo":
            await self.handle_paid_broadcast_photo_input(update, context, text)

        # Видео для базовых типов
        elif input_type == "welcome_video":
            await self.handle_photo_url_input(update, context, text, "welcome_video")
        elif input_type == "goodbye_video":
            await self.handle_photo_url_input(update, context, text, "goodbye_video")
        elif input_type == "renewal_video":
            await self.handle_photo_url_input(update, context, text, "renewal_video")
        elif input_type == "broadcast_video":
            await self.handle_photo_url_input(update, context, text, "broadcast_video", **waiting_data)
        elif input_type == "paid_broadcast_video":
            await self.handle_photo_url_input(update, context, text, "paid_broadcast_video", **waiting_data)
        elif input_type == "payment_message_video":
            await self.handle_payment_message_input(update, context, text, "payment_message_video")

        # Видео для массовых рассылок
        elif input_type == "mass_video":
            await self.handle_mass_video_input(update, context, text)
        elif input_type == "paid_mass_video":
            await self.handle_paid_mass_video_input(update, context, text)

        # Остальные типы
        elif input_type == "broadcast_timer":
            await self.handle_broadcast_timer(update, context, text)
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

        elif input_type in ["broadcast_photo", "welcome_photo", "goodbye_photo", "payment_message_photo", "renewal_photo",
                             "broadcast_video", "welcome_video", "goodbye_video", "payment_message_video", "renewal_video"]:
            if text.startswith("http://") or text.startswith("https://"):
                await self.handle_photo_url_input(update, context, text, input_type, **waiting_data)
            else:
                media_type = "видео" if "video" in input_type else "фото"
                await update.message.reply_text(f"❌ Отправьте {media_type} или ссылку на {media_type} (начинающуюся с http:// или https://)")

        elif input_type == "edit_button_text":
            await self._handle_edit_button_text_input(update, context, text, waiting_data)
        
        elif input_type == "edit_button_url":
            await self._handle_edit_button_url_input(update, context, text, waiting_data)

        elif input_type == "edit_button_count":
            await self._handle_edit_button_count_input(update, context, text, waiting_data)

        else:
            return False
        
        return True
    
    async def handle_add_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления новой кнопки"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Сохраняем текст и запрашиваем URL
            self.waiting_for[user_id]["button_text"] = text
            self.waiting_for[user_id]["step"] = "url"
            await update.message.reply_text(
                f"✅ Текст кнопки: <b>{text}</b>\n\n"
                "🔗 Теперь отправьте URL для кнопки:\n\n"
                "💡 <b>Варианты:</b>\n"
                "• Отправьте ссылку (https://example.com) - создастся URL кнопка\n"
                "• Отправьте <code>-</code> или оставьте пустым - создастся кнопка следующего сообщения\n"
                "• Отправьте <code>skip</code> - кнопка будет переходить к следующему сообщению",
                parse_mode='HTML'
            )
        elif current_step == "url":
            # Обрабатываем URL или его отсутствие
            message_number = waiting_data["message_number"]
            button_text = waiting_data["button_text"]
            
            # Определяем тип кнопки
            if text.strip() in ["-", "skip", "нет", ""] or not text.startswith("http"):
                # Callback кнопка (следующее сообщение)
                self.db.add_message_button(message_number, button_text, "", 1)  # Пустой URL
                await update.message.reply_text(f"✅ Добавлена callback кнопка: <b>{button_text}</b>\n\n📩 При нажатии будет отправлено следующее сообщение.", parse_mode='HTML')
            else:
                # URL кнопка
                if not (text.startswith("http://") or text.startswith("https://")):
                    await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                    return
                
                self.db.add_message_button(message_number, button_text, text, 1)
                await update.message.reply_text(f"✅ Добавлена URL кнопка: <b>{button_text}</b> → {text}", parse_mode='HTML')
            
            del self.waiting_for[user_id]
            await self.show_message_buttons_from_context(update, context, message_number)
    
    # === Обработчики для платных сообщений рассылки ===
    
    async def handle_paid_broadcast_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста для платного сообщения рассылки"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        message_number = waiting_data["message_number"]
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        self.db.update_paid_broadcast_message(message_number, text=text)
        await update.message.reply_text(f"✅ Текст платного сообщения {message_number} обновлён!")
        del self.waiting_for[user_id]
        await self.show_paid_message_edit_from_context(update, context, message_number)
    
    async def handle_paid_broadcast_delay_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода задержки для платного сообщения рассылки"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        message_number = waiting_data["message_number"]
        
        delay_hours, delay_display = self.parse_delay_input(text)
        
        if delay_hours is not None and delay_hours >= 0:  # Разрешаем 0 для мгновенной отправки
            self.db.update_paid_broadcast_message(message_number, delay_hours=delay_hours)
            await update.message.reply_text(f"✅ Задержка для платного сообщения {message_number} установлена на {delay_display}!")
            del self.waiting_for[user_id]
            await self.show_paid_message_edit_from_context(update, context, message_number)
        else:
            await update.message.reply_text(
                "❌ Неверный формат! Примеры правильного ввода:\n\n"
                "• <code>3м</code> или <code>3 минуты</code>\n"
                "• <code>2ч</code> или <code>2 часа</code>\n"
                "• <code>1.5</code> (для 1.5 часов)\n"
                "• <code>0</code> (для мгновенной отправки)",
                parse_mode='HTML'
            )
    
    async def handle_paid_broadcast_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода фото для платного сообщения рассылки"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        message_number = waiting_data["message_number"]
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
            return
        
        self.db.update_paid_broadcast_message(message_number, photo_url=text)
        await update.message.reply_text(f"✅ Фото для платного сообщения {message_number} обновлено!")
        del self.waiting_for[user_id]
        await self.show_paid_message_edit_from_context(update, context, message_number)
    
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

    async def _handle_mass_remove_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление видео из массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["video_data"] = None
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
    
    async def _handle_paid_mass_delete_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление медиа-альбома из платной массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["media_album"] = None
            await update.callback_query.answer("✅ Медиа-альбом удален!")
            await self.show_paid_send_all_menu(update, context)
    
    async def _handle_mass_delete_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление медиа-альбома из массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["media_album"] = None
            await update.callback_query.answer("✅ Медиа-альбом удален!")
            await self.show_send_all_menu(update, context)
    
    async def _handle_paid_mass_remove_photo(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление фото из платной массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["photo_data"] = None
            await self.show_paid_send_all_menu(update, context)

    async def _handle_paid_mass_remove_video(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление видео из платной массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["video_data"] = None
            await self.show_paid_send_all_menu(update, context)

    async def _handle_paid_mass_remove_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Удаление последней кнопки из платной массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts and self.broadcast_drafts[user_id]["buttons"]:
            self.broadcast_drafts[user_id]["buttons"].pop()
            await self.show_paid_send_all_menu(update, context)

    async def _handle_paid_mass_send_now(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Установка немедленной отправки платной массовой рассылки"""
        user_id = update.effective_user.id
        if user_id in self.broadcast_drafts:
            self.broadcast_drafts[user_id]["scheduled_hours"] = None
            await self.show_paid_mass_broadcast_preview(update, context)
    
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

        self.db.update_message_button(button_id, button_url=text)
        await update.message.reply_text("✅ URL кнопки обновлен!")
        del self.waiting_for[user_id]
        await self.show_button_edit_from_context(update, context, button_id)

    async def _handle_edit_button_count_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                            text: str, waiting_data: dict):
        """Обработка изменения количества сообщений для кнопки"""
        user_id = update.effective_user.id
        button_id = waiting_data["button_id"]

        try:
            count = int(text.strip())
            if count < 1:
                await update.message.reply_text("❌ Количество должно быть не меньше 1")
                return
            if count > 50:  # Разумный лимит
                await update.message.reply_text("❌ Максимум 50 сообщений за раз")
                return

            self.db.update_message_button(button_id, messages_count=count)

            # Правильное склонение
            if count == 1:
                msg_text = "сообщение"
            elif 2 <= count <= 4:
                msg_text = "сообщения"
            else:
                msg_text = "сообщений"

            await update.message.reply_text(f"✅ Кнопка будет отправлять {count} {msg_text}")
            del self.waiting_for[user_id]
            await self.show_button_edit_from_context(update, context, button_id)

        except ValueError:
            await update.message.reply_text("❌ Введите целое число от 1 до 50")

    # Обработчики ввода для платных массовых рассылок
    async def handle_paid_mass_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста для платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now(),
                "is_paid_broadcast": True
            }
        
        self.broadcast_drafts[user_id]["message_text"] = text
        self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
        
        await update.message.reply_text("✅ Текст сообщения для оплативших сохранен!")
        del self.waiting_for[user_id]
        
        await self.show_paid_send_all_menu_from_context(update, context)

    async def handle_paid_mass_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода фото для платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
            return
        
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now(),
                "is_paid_broadcast": True
            }
        
        self.broadcast_drafts[user_id]["photo_data"] = text
        self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
        
        await update.message.reply_text("✅ Фото для рассылки оплативших добавлено!")
        del self.waiting_for[user_id]
        
        await self.show_paid_send_all_menu_from_context(update, context)

    async def handle_paid_mass_time_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода времени для платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        if not text.strip():
            self.broadcast_drafts[user_id]["scheduled_hours"] = None
            await update.message.reply_text("✅ Рассылка для оплативших будет отправлена сейчас!")
        else:
            try:
                hours = float(text.strip())
                if hours < 0:
                    await update.message.reply_text("❌ Количество часов не может быть отрицательным")
                    return
                
                self.broadcast_drafts[user_id]["scheduled_hours"] = hours
                if hours == 0:
                    await update.message.reply_text("✅ Рассылка для оплативших будет отправлена немедленно!")
                else:
                    scheduled_time = datetime.now() + timedelta(hours=hours)
                    await update.message.reply_text(f"✅ Рассылка для оплативших запланирована на {scheduled_time.strftime('%d.%m.%Y %H:%M')}!")
                
            except ValueError:
                await update.message.reply_text("❌ Введите корректное число часов")
                return
        
        del self.waiting_for[user_id]
        await self.show_paid_send_all_menu_from_context(update, context)

    async def handle_paid_mass_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста кнопки для платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        self.waiting_for[user_id]["button_text"] = text
        self.waiting_for[user_id]["type"] = "paid_mass_button_url"
        
        await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")

    async def handle_paid_mass_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода URL кнопки для платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return

        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        if len(self.broadcast_drafts[user_id]["buttons"]) >= 10:
            await update.message.reply_text("❌ Максимум 10 кнопок на сообщение.")
            return
        
        button_text = self.waiting_for[user_id]["button_text"]
        
        self.broadcast_drafts[user_id]["buttons"].append({
            "text": button_text,
            "url": text
        })
        
        await update.message.reply_text("✅ Кнопка для оплативших добавлена!")
        del self.waiting_for[user_id]
        
        await self.show_paid_send_all_menu_from_context(update, context)

    async def show_paid_send_all_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение меню платной массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now(),
                "is_paid_broadcast": True
            }
        
        self.broadcast_drafts[user_id]["is_paid_broadcast"] = True
        
        await self.show_paid_send_all_menu(update, context)

    async def show_message_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Показать управление кнопками сообщения из контекста обработки ввода"""
        try:
            # Отправляем новое сообщение вместо редактирования
            await self.show_message_buttons(update, context, message_number)
        except Exception as e:
            logger.error(f"❌ Ошибка при показе кнопок сообщения {message_number}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка. Попробуйте еще раз.")
    
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

        # Видео для основных сообщений
        elif data.startswith("edit_video_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_video", message_number=message_number)
        elif data.startswith("remove_video_"):
            message_number = int(data.split("_")[2])
            self.db.update_broadcast_message(message_number, video_url="")
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
            # Создаем пустое сообщение и сразу открываем меню редактирования
            new_number = self.db.add_broadcast_message()
            logger.info(f"✅ Создано новое пустое сообщение #{new_number}")
            await self.show_message_edit(update, context, new_number)
        elif data.startswith("add_button_"):
            message_number = int(data.split("_")[2])
            # Проверяем лимит кнопок
            existing_buttons = self.db.get_message_buttons(message_number)
            if len(existing_buttons) >= 3:
                await query.answer("❌ Максимум 3 кнопки на сообщение!", show_alert=True)
                return False
            await self.request_text_input(update, context, "add_button", message_number=message_number, step="text")
        else:
            return False  # Не обработано
        
        return True  # Обработано
