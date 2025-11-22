"""
Функциональность управления основными рассылками для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import logging
import asyncio

logger = logging.getLogger(__name__)


class BroadcastsMixin:
    """Миксин для работы с основными рассылками"""
    
    async def show_broadcast_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления рассылкой"""
        messages = self.db.get_all_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url, video_url in messages:
            # Получаем количество кнопок для сообщения
            buttons = self.db.get_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            
            # Проверяем наличие медиа-альбома
            album_stats = self.db.get_media_album_stats(msg_num)
            if album_stats['total'] > 0:
                album_icon = f"🎬{album_stats['total']}"
            else:
                photo_icon = "🖼" if photo_url else ""
                video_icon = "🎥" if video_url else ""
                album_icon = f"{photo_icon}{video_icon}"

            # Форматируем отображение времени
            delay_str = self.format_delay_display(delay_hours)

            button_text = f"{album_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после регистрации.\n"
            "🖼 - сообщение содержит фото\n"
            "🎥 - сообщение содержит видео\n"
            "🎬N - медиа-альбом (N файлов)\n"
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_broadcast_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение меню рассылки"""
        user_id = update.effective_user.id
        
        messages = self.db.get_all_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url, video_url in messages:
            buttons = self.db.get_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            
            # Проверяем наличие медиа-альбома
            album_stats = self.db.get_media_album_stats(msg_num)
            if album_stats['total'] > 0:
                album_icon = f"🎬{album_stats['total']}"
            else:
                photo_icon = "🖼" if photo_url else ""
                video_icon = "🎥" if video_url else ""
                album_icon = f"{photo_icon}{video_icon}"

            delay_str = self.format_delay_display(delay_hours)

            button_text = f"{album_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после регистрации.\n"
            "🖼 - сообщение содержит фото\n"
            "🎥 - сообщение содержит видео\n"
            "🎬N - медиа-альбом (N файлов)\n"
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_broadcast_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статус рассылки"""
        broadcast_status = self.db.get_broadcast_status()
        
        status_text = "✅ Включена" if broadcast_status['enabled'] else "❌ Отключена"
        
        text = f"🔄 <b>Статус рассылки</b>\n\n<b>Текущий статус:</b> {status_text}\n"
        
        if not broadcast_status['enabled'] and broadcast_status['auto_resume_time']:
            resume_time = datetime.fromisoformat(broadcast_status['auto_resume_time'])
            text += f"<b>Автовозобновление:</b> {resume_time.strftime('%d.%m.%Y %H:%M')}\n"
        
        keyboard = []
        
        if broadcast_status['enabled']:
            keyboard.append([InlineKeyboardButton("🔴 Отключить рассылку", callback_data="disable_broadcast")])
        else:
            keyboard.append([InlineKeyboardButton("🟢 Включить рассылку", callback_data="enable_broadcast")])
            keyboard.append([InlineKeyboardButton("⏰ Установить таймер", callback_data="set_broadcast_timer")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню редактирования конкретного сообщения"""
        msg_data = self.db.get_broadcast_message(message_number)
        if not msg_data:
            await update.callback_query.answer("Сообщение не найдено!", show_alert=True)
            return

        text, delay_hours, photo_url, video_url = msg_data
        buttons = self.db.get_message_buttons(message_number)
        
        # Проверяем наличие медиа-альбома
        album_stats = self.db.get_media_album_stats(message_number)
        has_album = album_stats['total'] > 0

        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_delay_{message_number}")]
        ]
        
        # Если есть медиа-альбом - показываем управление альбомом
        if has_album:
            keyboard.append([InlineKeyboardButton("🎬 Управление медиа-альбомом", callback_data=f"manage_album_{message_number}")])
        else:
            # Если альбома нет - показываем старые кнопки фото/видео
            keyboard.append([InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_photo_{message_number}")])
            keyboard.append([InlineKeyboardButton("🎥 Изменить видео", callback_data=f"edit_video_{message_number}")])
            
            if photo_url:
                keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_photo_{message_number}")])

            if video_url:
                keyboard.append([InlineKeyboardButton("❌ Удалить видео", callback_data=f"remove_video_{message_number}")])
            
            # Кнопка создания альбома
            keyboard.append([InlineKeyboardButton("🎬 Создать медиа-альбом", callback_data=f"create_album_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        delay_str = self.format_delay_display_full(delay_hours)
        
        # Информация о медиа
        if has_album:
            media_info = (
                f"<b>Медиа-альбом:</b> {album_stats['total']} файлов "
                f"({album_stats['photos']} фото, {album_stats['videos']} видео)\n"
            )
        else:
            media_info = (
                f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}\n"
                f"<b>Видео:</b> {'Есть' if video_url else 'Нет'}\n"
            )

        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после регистрации\n"
            f"{media_info}"
            f"{buttons_info}\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )

        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)

    async def show_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Отправить НОВОЕ сообщение для редактирования"""
        user_id = update.effective_user.id

        msg_data = self.db.get_broadcast_message(message_number)
        if not msg_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Сообщение не найдено!")
            return

        text, delay_hours, photo_url, video_url = msg_data
        buttons = self.db.get_message_buttons(message_number)
        
        # Проверяем наличие медиа-альбома
        album_stats = self.db.get_media_album_stats(message_number)
        has_album = album_stats['total'] > 0

        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_delay_{message_number}")]
        ]
        
        # Если есть медиа-альбом - показываем управление альбомом
        if has_album:
            keyboard.append([InlineKeyboardButton("🎬 Управление медиа-альбомом", callback_data=f"manage_album_{message_number}")])
        else:
            # Если альбома нет - показываем старые кнопки фото/видео
            keyboard.append([InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_photo_{message_number}")])
            keyboard.append([InlineKeyboardButton("🎥 Изменить видео", callback_data=f"edit_video_{message_number}")])
            
            if photo_url:
                keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_photo_{message_number}")])

            if video_url:
                keyboard.append([InlineKeyboardButton("❌ Удалить видео", callback_data=f"remove_video_{message_number}")])
            
            # Кнопка создания альбома
            keyboard.append([InlineKeyboardButton("🎬 Создать медиа-альбом", callback_data=f"create_album_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        delay_str = self.format_delay_display_full(delay_hours)
        
        # Информация о медиа
        if has_album:
            media_info = (
                f"<b>Медиа-альбом:</b> {album_stats['total']} файлов "
                f"({album_stats['photos']} фото, {album_stats['videos']} видео)\n"
            )
        else:
            media_info = (
                f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}\n"
                f"<b>Видео:</b> {'Есть' if video_url else 'Нет'}\n"
            )

        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после регистрации\n"
            f"{media_info}"
            f"{buttons_info}\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )

        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_album_management_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню управления медиа-альбомом"""
        album_media = self.db.get_message_media_album(message_number)
        
        if not album_media:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        album_stats = self.db.get_media_album_stats(message_number)
        
        text = (
            f"🎬 <b>Медиа-альбом сообщения {message_number}</b>\n\n"
            f"📊 <b>Всего файлов:</b> {album_stats['total']}\n"
            f"🖼 <b>Фото:</b> {album_stats['photos']}\n"
            f"🎥 <b>Видео:</b> {album_stats['videos']}\n\n"
            f"<b>Список медиа:</b>\n"
        )
        
        for i, (media_id, media_type, media_url, position) in enumerate(album_media, 1):
            icon = "🖼" if media_type == 'photo' else "🎥"
            text += f"{i}. {icon} {media_type.capitalize()}\n"
        
        keyboard = [
            [InlineKeyboardButton("👁 Предпросмотр альбома", callback_data=f"preview_saved_album_{message_number}")],
            [InlineKeyboardButton("🔄 Пересоздать альбом", callback_data=f"recreate_album_{message_number}")],
            [InlineKeyboardButton("🗑 Удалить альбом", callback_data=f"delete_album_{message_number}")],
            [InlineKeyboardButton("« Назад", callback_data=f"edit_msg_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_saved_album_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать предпросмотр сохраненного медиа-альбома"""
        user_id = update.effective_user.id
        album_media = self.db.get_message_media_album(message_number)
        
        if not album_media:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        try:
            from telegram import InputMediaPhoto, InputMediaVideo
            
            # Получаем текст сообщения
            msg_data = self.db.get_broadcast_message(message_number)
            message_text = msg_data[0] if msg_data else "Предпросмотр медиа-альбома"
            
            media_group = []
            for i, (media_id, media_type, media_url, position) in enumerate(album_media):
                caption = f"📸 {message_text}" if i == 0 else None
                
                if media_type == 'photo':
                    media_group.append(InputMediaPhoto(media=media_url, caption=caption, parse_mode='HTML'))
                else:
                    media_group.append(InputMediaVideo(media=media_url, caption=caption, parse_mode='HTML'))
            
            await context.bot.send_media_group(
                chat_id=user_id,
                media=media_group
            )
            
            await update.callback_query.answer("✅ Предпросмотр отправлен!")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке предпросмотра сохраненного альбома: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке предпросмотра!", show_alert=True)

    async def show_scheduled_broadcasts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать запланированные рассылки"""
        broadcasts = self.db.get_scheduled_broadcasts(include_sent=False)
        
        keyboard = []
        
        if broadcasts:
            for broadcast_id, message_text, photo_url, video_url, scheduled_time, is_sent, created_at in broadcasts:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                time_str = scheduled_dt.strftime("%d.%m %H:%M")
                
                # Получаем количество кнопок
                buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
                button_icon = f"🔘{len(buttons)}" if buttons else ""
                
                # Проверяем медиа-альбом
                album_stats = self.db.get_scheduled_broadcast_media_stats(broadcast_id)
                if album_stats['total'] > 0:
                    media_icon = f"🎬{album_stats['total']}"
                else:
                    media_icon = "🖼" if photo_url else ""
                
                short_text = message_text[:20] + "..." if len(message_text) > 20 else message_text
                button_display = f"{media_icon}{button_icon} {time_str}: {short_text}"
                keyboard.append([InlineKeyboardButton(button_display, callback_data=f"edit_scheduled_broadcast_{broadcast_id}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "⏰ <b>Запланированные рассылки</b>\n\n"
            f"Активных рассылок: {len(broadcasts)}\n\n"
            "🖼 - сообщение с фото\n"
            "🎬N - медиа-альбом (N файлов)\n"
            "🔘N - количество кнопок\n\n"
            "💡 <i>Все ссылки получат UTM метки для отслеживания.</i>\n\n"
            "Выберите рассылку для редактирования:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # === ОБРАБОТЧИКИ ===
    
    async def handle_broadcast_timer(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка установки таймера рассылки"""
        user_id = update.effective_user.id
        
        try:
            hours = float(text)
            if hours < 1:
                raise ValueError("Время должно быть больше 0")
            
            resume_time = datetime.now() + timedelta(hours=hours)
            self.db.set_broadcast_status(False, resume_time.isoformat())
            
            await update.message.reply_text(
                f"✅ Рассылка отключена на {hours} часов. Автовозобновление: {resume_time.strftime('%d.%m.%Y %H:%M')}"
            )
            
            del self.waiting_for[user_id]
            await asyncio.sleep(2)
            await self.show_main_menu_safe(update, context)
            
        except ValueError:
            await update.message.reply_text("❌ Пожалуйста, введите корректное число часов (больше 0)")
    
    async def handle_add_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления нового сообщения"""
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
                "⏰ Теперь отправьте задержку:\n\n"
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
            
            if delay_hours is not None and delay_hours > 0:
                # Добавляем сообщение
                message_text = waiting_data["text"]
                new_number = self.db.add_broadcast_message(message_text, delay_hours)
                
                await update.message.reply_text(f"✅ Сообщение {new_number} добавлено с задержкой {delay_display}!")
                del self.waiting_for[user_id]
                await self.show_broadcast_menu_from_context(update, context)
            else:
                await update.message.reply_text(
                    "❌ Неверный формат! Примеры правильного ввода:\n\n"
                    "• <code>3м</code> или <code>3 минуты</code>\n"
                    "• <code>2ч</code> или <code>2 часа</code>\n"
                    "• <code>1.5</code> (для 1.5 часов)\n"
                    "• <code>0.05</code> (для 3 минут)",
                    parse_mode='HTML'
                )
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить сообщение заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    # === ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ===
    
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
        elif data.startswith("edit_video_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_video", message_number=message_number)
        elif data.startswith("remove_video_"):
            message_number = int(data.split("_")[2])
            self.db.update_broadcast_message(message_number, video_url="")
            await self.show_message_edit(update, context, message_number)
        
        # Медиа-альбомы
        elif data.startswith("create_album_"):
            message_number = int(data.split("_")[2])
            await self.show_create_media_album_menu(update, context, message_number)
        elif data.startswith("manage_album_"):
            message_number = int(data.split("_")[2])
            await self.show_album_management_menu(update, context, message_number)
        elif data.startswith("preview_saved_album_"):
            message_number = int(data.split("_")[3])
            await self.show_saved_album_preview(update, context, message_number)
        elif data.startswith("recreate_album_"):
            message_number = int(data.split("_")[2])
            # Удаляем старый альбом и открываем меню создания
            self.db.delete_message_media_album(message_number)
            await self.show_create_media_album_menu(update, context, message_number)
        elif data.startswith("delete_album_"):
            message_number = int(data.split("_")[2])
            await self.delete_saved_media_album(update, context, message_number)
        elif data.startswith("preview_album_"):
            message_number = int(data.split("_")[2])
            await self.show_album_preview(update, context, message_number)
        elif data.startswith("save_album_"):
            message_number = int(data.split("_")[2])
            await self.save_media_album(update, context, message_number)
        elif data.startswith("clear_album_"):
            message_number = int(data.split("_")[2])
            await self.clear_media_album_draft(update, context, message_number)
        
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
            # Удаляем и медиа-альбом если есть
            self.db.delete_message_media_album(message_number)
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
            
            # Инициализируем ожидание ввода кнопки
            user_id = update.callback_query.from_user.id
            self.waiting_for[user_id] = {
                "type": "add_button", 
                "message_number": message_number,
                "step": "text",
                "created_at": datetime.now()
            }
            
            await self.safe_edit_or_send_message(
                update, context,
                "✏️ Отправьте текст для кнопки:",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data=f"manage_buttons_{message_number}")]])
            )
        else:
            return False  # Не обработано
        
        return True  # Обработано
