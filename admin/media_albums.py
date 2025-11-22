"""
Функциональность управления медиа-альбомами для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MediaAlbumsMixin:
    """Миксин для работы с медиа-альбомами в админке"""
    
    # === ОСНОВНЫЕ СООБЩЕНИЯ РАССЫЛКИ ===
    
    async def show_create_media_album_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Показать меню создания медиа-альбома для сообщения"""
        user_id = update.effective_user.id
        
        # Инициализируем временное хранилище для медиа
        if user_id not in self.media_album_drafts:
            self.media_album_drafts[user_id] = {
                "message_number": message_number,
                "media_list": [],  # [(media_type, media_url), ...]
                "created_at": datetime.now()
            }
        
        draft = self.media_album_drafts[user_id]
        media_count = len(draft["media_list"])
        
        # Статистика
        photo_count = sum(1 for m in draft["media_list"] if m[0] == 'photo')
        video_count = sum(1 for m in draft["media_list"] if m[0] == 'video')
        
        text = (
            f"🎬 <b>Создание медиа-альбома</b>\n"
            f"Сообщение #{message_number}\n\n"
            f"📊 <b>Текущий альбом:</b> {media_count}/10 файлов\n"
            f"🖼 Фото: {photo_count}\n"
            f"🎥 Видео: {video_count}\n\n"
        )
        
        if media_count == 0:
            text += (
                "📸 <b>Отправьте файлы для альбома:</b>\n\n"
                "• Загрузите фото/видео напрямую в бота\n"
                "• Или отправьте ссылки (по одной на строку)\n\n"
                "💡 <i>Можно миксовать фото и видео (до 10 файлов)</i>"
            )
        else:
            text += "✅ <b>Медиа добавлены!</b>\n\nДобавьте еще или сохраните альбом."
        
        keyboard = []
        
        if media_count > 0:
            keyboard.append([InlineKeyboardButton("👁 Показать предпросмотр", callback_data=f"preview_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("✅ Сохранить альбом", callback_data=f"save_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("🗑 Очистить всё", callback_data=f"clear_album_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"edit_msg_{message_number}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_manage_media_album_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Показать меню управления существующим медиа-альбомом"""
        try:
            media_album = self.db.get_message_media_album(message_number)
            
            # Считаем статистику вручную
            total = len(media_album)
            photos = sum(1 for item in media_album if item[1] == 'photo')
            videos = sum(1 for item in media_album if item[1] == 'video')
            
            stats = {'total': total, 'photos': photos, 'videos': videos}
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке альбома: {e}")
            stats = {'total': 0, 'photos': 0, 'videos': 0}
        
        text = (
            f"🎬 <b>Управление медиа-альбомом</b>\n"
            f"Сообщение #{message_number}\n\n"
            f"📊 <b>Текущий альбом:</b> {stats['total']} файлов\n"
            f"🖼 Фото: {stats['photos']}\n"
            f"🎥 Видео: {stats['videos']}\n\n"
        )
        
        if stats['total'] == 0:
            text += "ℹ️ Альбом пустой. Создайте новый альбом."
        else:
            text += "✅ Альбом сохранен. Вы можете просмотреть, пересоздать или удалить его."
        
        keyboard = []
        
        if stats['total'] > 0:
            keyboard.append([InlineKeyboardButton("👁 Показать предпросмотр", callback_data=f"preview_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("🔄 Пересоздать", callback_data=f"create_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("🗑 Удалить альбом", callback_data=f"delete_album_{message_number}")])
        else:
            keyboard.append([InlineKeyboardButton("➕ Создать альбом", callback_data=f"create_album_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("❌ Назад", callback_data=f"edit_msg_{message_number}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_media_album_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """
        Показать предпросмотр медиа-альбома (alias для show_album_preview)
        Этот метод вызывается из handlers.py
        """
        await self.show_album_preview(update, context, message_number)
    
    async def handle_media_album_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка загруженных медиа или URL для альбома"""
        user_id = update.effective_user.id
        
        if user_id not in self.media_album_drafts:
            return
        
        draft = self.media_album_drafts[user_id]
        message_number = draft["message_number"]
        
        # Проверяем лимит
        if len(draft["media_list"]) >= 10:
            await update.message.reply_text("❌ Достигнут лимит в 10 файлов!")
            return
        
        media_added = []
        
        # Обработка фото
        if update.message.photo:
            photo = update.message.photo[-1]
            file_id = photo.file_id
            draft["media_list"].append(('photo', file_id))
            media_added.append("🖼 Фото")
            logger.info(f"Добавлено фото в черновик альбома для сообщения {message_number}")
        
        # Обработка видео
        elif update.message.video:
            video = update.message.video
            file_id = video.file_id
            draft["media_list"].append(('video', file_id))
            media_added.append("🎥 Видео")
            logger.info(f"Добавлено видео в черновик альбома для сообщения {message_number}")
        
        # Обработка группы медиа
        elif update.message.media_group_id:
            if update.message.photo:
                photo = update.message.photo[-1]
                draft["media_list"].append(('photo', photo.file_id))
                media_added.append("🖼 Фото")
            elif update.message.video:
                draft["media_list"].append(('video', update.message.video.file_id))
                media_added.append("🎥 Видео")
        
        # Обработка текста с URL
        elif update.message.text:
            text = update.message.text.strip()
            lines = text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if len(draft["media_list"]) >= 10:
                    await update.message.reply_text("❌ Достигнут лимит в 10 файлов!")
                    break
                
                if line.startswith('http://') or line.startswith('https://'):
                    lower_url = line.lower()
                    if any(ext in lower_url for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                        draft["media_list"].append(('photo', line))
                        media_added.append("🖼 Фото (URL)")
                    elif any(ext in lower_url for ext in ['.mp4', '.mov', '.avi', '.mkv']):
                        draft["media_list"].append(('video', line))
                        media_added.append("🎥 Видео (URL)")
                    else:
                        draft["media_list"].append(('photo', line))
                        media_added.append("🖼 Фото (URL)")
        
        if media_added:
            status = f"✅ Добавлено: {', '.join(media_added)}\n\n"
            status += f"📊 Всего в альбоме: {len(draft['media_list'])}/10"
            await update.message.reply_text(status)
            
            await self.show_create_media_album_menu_from_context(update, context, message_number)
    
    async def show_album_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Показать предпросмотр медиа-альбома"""
        user_id = update.effective_user.id
        
        # Проверяем сначала черновик, потом сохраненный альбом
        if user_id in self.media_album_drafts:
            draft = self.media_album_drafts[user_id]
            media_list = draft["media_list"]
            source = "черновика"
        else:
            media_album = self.db.get_message_media_album(message_number)
            media_list = [(media_type, media_url) for _, media_type, media_url, _ in media_album]
            source = "базы данных"
        
        if not media_list:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        try:
            preview_text = f"👁 <b>Предпросмотр альбома ({len(media_list)} файлов)</b>\n\n"
            for i, (media_type, media_url) in enumerate(media_list, 1):
                icon = "🖼" if media_type == 'photo' else "🎥"
                preview_text += f"{i}. {icon} {media_type.capitalize()}\n"
            
            await context.bot.send_message(
                chat_id=user_id,
                text=preview_text,
                parse_mode='HTML'
            )
            
            media_group = []
            for i, (media_type, media_url) in enumerate(media_list):
                caption = f"📸 Предпросмотр альбома (сообщение #{message_number})" if i == 0 else None
                
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
            logger.error(f"❌ Ошибка при отправке предпросмотра альбома: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке предпросмотра!", show_alert=True)
    
    async def save_media_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Сохранение медиа-альбома в базу данных"""
        user_id = update.effective_user.id
        
        if user_id not in self.media_album_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.media_album_drafts[user_id]
        media_list = draft["media_list"]
        
        if not media_list:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        try:
            self.db.delete_message_media_album(message_number)
            
            for position, (media_type, media_url) in enumerate(media_list, 1):
                self.db.add_media_to_album(message_number, media_type, media_url, position)
            
            del self.media_album_drafts[user_id]
            
            await update.callback_query.answer("✅ Медиа-альбом сохранен!")
            await self.show_message_edit(update, context, message_number)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при сохранении медиа-альбома: {e}")
            await update.callback_query.answer("❌ Ошибка при сохранении!", show_alert=True)
    
    async def clear_media_album_draft(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Очистить черновик медиа-альбома"""
        user_id = update.effective_user.id
        
        if user_id in self.media_album_drafts:
            self.media_album_drafts[user_id] = {
                "message_number": message_number,
                "media_list": [],
                "created_at": datetime.now()
            }
        
        await update.callback_query.answer("✅ Альбом очищен!")
        await self.show_create_media_album_menu(update, context, message_number)
    
    async def delete_saved_media_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Удалить сохраненный медиа-альбом из базы"""
        deleted_count = self.db.delete_message_media_album(message_number)
        
        if deleted_count > 0:
            await update.callback_query.answer(f"✅ Удалено {deleted_count} файлов из альбома!")
        else:
            await update.callback_query.answer("ℹ️ Альбом уже пуст")
        
        await self.show_message_edit(update, context, message_number)
    
    async def show_create_media_album_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Отправить НОВОЕ сообщение меню создания медиа-альбома"""
        user_id = update.effective_user.id
        
        if user_id not in self.media_album_drafts:
            self.media_album_drafts[user_id] = {
                "message_number": message_number,
                "media_list": [],
                "created_at": datetime.now()
            }
        
        draft = self.media_album_drafts[user_id]
        media_count = len(draft["media_list"])
        
        photo_count = sum(1 for m in draft["media_list"] if m[0] == 'photo')
        video_count = sum(1 for m in draft["media_list"] if m[0] == 'video')
        
        text = (
            f"🎬 <b>Создание медиа-альбома</b>\n"
            f"Сообщение #{message_number}\n\n"
            f"📊 <b>Текущий альбом:</b> {media_count}/10 файлов\n"
            f"🖼 Фото: {photo_count}\n"
            f"🎥 Видео: {video_count}\n\n"
        )
        
        if media_count == 0:
            text += (
                "📸 <b>Отправьте файлы для альбома:</b>\n\n"
                "• Загрузите фото/видео напрямую в бота\n"
                "• Или отправьте ссылки (по одной на строку)\n\n"
                "💡 <i>Можно миксовать фото и видео (до 10 файлов)</i>"
            )
        else:
            text += "✅ <b>Медиа добавлены!</b>\n\nДобавьте еще или сохраните альбом."
        
        keyboard = []
        
        if media_count > 0:
            keyboard.append([InlineKeyboardButton("👁 Показать предпросмотр", callback_data=f"preview_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("✅ Сохранить альбом", callback_data=f"save_album_{message_number}")])
            keyboard.append([InlineKeyboardButton("🗑 Очистить всё", callback_data=f"clear_album_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data=f"edit_msg_{message_number}")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    # === МАССОВЫЕ РАССЫЛКИ ===
    
    async def show_create_mass_media_album_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню создания медиа-альбома для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.mass_media_album_drafts:
            self.mass_media_album_drafts[user_id] = {
                "media_list": [],
                "created_at": datetime.now()
            }
        
        draft = self.mass_media_album_drafts[user_id]
        media_count = len(draft["media_list"])
        
        photo_count = sum(1 for m in draft["media_list"] if m[0] == 'photo')
        video_count = sum(1 for m in draft["media_list"] if m[0] == 'video')
        
        text = (
            f"🎬 <b>Создание медиа-альбома для рассылки</b>\n\n"
            f"📊 <b>Текущий альбом:</b> {media_count}/10 файлов\n"
            f"🖼 Фото: {photo_count}\n"
            f"🎥 Видео: {video_count}\n\n"
        )
        
        if media_count == 0:
            text += (
                "📸 <b>Отправьте файлы для альбома:</b>\n\n"
                "• Загрузите фото/видео напрямую в бота\n"
                "• Или отправьте ссылки (по одной на строку)\n\n"
                "💡 <i>Можно миксовать фото и видео (до 10 файлов)</i>"
            )
        else:
            text += "✅ <b>Медиа добавлены!</b>\n\nДобавьте еще или сохраните альбом."
        
        keyboard = []
        
        if media_count > 0:
            keyboard.append([InlineKeyboardButton("👁 Показать предпросмотр", callback_data="preview_mass_album")])
            keyboard.append([InlineKeyboardButton("✅ Сохранить альбом", callback_data="save_mass_album")])
            keyboard.append([InlineKeyboardButton("🗑 Очистить всё", callback_data="clear_mass_album")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_send_all")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def handle_mass_media_album_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка загруженных медиа или URL для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.mass_media_album_drafts:
            return
        
        draft = self.mass_media_album_drafts[user_id]
        
        if len(draft["media_list"]) >= 10:
            await update.message.reply_text("❌ Достигнут лимит в 10 файлов!")
            return
        
        media_added = []
        
        if update.message.photo:
            photo = update.message.photo[-1]
            file_id = photo.file_id
            draft["media_list"].append(('photo', file_id))
            media_added.append("🖼 Фото")
        
        elif update.message.video:
            video = update.message.video
            file_id = video.file_id
            draft["media_list"].append(('video', file_id))
            media_added.append("🎥 Видео")
        
        elif update.message.media_group_id:
            if update.message.photo:
                photo = update.message.photo[-1]
                draft["media_list"].append(('photo', photo.file_id))
                media_added.append("🖼 Фото")
            elif update.message.video:
                draft["media_list"].append(('video', update.message.video.file_id))
                media_added.append("🎥 Видео")
        
        elif update.message.text:
            text = update.message.text.strip()
            lines = text.split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                if len(draft["media_list"]) >= 10:
                    await update.message.reply_text("❌ Достигнут лимит в 10 файлов!")
                    break
                
                if line.startswith('http://') or line.startswith('https://'):
                    lower_url = line.lower()
                    if any(ext in lower_url for ext in ['.jpg', '.jpeg', '.png', '.gif', '.webp']):
                        draft["media_list"].append(('photo', line))
                        media_added.append("🖼 Фото (URL)")
                    elif any(ext in lower_url for ext in ['.mp4', '.mov', '.avi', '.mkv']):
                        draft["media_list"].append(('video', line))
                        media_added.append("🎥 Видео (URL)")
                    else:
                        draft["media_list"].append(('photo', line))
                        media_added.append("🖼 Фото (URL)")
        
        if media_added:
            status = f"✅ Добавлено: {', '.join(media_added)}\n\n"
            status += f"📊 Всего в альбоме: {len(draft['media_list'])}/10"
            await update.message.reply_text(status)
            
            await self.show_create_mass_media_album_menu_from_context(update, context)
    
    async def show_mass_album_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предпросмотр медиа-альбома для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.mass_media_album_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.mass_media_album_drafts[user_id]
        media_list = draft["media_list"]
        
        if not media_list:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        try:
            preview_text = f"👁 <b>Предпросмотр альбома ({len(media_list)} файлов)</b>\n\n"
            for i, (media_type, media_url) in enumerate(media_list, 1):
                icon = "🖼" if media_type == 'photo' else "🎥"
                preview_text += f"{i}. {icon} {media_type.capitalize()}\n"
            
            await context.bot.send_message(
                chat_id=user_id,
                text=preview_text,
                parse_mode='HTML'
            )
            
            media_group = []
            for i, (media_type, media_url) in enumerate(media_list):
                caption = "📸 Предпросмотр альбома рассылки" if i == 0 else None
                
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
            logger.error(f"❌ Ошибка при отправке предпросмотра альбома: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке предпросмотра!", show_alert=True)
    
    async def save_mass_media_album(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Сохранение медиа-альбома для массовой рассылки в черновик"""
        user_id = update.effective_user.id
        
        if user_id not in self.mass_media_album_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.mass_media_album_drafts[user_id]
        media_list = draft["media_list"]
        
        if not media_list:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "video_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        self.broadcast_drafts[user_id]["media_album"] = media_list.copy()
        
        del self.mass_media_album_drafts[user_id]
        
        await update.callback_query.answer("✅ Медиа-альбом сохранен!")
        await self.show_send_all_menu(update, context)
    
    async def clear_mass_media_album_draft(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Очистить черновик медиа-альбома для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id in self.mass_media_album_drafts:
            self.mass_media_album_drafts[user_id] = {
                "media_list": [],
                "created_at": datetime.now()
            }
        
        await update.callback_query.answer("✅ Альбом очищен!")
        await self.show_create_mass_media_album_menu(update, context)
    
    async def show_create_mass_media_album_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение меню создания медиа-альбома для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.mass_media_album_drafts:
            self.mass_media_album_drafts[user_id] = {
                "media_list": [],
                "created_at": datetime.now()
            }
        
        draft = self.mass_media_album_drafts[user_id]
        media_count = len(draft["media_list"])
        
        photo_count = sum(1 for m in draft["media_list"] if m[0] == 'photo')
        video_count = sum(1 for m in draft["media_list"] if m[0] == 'video')
        
        text = (
            f"🎬 <b>Создание медиа-альбома для рассылки</b>\n\n"
            f"📊 <b>Текущий альбом:</b> {media_count}/10 файлов\n"
            f"🖼 Фото: {photo_count}\n"
            f"🎥 Видео: {video_count}\n\n"
        )
        
        if media_count == 0:
            text += (
                "📸 <b>Отправьте файлы для альбома:</b>\n\n"
                "• Загрузите фото/видео напрямую в бота\n"
                "• Или отправьте ссылки (по одной на строку)\n\n"
                "💡 <i>Можно миксовать фото и видео (до 10 файлов)</i>"
            )
        else:
            text += "✅ <b>Медиа добавлены!</b>\n\nДобавьте еще или сохраните альбом."
        
        keyboard = []
        
        if media_count > 0:
            keyboard.append([InlineKeyboardButton("👁 Показать предпросмотр", callback_data="preview_mass_album")])
            keyboard.append([InlineKeyboardButton("✅ Сохранить альбом", callback_data="save_mass_album")])
            keyboard.append([InlineKeyboardButton("🗑 Очистить всё", callback_data="clear_mass_album")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_send_all")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
