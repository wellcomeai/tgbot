"""
Функциональность массовых рассылок для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import logging
import asyncio
import utm_utils

logger = logging.getLogger(__name__)


class MassBroadcastsMixin:
    """Миксин для работы с массовыми рассылками"""
    
    async def show_send_all_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню массовой рассылки с отдельными пунктами"""
        user_id = update.effective_user.id

        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "video_data": None,
                "media_album": None,  # 🎬 НОВОЕ: медиа-альбом
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        draft = self.broadcast_drafts[user_id]
        
        # Формируем информацию о текущем состоянии
        text = "📢 <b>Массовая рассылка</b>\n\n"
        
        # Текст
        if draft["message_text"]:
            text += f"📝 <b>Текст:</b> {draft['message_text'][:50]}{'...' if len(draft['message_text']) > 50 else ''}\n"
        else:
            text += "📝 <b>Текст:</b> <i>Не задан</i>\n"
        
        # 🎬 НОВОЕ: Проверяем медиа-альбом
        media_album = draft.get("media_album")
        if media_album and len(media_album) > 0:
            photo_count = sum(1 for m in media_album if m[0] == 'photo')
            video_count = sum(1 for m in media_album if m[0] == 'video')
            text += f"🎬 <b>Медиа-альбом:</b> {len(media_album)} файлов ({photo_count} фото, {video_count} видео)\n"
        else:
            # Старый формат: фото и видео
            if draft["photo_data"]:
                text += "🖼 <b>Фото:</b> Есть\n"
            else:
                text += "🖼 <b>Фото:</b> Нет\n"

            if draft.get("video_data"):
                text += "🎥 <b>Видео:</b> Есть\n"
            else:
                text += "🎥 <b>Видео:</b> Нет\n"

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
        
        # Получаем количество пользователей, завершивших воронку
        users_count = len(self.db.get_users_completed_funnel())
        text += f"\n👥 <b>Получателей:</b> {users_count} пользователей (завершивших воронку)\n"
        text += "\n💡 <i>Все ссылки автоматически получат UTM метки для отслеживания.</i>\n"
        
        text += "\n<b>Выберите действие:</b>"
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="mass_edit_text")],
        ]
        
        # 🎬 НОВОЕ: Кнопки управления медиа
        if media_album and len(media_album) > 0:
            # Если есть альбом - показываем только управление альбомом
            keyboard.append([InlineKeyboardButton("🎬 Управление медиа-альбомом", callback_data="mass_manage_album")])
        else:
            # Если альбома нет - показываем старые кнопки + создание альбома
            keyboard.append([InlineKeyboardButton("🖼 Добавить фото", callback_data="mass_add_photo")])
            keyboard.append([InlineKeyboardButton("🎥 Добавить видео", callback_data="mass_add_video")])
            keyboard.append([InlineKeyboardButton("🎬 Создать медиа-альбом", callback_data="mass_create_album")])
            
            # Кнопка удаления фото (если есть)
            if draft["photo_data"]:
                keyboard.append([InlineKeyboardButton("🗑 Удалить фото", callback_data="mass_remove_photo")])

            # Кнопка удаления видео (если есть)
            if draft.get("video_data"):
                keyboard.append([InlineKeyboardButton("🗑 Удалить видео", callback_data="mass_remove_video")])
        
        keyboard.append([InlineKeyboardButton("⏰ Время отправки", callback_data="mass_set_time")])
        keyboard.append([InlineKeyboardButton("🔘 Добавить кнопку", callback_data="mass_add_button")])
        
        # Кнопка удаления последней кнопки (если есть)
        if draft["buttons"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="mass_remove_button")])
        
        keyboard.append([InlineKeyboardButton("📋 Предпросмотр", callback_data="mass_preview")])
        
        # Кнопка отправки (только если есть текст)
        if draft["message_text"]:
            keyboard.append([InlineKeyboardButton("🚀 Отправить сейчас", callback_data="mass_send_now")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_send_all_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение вместо редактирования"""
        user_id = update.effective_user.id
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "video_data": None,
                "media_album": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }

        draft = self.broadcast_drafts[user_id]
        
        # Формируем информацию о текущем состоянии
        text = "📢 <b>Массовая рассылка</b>\n\n"
        
        # Текст
        if draft["message_text"]:
            text += f"📝 <b>Текст:</b> {draft['message_text'][:50]}{'...' if len(draft['message_text']) > 50 else ''}\n"
        else:
            text += "📝 <b>Текст:</b> <i>Не задан</i>\n"
        
        # 🎬 Медиа-альбом
        media_album = draft.get("media_album")
        if media_album and len(media_album) > 0:
            photo_count = sum(1 for m in media_album if m[0] == 'photo')
            video_count = sum(1 for m in media_album if m[0] == 'video')
            text += f"🎬 <b>Медиа-альбом:</b> {len(media_album)} файлов ({photo_count} фото, {video_count} видео)\n"
        else:
            if draft["photo_data"]:
                text += "🖼 <b>Фото:</b> Есть\n"
            else:
                text += "🖼 <b>Фото:</b> Нет\n"

            if draft.get("video_data"):
                text += "🎥 <b>Видео:</b> Есть\n"
            else:
                text += "🎥 <b>Видео:</b> Нет\n"

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
        
        # Получаем количество пользователей, завершивших воронку
        users_count = len(self.db.get_users_completed_funnel())
        text += f"\n👥 <b>Получателей:</b> {users_count} пользователей (завершивших воронку)\n"
        text += "\n💡 <i>Все ссылки автоматически получат UTM метки для отслеживания.</i>\n"
        
        text += "\n<b>Выберите действие:</b>"
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="mass_edit_text")],
        ]
        
        # Кнопки управления медиа
        if media_album and len(media_album) > 0:
            keyboard.append([InlineKeyboardButton("🎬 Управление медиа-альбомом", callback_data="mass_manage_album")])
        else:
            keyboard.append([InlineKeyboardButton("🖼 Добавить фото", callback_data="mass_add_photo")])
            keyboard.append([InlineKeyboardButton("🎥 Добавить видео", callback_data="mass_add_video")])
            keyboard.append([InlineKeyboardButton("🎬 Создать медиа-альбом", callback_data="mass_create_album")])
            
            if draft["photo_data"]:
                keyboard.append([InlineKeyboardButton("🗑 Удалить фото", callback_data="mass_remove_photo")])

            if draft.get("video_data"):
                keyboard.append([InlineKeyboardButton("🗑 Удалить видео", callback_data="mass_remove_video")])
        
        keyboard.append([InlineKeyboardButton("⏰ Время отправки", callback_data="mass_set_time")])
        keyboard.append([InlineKeyboardButton("🔘 Добавить кнопку", callback_data="mass_add_button")])
        
        if draft["buttons"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="mass_remove_button")])
        
        keyboard.append([InlineKeyboardButton("📋 Предпросмотр", callback_data="mass_preview")])
        
        if draft["message_text"]:
            keyboard.append([InlineKeyboardButton("🚀 Отправить сейчас", callback_data="mass_send_now")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_mass_album_management_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления медиа-альбомом массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.broadcast_drafts[user_id]
        media_album = draft.get("media_album", [])
        
        if not media_album or len(media_album) == 0:
            await update.callback_query.answer("❌ Альбом пустой!", show_alert=True)
            return
        
        photo_count = sum(1 for m in media_album if m[0] == 'photo')
        video_count = sum(1 for m in media_album if m[0] == 'video')
        
        text = (
            f"🎬 <b>Медиа-альбом массовой рассылки</b>\n\n"
            f"📊 <b>Всего файлов:</b> {len(media_album)}\n"
            f"🖼 <b>Фото:</b> {photo_count}\n"
            f"🎥 <b>Видео:</b> {video_count}\n\n"
            f"<b>Список медиа:</b>\n"
        )
        
        for i, (media_type, media_url) in enumerate(media_album, 1):
            icon = "🖼" if media_type == 'photo' else "🎥"
            text += f"{i}. {icon} {media_type.capitalize()}\n"
        
        keyboard = [
            [InlineKeyboardButton("👁 Предпросмотр альбома", callback_data="preview_mass_album")],
            [InlineKeyboardButton("🔄 Пересоздать альбом", callback_data="mass_recreate_album")],
            [InlineKeyboardButton("🗑 Удалить альбом", callback_data="mass_delete_album")],
            [InlineKeyboardButton("« Назад", callback_data="admin_send_all")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_mass_broadcast_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предварительный просмотр массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.broadcast_drafts[user_id]
        
        # Валидация
        if not draft["message_text"]:
            await update.callback_query.answer("❌ Сначала добавьте текст сообщения!", show_alert=True)
            return
        
        # Формируем превью
        preview_text = "📋 <b>Предварительный просмотр рассылки</b>\n\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            preview_text += f"⏰ <b>Запланировано на:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
            preview_text += f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n\n"
        else:
            preview_text += "🚀 <b>Отправка:</b> Немедленно\n\n"
        
        # Получатели
        users_count = len(self.db.get_users_completed_funnel())
        preview_text += f"👥 <b>Получателей:</b> {users_count} пользователей\n\n"
        
        # 🎬 НОВОЕ: Медиа-альбом
        media_album = draft.get("media_album")
        if media_album and len(media_album) > 0:
            photo_count = sum(1 for m in media_album if m[0] == 'photo')
            video_count = sum(1 for m in media_album if m[0] == 'video')
            preview_text += f"🎬 <b>Медиа-альбом:</b> {len(media_album)} файлов ({photo_count} фото, {video_count} видео)\n\n"
        elif draft["photo_data"]:
            preview_text += "🖼 <b>Фото:</b> Есть\n\n"
        
        # Текст сообщения
        preview_text += "📝 <b>Текст сообщения:</b>\n"
        preview_text += f"<code>{draft['message_text']}</code>\n\n"
        
        # Кнопки
        if draft["buttons"]:
            preview_text += f"🔘 <b>Кнопки ({len(draft['buttons'])}):</b>\n"
            for i, button in enumerate(draft["buttons"], 1):
                preview_text += f"{i}. {button['text']} → {button['url']}\n"
            preview_text += "\n"
        
        preview_text += "🔗 <i>Все ссылки получат UTM метки для отслеживания конверсий.</i>\n\n"
        preview_text += "✅ Подтвердите отправку:"
        
        keyboard = [
            [InlineKeyboardButton("✅ Отправить", callback_data="mass_confirm_send")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="admin_send_all")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем превью
        try:
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=preview_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
            # 🎬 НОВОЕ: Если есть медиа-альбом - отправляем его для предпросмотра
            if media_album and len(media_album) > 0:
                try:
                    media_group = []
                    for i, (media_type, media_url) in enumerate(media_album):
                        caption = "📸 <b>Предпросмотр альбома:</b>" if i == 0 else None
                        
                        if media_type == 'photo':
                            media_group.append(InputMediaPhoto(media=media_url, caption=caption, parse_mode='HTML'))
                        else:
                            media_group.append(InputMediaVideo(media=media_url, caption=caption, parse_mode='HTML'))
                    
                    await context.bot.send_media_group(
                        chat_id=user_id,
                        media=media_group
                    )
                except Exception as e:
                    if 'Event loop is closed' not in str(e):
                        logger.error(f"❌ Ошибка при отправке предпросмотра альбома: {e}")
            
            # Если есть обычное фото (не альбом)
            elif draft["photo_data"]:
                try:
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=draft["photo_data"],
                        caption="📸 <b>Предпросмотр фото:</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if 'Event loop is closed' not in str(e):
                        logger.error(f"❌ Ошибка при отправке предпросмотра фото: {e}")
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке предпросмотра: {e}")
    
    async def execute_mass_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выполнение массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.broadcast_drafts[user_id]
        
        # Валидация
        if not draft["message_text"]:
            await update.callback_query.answer("❌ Сначала добавьте текст сообщения!", show_alert=True)
            return
        
        try:
            if draft["scheduled_hours"]:
                # Запланированная рассылка
                scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
                
                # 🎬 НОВОЕ: Проверяем медиа-альбом
                media_album = draft.get("media_album")
                if media_album and len(media_album) > 0:
                    # Сохраняем рассылку БЕЗ photo_url/video_url
                    broadcast_id = self.db.add_scheduled_broadcast(
                        draft["message_text"],
                        scheduled_time,
                        photo_url=None,
                        video_url=None
                    )
                    
                    # Сохраняем медиа-альбом
                    for position, (media_type, media_url) in enumerate(media_album, 1):
                        self.db.add_scheduled_broadcast_media(broadcast_id, media_type, media_url, position)
                else:
                    # Старый формат: фото/видео
                    broadcast_id = self.db.add_scheduled_broadcast(
                        draft["message_text"],
                        scheduled_time,
                        draft["photo_data"],
                        draft.get("video_data")
                    )
                
                # Добавляем кнопки если есть
                for i, button in enumerate(draft["buttons"], 1):
                    self.db.add_scheduled_broadcast_button(
                        broadcast_id, 
                        button["text"], 
                        button["url"], 
                        i
                    )
                
                await update.callback_query.answer("✅ Рассылка запланирована!")
                
                result_text = (
                    f"⏰ <b>Рассылка запланирована!</b>\n\n"
                    f"📅 <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                    f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n"
                    f"📨 <b>ID рассылки:</b> #{broadcast_id}\n\n"
                    f"🔗 <i>Все ссылки получат UTM метки для отслеживания.</i>"
                )
                
            else:
                # Немедленная рассылка
                users_with_bot = self.db.get_users_completed_funnel()

                if not users_with_bot:
                    await update.callback_query.answer("❌ Нет пользователей, завершивших воронку!", show_alert=True)
                    return
                
                # Создаем клавиатуру если есть кнопки
                reply_markup = None
                if draft["buttons"]:
                    keyboard = []
                    for button in draft["buttons"]:
                        keyboard.append([InlineKeyboardButton(button["text"], url=button["url"])])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                
                sent_count = 0
                failed_count = 0
                
                await update.callback_query.answer("🚀 Начинаем рассылку...")
                
                # Отправляем прогресс для больших рассылок
                progress_message = None
                if len(users_with_bot) > 50:
                    try:
                        progress_message = await context.bot.send_message(
                            chat_id=user_id,
                            text="🚀 <b>Рассылка начата...</b>\n\n📊 Прогресс: 0%",
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Ошибка при отправке сообщения прогресса: {e}")
                
                # 🎬 НОВОЕ: Проверяем медиа-альбом
                media_album = draft.get("media_album")
                
                for i, user in enumerate(users_with_bot):
                    user_id_to_send = user[0]
                    try:
                        await asyncio.sleep(0.1)  # Небольшая задержка
                        
                        # Обрабатываем текст с UTM метками
                        processed_text = utm_utils.process_text_links(draft["message_text"], user_id_to_send)
                        
                        # Обрабатываем кнопки с UTM метками
                        processed_reply_markup = reply_markup
                        if draft["buttons"]:
                            keyboard = []
                            for button in draft["buttons"]:
                                processed_url = utm_utils.add_utm_to_url(button["url"], user_id_to_send)
                                keyboard.append([InlineKeyboardButton(button["text"], url=processed_url)])
                            processed_reply_markup = InlineKeyboardMarkup(keyboard)

                        # 🎬 НОВОЕ: Отправка медиа-альбома
                        if media_album and len(media_album) > 0:
                            # Формируем media_group
                            media_group = []
                            for idx, (media_type, media_url) in enumerate(media_album):
                                # Подпись только на первом медиа
                                caption = processed_text if idx == 0 else None
                                
                                if media_type == 'photo':
                                    media_group.append(InputMediaPhoto(media=media_url, caption=caption, parse_mode='HTML'))
                                else:
                                    media_group.append(InputMediaVideo(media=media_url, caption=caption, parse_mode='HTML'))
                            
                            # Отправляем альбом
                            await context.bot.send_media_group(chat_id=user_id_to_send, media=media_group)
                            
                            # Отправляем кнопки отдельным сообщением
                            if processed_reply_markup:
                                await context.bot.send_message(
                                    chat_id=user_id_to_send, 
                                    text="👇 <b>Выберите действие:</b>",
                                    reply_markup=processed_reply_markup,
                                    parse_mode='HTML'
                                )
                        
                        # Старый формат: фото + видео
                        elif draft["photo_data"] and draft.get("video_data"):
                            media_group = [
                                InputMediaPhoto(media=draft["photo_data"], caption=processed_text, parse_mode='HTML'),
                                InputMediaVideo(media=draft["video_data"])
                            ]
                            await context.bot.send_media_group(chat_id=user_id_to_send, media=media_group)
                            if processed_reply_markup:
                                await context.bot.send_message(chat_id=user_id_to_send, text="👇 Выберите действие:", reply_markup=processed_reply_markup)
                        
                        # Только фото
                        elif draft["photo_data"]:
                            await context.bot.send_photo(
                                chat_id=user_id_to_send,
                                photo=draft["photo_data"],
                                caption=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        
                        # Только видео
                        elif draft.get("video_data"):
                            await context.bot.send_video(
                                chat_id=user_id_to_send,
                                video=draft["video_data"],
                                caption=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        
                        # Только текст
                        else:
                            await context.bot.send_message(
                                chat_id=user_id_to_send,
                                text=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        sent_count += 1
                        
                        # Обновляем прогресс каждые 25 пользователей
                        if progress_message and i % 25 == 0:
                            progress = int((i / len(users_with_bot)) * 100)
                            try:
                                await progress_message.edit_text(
                                    f"🚀 <b>Рассылка в процессе...</b>\n\n"
                                    f"📊 Прогресс: {progress}%\n"
                                    f"✅ Отправлено: {sent_count}/{len(users_with_bot)}",
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                if 'Event loop is closed' not in str(e):
                                    logger.warning(f"⚠️ Ошибка при обновлении прогресса: {e}")
                        
                    except Exception as e:
                        failed_count += 1
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Не удалось отправить рассылку пользователю {user_id_to_send}: {e}")
                
                result_text = (
                    f"✅ <b>Рассылка завершена!</b>\n\n"
                    f"📤 <b>Успешно отправлено:</b> {sent_count}\n"
                    f"❌ <b>Ошибок:</b> {failed_count}\n\n"
                    f"🔗 <i>Все ссылки содержат UTM метки для отслеживания конверсий.</i>"
                )
            
            # Очищаем черновик
            if user_id in self.broadcast_drafts:
                del self.broadcast_drafts[user_id]
            
            # Отправляем результат
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=result_text,
                    parse_mode='HTML'
                )
                
                # Возвращаемся в главное меню через 3 секунды
                await asyncio.sleep(3)
                await self.show_main_menu_safe(update, context)
            except Exception as e:
                if 'Event loop is closed' not in str(e):
                    logger.error(f"❌ Ошибка при отправке результата рассылки: {e}")
            
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при выполнении рассылки: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке рассылки!", show_alert=True)
    
    # === ОБРАБОТЧИКИ ВВОДА ДЛЯ МАССОВЫХ РАССЫЛОК ===
    
    async def handle_mass_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста для массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "media_album": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        self.broadcast_drafts[user_id]["message_text"] = text
        
        await update.message.reply_text("✅ Текст сообщения сохранен!")
        del self.waiting_for[user_id]
        
        # Возвращаемся в меню
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода фото для массовой рассылки"""
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
                "created_at": datetime.now()
            }
        
        self.broadcast_drafts[user_id]["photo_data"] = text
        
        await update.message.reply_text("✅ Фото добавлено!")
        del self.waiting_for[user_id]
        
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_time_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода времени для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Если пустой текст - отправляем сейчас
        if not text.strip():
            self.broadcast_drafts[user_id]["scheduled_hours"] = None
            await update.message.reply_text("✅ Рассылка будет отправлена сейчас!")
        else:
            try:
                hours = float(text.strip())
                if hours <= 0:
                    await update.message.reply_text("❌ Количество часов должно быть больше 0")
                    return
                
                if hours > 8760:  # Больше года
                    await update.message.reply_text("❌ Слишком большое количество часов")
                    return
                
                self.broadcast_drafts[user_id]["scheduled_hours"] = hours
                scheduled_time = datetime.now() + timedelta(hours=hours)
                await update.message.reply_text(f"✅ Рассылка запланирована на {scheduled_time.strftime('%d.%m.%Y %H:%M')}!")
                
            except ValueError:
                await update.message.reply_text("❌ Введите корректное число часов")
                return
        
        del self.waiting_for[user_id]
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста кнопки для массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Сохраняем текст кнопки и переходим к вводу URL
        self.waiting_for[user_id]["button_text"] = text
        self.waiting_for[user_id]["type"] = "mass_button_url"
        
        await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
    
    async def handle_mass_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода URL кнопки для массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Проверяем лимит кнопок
        if len(self.broadcast_drafts[user_id]["buttons"]) >= 10:
            await update.message.reply_text("❌ Максимум 10 кнопок на сообщение.")
            return
        
        button_text = self.waiting_for[user_id]["button_text"]
        
        # Добавляем кнопку
        self.broadcast_drafts[user_id]["buttons"].append({
            "text": button_text,
            "url": text
        })
        
        await update.message.reply_text("✅ Кнопка добавлена!")
        del self.waiting_for[user_id]
        
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_video_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода видео для массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ Отправьте видео или ссылку на видео (начинающуюся с http:// или https://)")
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
        
        self.broadcast_drafts[user_id]["video_data"] = text
        
        await update.message.reply_text("✅ Видео добавлено!")
        del self.waiting_for[user_id]
        
        await self.show_send_all_menu_from_context(update, context)
