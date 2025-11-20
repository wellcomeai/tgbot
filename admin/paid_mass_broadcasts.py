"""
Функциональность массовых рассылок для оплативших пользователей в админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo
from telegram.ext import ContextTypes
from datetime import datetime, timedelta
import logging
import asyncio
import utm_utils

logger = logging.getLogger(__name__)


class PaidMassBroadcastsMixin:
    """Миксин для работы с массовыми рассылками для оплативших"""
    
    async def execute_paid_mass_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выполнение массовой рассылки для оплативших"""
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
                # Запланированная рассылка для оплативших
                scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
                broadcast_id = self.db.add_paid_scheduled_broadcast(
                    draft["message_text"],
                    scheduled_time,
                    draft["photo_data"],
                    draft.get("video_data")
                )
                
                # Добавляем кнопки если есть
                for i, button in enumerate(draft["buttons"], 1):
                    self.db.add_paid_scheduled_broadcast_button(
                        broadcast_id, 
                        button["text"], 
                        button["url"], 
                        i
                    )
                
                await update.callback_query.answer("✅ Рассылка для оплативших запланирована!")
                
                result_text = (
                    f"💰 <b>Рассылка для оплативших запланирована!</b>\n\n"
                    f"📅 <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                    f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n"
                    f"📨 <b>ID рассылки:</b> #{broadcast_id}\n\n"
                    f"🔗 <i>Все ссылки получат UTM метки для отслеживания.</i>"
                )
                
            else:
                # Немедленная рассылка для оплативших
                paid_users = self.db.get_users_with_payment()
                
                if not paid_users:
                    await update.callback_query.answer("❌ Нет оплативших пользователей для рассылки!", show_alert=True)
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
                
                await update.callback_query.answer("🚀 Начинаем рассылку для оплативших...")
                
                # Отправляем прогресс для больших рассылок
                progress_message = None
                if len(paid_users) > 10:
                    try:
                        progress_message = await context.bot.send_message(
                            chat_id=user_id,
                            text="💰 <b>Рассылка для оплативших начата...</b>\n\n📊 Прогресс: 0%",
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Ошибка при отправке сообщения прогресса: {e}")
                
                for i, user in enumerate(paid_users):
                    user_id_to_send = user[0]
                    try:
                        await asyncio.sleep(0.1)  # Небольшая задержка
                        
                        # Обрабатываем текст и кнопки с UTM метками
                        processed_text = utm_utils.process_text_links(draft["message_text"], user_id_to_send)
                        
                        processed_reply_markup = reply_markup
                        if draft["buttons"]:
                            keyboard = []
                            for button in draft["buttons"]:
                                processed_url = utm_utils.add_utm_to_url(button["url"], user_id_to_send)
                                keyboard.append([InlineKeyboardButton(button["text"], url=processed_url)])
                            processed_reply_markup = InlineKeyboardMarkup(keyboard)

                        photo_data = draft["photo_data"]
                        video_data = draft.get("video_data")

                        if photo_data and video_data:
                            media_group = [
                                InputMediaPhoto(media=photo_data, caption=processed_text, parse_mode='HTML'),
                                InputMediaVideo(media=video_data)
                            ]
                            await context.bot.send_media_group(chat_id=user_id_to_send, media=media_group)
                            if processed_reply_markup:
                                await context.bot.send_message(chat_id=user_id_to_send, text="👇 Выберите действие:", reply_markup=processed_reply_markup)
                        elif photo_data:
                            await context.bot.send_photo(
                                chat_id=user_id_to_send,
                                photo=photo_data,
                                caption=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        elif video_data:
                            await context.bot.send_video(
                                chat_id=user_id_to_send,
                                video=video_data,
                                caption=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=user_id_to_send,
                                text=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        sent_count += 1
                        
                        # Обновляем прогресс каждые 5 пользователей
                        if progress_message and i % 5 == 0:
                            progress = int((i / len(paid_users)) * 100)
                            try:
                                await progress_message.edit_text(
                                    f"💰 <b>Рассылка для оплативших в процессе...</b>\n\n"
                                    f"📊 Прогресс: {progress}%\n"
                                    f"✅ Отправлено: {sent_count}/{len(paid_users)}",
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                if 'Event loop is closed' not in str(e):
                                    logger.warning(f"⚠️ Ошибка при обновлении прогресса: {e}")
                        
                    except Exception as e:
                        failed_count += 1
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Не удалось отправить рассылку оплатившему пользователю {user_id_to_send}: {e}")
                
                result_text = (
                    f"💰 <b>Рассылка для оплативших завершена!</b>\n\n"
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
                logger.error(f"❌ Ошибка при выполнении рассылки для оплативших: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке рассылки!", show_alert=True)
    
    async def show_paid_mass_broadcast_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предварительный просмотр массовой рассылки для оплативших"""
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
        preview_text = "💰 <b>Предварительный просмотр рассылки для оплативших</b>\n\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            preview_text += f"⏰ <b>Запланировано на:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
            preview_text += f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n\n"
        else:
            preview_text += "🚀 <b>Отправка:</b> Немедленно\n\n"
        
        # Получатели
        paid_users = self.db.get_users_with_payment()
        users_count = len(paid_users)
        preview_text += f"👥 <b>Получателей:</b> {users_count} оплативших пользователей\n\n"
        
        # Фото
        if draft["photo_data"]:
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
            [InlineKeyboardButton("✅ Отправить", callback_data="paid_mass_confirm_send")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="paid_send_all")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_paid_broadcast")]
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
            
            # Если есть фото, отправляем его для предпросмотра
            if draft["photo_data"]:
                try:
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=draft["photo_data"],
                        caption="💰 <b>Предпросмотр фото для оплативших:</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if 'Event loop is closed' not in str(e):
                        logger.error(f"❌ Ошибка при отправке предпросмотра фото: {e}")
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке предпросмотра: {e}")
