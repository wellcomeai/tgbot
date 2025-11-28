from datetime import datetime, timedelta
from telegram.ext import ContextTypes
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, ReplyKeyboardRemove
from telegram.error import Forbidden, BadRequest
import logging
import asyncio
import utm_utils

logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self, db):
        self.db = db
    
    async def schedule_user_messages(self, context: ContextTypes.DEFAULT_TYPE, user_id):
        """Запланировать отправку всех сообщений для пользователя"""
        try:
            logger.info(f"🔄 Начинаем планирование сообщений для пользователя {user_id}")
            
            # Получаем актуальную информацию о пользователе
            user_info = self.db.get_user(user_id)
            if not user_info:
                logger.error(f"❌ Пользователь {user_id} не найден в базе данных")
                return False
                
            user_id_db, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at = user_info
            
            # Проверяем, что пользователь активен
            if not is_active:
                logger.warning(f"⚠️ Пользователь {user_id} неактивен (is_active = {is_active})")
                return False
                
            # Проверяем, что пользователь дал согласие на получение сообщений
            if not bot_started:
                logger.warning(f"⚠️ Пользователь {user_id} не дал согласие на получение сообщений (bot_started = {bot_started})")
                return False
            
            # НОВАЯ ПРОВЕРКА: Если пользователь уже оплатил, не планируем сообщения
            if has_paid:
                logger.info(f"💰 Пользователь {user_id} уже оплатил, планирование сообщений пропущено")
                return True
            
            # Проверяем, есть ли уже запланированные сообщения
            existing_messages = self.db.get_user_scheduled_messages(user_id)
            if existing_messages:
                logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
                # Выводим детали существующих сообщений
                for msg_id, message_number, scheduled_time, is_sent in existing_messages:
                    logger.debug(f"   - Сообщение {message_number}: {scheduled_time} (отправлено: {is_sent})")
                return True
            
            # Получаем все сообщения рассылки
            messages = self.db.get_all_broadcast_messages()
            if not messages:
                logger.error("❌ Нет сообщений рассылки в базе данных")
                return False
            
            logger.info(f"📋 Найдено {len(messages)} сообщений рассылки для планирования")
            
            current_time = datetime.now()
            logger.info(f"⏰ Планирование сообщений для пользователя {user_id} (@{username}), текущее время: {current_time}")

            scheduled_count = 0
            for message_number, text, delay_hours, photo_url, video_url in messages:
                try:
                    # Вычисляем время отправки
                    scheduled_time = current_time + timedelta(hours=delay_hours)
                    
                    # Добавляем в расписание
                    self.db.schedule_message(user_id, message_number, scheduled_time)
                    scheduled_count += 1
                    
                    # Форматируем время для логов
                    time_diff = scheduled_time - current_time
                    if time_diff.total_seconds() < 3600:  # Меньше часа
                        time_str = f"{int(time_diff.total_seconds() / 60)} минут"
                    else:
                        time_str = f"{delay_hours} часов"
                    
                    logger.info(f"✅ Запланировано сообщение {message_number} для пользователя {user_id} на {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')} (через {time_str})")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при планировании сообщения {message_number} для пользователя {user_id}: {e}")
                    # Продолжаем планирование остальных сообщений
                    continue
            
            if scheduled_count > 0:
                logger.info(f"🎉 Всего запланировано {scheduled_count} сообщений для пользователя {user_id}")
                
                # Проверяем, что сообщения действительно добавились в БД
                verification_messages = self.db.get_user_scheduled_messages(user_id)
                if len(verification_messages) != scheduled_count:
                    logger.error(f"❌ Проверка не пройдена! Ожидалось {scheduled_count} сообщений, найдено {len(verification_messages)}")
                    return False
                
                return True
            else:
                logger.error(f"❌ Не удалось запланировать ни одного сообщения для пользователя {user_id}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при планировании сообщений для пользователя {user_id}: {e}", exc_info=True)
            return False
    
    async def ensure_user_messages_scheduled(self, context: ContextTypes.DEFAULT_TYPE, user_id):
        """Убедиться, что у пользователя запланированы сообщения"""
        try:
            # Проверяем, есть ли уже запланированные сообщения
            existing_messages = self.db.get_user_scheduled_messages(user_id)
            if existing_messages:
                logger.debug(f"✅ У пользователя {user_id} уже есть {len(existing_messages)} запланированных сообщений")
                return True
            
            # Если нет - планируем
            return await self.schedule_user_messages(context, user_id)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке/планировании сообщений для пользователя {user_id}: {e}")
            return False
    
    def process_message_content(self, text, buttons, user_id):
        """Обработка контента сообщения с добавлением UTM меток"""
        try:
            # Обрабатываем ссылки в тексте
            processed_text = utm_utils.process_text_links(text, user_id)

            # Обрабатываем кнопки
            processed_buttons = utm_utils.process_message_buttons(buttons, user_id)

            return processed_text, processed_buttons

        except Exception as e:
            logger.error(f"❌ Ошибка при обработке контента сообщения для пользователя {user_id}: {e}")
            # Возвращаем оригинальный контент в случае ошибки
            return text, buttons

    async def send_message_with_media(self, context, user_id, text, photo_url, video_url, reply_markup, message_number=None, broadcast_id=None):
        """
        Универсальная отправка сообщения с медиа (фото/видео/альбом)

        Args:
            context: Контекст бота
            user_id: ID пользователя
            text: Текст сообщения
            photo_url: URL или file_id фото (может быть None)
            video_url: URL или file_id видео (может быть None)
            reply_markup: Клавиатура с кнопками (может быть None)
            message_number: Номер сообщения воронки для проверки медиа-альбома (может быть None)
            broadcast_id: ID массовой рассылки для проверки медиа-альбома (может быть None)
        """
        try:
            # ✅ КРИТИЧНО: Убираем клавиатуру при первом сообщении воронки
            remove_keyboard = False
            if message_number == 1:
                remove_keyboard = True
                logger.debug(f"⌨️ Первое сообщение воронки - будем убирать клавиатуру")
            
            # 🎬 ПРОВЕРЯЕМ МЕДИА-АЛЬБОМ
            media_album = None
            
            # Для сообщений воронки
            if message_number is not None:
                media_album = self.db.get_message_media_album(message_number)
                if media_album and len(media_album) > 0:
                    logger.info(f"🎬 Найден медиа-альбом для сообщения {message_number}: {len(media_album)} файлов")
            
            # Для массовых рассылок
            elif broadcast_id is not None:
                media_album = self.db.get_scheduled_broadcast_media_album(broadcast_id)
                if media_album and len(media_album) > 0:
                    logger.info(f"🎬 Найден медиа-альбом для рассылки #{broadcast_id}: {len(media_album)} файлов")
            
            # Если есть медиа-альбом - отправляем его
            if media_album and len(media_album) > 0:
                # ⚠️ ВАЖНО: Telegram ограничивает caption медиа-группы до 1024 символов
                caption_text = text
                if len(text) > 1024:
                    caption_text = text[:1020] + "..."
                    logger.warning(f"⚠️ Текст обрезан до 1024 символов для медиа-альбома")
                
                # Собираем медиа-группу
                media_group = []
                for i, (media_id, media_type, media_url, position) in enumerate(media_album):
                    # Caption только к первому элементу
                    caption = caption_text if i == 0 else None
                    
                    if media_type == 'photo':
                        media_group.append(InputMediaPhoto(
                            media=media_url,
                            caption=caption,
                            parse_mode='HTML' if caption else None
                        ))
                    elif media_type == 'video':
                        media_group.append(InputMediaVideo(
                            media=media_url,
                            caption=caption,
                            parse_mode='HTML' if caption else None
                        ))
                
                # Отправляем медиа-группу
                await context.bot.send_media_group(
                    chat_id=user_id,
                    media=media_group
                )
                logger.info(f"🎬 Отправлен медиа-альбом ({len(media_group)} файлов) пользователю {user_id}")
                
                # ✅ Убираем клавиатуру если нужно (для первого сообщения)
                if remove_keyboard and not reply_markup:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="⌨️",  # Невидимый символ
                        reply_markup=ReplyKeyboardRemove()
                    )
                    # Удаляем это сообщение сразу
                    try:
                        # Получаем ID последнего сообщения
                        pass  # Telegram API не позволяет удалять сразу после отправки
                    except:
                        pass
                    logger.debug(f"⌨️ Клавиатура убрана")
                
                # Кнопки отправляем отдельным сообщением
                if reply_markup:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="Хочешь увидеть те самые предложения, которые получают наши пользователи?",
                        reply_markup=reply_markup
                    )
                    logger.debug(f"🔘 Отправлены кнопки после медиа-альбома")
                
                return  # ✅ Готово!
            
            # ✅ ЛОГИКА УДАЛЕНИЯ КЛАВИАТУРЫ ДЛЯ ПЕРВОГО СООБЩЕНИЯ
            # Если это первое сообщение И нет inline кнопок - добавляем ReplyKeyboardRemove
            final_reply_markup = reply_markup
            if remove_keyboard and not reply_markup:
                final_reply_markup = ReplyKeyboardRemove()
                logger.debug(f"⌨️ Добавлен ReplyKeyboardRemove к первому сообщению")
            
            # Если медиа-альбома нет - используем одиночные фото/видео
            if photo_url and video_url:
                # Отправляем медиагруппу (фото + видео)
                media_group = [
                    InputMediaPhoto(media=photo_url, caption=text, parse_mode='HTML'),
                    InputMediaVideo(media=video_url)
                ]
                await context.bot.send_media_group(
                    chat_id=user_id,
                    media=media_group
                )

                # Убираем клавиатуру если нужно
                if remove_keyboard and not reply_markup:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=".",
                        reply_markup=ReplyKeyboardRemove()
                    )
                    logger.debug(f"⌨️ Клавиатура убрана после медиагруппы")

                # Кнопки отправляем отдельным сообщением
                if reply_markup:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text="Хочешь увидеть те самые предложения, которые получают наши пользователи?",
                        reply_markup=reply_markup
                    )
                logger.debug(f"🖼️🎥 Отправлена медиагруппа (фото + видео)")

            elif photo_url:
                # Только фото
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=photo_url,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=final_reply_markup
                )
                logger.debug(f"🖼️ Отправлено сообщение с фото")

            elif video_url:
                # Только видео
                await context.bot.send_video(
                    chat_id=user_id,
                    video=video_url,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=final_reply_markup
                )
                logger.debug(f"🎥 Отправлено сообщение с видео")

            else:
                # Только текст
                await context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    reply_markup=final_reply_markup
                )
                logger.debug(f"📝 Отправлено текстовое сообщение")

        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения с медиа пользователю {user_id}: {e}")
            raise

    async def send_scheduled_messages(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить все запланированные сообщения, время которых настало"""
        try:
            current_time = datetime.now()
            logger.debug(f"🔄 Проверка запланированных сообщений на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Проверяем статус рассылки
            broadcast_status = self.db.get_broadcast_status()
            
            # Если рассылка отключена, проверяем время автовозобновления
            if not broadcast_status['enabled']:
                if broadcast_status['auto_resume_time']:
                    resume_time = datetime.fromisoformat(broadcast_status['auto_resume_time'])
                    if current_time >= resume_time:
                        # Автоматически включаем рассылку
                        self.db.set_broadcast_status(True, None)
                        logger.info("✅ Рассылка автоматически возобновлена")
                    else:
                        logger.debug(f"❌ Рассылка отключена до {resume_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        return
                else:
                    logger.debug("❌ Рассылка отключена без таймера")
                    return
            
            # Получаем сообщения, готовые к отправке
            pending_messages = self.db.get_pending_messages_for_active_users()
            
            if not pending_messages:
                logger.debug("📭 Нет сообщений для отправки")
                return
            
            logger.info(f"📬 Найдено {len(pending_messages)} сообщений для отправки")

            sent_count = 0
            failed_count = 0

            for message_id, user_id, message_number, text, photo_url, video_url in pending_messages:
                try:
                    logger.debug(f"📤 Отправляем сообщение {message_number} пользователю {user_id}")
                    
                    # Проверяем, что пользователь не оплатил
                    user_info = self.db.get_user(user_id)
                    if user_info and user_info[6]:  # has_paid = True
                        logger.info(f"💰 Пользователь {user_id} оплатил, пропускаем сообщение {message_number}")
                        self.db.mark_message_sent(message_id)
                        continue
                    
                    # Небольшая задержка между отправками
                    await asyncio.sleep(0.1)
                    
                    # Получаем кнопки
                    buttons = self.db.get_message_buttons(message_number)

                    # Обрабатываем контент с UTM метками
                    processed_text, processed_buttons = self.process_message_content(text, buttons, user_id)

                    reply_markup = None
                    if processed_buttons:
                        keyboard = []

                        for button_id, button_text, button_url, position, messages_count in processed_buttons:
                            if button_url and button_url.strip():
                                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                            else:
                                # Передаем messages_count в callback_data
                                keyboard.append([InlineKeyboardButton(
                                    button_text,
                                    callback_data=f"next_msg_{user_id}_{messages_count or 1}"
                                )])

                        reply_markup = InlineKeyboardMarkup(keyboard)
                        logger.debug(f"🔘 Добавлены кнопки к сообщению {message_number}: {len(processed_buttons)} кнопок")

                    # ✅ Отправляем с проверкой медиа-альбома и удалением клавиатуры
                    await self.send_message_with_media(
                        context,
                        user_id,
                        processed_text,
                        photo_url,
                        video_url,
                        reply_markup,
                        message_number=message_number
                    )

                    # Отмечаем как отправленное
                    self.db.mark_message_sent(message_id)
                    
                    # 📊 Логируем отправку для воронки
                    self.db.log_message_delivery(user_id, message_number)
                    
                    sent_count += 1
                    
                    logger.info(f"✅ Отправлено сообщение {message_number} пользователю {user_id} с UTM метками")
                    
                except Forbidden as e:
                    logger.warning(f"❌ Пользователь {user_id} заблокировал бота: {e}")
                    self.db.mark_message_sent(message_id)
                    self.db.deactivate_user(user_id)
                    failed_count += 1
                    
                except BadRequest as e:
                    logger.error(f"❌ BadRequest для пользователя {user_id}: {e}")
                    self.db.mark_message_sent(message_id)
                    failed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Не удалось отправить сообщение {message_id} пользователю {user_id}: {e}")
                    failed_count += 1
            
            if sent_count > 0 or failed_count > 0:
                logger.info(f"📊 Результаты рассылки: отправлено {sent_count}, ошибок {failed_count}")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в send_scheduled_messages: {e}", exc_info=True)
    
    async def send_next_scheduled_message(self, context: ContextTypes.DEFAULT_TYPE, user_id, count=1):
        """
        Отправить следующее(ие) запланированное(ые) сообщение(я) для пользователя

        Args:
            context: Контекст бота
            user_id: ID пользователя
            count: Количество сообщений для отправки (по умолчанию 1)
        """
        return await self.send_multiple_next_messages(context, user_id, count)

    async def send_multiple_next_messages(self, context: ContextTypes.DEFAULT_TYPE, user_id, count=1):
        """
        Отправить N следующих запланированных сообщений подряд

        Args:
            context: Контекст бота
            user_id: ID пользователя
            count: Количество сообщений для отправки

        Returns:
            bool: True если отправлено хотя бы одно сообщение
        """
        try:
            if count < 1:
                logger.warning(f"⚠️ Некорректное количество сообщений: {count}")
                return False

            logger.info(f"📬 Отправляем {count} сообщений пользователю {user_id}")
            sent_count = 0

            for i in range(count):
                # Получаем следующее неотправленное сообщение
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT sm.id, sm.message_number, bm.text, bm.photo_url, bm.video_url
                    FROM scheduled_messages sm
                    JOIN broadcast_messages bm ON sm.message_number = bm.message_number
                    WHERE sm.user_id = ? AND sm.is_sent = 0
                    ORDER BY sm.message_number ASC
                    LIMIT 1
                ''', (user_id,))

                result = cursor.fetchone()
                conn.close()

                if not result:
                    if sent_count == 0:
                        logger.info(f"ℹ️ Нет сообщений для отправки пользователю {user_id}")
                    else:
                        logger.info(f"ℹ️ Больше нет сообщений (отправлено {sent_count} из {count})")
                    break

                message_id, message_number, text, photo_url, video_url = result

                # Получаем кнопки
                buttons = self.db.get_message_buttons(message_number)
                processed_text, processed_buttons = self.process_message_content(text, buttons, user_id)

                # Формируем клавиатуру с учетом messages_count
                reply_markup = None
                if processed_buttons:
                    keyboard = []

                    for button_id, button_text, button_url, position, messages_count in processed_buttons:
                        if button_url and button_url.strip():
                            # URL кнопка
                            keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                        else:
                            # Callback кнопка - передаем messages_count в callback_data
                            keyboard.append([InlineKeyboardButton(
                                button_text,
                                callback_data=f"next_msg_{user_id}_{messages_count or 1}"
                            )])

                    reply_markup = InlineKeyboardMarkup(keyboard)

                # Отправляем сообщение (здесь НЕ убираем клавиатуру, так как это не первое сообщение воронки)
                await self.send_message_with_media(
                    context,
                    user_id,
                    processed_text,
                    photo_url,
                    video_url,
                    reply_markup,
                    message_number=message_number
                )

                # Отмечаем как отправленное
                self.db.mark_message_sent(message_id)
                self.db.log_message_delivery(user_id, message_number)

                sent_count += 1
                logger.info(f"✅ Отправлено сообщение {message_number} ({sent_count}/{count}) пользователю {user_id}")

                # Задержка между сообщениями (кроме последнего)
                if i < count - 1:
                    await asyncio.sleep(1)

            return sent_count > 0

        except Exception as e:
            logger.error(f"❌ Ошибка при множественной отправке сообщений пользователю {user_id}: {e}")
            return False
    
    async def send_scheduled_broadcasts(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить запланированные массовые рассылки"""
        try:
            current_time = datetime.now()
            logger.debug(f"📡 Проверка запланированных рассылок на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Проверяем статус рассылки
            broadcast_status = self.db.get_broadcast_status()
            
            if not broadcast_status['enabled']:
                logger.debug("❌ Массовые рассылки отключены")
                return
            
            # Получаем рассылки, готовые к отправке
            pending_broadcasts = self.db.get_pending_broadcasts()
            
            if not pending_broadcasts:
                logger.debug("📭 Нет запланированных рассылок для отправки")
                return
            
            logger.info(f"📡 Найдено {len(pending_broadcasts)} запланированных рассылок для отправки")

            # Получаем пользователей, завершивших воронку
            users_with_bot = self.db.get_users_completed_funnel()

            if not users_with_bot:
                logger.debug("📭 Нет пользователей, завершивших воронку - отменяем рассылки")
                for broadcast_id, message_text, photo_url, video_url, scheduled_time in pending_broadcasts:
                    self.db.mark_broadcast_sent(broadcast_id)
                    logger.debug(f"✅ Рассылка #{broadcast_id} отменена (нет завершивших воронку)")
                return

            logger.info(f"👥 Будем отправлять рассылки {len(users_with_bot)} пользователям, завершившим воронку")

            for broadcast_id, message_text, photo_url, video_url, scheduled_time in pending_broadcasts:
                try:
                    logger.info(f"📤 Начинаем отправку рассылки #{broadcast_id}")
                    
                    # Получаем кнопки для этой рассылки
                    buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
                    
                    sent_count = 0
                    failed_count = 0
                    
                    # Отправляем всем пользователям
                    for user in users_with_bot:
                        user_id = user[0]
                        
                        try:
                            await asyncio.sleep(0.1)
                            
                            # Обрабатываем контент с UTM метками
                            processed_text, processed_buttons = self.process_message_content(message_text, buttons, user_id)
                            
                            reply_markup = None
                            if processed_buttons:
                                keyboard = []
                                
                                for button_id, button_text, button_url, position in processed_buttons:
                                    if button_url and button_url.strip():
                                        keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                                    else:
                                        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"next_msg_{user_id}")])
                                
                                reply_markup = InlineKeyboardMarkup(keyboard)
                                logger.debug(f"🔘 Добавлены кнопки к рассылке #{broadcast_id} для пользователя {user_id}")

                            # ✅ Отправляем с проверкой медиа-альбома рассылки
                            await self.send_message_with_media(
                                context,
                                user_id,
                                processed_text,
                                photo_url,
                                video_url,
                                reply_markup,
                                broadcast_id=broadcast_id
                            )

                            # 📊 Логируем отправку массовой рассылки
                            self.db.log_message_delivery(user_id, -broadcast_id)
                            
                            sent_count += 1
                            
                        except Forbidden as e:
                            logger.warning(f"❌ Пользователь {user_id} заблокировал бота при рассылке #{broadcast_id}: {e}")
                            self.db.deactivate_user(user_id)
                            failed_count += 1
                            
                        except BadRequest as e:
                            logger.error(f"❌ BadRequest для пользователя {user_id} при рассылке #{broadcast_id}: {e}")
                            failed_count += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Не удалось отправить рассылку #{broadcast_id} пользователю {user_id}: {e}")
                            failed_count += 1
                    
                    # Отмечаем рассылку как отправленную
                    self.db.mark_broadcast_sent(broadcast_id)
                    
                    logger.info(f"✅ Рассылка #{broadcast_id} завершена с UTM метками: отправлено {sent_count}, ошибок {failed_count}")
                    
                    # Пауза между рассылками
                    if len(pending_broadcasts) > 1:
                        await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Критическая ошибка при отправке рассылки #{broadcast_id}: {e}")
                    self.db.mark_broadcast_sent(broadcast_id)
            
            logger.info(f"📊 Обработка запланированных рассылок завершена")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в send_scheduled_broadcasts: {e}", exc_info=True)
    
    def reschedule_all_messages(self):
        """Перепланировать все сообщения для всех пользователей (при изменении задержек)"""
        # Эта функция может быть полезна, если админ изменил задержки
        # и хочет применить их ко всем будущим сообщениям
        # TODO: Реализовать при необходимости
        pass
    
    async def cancel_user_remaining_messages(self, user_id):
        """Отмена оставшихся сообщений для оплатившего пользователя"""
        try:
            cancelled_count = self.db.cancel_remaining_messages(user_id)
            logger.info(f"🚫 Отменено {cancelled_count} запланированных сообщений для оплатившего пользователя {user_id}")
            return cancelled_count
        except Exception as e:
            logger.error(f"❌ Ошибка при отмене сообщений для пользователя {user_id}: {e}")
            return 0

    # ===== ПЛАТНЫЕ РАССЫЛКИ =====

    async def schedule_paid_user_messages(self, context: ContextTypes.DEFAULT_TYPE, user_id):
        """Запланировать отправку всех сообщений для оплатившего пользователя"""
        try:
            logger.info(f"💰 Начинаем планирование платных сообщений для пользователя {user_id}")
            
            user_info = self.db.get_user(user_id)
            if not user_info:
                logger.error(f"❌ Пользователь {user_id} не найден в базе данных")
                return False
                
            user_id_db, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at = user_info
            
            if not is_active:
                logger.warning(f"⚠️ Пользователь {user_id} неактивен")
                return False
            
            if not has_paid:
                logger.warning(f"⚠️ Пользователь {user_id} не оплатил")
                return False
            
            # Проверяем, есть ли уже запланированные платные сообщения
            existing_messages = self.db.get_user_paid_scheduled_messages(user_id)
            if existing_messages:
                logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных платных сообщений")
                return True
            
            # Получаем все сообщения рассылки для оплативших
            messages = self.db.get_all_paid_broadcast_messages()
            if not messages:
                logger.warning("⚠️ Нет сообщений платной рассылки в базе данных")
                return True
            
            logger.info(f"📋 Найдено {len(messages)} платных сообщений рассылки для планирования")
            
            current_time = datetime.now()
            logger.info(f"💰 ⏰ Планирование платных сообщений для пользователя {user_id} (@{username}), текущее время: {current_time}")

            scheduled_count = 0
            for message_number, text, delay_hours, photo_url, video_url in messages:
                try:
                    scheduled_time = current_time + timedelta(hours=delay_hours)
                    
                    success = self.db.schedule_paid_message(user_id, message_number, scheduled_time)
                    if success:
                        scheduled_count += 1
                        
                        time_diff = scheduled_time - current_time
                        if time_diff.total_seconds() < 3600:
                            time_str = f"{int(time_diff.total_seconds() / 60)} минут"
                        else:
                            time_str = f"{delay_hours} часов"
                        
                        logger.info(f"✅ Запланировано платное сообщение {message_number} для пользователя {user_id} на {scheduled_time.strftime('%Y-%m-%d %H:%M:%S')} (через {time_str})")
                    else:
                        logger.error(f"❌ Не удалось запланировать платное сообщение {message_number}")
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при планировании платного сообщения {message_number}: {e}")
                    continue
            
            if scheduled_count > 0:
                logger.info(f"💰 🎉 Всего запланировано {scheduled_count} платных сообщений для пользователя {user_id}")
                return True
            else:
                logger.warning(f"⚠️ Не удалось запланировать ни одного платного сообщения")
                return False
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при планировании платных сообщений: {e}", exc_info=True)
            return False

    async def send_scheduled_paid_messages(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить все запланированные платные сообщения"""
        try:
            current_time = datetime.now()
            logger.debug(f"💰 🔄 Проверка запланированных платных сообщений на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            broadcast_status = self.db.get_broadcast_status()
            
            if not broadcast_status['enabled']:
                logger.debug("❌ Платные рассылки отключены")
                return
            
            pending_messages = self.db.get_pending_paid_messages()
            
            if not pending_messages:
                logger.debug("💰 📭 Нет платных сообщений для отправки")
                return
            
            logger.info(f"💰 📬 Найдено {len(pending_messages)} платных сообщений для отправки")

            sent_count = 0
            failed_count = 0

            for message_id, user_id, message_number, text, photo_url, video_url in pending_messages:
                try:
                    logger.debug(f"💰 📤 Отправляем платное сообщение {message_number} пользователю {user_id}")
                    
                    # Убеждаемся, что пользователь еще оплачен и активен
                    user_info = self.db.get_user(user_id)
                    if not user_info or not user_info[4] or not user_info[6]:
                        logger.warning(f"💰 ⚠️ Пользователь {user_id} больше не активен или не оплачен")
                        self.db.mark_paid_message_sent(message_id)
                        continue
                    
                    await asyncio.sleep(0.1)
                    
                    # Получаем кнопки
                    buttons = self.db.get_paid_message_buttons(message_number)
                    
                    # Обрабатываем контент с UTM метками
                    processed_text, processed_buttons = self.process_message_content(text, buttons, user_id)
                    
                    reply_markup = None
                    if processed_buttons:
                        keyboard = []
                        
                        for button_id, button_text, button_url, position in processed_buttons:
                            if button_url and button_url.strip():
                                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                            else:
                                keyboard.append([InlineKeyboardButton(button_text, callback_data=f"next_msg_{user_id}")])
                        
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        logger.debug(f"💰 🔘 Добавлены кнопки к платному сообщению {message_number}")

                    # ✅ Отправляем платное сообщение (без медиа-альбома для платных пока)
                    await self.send_message_with_media(
                        context,
                        user_id,
                        processed_text,
                        photo_url,
                        video_url,
                        reply_markup,
                        message_number=None  # Для платных сообщений медиа-альбомы в другой таблице
                    )

                    self.db.mark_paid_message_sent(message_id)
                    
                    # 📊 Логируем отправку платного сообщения
                    self.db.log_message_delivery(user_id, message_number)
                    
                    sent_count += 1
                    
                    logger.info(f"✅ Отправлено платное сообщение {message_number} пользователю {user_id}")
                    
                except Forbidden as e:
                    logger.warning(f"❌ Пользователь {user_id} заблокировал бота: {e}")
                    self.db.mark_paid_message_sent(message_id)
                    self.db.deactivate_user(user_id)
                    failed_count += 1
                    
                except BadRequest as e:
                    logger.error(f"❌ BadRequest для пользователя {user_id}: {e}")
                    self.db.mark_paid_message_sent(message_id)
                    failed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Не удалось отправить платное сообщение {message_id}: {e}")
                    failed_count += 1
            
            if sent_count > 0 or failed_count > 0:
                logger.info(f"💰 📊 Результаты платной рассылки: отправлено {sent_count}, ошибок {failed_count}")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в send_scheduled_paid_messages: {e}", exc_info=True)

    async def send_scheduled_paid_broadcasts(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить запланированные массовые рассылки для оплативших"""
        try:
            current_time = datetime.now()
            logger.debug(f"💰 📡 Проверка запланированных рассылок для оплативших на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            broadcast_status = self.db.get_broadcast_status()
            
            if not broadcast_status['enabled']:
                logger.debug("❌ Массовые рассылки для оплативших отключены")
                return
            
            pending_broadcasts = self.db.get_pending_paid_broadcasts()
            
            if not pending_broadcasts:
                logger.debug("💰 📭 Нет запланированных рассылок для оплативших")
                return
            
            logger.info(f"💰 📡 Найдено {len(pending_broadcasts)} запланированных рассылок для оплативших")
            
            paid_users = self.db.get_users_with_payment()
            
            if not paid_users:
                logger.warning("⚠️ Нет оплативших пользователей для массовой рассылки")
                for broadcast_id, message_text, photo_url, video_url, scheduled_time in pending_broadcasts:
                    self.db.mark_paid_broadcast_sent(broadcast_id)
                return

            logger.info(f"💰 👥 Будем отправлять рассылки {len(paid_users)} оплатившим пользователям")

            for broadcast_id, message_text, photo_url, video_url, scheduled_time in pending_broadcasts:
                try:
                    logger.info(f"💰 📤 Начинаем отправку рассылки для оплативших #{broadcast_id}")
                    
                    buttons = self.db.get_paid_scheduled_broadcast_buttons(broadcast_id)
                    
                    sent_count = 0
                    failed_count = 0
                    
                    for user in paid_users:
                        user_id = user[0]
                        
                        try:
                            await asyncio.sleep(0.1)
                            
                            processed_text, processed_buttons = self.process_message_content(message_text, buttons, user_id)
                            
                            reply_markup = None
                            if processed_buttons:
                                keyboard = []
                                
                                for button_id, button_text, button_url, position in processed_buttons:
                                    if button_url and button_url.strip():
                                        keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                                    else:
                                        keyboard.append([InlineKeyboardButton(button_text, callback_data=f"next_msg_{user_id}")])
                                
                                reply_markup = InlineKeyboardMarkup(keyboard)

                            # Отправляем (без медиа-альбома для платных массовых пока)
                            await self.send_message_with_media(
                                context,
                                user_id,
                                processed_text,
                                photo_url,
                                video_url,
                                reply_markup
                            )

                            # 📊 Логируем отправку платной массовой рассылки
                            self.db.log_message_delivery(user_id, -(broadcast_id + 10000))
                            
                            sent_count += 1
                            
                        except Forbidden as e:
                            logger.warning(f"❌ Пользователь {user_id} заблокировал бота: {e}")
                            self.db.deactivate_user(user_id)
                            failed_count += 1
                            
                        except BadRequest as e:
                            logger.error(f"❌ BadRequest для пользователя {user_id}: {e}")
                            failed_count += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Не удалось отправить рассылку #{broadcast_id} пользователю {user_id}: {e}")
                            failed_count += 1
                    
                    self.db.mark_paid_broadcast_sent(broadcast_id)
                    
                    logger.info(f"✅ Рассылка для оплативших #{broadcast_id} завершена: отправлено {sent_count}, ошибок {failed_count}")
                    
                    if len(pending_broadcasts) > 1:
                        await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Критическая ошибка при отправке рассылки #{broadcast_id}: {e}")
                    self.db.mark_paid_broadcast_sent(broadcast_id)
            
            logger.info(f"💰 📊 Обработка запланированных рассылок для оплативших завершена")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в send_scheduled_paid_broadcasts: {e}", exc_info=True)

    async def check_expired_subscriptions(self, context: ContextTypes.DEFAULT_TYPE):
        """Проверка истекших подписок и отправка уведомлений о продлении"""
        try:
            from datetime import date, datetime
            import pytz
            
            current_time = datetime.now()
            logger.info(f"🔄 Проверка истекших подписок на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            expired_users = self.db.get_expired_subscriptions()
            
            if not expired_users:
                logger.debug("📭 Нет пользователей с истекшими подписками")
                return
            
            logger.info(f"⏰ Найдено {len(expired_users)} пользователей с истекшими подписками")
            
            renewal_data = self.db.get_renewal_message()
            
            if not renewal_data or not renewal_data.get('text'):
                logger.error("❌ Не настроено сообщение о продлении подписки")
                return
            
            sent_count = 0
            failed_count = 0
            
            for user_id, username, first_name, payed_till in expired_users:
                try:
                    logger.info(f"📤 Отправляем уведомление о продлении пользователю {user_id} (@{username})")
                    
                    processed_text = utm_utils.process_text_links(renewal_data['text'], user_id)
                    
                    reply_markup = None
                    if renewal_data.get('button_text') and renewal_data.get('button_url'):
                        processed_url = utm_utils.add_utm_to_url(renewal_data['button_url'], user_id)
                        
                        keyboard = [[InlineKeyboardButton(
                            renewal_data['button_text'], 
                            url=processed_url
                        )]]
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        logger.debug(f"🔘 Добавлена кнопка продления с UTM метками")

                    photo_url = renewal_data.get('photo_url')
                    video_url = renewal_data.get('video_url')

                    await self.send_message_with_media(
                        context,
                        user_id,
                        processed_text,
                        photo_url,
                        video_url,
                        reply_markup
                    )

                    expire_success = self.db.expire_user_subscription(user_id)
                    
                    if expire_success:
                        schedule_success = await self.schedule_user_messages(context, user_id)
                        
                        if schedule_success:
                            logger.info(f"✅ Пользователь {user_id} переведен на обычные рассылки после истечения подписки")
                        else:
                            logger.warning(f"⚠️ Не удалось запланировать обычные сообщения для пользователя {user_id}")
                    else:
                        logger.error(f"❌ Не удалось завершить подписку пользователя {user_id}")
                    
                    sent_count += 1
                    
                    await asyncio.sleep(0.2)
                    
                except Forbidden as e:
                    logger.warning(f"❌ Пользователь {user_id} заблокировал бота: {e}")
                    self.db.expire_user_subscription(user_id)
                    self.db.deactivate_user(user_id)
                    failed_count += 1
                    
                except BadRequest as e:
                    logger.error(f"❌ BadRequest для пользователя {user_id}: {e}")
                    self.db.expire_user_subscription(user_id)
                    failed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Не удалось отправить уведомление о продлении пользователю {user_id}: {e}")
                    failed_count += 1
            
            logger.info(f"📊 Проверка истекших подписок завершена: уведомлений отправлено {sent_count}, ошибок {failed_count}")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в check_expired_subscriptions: {e}", exc_info=True)
