from datetime import datetime, timedelta
from telegram.ext import ContextTypes
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
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
            for message_number, text, delay_hours, photo_url in messages:
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
            
            # Получаем сообщения, готовые к отправке (только для пользователей с bot_started = 1 и has_paid = 0)
            pending_messages = self.db.get_pending_messages_for_active_users()
            
            if not pending_messages:
                logger.debug("📭 Нет сообщений для отправки")
                return
            
            logger.info(f"📬 Найдено {len(pending_messages)} сообщений для отправки")
            
            sent_count = 0
            failed_count = 0
            
            for message_id, user_id, message_number, text, photo_url in pending_messages:
                try:
                    logger.debug(f"📤 Отправляем сообщение {message_number} пользователю {user_id}")
                    
                    # НОВАЯ ПРОВЕРКА: Убеждаемся, что пользователь не оплатил за время ожидания
                    user_info = self.db.get_user(user_id)
                    if user_info and user_info[6]:  # has_paid = True
                        logger.info(f"💰 Пользователь {user_id} оплатил, пропускаем сообщение {message_number}")
                        self.db.mark_message_sent(message_id)
                        continue
                    
                    # Небольшая задержка между отправками для избежания лимитов
                    await asyncio.sleep(0.1)
                    
                    # Получаем кнопки для этого сообщения
                    buttons = self.db.get_message_buttons(message_number)
                    
                    # НОВОЕ: Обрабатываем контент с UTM метками
                    processed_text, processed_buttons = self.process_message_content(text, buttons, user_id)
                    
                    reply_markup = None
                    if processed_buttons:
                        # Создаем клавиатуру с обработанными кнопками
                        keyboard = []
                        for button_id, button_text, button_url, position in processed_buttons:
                            keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                        
                        reply_markup = InlineKeyboardMarkup(keyboard)
                        logger.debug(f"🔘 Добавлены кнопки к сообщению {message_number}: {len(processed_buttons)} кнопок с UTM метками")
                    
                    # Отправляем сообщение
                    if photo_url:
                        # Отправляем с фото
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=photo_url,
                            caption=processed_text,
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                        logger.debug(f"🖼️ Отправлено сообщение с фото")
                    else:
                        # Отправляем только текст
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=processed_text,
                            parse_mode='HTML',
                            disable_web_page_preview=True,
                            reply_markup=reply_markup
                        )
                        logger.debug(f"📝 Отправлено текстовое сообщение")
                    
                    # Отмечаем как отправленное
                    self.db.mark_message_sent(message_id)
                    sent_count += 1
                    
                    logger.info(f"✅ Отправлено сообщение {message_number} пользователю {user_id} с UTM метками")
                    
                except Forbidden as e:
                    # Пользователь заблокировал бота
                    logger.warning(f"❌ Пользователь {user_id} заблокировал бота: {e}")
                    # Отмечаем сообщение как отправленное, чтобы не пытаться снова
                    self.db.mark_message_sent(message_id)
                    # Деактивируем пользователя
                    self.db.deactivate_user(user_id)
                    failed_count += 1
                    
                except BadRequest as e:
                    # Неверный chat_id или другая ошибка
                    logger.error(f"❌ BadRequest для пользователя {user_id}: {e}")
                    # Отмечаем как отправленное, чтобы не зацикливаться
                    self.db.mark_message_sent(message_id)
                    failed_count += 1
                    
                except Exception as e:
                    logger.error(f"❌ Не удалось отправить сообщение {message_id} пользователю {user_id}: {e}")
                    failed_count += 1
                    # Не отмечаем как отправленное - попробуем еще раз позже
            
            if sent_count > 0 or failed_count > 0:
                logger.info(f"📊 Результаты рассылки: отправлено {sent_count}, ошибок {failed_count}")
            
            # Проверяем, есть ли еще запланированные сообщения
            remaining_messages = self.db.get_pending_messages_for_active_users()
            if remaining_messages:
                next_time = min([datetime.fromisoformat(msg[3]) for msg in remaining_messages if len(msg) > 3])
                logger.debug(f"⏳ Следующее сообщение запланировано на {next_time.strftime('%Y-%m-%d %H:%M:%S')}")
                        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в send_scheduled_messages: {e}", exc_info=True)
    
    async def send_scheduled_broadcasts(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить запланированные массовые рассылки"""
        try:
            current_time = datetime.now()
            logger.debug(f"📡 Проверка запланированных рассылок на {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Проверяем статус рассылки
            broadcast_status = self.db.get_broadcast_status()
            
            # Если рассылка отключена, пропускаем
            if not broadcast_status['enabled']:
                logger.debug("❌ Массовые рассылки отключены")
                return
            
            # Получаем рассылки, готовые к отправке
            pending_broadcasts = self.db.get_pending_broadcasts()
            
            if not pending_broadcasts:
                logger.debug("📭 Нет запланированных рассылок для отправки")
                return
            
            logger.info(f"📡 Найдено {len(pending_broadcasts)} запланированных рассылок для отправки")
            
            # Получаем пользователей, которые могут получать рассылки (активные, с bot_started=1)
            users_with_bot = self.db.get_users_with_bot_started()
            
            if not users_with_bot:
                logger.warning("⚠️ Нет пользователей для массовой рассылки")
                # Отмечаем рассылки как отправленные
                for broadcast_id, message_text, photo_url, scheduled_time in pending_broadcasts:
                    self.db.mark_broadcast_sent(broadcast_id)
                return
            
            logger.info(f"👥 Будем отправлять рассылки {len(users_with_bot)} пользователям")
            
            for broadcast_id, message_text, photo_url, scheduled_time in pending_broadcasts:
                try:
                    logger.info(f"📤 Начинаем отправку рассылки #{broadcast_id}")
                    
                    # Получаем кнопки для этой рассылки
                    buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
                    
                    sent_count = 0
                    failed_count = 0
                    
                    # Отправляем всем пользователям
                    for user in users_with_bot:
                        user_id = user[0]
                        has_paid = user[6] if len(user) > 6 else False
                        
                        try:
                            # Небольшая задержка между отправками
                            await asyncio.sleep(0.1)
                            
                            # НОВОЕ: Обрабатываем контент с UTM метками для каждого пользователя
                            processed_text, processed_buttons = self.process_message_content(message_text, buttons, user_id)
                            
                            reply_markup = None
                            if processed_buttons:
                                # Создаем клавиатуру с обработанными кнопками
                                keyboard = []
                                for button_id, button_text, button_url, position in processed_buttons:
                                    keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                                
                                reply_markup = InlineKeyboardMarkup(keyboard)
                                logger.debug(f"🔘 Добавлены кнопки к рассылке #{broadcast_id} для пользователя {user_id}: {len(processed_buttons)} кнопок с UTM метками")
                            
                            if photo_url:
                                # Отправляем с фото
                                await context.bot.send_photo(
                                    chat_id=user_id,
                                    photo=photo_url,
                                    caption=processed_text,
                                    parse_mode='HTML',
                                    reply_markup=reply_markup
                                )
                            else:
                                # Отправляем только текст
                                await context.bot.send_message(
                                    chat_id=user_id,
                                    text=processed_text,
                                    parse_mode='HTML',
                                    disable_web_page_preview=True,
                                    reply_markup=reply_markup
                                )
                            
                            sent_count += 1
                            
                        except Forbidden as e:
                            # Пользователь заблокировал бота
                            logger.warning(f"❌ Пользователь {user_id} заблокировал бота при рассылке #{broadcast_id}: {e}")
                            # Деактивируем пользователя
                            self.db.deactivate_user(user_id)
                            failed_count += 1
                            
                        except BadRequest as e:
                            # Неверный chat_id или другая ошибка
                            logger.error(f"❌ BadRequest для пользователя {user_id} при рассылке #{broadcast_id}: {e}")
                            failed_count += 1
                            
                        except Exception as e:
                            logger.error(f"❌ Не удалось отправить рассылку #{broadcast_id} пользователю {user_id}: {e}")
                            failed_count += 1
                    
                    # Отмечаем рассылку как отправленную
                    self.db.mark_broadcast_sent(broadcast_id)
                    
                    logger.info(f"✅ Рассылка #{broadcast_id} завершена с UTM метками: отправлено {sent_count}, ошибок {failed_count}")
                    
                    # Пауза между разными рассылками
                    if len(pending_broadcasts) > 1:
                        await asyncio.sleep(2)
                    
                except Exception as e:
                    logger.error(f"❌ Критическая ошибка при отправке рассылки #{broadcast_id}: {e}")
                    # Отмечаем как отправленную, чтобы не зацикливаться
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
