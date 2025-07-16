from datetime import datetime, timedelta
from telegram.ext import ContextTypes
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden, BadRequest
import logging
import asyncio

logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self, db):
        self.db = db
    
    async def schedule_user_messages(self, context: ContextTypes.DEFAULT_TYPE, user_id):
        """Запланировать отправку всех сообщений для пользователя"""
        try:
            # Проверяем, что пользователь дал согласие на получение сообщений
            user_info = self.db.get_user(user_id)
            if not user_info or not user_info[5]:  # bot_started = False
                logger.warning(f"Попытка запланировать сообщения для пользователя {user_id}, который не дал согласие")
                return False
            
            # Получаем все сообщения рассылки
            messages = self.db.get_all_broadcast_messages()
            current_time = datetime.now()
            
            for message_number, text, delay_hours, photo_url in messages:
                # Вычисляем время отправки
                scheduled_time = current_time + timedelta(hours=delay_hours)
                
                # Добавляем в расписание
                self.db.schedule_message(user_id, message_number, scheduled_time)
                
                logger.info(f"Запланировано сообщение {message_number} для пользователя {user_id} на {scheduled_time}")
            
            return True
        except Exception as e:
            logger.error(f"Ошибка при планировании сообщений для пользователя {user_id}: {e}")
            return False
    
    async def send_scheduled_messages(self, context: ContextTypes.DEFAULT_TYPE):
        """Отправить все запланированные сообщения, время которых настало"""
        try:
            # Проверяем статус рассылки
            broadcast_status = self.db.get_broadcast_status()
            
            # Если рассылка отключена, проверяем время автовозобновления
            if not broadcast_status['enabled']:
                if broadcast_status['auto_resume_time']:
                    resume_time = datetime.fromisoformat(broadcast_status['auto_resume_time'])
                    if datetime.now() >= resume_time:
                        # Автоматически включаем рассылку
                        self.db.set_broadcast_status(True, None)
                        logger.info("Рассылка автоматически возобновлена")
                    else:
                        # Рассылка еще отключена
                        return
                else:
                    # Рассылка отключена без таймера
                    return
            
            # Получаем сообщения, готовые к отправке (только для пользователей с bot_started = 1)
            pending_messages = self.db.get_pending_messages_for_active_users()
            
            if pending_messages:
                logger.info(f"Найдено {len(pending_messages)} сообщений для отправки")
            
            for message_id, user_id, message_number, text, photo_url in pending_messages:
                try:
                    # Небольшая задержка между отправками для избежания лимитов
                    await asyncio.sleep(0.1)
                    
                    # Получаем кнопки для этого сообщения
                    buttons = self.db.get_message_buttons(message_number)
                    reply_markup = None
                    
                    if buttons:
                        # Создаем клавиатуру с кнопками
                        keyboard = []
                        for button_id, button_text, button_url, position in buttons:
                            keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
                        
                        reply_markup = InlineKeyboardMarkup(keyboard)
                    
                    # Отправляем сообщение
                    if photo_url:
                        # Отправляем с фото
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=photo_url,
                            caption=text,
                            parse_mode='HTML',
                            reply_markup=reply_markup
                        )
                    else:
                        # Отправляем только текст
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            parse_mode='HTML',
                            disable_web_page_preview=True,
                            reply_markup=reply_markup
                        )
                    
                    # Отмечаем как отправленное
                    self.db.mark_message_sent(message_id)
                    
                    logger.info(f"Отправлено сообщение {message_number} пользователю {user_id}")
                    
                except Forbidden as e:
                    # Пользователь заблокировал бота
                    logger.warning(f"Пользователь {user_id} заблокировал бота: {e}")
                    # Отмечаем сообщение как отправленное, чтобы не пытаться снова
                    self.db.mark_message_sent(message_id)
                    # Деактивируем пользователя
                    self.db.deactivate_user(user_id)
                    
                except BadRequest as e:
                    # Неверный chat_id или другая ошибка
                    logger.error(f"BadRequest для пользователя {user_id}: {e}")
                    # Отмечаем как отправленное, чтобы не зацикливаться
                    self.db.mark_message_sent(message_id)
                    
                except Exception as e:
                    logger.error(f"Не удалось отправить сообщение {message_id} пользователю {user_id}: {e}")
                    # Не отмечаем как отправленное - попробуем еще раз позже
                        
        except Exception as e:
            logger.error(f"Критическая ошибка в send_scheduled_messages: {e}", exc_info=True)
    
    def reschedule_all_messages(self):
        """Перепланировать все сообщения для всех пользователей (при изменении задержек)"""
        # Эта функция может быть полезна, если админ изменил задержки
        # и хочет применить их ко всем будущим сообщениям
        # TODO: Реализовать при необходимости
        pass
