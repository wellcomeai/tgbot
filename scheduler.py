from datetime import datetime, timedelta
from telegram.ext import ContextTypes
from telegram.error import Forbidden, BadRequest
import logging
import asyncio

logger = logging.getLogger(__name__)

class MessageScheduler:
    def __init__(self, db):
        self.db = db
    
    async def schedule_user_messages(self, context: ContextTypes.DEFAULT_TYPE, user_id):
        """Запланировать отправку всех 7 сообщений для пользователя"""
        try:
            # Получаем все сообщения рассылки
            messages = self.db.get_all_broadcast_messages()
            current_time = datetime.now()
            
            for message_number, text, delay_hours in messages:
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
            # Получаем сообщения, готовые к отправке
            pending_messages = self.db.get_pending_messages()
            
            if pending_messages:
                logger.info(f"Найдено {len(pending_messages)} сообщений для отправки")
            
            for message_id, user_id, message_number, text in pending_messages:
                try:
                    # Небольшая задержка между отправками для избежания лимитов
                    await asyncio.sleep(0.1)
                    
                    # Отправляем сообщение
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=text,
                        parse_mode='HTML',
                        disable_web_page_preview=True
                    )
                    
                    # Отмечаем как отправленное
                    self.db.mark_message_sent(message_id)
                    
                    logger.info(f"Отправлено сообщение {message_number} пользователю {user_id}")
                    
                except Forbidden as e:
                    # Пользователь заблокировал бота
                    logger.warning(f"Пользователь {user_id} заблокировал бота: {e}")
                    # Отмечаем сообщение как отправленное, чтобы не пытаться снова
                    self.db.mark_message_sent(message_id)
                    # TODO: Можно добавить деактивацию пользователя
                    
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
