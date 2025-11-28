"""
Broadcast message module for database operations
"""

import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class MessagesMixin:
    """Mixin for broadcast message database operations"""

    def get_broadcast_message(self, message_number):
        """Получение сообщения рассылки по номеру"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT text, delay_hours, photo_url, video_url FROM broadcast_messages
                WHERE message_number = ?
            ''', (message_number,))
            result = cursor.fetchone()
            return result
        finally:
            if conn:
                conn.close()

    def get_all_broadcast_messages(self):
        """Получение всех сообщений рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT * FROM broadcast_messages ORDER BY message_number')
            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def add_broadcast_message(self, text="", delay_hours=0.05, photo_url=None, video_url=None):
        """Добавление нового сообщения рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Находим следующий доступный номер сообщения
            cursor.execute('SELECT MAX(message_number) FROM broadcast_messages')
            max_number = cursor.fetchone()[0]
            next_number = (max_number or 0) + 1

            cursor.execute('''
                INSERT INTO broadcast_messages (message_number, text, delay_hours, photo_url, video_url)
                VALUES (?, ?, ?, ?, ?)
            ''', (next_number, text, delay_hours, photo_url, video_url))

            conn.commit()
            logger.info(f"Добавлено сообщение рассылки #{next_number}")
            return next_number
        finally:
            if conn:
                conn.close()

    def update_broadcast_message(self, message_number, text=None, delay_hours=None, photo_url=None, video_url=None):
        """Обновление сообщения рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if text is not None:
                cursor.execute('''
                    UPDATE broadcast_messages SET text = ?
                    WHERE message_number = ?
                ''', (text, message_number))

            if delay_hours is not None:
                cursor.execute('''
                    UPDATE broadcast_messages SET delay_hours = ?
                    WHERE message_number = ?
                ''', (delay_hours, message_number))

            if photo_url is not None:
                cursor.execute('''
                    UPDATE broadcast_messages SET photo_url = ?
                    WHERE message_number = ?
                ''', (photo_url if photo_url else None, message_number))

            if video_url is not None:
                cursor.execute('''
                    UPDATE broadcast_messages SET video_url = ?
                    WHERE message_number = ?
                ''', (video_url if video_url else None, message_number))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def delete_broadcast_message(self, message_number):
        """Удаление сообщения рассылки и всех его запланированных отправок"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем запланированные отправки
            cursor.execute('''
                DELETE FROM scheduled_messages
                WHERE message_number = ? AND is_sent = 0
            ''', (message_number,))

            # Удаляем кнопки сообщения
            cursor.execute('''
                DELETE FROM message_buttons
                WHERE message_number = ?
            ''', (message_number,))

            # Удаляем само сообщение
            cursor.execute('''
                DELETE FROM broadcast_messages
                WHERE message_number = ?
            ''', (message_number,))

            conn.commit()
            logger.info(f"Удалено сообщение рассылки #{message_number}")
        finally:
            if conn:
                conn.close()

    def schedule_message(self, user_id, message_number, scheduled_time):
        """Планирование отправки сообщения с проверками"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Проверяем, существует ли пользователь и активен ли он
            cursor.execute('''
                SELECT user_id, is_active, bot_started, has_paid
                FROM users
                WHERE user_id = ?
            ''', (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                logger.error(f"❌ Попытка запланировать сообщение для несуществующего пользователя {user_id}")
                return False

            user_id_db, is_active, bot_started, has_paid = user_data

            if not is_active:
                logger.error(f"❌ Попытка запланировать сообщение для неактивного пользователя {user_id}")
                return False

            if not bot_started:
                logger.error(f"❌ Попытка запланировать сообщение для пользователя {user_id}, который не дал согласие")
                return False

            # НОВАЯ ПРОВЕРКА: Если пользователь уже оплатил, не планируем сообщения
            if has_paid:
                logger.info(f"ℹ️ Пользователь {user_id} уже оплатил, пропускаем планирование сообщения {message_number}")
                return True  # Возвращаем True, так как это не ошибка

            # Проверяем, существует ли сообщение рассылки
            cursor.execute('''
                SELECT message_number
                FROM broadcast_messages
                WHERE message_number = ?
            ''', (message_number,))
            message_data = cursor.fetchone()

            if not message_data:
                logger.error(f"❌ Попытка запланировать несуществующее сообщение {message_number}")
                return False

            # Проверяем, нет ли уже запланированного сообщения
            cursor.execute('''
                SELECT id FROM scheduled_messages
                WHERE user_id = ? AND message_number = ? AND is_sent = 0
            ''', (user_id, message_number))
            existing_message = cursor.fetchone()

            if existing_message:
                logger.debug(f"ℹ️ Сообщение {message_number} уже запланировано для пользователя {user_id}")
                return True

            # Планируем сообщение
            cursor.execute('''
                INSERT INTO scheduled_messages (user_id, message_number, scheduled_time)
                VALUES (?, ?, ?)
            ''', (user_id, message_number, scheduled_time))

            conn.commit()
            logger.debug(f"✅ Запланировано сообщение {message_number} для пользователя {user_id} на {scheduled_time}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при планировании сообщения для пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def get_pending_messages(self):
        """Получение сообщений, готовых к отправке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            current_time = datetime.now()
            cursor.execute('''
                SELECT sm.id, sm.user_id, sm.message_number, bm.text, bm.photo_url, bm.video_url
                FROM scheduled_messages sm
                JOIN broadcast_messages bm ON sm.message_number = bm.message_number
                WHERE sm.is_sent = 0 AND sm.scheduled_time <= ?
            ''', (current_time,))

            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def get_pending_messages_for_active_users(self):
        """Получение сообщений для активных пользователей, которые дали согласие и НЕ ОПЛАТИЛИ"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            current_time = datetime.now()

            # Сначала получаем общую статистику для логирования
            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 0')
            total_scheduled = cursor.fetchone()[0]

            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_messages sm
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.is_sent = 0 AND u.is_active = 1 AND u.bot_started = 1 AND u.has_paid = 0
            ''')
            active_scheduled = cursor.fetchone()[0]

            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_messages sm
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.is_sent = 0 AND sm.scheduled_time <= ? AND u.is_active = 1 AND u.bot_started = 1 AND u.has_paid = 0
            ''', (current_time,))
            ready_to_send = cursor.fetchone()[0]

            if total_scheduled > 0:
                logger.debug(f"📊 Статистика сообщений: всего запланировано {total_scheduled}, для активных неоплативших {active_scheduled}, готово к отправке {ready_to_send}")

            # Получаем сообщения готовые к отправке (ТОЛЬКО ДЛЯ НЕОПЛАТИВШИХ)
            cursor.execute('''
                SELECT sm.id, sm.user_id, sm.message_number, bm.text, bm.photo_url, bm.video_url, sm.scheduled_time
                FROM scheduled_messages sm
                JOIN broadcast_messages bm ON sm.message_number = bm.message_number
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.is_sent = 0
                AND sm.scheduled_time <= ?
                AND u.is_active = 1
                AND u.bot_started = 1
                AND u.has_paid = 0
                ORDER BY sm.scheduled_time ASC
            ''', (current_time,))

            messages = cursor.fetchall()

            # Логируем детали каждого сообщения
            for msg in messages:
                message_id, user_id, message_number, text, photo_url, video_url, scheduled_time = msg
                scheduled_dt = datetime.fromisoformat(scheduled_time) if isinstance(scheduled_time, str) else scheduled_time
                delay_minutes = int((current_time - scheduled_dt).total_seconds() / 60)
                logger.debug(f"📬 Сообщение {message_number} для пользователя {user_id} (опоздание: {delay_minutes} мин)")

            return [(m[0], m[1], m[2], m[3], m[4], m[5]) for m in messages]  # Возвращаем без scheduled_time
        finally:
            if conn:
                conn.close()

    def get_user_scheduled_messages(self, user_id):
        """Получение запланированных сообщений для пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, message_number, scheduled_time, is_sent
                FROM scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))

            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def get_user_scheduled_messages_count(self, user_id):
        """Получение количества запланированных сообщений для пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))
            count = cursor.fetchone()[0]

            return count

        except Exception as e:
            logger.error(f"❌ Ошибка при получении количества запланированных сообщений для пользователя {user_id}: {e}")
            return 0
        finally:
            if conn:
                conn.close()

    def mark_message_sent(self, message_id):
        """Отметка сообщения как отправленного"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE scheduled_messages SET is_sent = 1
                WHERE id = ?
            ''', (message_id,))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def cancel_user_messages(self, user_id):
        """Удаляет ВСЕ запланированные сообщения пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем ВСЕ сообщения (и отправленные, и запланированные)
            cursor.execute('''
                DELETE FROM scheduled_messages
                WHERE user_id = ?
            ''', (user_id,))

            affected = cursor.rowcount
            conn.commit()

            logger.info(f"🗑️ Удалено {affected} запланированных сообщений для пользователя {user_id}")
            return affected

        except Exception as e:
            logger.error(f"❌ Ошибка при удалении сообщений для пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return 0
        finally:
            if conn:
                conn.close()

    def cancel_remaining_messages(self, user_id):
        """Отмена оставшихся запланированных сообщений для оплатившего пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Считаем количество отменяемых сообщений
            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))
            count = cursor.fetchone()[0]

            # Удаляем неотправленные сообщения
            cursor.execute('''
                DELETE FROM scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))

            conn.commit()
            logger.info(f"🚫 Отменено {count} запланированных сообщений для оплатившего пользователя {user_id}")
            return count

        except Exception as e:
            logger.error(f"❌ Ошибка при отмене сообщений для пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return 0
        finally:
            if conn:
                conn.close()
