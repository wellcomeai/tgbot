"""
Paid broadcast message module for database operations
"""

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class PaidMessagesMixin:
    """Mixin for paid broadcast message database operations"""

    def get_paid_broadcast_message(self, message_number):
        """Получение сообщения рассылки для оплативших по номеру"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT text, delay_hours, photo_url, video_url FROM paid_broadcast_messages
                WHERE message_number = ?
            ''', (message_number,))
            result = cursor.fetchone()
            return result
        finally:
            if conn:
                conn.close()

    def get_all_paid_broadcast_messages(self):
        """Получение всех сообщений рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT * FROM paid_broadcast_messages ORDER BY message_number')
            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def add_paid_broadcast_message(self, text, delay_hours, photo_url=None, video_url=None):
        """Добавление нового сообщения рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Находим следующий доступный номер сообщения
            cursor.execute('SELECT MAX(message_number) FROM paid_broadcast_messages')
            max_number = cursor.fetchone()[0]
            next_number = (max_number or 0) + 1

            cursor.execute('''
                INSERT INTO paid_broadcast_messages (message_number, text, delay_hours, photo_url, video_url)
                VALUES (?, ?, ?, ?, ?)
            ''', (next_number, text, delay_hours, photo_url, video_url))

            conn.commit()
            logger.info(f"Добавлено сообщение рассылки для оплативших #{next_number}")
            return next_number
        finally:
            if conn:
                conn.close()

    def update_paid_broadcast_message(self, message_number, text=None, delay_hours=None, photo_url=None, video_url=None):
        """Обновление сообщения рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if text is not None:
                cursor.execute('''
                    UPDATE paid_broadcast_messages SET text = ?
                    WHERE message_number = ?
                ''', (text, message_number))

            if delay_hours is not None:
                cursor.execute('''
                    UPDATE paid_broadcast_messages SET delay_hours = ?
                    WHERE message_number = ?
                ''', (delay_hours, message_number))

            if photo_url is not None:
                cursor.execute('''
                    UPDATE paid_broadcast_messages SET photo_url = ?
                    WHERE message_number = ?
                ''', (photo_url if photo_url else None, message_number))

            if video_url is not None:
                cursor.execute('''
                    UPDATE paid_broadcast_messages SET video_url = ?
                    WHERE message_number = ?
                ''', (video_url if video_url else None, message_number))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def delete_paid_broadcast_message(self, message_number):
        """Удаление сообщения рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем запланированные отправки
            cursor.execute('''
                DELETE FROM paid_scheduled_messages
                WHERE message_number = ? AND is_sent = 0
            ''', (message_number,))

            # Удаляем кнопки сообщения
            cursor.execute('''
                DELETE FROM paid_message_buttons
                WHERE message_number = ?
            ''', (message_number,))

            # Удаляем само сообщение
            cursor.execute('''
                DELETE FROM paid_broadcast_messages
                WHERE message_number = ?
            ''', (message_number,))

            conn.commit()
            logger.info(f"Удалено сообщение рассылки для оплативших #{message_number}")
        finally:
            if conn:
                conn.close()

    def schedule_paid_message(self, user_id, message_number, scheduled_time):
        """Планирование отправки сообщения для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Проверяем, что пользователь оплатил
            cursor.execute('''
                SELECT has_paid FROM users WHERE user_id = ?
            ''', (user_id,))
            user_data = cursor.fetchone()

            if not user_data or not user_data[0]:
                logger.error(f"❌ Попытка запланировать платное сообщение для неоплатившего пользователя {user_id}")
                return False

            # Проверяем, нет ли уже запланированного сообщения
            cursor.execute('''
                SELECT id FROM paid_scheduled_messages
                WHERE user_id = ? AND message_number = ? AND is_sent = 0
            ''', (user_id, message_number))
            existing_message = cursor.fetchone()

            if existing_message:
                logger.debug(f"ℹ️ Платное сообщение {message_number} уже запланировано для пользователя {user_id}")
                return True

            # Планируем сообщение
            cursor.execute('''
                INSERT INTO paid_scheduled_messages (user_id, message_number, scheduled_time)
                VALUES (?, ?, ?)
            ''', (user_id, message_number, scheduled_time))

            conn.commit()
            logger.debug(f"✅ Запланировано платное сообщение {message_number} для пользователя {user_id} на {scheduled_time}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при планировании платного сообщения для пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def get_pending_paid_messages(self):
        """Получение платных сообщений, готовых к отправке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            current_time = datetime.now()
            cursor.execute('''
                SELECT psm.id, psm.user_id, psm.message_number, pbm.text, pbm.photo_url, pbm.video_url, psm.scheduled_time
                FROM paid_scheduled_messages psm
                JOIN paid_broadcast_messages pbm ON psm.message_number = pbm.message_number
                JOIN users u ON psm.user_id = u.user_id
                WHERE psm.is_sent = 0
                AND psm.scheduled_time <= ?
                AND u.is_active = 1
                AND u.has_paid = 1
                ORDER BY psm.scheduled_time ASC
            ''', (current_time,))

            messages = cursor.fetchall()
            return [(m[0], m[1], m[2], m[3], m[4], m[5]) for m in messages]  # Возвращаем без scheduled_time
        finally:
            if conn:
                conn.close()

    def get_user_paid_scheduled_messages(self, user_id):
        """Получение запланированных платных сообщений для пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, message_number, scheduled_time, is_sent
                FROM paid_scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))

            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def mark_paid_message_sent(self, message_id):
        """Отметка платного сообщения как отправленного"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE paid_scheduled_messages SET is_sent = 1
                WHERE id = ?
            ''', (message_id,))

            conn.commit()
        finally:
            if conn:
                conn.close()
