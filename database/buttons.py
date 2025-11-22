"""
Button management module for database operations
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


class ButtonsMixin:
    """Mixin for button-related database operations"""

    # ===== МЕТОДЫ ДЛЯ КНОПОК СООБЩЕНИЙ РАССЫЛКИ =====

    def get_message_buttons(self, message_number):
        """Получение всех кнопок для конкретного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM message_buttons
                WHERE message_number = ?
                ORDER BY position
            ''', (message_number,))

            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def add_message_button(self, message_number, button_text, button_url, position=1):
        """Добавление кнопки к сообщению"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO message_buttons (message_number, button_text, button_url, position)
                VALUES (?, ?, ?, ?)
            ''', (message_number, button_text, button_url, position))

            conn.commit()
            logger.info(f"Добавлена кнопка к сообщению #{message_number}")
        finally:
            if conn:
                conn.close()

    def update_message_button(self, button_id, button_text=None, button_url=None):
        """Обновление кнопки сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if button_text is not None:
                cursor.execute('''
                    UPDATE message_buttons SET button_text = ?
                    WHERE id = ?
                ''', (button_text, button_id))

            if button_url is not None:
                cursor.execute('''
                    UPDATE message_buttons SET button_url = ?
                    WHERE id = ?
                ''', (button_url, button_id))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def delete_message_button(self, button_id):
        """Удаление кнопки сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('DELETE FROM message_buttons WHERE id = ?', (button_id,))
            conn.commit()
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ КНОПОК ПЛАТНЫХ СООБЩЕНИЙ =====

    def get_paid_message_buttons(self, message_number):
        """Получение всех кнопок для конкретного сообщения оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM paid_message_buttons
                WHERE message_number = ?
                ORDER BY position
            ''', (message_number,))

            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def add_paid_message_button(self, message_number, button_text, button_url, position=1):
        """Добавление кнопки к сообщению для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO paid_message_buttons (message_number, button_text, button_url, position)
                VALUES (?, ?, ?, ?)
            ''', (message_number, button_text, button_url, position))

            conn.commit()
            logger.info(f"Добавлена кнопка к сообщению для оплативших #{message_number}")
        finally:
            if conn:
                conn.close()

    def update_paid_message_button(self, button_id, button_text=None, button_url=None):
        """Обновление кнопки сообщения для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if button_text is not None:
                cursor.execute('''
                    UPDATE paid_message_buttons SET button_text = ?
                    WHERE id = ?
                ''', (button_text, button_id))

            if button_url is not None:
                cursor.execute('''
                    UPDATE paid_message_buttons SET button_url = ?
                    WHERE id = ?
                ''', (button_url, button_id))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def delete_paid_message_button(self, button_id):
        """Удаление кнопки сообщения для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('DELETE FROM paid_message_buttons WHERE id = ?', (button_id,))
            conn.commit()
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ КНОПОК ПРИВЕТСТВЕННОГО СООБЩЕНИЯ =====

    def get_welcome_buttons(self):
        """Получение всех кнопок приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, position
                FROM welcome_buttons
                ORDER BY position
            ''')
            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def get_welcome_button_by_text(self, button_text):
        """Получение кнопки приветствия по тексту"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, position
                FROM welcome_buttons
                WHERE button_text = ?
            ''', (button_text,))

            button = cursor.fetchone()
            return button
        finally:
            if conn:
                conn.close()

    def add_welcome_button(self, button_text, position=1):
        """Добавление кнопки к приветственному сообщению"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO welcome_buttons (button_text, position)
                VALUES (?, ?)
            ''', (button_text, position))

            button_id = cursor.lastrowid
            conn.commit()

            logger.info(f"Добавлена механическая кнопка приветствия: {button_text}")
            return button_id
        finally:
            if conn:
                conn.close()

    def update_welcome_button(self, button_id, button_text=None):
        """Обновление кнопки приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if button_text is not None:
                cursor.execute('''
                    UPDATE welcome_buttons SET button_text = ? WHERE id = ?
                ''', (button_text, button_id))

            conn.commit()
            logger.info(f"Обновлена кнопка приветствия #{button_id}")
        finally:
            if conn:
                conn.close()

    def delete_welcome_button(self, button_id):
        """Удаление кнопки приветственного сообщения и всех связанных сообщений"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем связанные последующие сообщения
            cursor.execute('DELETE FROM welcome_follow_messages WHERE welcome_button_id = ?', (button_id,))

            # Удаляем саму кнопку
            cursor.execute('DELETE FROM welcome_buttons WHERE id = ?', (button_id,))

            conn.commit()
            logger.info(f"Удалена кнопка приветствия #{button_id} со всеми связанными сообщениями")
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ ПОСЛЕДУЮЩИХ СООБЩЕНИЙ ПОСЛЕ КНОПОК =====

    def get_welcome_follow_messages(self, welcome_button_id):
        """Получение всех последующих сообщений для кнопки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, message_number, text, photo_url, video_url
                FROM welcome_follow_messages
                WHERE welcome_button_id = ?
                ORDER BY message_number
            ''', (welcome_button_id,))

            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()

    def add_welcome_follow_message(self, welcome_button_id, text, photo_url=None, video_url=None):
        """Добавление последующего сообщения для кнопки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Определяем номер сообщения
            cursor.execute('''
                SELECT MAX(message_number) FROM welcome_follow_messages
                WHERE welcome_button_id = ?
            ''', (welcome_button_id,))
            max_number = cursor.fetchone()[0]
            message_number = (max_number or 0) + 1

            cursor.execute('''
                INSERT INTO welcome_follow_messages (welcome_button_id, message_number, text, photo_url, video_url)
                VALUES (?, ?, ?, ?, ?)
            ''', (welcome_button_id, message_number, text, photo_url, video_url))

            conn.commit()
            logger.info(f"Добавлено последующее сообщение {message_number} для кнопки {welcome_button_id}")
            return message_number
        finally:
            if conn:
                conn.close()

    def update_welcome_follow_message(self, message_id, text=None, photo_url=None, video_url=None):
        """Обновление последующего сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if text is not None:
                cursor.execute('''
                    UPDATE welcome_follow_messages SET text = ? WHERE id = ?
                ''', (text, message_id))

            if photo_url is not None:
                cursor.execute('''
                    UPDATE welcome_follow_messages SET photo_url = ? WHERE id = ?
                ''', (photo_url if photo_url else None, message_id))

            if video_url is not None:
                cursor.execute('''
                    UPDATE welcome_follow_messages SET video_url = ? WHERE id = ?
                ''', (video_url if video_url else None, message_id))

            conn.commit()
            logger.info(f"Обновлено последующее сообщение #{message_id}")
        finally:
            if conn:
                conn.close()

    def delete_welcome_follow_message(self, message_id):
        """Удаление последующего сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('DELETE FROM welcome_follow_messages WHERE id = ?', (message_id,))
            conn.commit()
            logger.info(f"Удалено последующее сообщение #{message_id}")
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ КНОПОК ПРОЩАЛЬНОГО СООБЩЕНИЯ =====

    def get_goodbye_buttons(self):
        """Получение всех кнопок прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM goodbye_buttons
                ORDER BY position
            ''')
            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def get_goodbye_button_by_text(self, button_text):
        """Получить кнопку прощания по тексту"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM goodbye_buttons
                WHERE button_text = ?
            ''', (button_text,))
            button = cursor.fetchone()
            return button
        finally:
            if conn:
                conn.close()

    def add_goodbye_button(self, button_text, button_url):
        """Добавить инлайн кнопку прощания с URL"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Получаем максимальную позицию
            cursor.execute('SELECT MAX(position) FROM goodbye_buttons')
            max_pos = cursor.fetchone()[0]
            position = (max_pos or 0) + 1

            cursor.execute('''
                INSERT INTO goodbye_buttons (button_text, button_url, position)
                VALUES (?, ?, ?)
            ''', (button_text, button_url, position))

            button_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Добавлена инлайн кнопка прощания: {button_text} -> {button_url}")
            return button_id
        finally:
            if conn:
                conn.close()

    def update_goodbye_button(self, button_id, button_text=None, button_url=None):
        """Обновление инлайн кнопки прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Обновляем оба поля одновременно если переданы оба значения
            if button_text is not None and button_url is not None:
                cursor.execute('''
                    UPDATE goodbye_buttons
                    SET button_text = ?, button_url = ?
                    WHERE id = ?
                ''', (button_text, button_url, button_id))
            # Обновляем только текст
            elif button_text is not None:
                cursor.execute('''
                    UPDATE goodbye_buttons
                    SET button_text = ?
                    WHERE id = ?
                ''', (button_text, button_id))
            # Обновляем только URL
            elif button_url is not None:
                cursor.execute('''
                    UPDATE goodbye_buttons
                    SET button_url = ?
                    WHERE id = ?
                ''', (button_url, button_id))

            conn.commit()
            logger.info(f"Обновлена инлайн кнопка прощания #{button_id}")
        finally:
            if conn:
                conn.close()

    def delete_goodbye_button(self, button_id):
        """Удаление инлайн кнопки прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Получаем позицию удаляемой кнопки
            cursor.execute('SELECT position FROM goodbye_buttons WHERE id = ?', (button_id,))
            result = cursor.fetchone()

            if result:
                position = result[0]

                # Удаляем кнопку
                cursor.execute('DELETE FROM goodbye_buttons WHERE id = ?', (button_id,))

                # Обновляем позиции оставшихся кнопок
                cursor.execute('''
                    UPDATE goodbye_buttons
                    SET position = position - 1
                    WHERE position > ?
                ''', (position,))

                conn.commit()
                logger.info(f"Удалена инлайн кнопка прощания #{button_id}")
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ КНОПОК ЗАПЛАНИРОВАННЫХ РАССЫЛОК =====

    def get_scheduled_broadcast_buttons(self, broadcast_id):
        """Получение кнопок для запланированной рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM scheduled_broadcast_buttons
                WHERE broadcast_id = ?
                ORDER BY position
            ''', (broadcast_id,))

            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def add_scheduled_broadcast_button(self, broadcast_id, button_text, button_url, position=1):
        """Добавление кнопки к запланированной рассылке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO scheduled_broadcast_buttons (broadcast_id, button_text, button_url, position)
                VALUES (?, ?, ?, ?)
            ''', (broadcast_id, button_text, button_url, position))

            conn.commit()
            logger.info(f"Добавлена кнопка к рассылке #{broadcast_id}")
        finally:
            if conn:
                conn.close()

    def delete_scheduled_broadcast_button(self, button_id):
        """Удаление кнопки запланированной рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('DELETE FROM scheduled_broadcast_buttons WHERE id = ?', (button_id,))
            conn.commit()
            logger.info(f"Удалена кнопка запланированной рассылки #{button_id}")
        finally:
            if conn:
                conn.close()

    # ===== МЕТОДЫ ДЛЯ КНОПОК ПЛАТНЫХ ЗАПЛАНИРОВАННЫХ РАССЫЛОК =====

    def get_paid_scheduled_broadcast_buttons(self, broadcast_id):
        """Получение кнопок для запланированной рассылки оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, button_text, button_url, position
                FROM paid_scheduled_broadcast_buttons
                WHERE broadcast_id = ?
                ORDER BY position
            ''', (broadcast_id,))

            buttons = cursor.fetchall()
            return buttons
        finally:
            if conn:
                conn.close()

    def add_paid_scheduled_broadcast_button(self, broadcast_id, button_text, button_url, position=1):
        """Добавление кнопки к запланированной рассылке для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO paid_scheduled_broadcast_buttons (broadcast_id, button_text, button_url, position)
                VALUES (?, ?, ?, ?)
            ''', (broadcast_id, button_text, button_url, position))

            conn.commit()
            logger.info(f"Добавлена кнопка к рассылке для оплативших #{broadcast_id}")
        finally:
            if conn:
                conn.close()
