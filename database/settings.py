"""
Settings management module for database operations
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


class SettingsMixin:
    """Mixin for settings-related database operations"""

    def get_welcome_message(self):
        """Получение приветственного сообщения и фото"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM settings WHERE key = "welcome_message"')
            message = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "welcome_photo_url"')
            photo = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "welcome_video_url"')
            video = cursor.fetchone()

            return {
                'text': message[0] if message else "Добро пожаловать!",
                'photo': photo[0] if photo and photo[0] else None,
                'video': video[0] if video and video[0] else None
            }
        finally:
            if conn:
                conn.close()

    def set_welcome_message(self, message, photo_url=None, video_url=None):
        """Установка приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE settings SET value = ? WHERE key = "welcome_message"
            ''', (message,))

            if photo_url is not None:
                cursor.execute('''
                    UPDATE settings SET value = ? WHERE key = "welcome_photo_url"
                ''', (photo_url,))

            if video_url is not None:
                cursor.execute('''
                    UPDATE settings SET value = ? WHERE key = "welcome_video_url"
                ''', (video_url,))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def get_goodbye_message(self):
        """Получение прощального сообщения и фото"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM settings WHERE key = "goodbye_message"')
            message = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "goodbye_photo_url"')
            photo = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "goodbye_video_url"')
            video = cursor.fetchone()

            return {
                'text': message[0] if message else "До свидания!",
                'photo': photo[0] if photo and photo[0] else None,
                'video': video[0] if video and video[0] else None
            }
        finally:
            if conn:
                conn.close()

    def set_goodbye_message(self, message, photo_url=None, video_url=None):
        """Установка прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE settings SET value = ? WHERE key = "goodbye_message"
            ''', (message,))

            if photo_url is not None:
                cursor.execute('''
                    UPDATE settings SET value = ? WHERE key = "goodbye_photo_url"
                ''', (photo_url,))

            if video_url is not None:
                cursor.execute('''
                    UPDATE settings SET value = ? WHERE key = "goodbye_video_url"
                ''', (video_url,))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def is_success_message_enabled(self):
        """Проверить включено ли сообщение подтверждения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM settings WHERE key = "success_message_enabled"')
            result = cursor.fetchone()

            # По умолчанию включено
            if result is None:
                return True

            return result[0] == "1" or result[0] == "True"

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке статуса сообщения подтверждения: {e}")
            return True  # По умолчанию включено в случае ошибки
        finally:
            if conn:
                conn.close()

    def set_success_message_enabled(self, enabled: bool):
        """Включить/выключить сообщение подтверждения"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            value = "1" if enabled else "0"
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value)
                VALUES (?, ?)
            ''', ('success_message_enabled', value))

            conn.commit()
            logger.info(f"✅ Сообщение подтверждения {'включено' if enabled else 'выключено'}")

        except Exception as e:
            logger.error(f"❌ Ошибка при изменении статуса сообщения подтверждения: {e}")
            try:
                conn.rollback()
            except:
                pass
        finally:
            if conn:
                conn.close()
