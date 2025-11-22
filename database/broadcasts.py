"""
Broadcast management module for database operations
"""

import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class BroadcastsMixin:
    """Mixin for broadcast management database operations"""

    def get_scheduled_broadcasts(self, include_sent=False):
        """Получение запланированных массовых рассылок"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if include_sent:
                cursor.execute('''
                    SELECT id, message_text, photo_url, video_url, scheduled_time, is_sent, created_at
                    FROM scheduled_broadcasts
                    ORDER BY scheduled_time
                ''')
            else:
                cursor.execute('''
                    SELECT id, message_text, photo_url, video_url, scheduled_time, is_sent, created_at
                    FROM scheduled_broadcasts
                    WHERE is_sent = 0
                    ORDER BY scheduled_time
                ''')

            broadcasts = cursor.fetchall()
            return broadcasts
        finally:
            if conn:
                conn.close()

    def add_scheduled_broadcast(self, message_text, scheduled_time, photo_url=None, video_url=None):
        """Добавление запланированной массовой рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO scheduled_broadcasts (message_text, photo_url, video_url, scheduled_time)
                VALUES (?, ?, ?, ?)
            ''', (message_text, photo_url, video_url, scheduled_time))

            broadcast_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Добавлена запланированная рассылка #{broadcast_id} на {scheduled_time}")
            return broadcast_id
        finally:
            if conn:
                conn.close()

    def delete_scheduled_broadcast(self, broadcast_id):
        """Удаление запланированной рассылки и всех её кнопок"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем кнопки
            cursor.execute('DELETE FROM scheduled_broadcast_buttons WHERE broadcast_id = ?', (broadcast_id,))

            # Удаляем рассылку
            cursor.execute('DELETE FROM scheduled_broadcasts WHERE id = ?', (broadcast_id,))

            conn.commit()
            logger.info(f"Удалена запланированная рассылка #{broadcast_id}")
        finally:
            if conn:
                conn.close()

    def mark_broadcast_sent(self, broadcast_id):
        """Отметить рассылку как отправленную"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE scheduled_broadcasts SET is_sent = 1 WHERE id = ?
            ''', (broadcast_id,))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def get_pending_broadcasts(self):
        """Получение рассылок, готовых к отправке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            current_time = datetime.now()
            cursor.execute('''
                SELECT id, message_text, photo_url, video_url, scheduled_time
                FROM scheduled_broadcasts
                WHERE is_sent = 0 AND scheduled_time <= ?
                ORDER BY scheduled_time
            ''', (current_time,))

            broadcasts = cursor.fetchall()
            return broadcasts
        finally:
            if conn:
                conn.close()

    def get_paid_scheduled_broadcasts(self, include_sent=False):
        """Получение запланированных массовых рассылок для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if include_sent:
                cursor.execute('''
                    SELECT id, message_text, photo_url, video_url, scheduled_time, is_sent, created_at
                    FROM paid_scheduled_broadcasts
                    ORDER BY scheduled_time
                ''')
            else:
                cursor.execute('''
                    SELECT id, message_text, photo_url, video_url, scheduled_time, is_sent, created_at
                    FROM paid_scheduled_broadcasts
                    WHERE is_sent = 0
                    ORDER BY scheduled_time
                ''')

            broadcasts = cursor.fetchall()
            return broadcasts
        finally:
            if conn:
                conn.close()

    def add_paid_scheduled_broadcast(self, message_text, scheduled_time, photo_url=None, video_url=None):
        """Добавление запланированной массовой рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO paid_scheduled_broadcasts (message_text, photo_url, video_url, scheduled_time)
                VALUES (?, ?, ?, ?)
            ''', (message_text, photo_url, video_url, scheduled_time))

            broadcast_id = cursor.lastrowid
            conn.commit()
            logger.info(f"Добавлена запланированная рассылка для оплативших #{broadcast_id} на {scheduled_time}")
            return broadcast_id
        finally:
            if conn:
                conn.close()

    def delete_paid_scheduled_broadcast(self, broadcast_id):
        """Удаление запланированной рассылки для оплативших"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Удаляем кнопки
            cursor.execute('DELETE FROM paid_scheduled_broadcast_buttons WHERE broadcast_id = ?', (broadcast_id,))

            # Удаляем рассылку
            cursor.execute('DELETE FROM paid_scheduled_broadcasts WHERE id = ?', (broadcast_id,))

            conn.commit()
            logger.info(f"Удалена запланированная рассылка для оплативших #{broadcast_id}")
        finally:
            if conn:
                conn.close()

    def mark_paid_broadcast_sent(self, broadcast_id):
        """Отметить рассылку для оплативших как отправленную"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE paid_scheduled_broadcasts SET is_sent = 1 WHERE id = ?
            ''', (broadcast_id,))

            conn.commit()
        finally:
            if conn:
                conn.close()

    def get_pending_paid_broadcasts(self):
        """Получение рассылок для оплативших, готовых к отправке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            current_time = datetime.now()
            cursor.execute('''
                SELECT id, message_text, photo_url, video_url, scheduled_time
                FROM paid_scheduled_broadcasts
                WHERE is_sent = 0 AND scheduled_time <= ?
                ORDER BY scheduled_time
            ''', (current_time,))

            broadcasts = cursor.fetchall()
            return broadcasts
        finally:
            if conn:
                conn.close()

    def get_broadcast_status(self):
        """Получение текущего статуса рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM broadcast_settings WHERE key = "broadcast_enabled"')
            enabled = cursor.fetchone()

            cursor.execute('SELECT value FROM broadcast_settings WHERE key = "auto_resume_time"')
            resume_time = cursor.fetchone()

            return {
                'enabled': enabled[0] == '1' if enabled else True,
                'auto_resume_time': resume_time[0] if resume_time and resume_time[0] else None
            }
        finally:
            if conn:
                conn.close()

    def set_broadcast_status(self, enabled, auto_resume_time=None):
        """Установка статуса рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE broadcast_settings SET value = ? WHERE key = "broadcast_enabled"
            ''', ('1' if enabled else '0',))

            if auto_resume_time is not None:
                cursor.execute('''
                    UPDATE broadcast_settings SET value = ? WHERE key = "auto_resume_time"
                ''', (auto_resume_time,))

            conn.commit()
        finally:
            if conn:
                conn.close()
