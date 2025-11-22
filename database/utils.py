"""
Database utilities module for database operations
"""

import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UtilsMixin:
    """Mixin for database utility operations"""

    def get_database_health_check(self):
        """Проверка состояния базы данных"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            health_info = {}

            # Общая статистика
            cursor.execute('SELECT COUNT(*) FROM users')
            health_info['total_users'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
            health_info['active_users'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM users WHERE bot_started = 1')
            health_info['bot_started_users'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM users WHERE has_paid = 1')
            health_info['paid_users'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 0')
            health_info['pending_messages'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 1')
            health_info['sent_messages'] = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM payments')
            health_info['total_payments'] = cursor.fetchone()[0]

            # Проверка на потерянные сообщения (запланированные для неактивных пользователей)
            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_messages sm
                JOIN users u ON sm.user_id = u.user_id
                WHERE sm.is_sent = 0 AND (u.is_active = 0 OR u.bot_started = 0)
            ''')
            health_info['orphaned_messages'] = cursor.fetchone()[0]

            # Проверка на дубликаты
            cursor.execute('''
                SELECT COUNT(*) FROM (
                    SELECT user_id, message_number, COUNT(*) as cnt
                    FROM scheduled_messages
                    WHERE is_sent = 0
                    GROUP BY user_id, message_number
                    HAVING cnt > 1
                )
            ''')
            health_info['duplicate_messages'] = cursor.fetchone()[0]

            return health_info

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке состояния базы данных: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def cleanup_old_scheduled_messages(self, days_old=7):
        """Очистка старых отправленных сообщений"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)

            cursor.execute('''
                DELETE FROM scheduled_messages
                WHERE is_sent = 1 AND scheduled_time < ?
            ''', (cutoff_date,))

            deleted_count = cursor.rowcount
            conn.commit()

            if deleted_count > 0:
                logger.info(f"🧹 Очищено {deleted_count} старых отправленных сообщений")

            return deleted_count

        except Exception as e:
            logger.error(f"❌ Ошибка при очистке старых сообщений: {e}")
            return 0
        finally:
            if conn:
                conn.close()
