"""
Payment management module for database operations
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


class PaymentsMixin:
    """Mixin for payment-related database operations"""

    def log_payment(self, user_id, amount, payment_status, utm_source=None, utm_id=None):
        """Логирование платежа"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO payments (user_id, amount, payment_status, utm_source, utm_id)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, amount, payment_status, utm_source, utm_id))

            conn.commit()
            logger.info(f"💰 Зафиксирован платеж: пользователь {user_id}, {amount}, статус {payment_status}")
            return cursor.lastrowid

        except Exception as e:
            logger.error(f"❌ Ошибка при логировании платежа: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            if conn:
                conn.close()

    def get_payment_statistics(self):
        """Получение статистики платежей"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Общее количество платежей
            cursor.execute('SELECT COUNT(*) FROM payments WHERE payment_status = "success"')
            total_payments = cursor.fetchone()[0]

            # Общее количество пользователей, начавших разговор с ботом
            cursor.execute('SELECT COUNT(*) FROM users WHERE bot_started = 1 AND is_active = 1')
            total_users = cursor.fetchone()[0]

            # Количество оплативших
            cursor.execute('SELECT COUNT(*) FROM users WHERE has_paid = 1')
            paid_users = cursor.fetchone()[0]

            # Конверсия
            conversion_rate = (paid_users / total_users * 100) if total_users > 0 else 0

            # Средний чек
            cursor.execute('SELECT AVG(CAST(amount AS REAL)) FROM payments WHERE payment_status = "success" AND amount != ""')
            avg_amount_result = cursor.fetchone()
            avg_amount = avg_amount_result[0] if avg_amount_result[0] is not None else 0

            # Последние платежи
            cursor.execute('''
                SELECT p.user_id, u.first_name, u.username, p.amount, p.created_at
                FROM payments p
                JOIN users u ON p.user_id = u.user_id
                WHERE p.payment_status = "success"
                ORDER BY p.created_at DESC
                LIMIT 10
            ''')
            recent_payments = cursor.fetchall()

            # Платежи по UTM источникам
            cursor.execute('''
                SELECT utm_source, COUNT(*) as count
                FROM payments
                WHERE payment_status = "success" AND utm_source IS NOT NULL
                GROUP BY utm_source
            ''')
            utm_sources = cursor.fetchall()

            return {
                'total_payments': total_payments,
                'total_users': total_users,
                'paid_users': paid_users,
                'conversion_rate': round(conversion_rate, 2),
                'avg_amount': round(avg_amount, 2) if avg_amount else 0,
                'recent_payments': recent_payments,
                'utm_sources': utm_sources
            }

        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики платежей: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_payment_success_message(self):
        """Получение сообщения об успешной оплате"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM settings WHERE key = "payment_success_message"')
            message = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "payment_success_photo_url"')
            photo = cursor.fetchone()

            cursor.execute('SELECT value FROM settings WHERE key = "payment_success_video_url"')
            video = cursor.fetchone()

            return {
                'text': message[0] if message else None,
                'photo_url': photo[0] if photo and photo[0] else None,
                'video_url': video[0] if video and video[0] else None
            }

        except Exception as e:
            logger.error(f"❌ Ошибка при получении сообщения об оплате: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def set_payment_success_message(self, text, photo_url=None, video_url=None):
        """Установка сообщения об успешной оплате"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO settings (key, value)
                VALUES ('payment_success_message', ?)
            ''', (text,))

            if photo_url is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO settings (key, value)
                    VALUES ('payment_success_photo_url', ?)
                ''', (photo_url,))

            if video_url is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO settings (key, value)
                    VALUES ('payment_success_video_url', ?)
                ''', (video_url,))

            conn.commit()
            logger.info("✅ Сообщение об успешной оплате обновлено")

        except Exception as e:
            logger.error(f"❌ Ошибка при установке сообщения об оплате: {e}")
        finally:
            if conn:
                conn.close()

    def get_renewal_message(self):
        """Получение сообщения о продлении подписки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('SELECT value FROM renewal_settings WHERE key = "renewal_message"')
            message = cursor.fetchone()

            cursor.execute('SELECT value FROM renewal_settings WHERE key = "renewal_photo_url"')
            photo = cursor.fetchone()

            cursor.execute('SELECT value FROM renewal_settings WHERE key = "renewal_video_url"')
            video = cursor.fetchone()

            cursor.execute('SELECT value FROM renewal_settings WHERE key = "renewal_button_text"')
            button_text = cursor.fetchone()

            cursor.execute('SELECT value FROM renewal_settings WHERE key = "renewal_button_url"')
            button_url = cursor.fetchone()

            return {
                'text': message[0] if message else None,
                'photo_url': photo[0] if photo and photo[0] else None,
                'video_url': video[0] if video and video[0] else None,
                'button_text': button_text[0] if button_text else None,
                'button_url': button_url[0] if button_url and button_url[0] else None
            }

        except Exception as e:
            logger.error(f"❌ Ошибка при получении сообщения о продлении: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def set_renewal_message(self, text=None, photo_url=None, video_url=None, button_text=None, button_url=None):
        """Установка сообщения о продлении подписки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if text is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO renewal_settings (key, value)
                    VALUES ('renewal_message', ?)
                ''', (text,))

            if photo_url is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO renewal_settings (key, value)
                    VALUES ('renewal_photo_url', ?)
                ''', (photo_url,))

            if video_url is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO renewal_settings (key, value)
                    VALUES ('renewal_video_url', ?)
                ''', (video_url,))

            if button_text is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO renewal_settings (key, value)
                    VALUES ('renewal_button_text', ?)
                ''', (button_text,))

            if button_url is not None:
                cursor.execute('''
                    INSERT OR REPLACE INTO renewal_settings (key, value)
                    VALUES ('renewal_button_url', ?)
                ''', (button_url,))

            conn.commit()
            logger.info("✅ Сообщение о продлении подписки обновлено")

        except Exception as e:
            logger.error(f"❌ Ошибка при установке сообщения о продлении: {e}")
        finally:
            if conn:
                conn.close()
