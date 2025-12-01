"""
Funnel analytics module for database operations
"""

import sqlite3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class FunnelMixin:
    """Mixin for funnel analytics database operations"""

    def log_message_delivery(self, user_id, message_number):
        """Логирование отправки сообщения пользователю"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO message_deliveries (user_id, message_number)
                VALUES (?, ?)
            ''', (user_id, message_number))

            conn.commit()
            logger.debug(f"📬 Залогирована отправка сообщения {message_number} пользователю {user_id}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при логировании отправки сообщения {message_number} пользователю {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def log_button_click(self, user_id, message_number, button_id, button_type, button_text):
        """
        Логирование клика по кнопке

        Args:
            user_id: ID пользователя
            message_number: Номер сообщения
            button_id: ID кнопки (None для callback кнопки "следующее сообщение")
            button_type: Тип кнопки ('callback' или 'url')
            button_text: Текст кнопки
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO button_clicks (user_id, message_number, button_id, button_type, button_text)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, message_number, button_id, button_type, button_text))

            conn.commit()
            logger.debug(f"🔘 Залогирован клик по кнопке '{button_text}' ({button_type}) в сообщении {message_number} от пользователя {user_id}")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при логировании клика по кнопке: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def get_funnel_data(self):
        """
        Получение данных воронки для всех сообщений

        Returns:
            List[Dict]: Список с данными по каждому сообщению:
            {
                'message_number': int,
                'message_text': str (первые 50 символов),
                'delivered': int (кол-во получивших),
                'clicked_callback': int (кол-во кликнувших callback кнопку),
                'clicked_url': int (кол-во кликнувших URL кнопку),
                'conversion_rate': float (% кликнувших callback),
                'dropped': int (кол-во отвалившихся),
                'drop_rate': float (% отвалившихся)
            }
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Получаем все сообщения рассылки
            cursor.execute('''
                SELECT message_number, text FROM broadcast_messages
                ORDER BY message_number
            ''')
            messages = cursor.fetchall()

            funnel_data = []

            for message_number, message_text in messages:
                # Количество получивших сообщение
                cursor.execute('''
                    SELECT COUNT(DISTINCT user_id)
                    FROM message_deliveries
                    WHERE message_number = ?
                ''', (message_number,))
                delivered = cursor.fetchone()[0]

                if delivered == 0:
                    # Сообщение еще никому не отправлялось
                    funnel_data.append({
                        'message_number': message_number,
                        'message_text': message_text[:50] + ('...' if len(message_text) > 50 else ''),
                        'delivered': 0,
                        'clicked_callback': 0,
                        'clicked_url': 0,
                        'conversion_rate': 0.0,
                        'dropped': 0,
                        'drop_rate': 0.0
                    })
                    continue

                # Количество кликнувших callback кнопку (в течение 10 минут)
                cursor.execute('''
                    SELECT COUNT(DISTINCT bc.user_id)
                    FROM button_clicks bc
                    JOIN message_deliveries md ON bc.user_id = md.user_id AND bc.message_number = md.message_number
                    WHERE bc.message_number = ?
                    AND bc.button_type = 'callback'
                    AND (julianday(bc.clicked_at) - julianday(md.delivered_at)) * 24 * 60 <= 10
                ''', (message_number,))
                clicked_callback = cursor.fetchone()[0]

                # Количество кликнувших URL кнопку (в течение 10 минут)
                cursor.execute('''
                    SELECT COUNT(DISTINCT bc.user_id)
                    FROM button_clicks bc
                    JOIN message_deliveries md ON bc.user_id = md.user_id AND bc.message_number = md.message_number
                    WHERE bc.message_number = ?
                    AND bc.button_type = 'url'
                    AND (julianday(bc.clicked_at) - julianday(md.delivered_at)) * 24 * 60 <= 10
                ''', (message_number,))
                clicked_url = cursor.fetchone()[0]

                # Конверсия по callback кнопкам (основная метрика)
                conversion_rate = (clicked_callback / delivered * 100) if delivered > 0 else 0

                # Отвалившиеся = не кликнули callback кнопку в течение 10 минут
                dropped = delivered - clicked_callback
                drop_rate = (dropped / delivered * 100) if delivered > 0 else 0

                funnel_data.append({
                    'message_number': message_number,
                    'message_text': message_text[:50] + ('...' if len(message_text) > 50 else ''),
                    'delivered': delivered,
                    'clicked_callback': clicked_callback,
                    'clicked_url': clicked_url,
                    'conversion_rate': round(conversion_rate, 2),
                    'dropped': dropped,
                    'drop_rate': round(drop_rate, 2)
                })

            return funnel_data

        except Exception as e:
            logger.error(f"❌ Ошибка при получении данных воронки: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_message_details(self, message_number):
        """
        Получение детальной статистики по конкретному сообщению

        Returns:
            Dict: {
                'message_number': int,
                'message_text': str,
                'delivered': int,
                'clicked_callback_count': int,
                'clicked_url_count': int,
                'not_clicked': int,
                'avg_reaction_time_seconds': float,
                'button_details': List[Dict] - детализация по каждой кнопке
            }
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Получаем текст сообщения
            cursor.execute('''
                SELECT text FROM broadcast_messages WHERE message_number = ?
            ''', (message_number,))
            message_data = cursor.fetchone()

            if not message_data:
                return None

            message_text = message_data[0]

            # Количество получивших
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id)
                FROM message_deliveries
                WHERE message_number = ?
            ''', (message_number,))
            delivered = cursor.fetchone()[0]

            if delivered == 0:
                return {
                    'message_number': message_number,
                    'message_text': message_text,
                    'delivered': 0,
                    'clicked_callback_count': 0,
                    'clicked_url_count': 0,
                    'not_clicked': 0,
                    'avg_reaction_time_seconds': 0,
                    'button_details': []
                }

            # Количество кликнувших callback кнопку
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id)
                FROM button_clicks
                WHERE message_number = ? AND button_type = 'callback'
            ''', (message_number,))
            clicked_callback = cursor.fetchone()[0]

            # Количество кликнувших URL кнопку
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id)
                FROM button_clicks
                WHERE message_number = ? AND button_type = 'url'
            ''', (message_number,))
            clicked_url = cursor.fetchone()[0]

            # Не нажали ничего
            not_clicked = delivered - max(clicked_callback, clicked_url)

            # Среднее время реакции (в секундах)
            cursor.execute('''
                SELECT AVG((julianday(bc.clicked_at) - julianday(md.delivered_at)) * 24 * 60 * 60)
                FROM button_clicks bc
                JOIN message_deliveries md ON bc.user_id = md.user_id AND bc.message_number = md.message_number
                WHERE bc.message_number = ?
            ''', (message_number,))
            avg_time_result = cursor.fetchone()
            avg_reaction_time = avg_time_result[0] if avg_time_result[0] else 0

            # Детализация по кнопкам
            cursor.execute('''
                SELECT
                    button_text,
                    button_type,
                    COUNT(*) as click_count
                FROM button_clicks
                WHERE message_number = ?
                GROUP BY button_text, button_type
                ORDER BY click_count DESC
            ''', (message_number,))

            button_details = []
            for button_text, button_type, click_count in cursor.fetchall():
                percentage = (click_count / delivered * 100) if delivered > 0 else 0
                button_details.append({
                    'button_text': button_text,
                    'button_type': button_type,
                    'click_count': click_count,
                    'percentage': round(percentage, 2)
                })

            return {
                'message_number': message_number,
                'message_text': message_text,
                'delivered': delivered,
                'clicked_callback_count': clicked_callback,
                'clicked_url_count': clicked_url,
                'not_clicked': not_clicked,
                'avg_reaction_time_seconds': round(avg_reaction_time, 2),
                'button_details': button_details
            }

        except Exception as e:
            logger.error(f"❌ Ошибка при получении детальной статистики сообщения {message_number}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_biggest_drop_message(self):
        """
        Определение сообщения с самым большим отвалом

        Returns:
            Dict или None: информация о сообщении с максимальным отвалом
        """
        funnel_data = self.get_funnel_data()

        if not funnel_data:
            return None

        # Фильтруем сообщения с отправками
        messages_with_deliveries = [msg for msg in funnel_data if msg['delivered'] > 0]

        if not messages_with_deliveries:
            return None

        # Находим сообщение с максимальным drop_rate
        biggest_drop = max(messages_with_deliveries, key=lambda x: x['drop_rate'])

        return biggest_drop

    def get_biggest_drop_summary(self):
        """
        Получение краткой сводки о проблемах воронки для дашборда
        
        Returns:
            dict или None: {
                'has_problems': bool - есть ли проблемы (отвал > 30%),
                'message_number': int - номер проблемного сообщения,
                'drop_rate': float - процент отвала,
                'message_text': str - краткий текст сообщения (30 символов),
                'total_messages_with_data': int - сколько сообщений имеют данные
            }
        """
        try:
            funnel_data = self.get_funnel_data()
            
            if not funnel_data:
                return None
            
            # Фильтруем сообщения с отправками
            messages_with_deliveries = [msg for msg in funnel_data if msg['delivered'] > 0]
            
            if not messages_with_deliveries:
                return {
                    'has_problems': False,
                    'message_number': None,
                    'drop_rate': 0.0,
                    'message_text': 'Нет данных',
                    'total_messages_with_data': 0
                }
            
            # Находим сообщение с максимальным отвалом
            biggest_drop = max(messages_with_deliveries, key=lambda x: x['drop_rate'])
            
            # Считаем проблемным, если отвал > 30%
            has_problems = biggest_drop['drop_rate'] >= 30.0
            
            return {
                'has_problems': has_problems,
                'message_number': biggest_drop['message_number'],
                'drop_rate': biggest_drop['drop_rate'],
                'message_text': biggest_drop['message_text'][:30] + ('...' if len(biggest_drop['message_text']) > 30 else ''),
                'total_messages_with_data': len(messages_with_deliveries)
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении краткой сводки воронки: {e}")
            return None

    def cleanup_old_funnel_data(self, days_old=30):
        """
        Очистка старых данных воронки (старше X дней)

        Args:
            days_old: количество дней для хранения данных
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cutoff_date = datetime.now() - timedelta(days=days_old)

            # Удаляем старые отправки
            cursor.execute('''
                DELETE FROM message_deliveries
                WHERE delivered_at < ?
            ''', (cutoff_date,))
            deliveries_deleted = cursor.rowcount

            # Удаляем старые клики
            cursor.execute('''
                DELETE FROM button_clicks
                WHERE clicked_at < ?
            ''', (cutoff_date,))
            clicks_deleted = cursor.rowcount

            conn.commit()

            if deliveries_deleted > 0 or clicks_deleted > 0:
                logger.info(f"🧹 Очищено {deliveries_deleted} старых отправок и {clicks_deleted} старых кликов")

            return deliveries_deleted, clicks_deleted

        except Exception as e:
            logger.error(f"❌ Ошибка при очистке старых данных воронки: {e}")
            return 0, 0
        finally:
            if conn:
                conn.close()
