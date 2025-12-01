"""
User management module for database operations
"""

import sqlite3
import logging
import csv
import io
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UsersMixin:
    """Mixin for user-related database operations"""

    def add_user(self, user_id, username, first_name):
        """Добавление нового пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO users (user_id, username, first_name, is_active, bot_started, has_paid)
                VALUES (?, ?, ?, 1, 0, 0)
            ''', (user_id, username, first_name))

            conn.commit()
            logger.info(f"✅ Добавлен пользователь {user_id} (@{username})")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def get_user(self, user_id):
        """Получение информации о пользователе"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE user_id = ?
            ''', (user_id,))
            user = cursor.fetchone()
            return user
        finally:
            if conn:
                conn.close()

    def get_user_with_debug(self, user_id):
        """Получение информации о пользователе с отладочной информацией"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE user_id = ?
            ''', (user_id,))
            user = cursor.fetchone()

            if user:
                logger.debug(f"🔍 Пользователь {user_id}: active={user[4]}, bot_started={user[5]}, has_paid={user[6]}")
            else:
                logger.debug(f"🔍 Пользователь {user_id} не найден в базе")

            return user

        except Exception as e:
            logger.error(f"❌ Ошибка при получении пользователя {user_id}: {e}")
            return None
        finally:
            if conn:
                conn.close()

    def get_all_users(self):
        """Получение всех активных пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE is_active = 1
            ''')
            users = cursor.fetchall()
            return users
        finally:
            if conn:
                conn.close()

    def get_latest_users(self, limit=10):
        """Получение последних зарегистрированных пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users
                WHERE is_active = 1
                ORDER BY joined_at DESC
                LIMIT ?
            ''', (limit,))
            users = cursor.fetchall()
            return users
        finally:
            if conn:
                conn.close()

    def get_users_with_bot_started(self):
        """Получить только пользователей, которые начали разговор с ботом"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE is_active = 1 AND bot_started = 1
            ''')
            users = cursor.fetchall()
            return users
        finally:
            if conn:
                conn.close()

    def get_users_completed_funnel(self):
        """
        Получить пользователей, которые завершили воронку (получили последнее сообщение)

        Returns:
            List: Список пользователей, получивших последнее сообщение воронки
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Находим максимальный номер сообщения в воронке
            cursor.execute('SELECT MAX(message_number) FROM broadcast_messages')
            max_message_result = cursor.fetchone()

            if not max_message_result or max_message_result[0] is None:
                logger.warning("⚠️ Нет сообщений в воронке broadcast_messages")
                return []

            max_message = max_message_result[0]
            logger.debug(f"📊 Максимальный номер сообщения воронки: {max_message}")

            # Получаем пользователей, получивших последнее сообщение
            cursor.execute('''
                SELECT DISTINCT u.user_id, u.username, u.first_name, u.joined_at,
                       u.is_active, u.bot_started, u.has_paid, u.paid_at
                FROM users u
                INNER JOIN message_deliveries md ON u.user_id = md.user_id
                WHERE u.is_active = 1
                AND u.bot_started = 1
                AND u.has_paid = 0
                AND md.message_number = ?
                ORDER BY u.joined_at DESC
            ''', (max_message,))

            users = cursor.fetchall()
            logger.info(f"✅ Найдено {len(users)} пользователей, завершивших воронку")
            return users

        except Exception as e:
            logger.error(f"❌ Ошибка при получении пользователей, завершивших воронку: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def get_users_with_payment(self):
        """Получить только пользователей, которые оплатили"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE is_active = 1 AND has_paid = 1
            ''')
            users = cursor.fetchall()
            return users
        finally:
            if conn:
                conn.close()

    def mark_user_started_bot(self, user_id):
        """Пометить пользователя как начавшего разговор с ботом"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Сначала проверяем, существует ли пользователь
            cursor.execute('SELECT user_id, bot_started, is_active, has_paid FROM users WHERE user_id = ?', (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                logger.error(f"❌ Попытка пометить несуществующего пользователя {user_id} как начавшего разговор с ботом")
                return False

            user_id_db, current_bot_started, is_active, has_paid = user_data

            # Если пользователь неактивен, активируем его
            if not is_active:
                cursor.execute('UPDATE users SET is_active = 1 WHERE user_id = ?', (user_id,))
                logger.info(f"✅ Пользователь {user_id} реактивирован")

            # Если уже помечен как начавший разговор, все равно считаем успехом
            if current_bot_started:
                logger.debug(f"ℹ️ Пользователь {user_id} уже помечен как начавший разговор с ботом")
                return True

            # Обновляем статус bot_started
            cursor.execute('''
                UPDATE users SET bot_started = 1 WHERE user_id = ?
            ''', (user_id,))

            # Проверяем, что обновление произошло
            if cursor.rowcount == 0:
                logger.error(f"❌ Не удалось обновить статус bot_started для пользователя {user_id}")
                return False

            conn.commit()
            logger.info(f"✅ Пользователь {user_id} помечен как начавший разговор с ботом")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении статуса bot_started для пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def mark_user_paid(self, user_id, amount, payment_status, payed_till=None):
        """Отметить пользователя как оплатившего"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            if payed_till:
                cursor.execute('''
                    UPDATE users
                    SET has_paid = 1, paid_at = CURRENT_TIMESTAMP, payed_till = ?
                    WHERE user_id = ?
                ''', (payed_till, user_id))
                logger.info(f"✅ Пользователь {user_id} отмечен как оплативший ({amount}) до {payed_till}")
            else:
                cursor.execute('''
                    UPDATE users
                    SET has_paid = 1, paid_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                ''', (user_id,))
                logger.info(f"✅ Пользователь {user_id} отмечен как оплативший ({amount})")

            if cursor.rowcount == 0:
                logger.error(f"❌ Пользователь {user_id} не найден при отметке оплаты")
                return False

            conn.commit()
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при отметке пользователя {user_id} как оплатившего: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def deactivate_user(self, user_id):
        """Деактивация пользователя при отписке"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                UPDATE users SET is_active = 0, unsubscribed_at = CURRENT_TIMESTAMP WHERE user_id = ?
            ''', (user_id,))

            conn.commit()
            logger.info(f"✅ Деактивирован пользователь {user_id}")
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при деактивации пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def ensure_user_exists_and_active(self, user_id, username=None, first_name=None):
        """Убедиться, что пользователь существует и активен"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Проверяем, существует ли пользователь
            cursor.execute('SELECT user_id, is_active FROM users WHERE user_id = ?', (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                # Если пользователя нет, создаем его
                cursor.execute('''
                    INSERT INTO users (user_id, username, first_name, is_active, bot_started, has_paid)
                    VALUES (?, ?, ?, 1, 0, 0)
                ''', (user_id, username or '', first_name or ''))
                logger.info(f"✅ Создан новый пользователь {user_id}")
            else:
                # Если пользователь есть, но неактивен - активируем
                if not user_data[1]:
                    cursor.execute('UPDATE users SET is_active = 1 WHERE user_id = ?', (user_id,))
                    logger.info(f"✅ Пользователь {user_id} реактивирован")

            conn.commit()
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при обеспечении существования пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def export_users_to_csv(self):
        """Экспорт всех пользователей в CSV формат"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users
                ORDER BY joined_at DESC
            ''')
            users = cursor.fetchall()

            # Создаем CSV в памяти
            output = io.StringIO()
            writer = csv.writer(output)

            # Заголовки
            writer.writerow(['ID', 'Username', 'Имя', 'Дата регистрации', 'Статус', 'Разговор с ботом', 'Оплатил', 'Дата оплаты'])

            # Данные
            for user in users:
                user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at = user
                status = 'Активен' if is_active else 'Отписался'
                bot_status = 'Да' if bot_started else 'Нет'
                paid_status = 'Да' if has_paid else 'Нет'
                paid_date = paid_at if paid_at else ''
                writer.writerow([user_id, username or '', first_name or '', joined_at, status, bot_status, paid_status, paid_date])

            # Возвращаем CSV как строку
            csv_content = output.getvalue()
            output.close()

            return csv_content
        finally:
            if conn:
                conn.close()

    def get_user_statistics(self):
        """
        Получение расширенной статистики пользователей для дашборда
        
        Returns:
            dict: Статистика с детализацией по периодам
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            now = datetime.now()
            today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            yesterday_start = today_start - timedelta(days=1)
            week_ago = now - timedelta(days=7)
            month_ago = now - timedelta(days=30)

            stats = {}

            # ===== БАЗОВЫЕ МЕТРИКИ =====
            
            # Общее количество активных пользователей
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
            stats['total_users'] = cursor.fetchone()[0]

            # Пользователи, которые начали разговор с ботом
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1 AND bot_started = 1')
            stats['bot_started_users'] = cursor.fetchone()[0]

            # Оплатившие пользователи
            cursor.execute('SELECT COUNT(*) FROM users WHERE has_paid = 1')
            stats['paid_users'] = cursor.fetchone()[0]

            # Количество отправленных сообщений
            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 1')
            stats['sent_messages'] = cursor.fetchone()[0]

            # Количество отписавшихся (всего)
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 0')
            stats['unsubscribed'] = cursor.fetchone()[0]

            # ===== НОВЫЕ МЕТРИКИ ДЛЯ ДАШБОРДА =====

            # Новые пользователи за сегодня
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND is_active = 1
            ''', (today_start,))
            stats['new_users_today'] = cursor.fetchone()[0]

            # Новые пользователи за вчера (для сравнения)
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND joined_at < ? AND is_active = 1
            ''', (yesterday_start, today_start))
            stats['new_users_yesterday'] = cursor.fetchone()[0]

            # Новые пользователи за 7 дней
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND is_active = 1
            ''', (week_ago,))
            stats['new_users_7d'] = cursor.fetchone()[0]

            # Новые пользователи за 30 дней
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND is_active = 1
            ''', (month_ago,))
            stats['new_users_30d'] = cursor.fetchone()[0]

            # Новые за 24 часа (для обратной совместимости)
            yesterday_24h = now - timedelta(days=1)
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND is_active = 1
            ''', (yesterday_24h,))
            stats['new_users_24h'] = cursor.fetchone()[0]

            # ===== СЕГОДНЯШНЯЯ АКТИВНОСТЬ =====

            # Начали разговор сегодня
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE joined_at >= ? AND is_active = 1 AND bot_started = 1
            ''', (today_start,))
            stats['bot_started_today'] = cursor.fetchone()[0]

            # Оплатили сегодня
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE paid_at >= ? AND has_paid = 1
            ''', (today_start,))
            stats['paid_today'] = cursor.fetchone()[0]

            # Отписались сегодня
            cursor.execute('''
                SELECT COUNT(*) FROM users
                WHERE unsubscribed_at >= ? AND is_active = 0
            ''', (today_start,))
            stats['unsubscribed_today'] = cursor.fetchone()[0]

            # ===== ПРОЦЕНТНЫЕ ИЗМЕНЕНИЯ =====

            # Изменение относительно вчера
            if stats['new_users_yesterday'] > 0:
                change = ((stats['new_users_today'] - stats['new_users_yesterday']) / stats['new_users_yesterday']) * 100
                stats['new_users_change_percent'] = round(change, 1)
            else:
                stats['new_users_change_percent'] = 100.0 if stats['new_users_today'] > 0 else 0.0

            # Конверсия (% оплативших от начавших разговор)
            if stats['bot_started_users'] > 0:
                stats['conversion_rate'] = round((stats['paid_users'] / stats['bot_started_users']) * 100, 2)
            else:
                stats['conversion_rate'] = 0.0

            logger.debug(f"📊 Статистика пользователей обновлена: {stats['total_users']} активных, {stats['new_users_today']} новых сегодня")

            return stats

        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики пользователей: {e}")
            # Возвращаем базовую статистику с нулями в случае ошибки
            return {
                'total_users': 0,
                'bot_started_users': 0,
                'paid_users': 0,
                'sent_messages': 0,
                'unsubscribed': 0,
                'new_users_today': 0,
                'new_users_yesterday': 0,
                'new_users_7d': 0,
                'new_users_30d': 0,
                'new_users_24h': 0,
                'bot_started_today': 0,
                'paid_today': 0,
                'unsubscribed_today': 0,
                'new_users_change_percent': 0.0,
                'conversion_rate': 0.0
            }
        finally:
            if conn:
                conn.close()

    def debug_user_state(self, user_id):
        """Отладка состояния пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            debug_info = {}

            # Информация о пользователе
            cursor.execute('''
                SELECT user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at
                FROM users WHERE user_id = ?
            ''', (user_id,))
            user_data = cursor.fetchone()

            if not user_data:
                debug_info['error'] = f"Пользователь {user_id} не найден"
                return debug_info

            debug_info['user'] = {
                'user_id': user_data[0],
                'username': user_data[1],
                'first_name': user_data[2],
                'joined_at': user_data[3],
                'is_active': bool(user_data[4]),
                'bot_started': bool(user_data[5]),
                'has_paid': bool(user_data[6]),
                'paid_at': user_data[7]
            }

            # Запланированные сообщения
            cursor.execute('''
                SELECT id, message_number, scheduled_time, is_sent
                FROM scheduled_messages
                WHERE user_id = ?
                ORDER BY message_number
            ''', (user_id,))

            scheduled_messages = cursor.fetchall()
            debug_info['scheduled_messages'] = []

            for msg in scheduled_messages:
                debug_info['scheduled_messages'].append({
                    'id': msg[0],
                    'message_number': msg[1],
                    'scheduled_time': msg[2],
                    'is_sent': bool(msg[3])
                })

            # Проверяем, какие сообщения должны быть
            cursor.execute('SELECT message_number FROM broadcast_messages ORDER BY message_number')
            all_messages = [row[0] for row in cursor.fetchall()]

            scheduled_numbers = [msg['message_number'] for msg in debug_info['scheduled_messages']]
            missing_messages = [num for num in all_messages if num not in scheduled_numbers]

            debug_info['missing_messages'] = missing_messages
            debug_info['total_messages_expected'] = len(all_messages)
            debug_info['total_messages_scheduled'] = len(scheduled_messages)

            return debug_info

        except Exception as e:
            logger.error(f"❌ Ошибка при отладке состояния пользователя {user_id}: {e}")
            return {'error': str(e)}
        finally:
            if conn:
                conn.close()

    def expire_user_subscription(self, user_id):
        """Завершить подписку пользователя и перевести на обычные рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            # Сбрасываем статус оплаты
            cursor.execute('''
                UPDATE users
                SET has_paid = 0, payed_till = NULL
                WHERE user_id = ?
            ''', (user_id,))

            if cursor.rowcount == 0:
                logger.error(f"❌ Пользователь {user_id} не найден при завершении подписки")
                return False

            # Отменяем все неотправленные платные сообщения
            cursor.execute('''
                DELETE FROM paid_scheduled_messages
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))

            cancelled_paid_count = cursor.rowcount

            conn.commit()

            logger.info(f"✅ Подписка пользователя {user_id} завершена, отменено {cancelled_paid_count} платных сообщений")
            return True

        except Exception as e:
            logger.error(f"❌ Ошибка при завершении подписки пользователя {user_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return False
        finally:
            if conn:
                conn.close()

    def get_expired_subscriptions(self):
        """Получить пользователей с истекшей подпиской на сегодня"""
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            from datetime import date
            today = date.today()

            cursor.execute('''
                SELECT user_id, username, first_name, payed_till
                FROM users
                WHERE has_paid = 1
                AND is_active = 1
                AND payed_till = ?
            ''', (today,))

            expired_users = cursor.fetchall()
            return expired_users

        except Exception as e:
            logger.error(f"❌ Ошибка при получении истекших подписок: {e}")
            return []
        finally:
            if conn:
                conn.close()
