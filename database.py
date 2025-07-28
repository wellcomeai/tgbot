import sqlite3
import json
import os
import csv
import io
from pathlib import Path
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path=None):
        """Инициализация базы данных для Render с Disk"""
        if db_path is None:
            # Проверяем переменную окружения для Render Disk
            render_disk_path = os.environ.get('RENDER_DISK_PATH', '/data')
            
            if render_disk_path and os.path.exists(render_disk_path):
                # Используем Render Disk для persistent storage
                db_dir = Path(render_disk_path)
                logger.info(f"🗄️ Используем Render Disk: {db_dir}")
            else:
                # Локальная разработка или fallback
                project_dir = Path(__file__).parent
                db_dir = project_dir / 'data'
                logger.info(f"📂 Используем локальную папку: {db_dir}")
            
            # Создаем директорию если её нет
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = db_dir / 'bot_database.db'
        
        self.db_path = str(db_path)
        
        # Проверяем права доступа
        try:
            db_dir = Path(self.db_path).parent
            if not db_dir.exists():
                db_dir.mkdir(parents=True, exist_ok=True)
                logger.info(f"Создана директория для БД: {db_dir}")
            
            # Проверяем права на запись
            test_file = db_dir / 'test_write.tmp'
            try:
                test_file.write_text('test')
                test_file.unlink()
                logger.info(f"✅ Права на запись в {db_dir} подтверждены")
            except Exception as e:
                logger.error(f"❌ Нет прав на запись в {db_dir}: {e}")
                raise
                
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке директории БД: {e}")
            raise
        
        self.init_db()
        logger.info(f"✅ База данных инициализирована: {self.db_path}")
    
    def init_db(self):
        """Создание таблиц в базе данных"""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            cursor = conn.cursor()
            
            # Включаем WAL режим для лучшей производительности и конкуррентности
            cursor.execute('PRAGMA journal_mode=WAL')
            cursor.execute('PRAGMA synchronous=NORMAL')
            cursor.execute('PRAGMA cache_size=10000')
            cursor.execute('PRAGMA temp_store=MEMORY')
            
            # Таблица пользователей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active INTEGER DEFAULT 1,
                    bot_started INTEGER DEFAULT 0,
                    has_paid INTEGER DEFAULT 0,
                    paid_at TIMESTAMP DEFAULT NULL
                )
            ''')
            
            # Добавляем новые колонки для платежей если их нет
            cursor.execute("PRAGMA table_info(users)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'bot_started' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN bot_started INTEGER DEFAULT 0')
                logger.info("Добавлена колонка bot_started в users")
            
            if 'has_paid' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN has_paid INTEGER DEFAULT 0')
                logger.info("Добавлена колонка has_paid в users")
            
            if 'paid_at' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN paid_at TIMESTAMP DEFAULT NULL')
                logger.info("Добавлена колонка paid_at в users")
            
            # Новая таблица платежей
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    amount TEXT,
                    payment_status TEXT,
                    utm_source TEXT,
                    utm_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                )
            ''')
            
            # Обновляем таблицу сообщений рассылки - добавляем поле для фото
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_messages (
                    message_number INTEGER PRIMARY KEY,
                    text TEXT NOT NULL,
                    delay_hours INTEGER DEFAULT 24,
                    photo_url TEXT DEFAULT NULL
                )
            ''')
            
            # Добавляем колонку photo_url если её нет (для существующих БД)
            cursor.execute("PRAGMA table_info(broadcast_messages)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'photo_url' not in columns:
                cursor.execute('ALTER TABLE broadcast_messages ADD COLUMN photo_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка photo_url в broadcast_messages")
            
            # Таблица кнопок для сообщений рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_number INTEGER,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1,
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')
            
            # НОВАЯ: Таблица кнопок для приветственного сообщения (механические кнопки)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS welcome_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    button_text TEXT NOT NULL UNIQUE,
                    position INTEGER DEFAULT 1
                )
            ''')
            
            # НОВАЯ: Таблица последующих сообщений после нажатия кнопок приветствия
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS welcome_follow_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    welcome_button_id INTEGER,
                    message_number INTEGER,
                    text TEXT NOT NULL,
                    photo_url TEXT DEFAULT NULL,
                    FOREIGN KEY (welcome_button_id) REFERENCES welcome_buttons(id)
                )
            ''')
            
            # Проверяем, есть ли старая структура с callback_data и обновляем
            cursor.execute("PRAGMA table_info(welcome_buttons)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'callback_data' in columns:
                # Создаем новую таблицу
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS welcome_buttons_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        button_text TEXT NOT NULL UNIQUE,
                        position INTEGER DEFAULT 1
                    )
                ''')
                
                # Копируем данные, убирая callback_data
                cursor.execute('''
                    INSERT INTO welcome_buttons_new (id, button_text, position)
                    SELECT id, button_text, position FROM welcome_buttons
                ''')
                
                # Удаляем старую таблицу и переименовываем новую
                cursor.execute('DROP TABLE welcome_buttons')
                cursor.execute('ALTER TABLE welcome_buttons_new RENAME TO welcome_buttons')
                
                logger.info("Обновлена структура таблицы welcome_buttons для механических кнопок")
            
            # НОВАЯ: Таблица кнопок для прощального сообщения
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS goodbye_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1
                )
            ''')
            
            # НОВАЯ: Таблица запланированных массовых рассылок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_text TEXT NOT NULL,
                    photo_url TEXT DEFAULT NULL,
                    scheduled_time TIMESTAMP NOT NULL,
                    is_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # НОВАЯ: Таблица кнопок для запланированных рассылок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_broadcast_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    broadcast_id INTEGER,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1,
                    FOREIGN KEY (broadcast_id) REFERENCES scheduled_broadcasts(id)
                )
            ''')
            
            # Таблица для управления статусом рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            # Инициализация настроек рассылки
            cursor.execute('''
                INSERT OR IGNORE INTO broadcast_settings (key, value) 
                VALUES ('broadcast_enabled', '1')
            ''')
            
            cursor.execute('''
                INSERT OR IGNORE INTO broadcast_settings (key, value) 
                VALUES ('auto_resume_time', '')
            ''')
            
            # Таблица запланированных сообщений (автоматическая рассылка)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    message_number INTEGER,
                    scheduled_time TIMESTAMP,
                    is_sent INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')
            
            # Таблица настроек - добавляем поле для фото приветствия и сообщения при отписке
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            
            # Инициализация приветственного сообщения
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('welcome_message', ?)
            ''', ("🎉 <b>Добро пожаловать!</b>\n\n"
                 "Рады видеть вас в нашем канале! 🚀\n\n"
                 "Для получения полезных материалов выберите одно из действий ниже:",))
            
            # Добавляем сообщение при отписке
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('goodbye_message', ?)
            ''', ("😢 Жаль, что вы покидаете нас!\n\n"
                 "Если передумаете - всегда будем рады видеть вас снова в нашем канале.\n\n"
                 "Удачи! 👋",))
            
            # Добавляем URL фото для приветствия (опционально)
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('welcome_photo_url', '')
            ''')
            
            # Добавляем URL фото для прощания (опционально)
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('goodbye_photo_url', '')
            ''')
            
            # НОВЫЕ настройки для сообщений после оплаты
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('payment_success_message', ?)
            ''', ("🎉 <b>Спасибо за покупку!</b>\n\n"
                 "💰 Ваш платеж успешно обработан!\n\n"
                 "✅ Вы получили полный доступ ко всем материалам.\n\n"
                 "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                 "🙏 Благодарим за доверие!",))
            
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value) 
                VALUES ('payment_success_photo_url', '')
            ''')
            
            # Инициализация сообщений рассылки по умолчанию
            default_messages = [
                ("Сообщение 1: Основы работы с нашим сервисом 📚", 0.05, None),    # 3 минуты
                ("Сообщение 2: Продвинутые функции и возможности 🔧", 4, None),   # 4 часа
                ("Сообщение 3: Лучшие практики и советы 💡", 8, None),          # 8 часов
                ("Сообщение 4: Частые вопросы и ответы ❓", 12, None),           # 12 часов
                ("Сообщение 5: Примеры успешных кейсов 📈", 16, None),          # 16 часов
                ("Сообщение 6: Дополнительные ресурсы 📖", 20, None),           # 20 часов
                ("Сообщение 7: Благодарность и обратная связь 🙏", 23, None)     # 23 часа
            ]
            
            for i, (text, delay, photo) in enumerate(default_messages, 1):
                cursor.execute('''
                    INSERT OR IGNORE INTO broadcast_messages (message_number, text, delay_hours, photo_url)
                    VALUES (?, ?, ?, ?)
                ''', (i, text, delay, photo))
            
            conn.commit()
            
            # Создаем индексы для производительности
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_bot_started ON users(bot_started)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_paid ON users(has_paid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_messages_time ON scheduled_messages(scheduled_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_messages_sent ON scheduled_messages(is_sent)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(payment_status)')
            
            conn.commit()
            logger.info("✅ Индексы созданы для оптимизации производительности")
            
        except sqlite3.Error as e:
            logger.error(f"❌ Ошибка при инициализации базы данных: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()
    
    def _get_connection(self):
        """Получить соединение с БД с retry логикой и оптимизацией"""
        max_retries = 5
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(
                    self.db_path, 
                    timeout=30,
                    check_same_thread=False,
                    isolation_level=None  # Автокоммит
                )
                
                # Оптимизация для Render
                conn.execute('PRAGMA journal_mode=WAL')
                conn.execute('PRAGMA synchronous=NORMAL')
                conn.execute('PRAGMA cache_size=10000')
                conn.execute('PRAGMA temp_store=MEMORY')
                conn.execute('PRAGMA mmap_size=268435456')  # 256MB
                
                return conn
            except sqlite3.OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ Не удалось подключиться к БД, попытка {attempt + 1}/{max_retries}: {e}")
                    import time
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    logger.error(f"❌ Критическая ошибка подключения к БД после {max_retries} попыток: {e}")
                    raise
    
    def get_database_info(self):
        """Получение информации о базе данных для диагностики"""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            info = {
                'db_path': self.db_path,
                'db_size_mb': round(os.path.getsize(self.db_path) / (1024 * 1024), 2) if os.path.exists(self.db_path) else 0,
                'disk_space_mb': self._get_disk_space(),
                'render_disk_path': os.environ.get('RENDER_DISK_PATH', '/data'),
                'wal_files': self._check_wal_files()
            }
            
            # Проверяем целостность БД
            cursor.execute('PRAGMA integrity_check')
            integrity = cursor.fetchone()[0]
            info['integrity'] = integrity
            
            # Количество таблиц
            cursor.execute("SELECT count(*) FROM sqlite_master WHERE type='table'")
            info['tables_count'] = cursor.fetchone()[0]
            
            # Количество записей в основных таблицах
            try:
                cursor.execute('SELECT COUNT(*) FROM users')
                info['users_count'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM scheduled_messages')
                info['scheduled_messages_count'] = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM payments')
                info['payments_count'] = cursor.fetchone()[0]
            except:
                info['users_count'] = 'N/A'
                info['scheduled_messages_count'] = 'N/A' 
                info['payments_count'] = 'N/A'
            
            conn.close()
            return info
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении информации о БД: {e}")
            return {'error': str(e), 'db_path': self.db_path}
    
    def _get_disk_space(self):
        """Получение информации о свободном месте на диске"""
        try:
            import shutil
            total, used, free = shutil.disk_usage(Path(self.db_path).parent)
            return {
                'total_mb': round(total / (1024 * 1024), 2),
                'used_mb': round(used / (1024 * 1024), 2),
                'free_mb': round(free / (1024 * 1024), 2)
            }
        except:
            return 'N/A'
    
    def _check_wal_files(self):
        """Проверка WAL файлов"""
        try:
            db_dir = Path(self.db_path).parent
            wal_files = list(db_dir.glob('*.wal')) + list(db_dir.glob('*.shm'))
            return [f.name for f in wal_files]
        except:
            return []

    # ===== НОВЫЕ МЕТОДЫ ДЛЯ РАБОТЫ С ПЛАТЕЖАМИ =====
    
    def mark_user_paid(self, user_id, amount, payment_status):
        """Отметить пользователя как оплатившего"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE users 
                SET has_paid = 1, paid_at = CURRENT_TIMESTAMP 
                WHERE user_id = ?
            ''', (user_id,))
            
            if cursor.rowcount == 0:
                logger.error(f"❌ Пользователь {user_id} не найден при отметке оплаты")
                return False
            
            conn.commit()
            logger.info(f"✅ Пользователь {user_id} отмечен как оплативший ({amount})")
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
    
    def get_payment_success_message(self):
        """Получение сообщения об успешной оплате"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT value FROM settings WHERE key = "payment_success_message"')
            message = cursor.fetchone()
            
            cursor.execute('SELECT value FROM settings WHERE key = "payment_success_photo_url"')
            photo = cursor.fetchone()
            
            return {
                'text': message[0] if message else None,
                'photo_url': photo[0] if photo and photo[0] else None
            }
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении сообщения об оплате: {e}")
            return None
        finally:
            if conn:
                conn.close()
    
    def set_payment_success_message(self, text, photo_url=None):
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
            
            conn.commit()
            logger.info("✅ Сообщение об успешной оплате обновлено")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при установке сообщения об оплате: {e}")
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
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ (без изменений, но с улучшенной обработкой ошибок) =====
    
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
    
    def deactivate_user(self, user_id):
        """Деактивация пользователя при отписке"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                UPDATE users SET is_active = 0 WHERE user_id = ?
            ''', (user_id,))
            
            conn.commit()
            logger.info(f"Деактивирован пользователь {user_id}")
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
    
    def get_welcome_message(self):
        """Получение приветственного сообщения и фото"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT value FROM settings WHERE key = "welcome_message"')
            message = cursor.fetchone()
            
            cursor.execute('SELECT value FROM settings WHERE key = "welcome_photo_url"')
            photo = cursor.fetchone()
            
            return {
                'text': message[0] if message else "Добро пожаловать!",
                'photo': photo[0] if photo and photo[0] else None
            }
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
            
            return {
                'text': message[0] if message else "До свидания!",
                'photo': photo[0] if photo and photo[0] else None
            }
        finally:
            if conn:
                conn.close()
    
    def set_welcome_message(self, message, photo_url=None):
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
            
            conn.commit()
        finally:
            if conn:
                conn.close()
    
    def set_goodbye_message(self, message, photo_url=None):
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
                SELECT id, message_number, text, photo_url 
                FROM welcome_follow_messages 
                WHERE welcome_button_id = ? 
                ORDER BY message_number
            ''', (welcome_button_id,))
            
            messages = cursor.fetchall()
            return messages
        finally:
            if conn:
                conn.close()
    
    def add_welcome_follow_message(self, welcome_button_id, text, photo_url=None):
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
                INSERT INTO welcome_follow_messages (welcome_button_id, message_number, text, photo_url)
                VALUES (?, ?, ?, ?)
            ''', (welcome_button_id, message_number, text, photo_url))
            
            conn.commit()
            logger.info(f"Добавлено последующее сообщение {message_number} для кнопки {welcome_button_id}")
            return message_number
        finally:
            if conn:
                conn.close()
    
    def update_welcome_follow_message(self, message_id, text=None, photo_url=None):
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
    
    def add_goodbye_button(self, button_text, button_url, position=1):
        """Добавление кнопки к прощальному сообщению"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO goodbye_buttons (button_text, button_url, position)
                VALUES (?, ?, ?)
            ''', (button_text, button_url, position))
            
            conn.commit()
            logger.info(f"Добавлена кнопка прощания: {button_text}")
        finally:
            if conn:
                conn.close()
    
    def update_goodbye_button(self, button_id, button_text=None, button_url=None):
        """Обновление кнопки прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if button_text is not None:
                cursor.execute('''
                    UPDATE goodbye_buttons SET button_text = ? WHERE id = ?
                ''', (button_text, button_id))
            
            if button_url is not None:
                cursor.execute('''
                    UPDATE goodbye_buttons SET button_url = ? WHERE id = ?
                ''', (button_url, button_id))
            
            conn.commit()
            logger.info(f"Обновлена кнопка прощания #{button_id}")
        finally:
            if conn:
                conn.close()
    
    def delete_goodbye_button(self, button_id):
        """Удаление кнопки прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM goodbye_buttons WHERE id = ?', (button_id,))
            conn.commit()
            logger.info(f"Удалена кнопка прощания #{button_id}")
        finally:
            if conn:
                conn.close()
    
    # ===== МЕТОДЫ ДЛЯ ЗАПЛАНИРОВАННЫХ МАССОВЫХ РАССЫЛОК =====
    
    def get_scheduled_broadcasts(self, include_sent=False):
        """Получение запланированных массовых рассылок"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if include_sent:
                cursor.execute('''
                    SELECT id, message_text, photo_url, scheduled_time, is_sent, created_at
                    FROM scheduled_broadcasts 
                    ORDER BY scheduled_time
                ''')
            else:
                cursor.execute('''
                    SELECT id, message_text, photo_url, scheduled_time, is_sent, created_at
                    FROM scheduled_broadcasts 
                    WHERE is_sent = 0
                    ORDER BY scheduled_time
                ''')
            
            broadcasts = cursor.fetchall()
            return broadcasts
        finally:
            if conn:
                conn.close()
    
    def add_scheduled_broadcast(self, message_text, scheduled_time, photo_url=None):
        """Добавление запланированной массовой рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT INTO scheduled_broadcasts (message_text, photo_url, scheduled_time)
                VALUES (?, ?, ?)
            ''', (message_text, photo_url, scheduled_time))
            
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
                SELECT id, message_text, photo_url, scheduled_time
                FROM scheduled_broadcasts 
                WHERE is_sent = 0 AND scheduled_time <= ?
                ORDER BY scheduled_time
            ''', (current_time,))
            
            broadcasts = cursor.fetchall()
            return broadcasts
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
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ (без изменений) =====
    
    def get_broadcast_message(self, message_number):
        """Получение сообщения рассылки по номеру"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                SELECT text, delay_hours, photo_url FROM broadcast_messages 
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
    
    def add_broadcast_message(self, text, delay_hours, photo_url=None):
        """Добавление нового сообщения рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Находим следующий доступный номер сообщения
            cursor.execute('SELECT MAX(message_number) FROM broadcast_messages')
            max_number = cursor.fetchone()[0]
            next_number = (max_number or 0) + 1
            
            cursor.execute('''
                INSERT INTO broadcast_messages (message_number, text, delay_hours, photo_url)
                VALUES (?, ?, ?, ?)
            ''', (next_number, text, delay_hours, photo_url))
            
            conn.commit()
            logger.info(f"Добавлено сообщение рассылки #{next_number}")
            return next_number
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
    
    def update_broadcast_message(self, message_number, text=None, delay_hours=None, photo_url=None):
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
            
            conn.commit()
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
                SELECT sm.id, sm.user_id, sm.message_number, bm.text, bm.photo_url
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
                SELECT sm.id, sm.user_id, sm.message_number, bm.text, bm.photo_url, sm.scheduled_time
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
                message_id, user_id, message_number, text, photo_url, scheduled_time = msg
                scheduled_dt = datetime.fromisoformat(scheduled_time) if isinstance(scheduled_time, str) else scheduled_time
                delay_minutes = int((current_time - scheduled_dt).total_seconds() / 60)
                logger.debug(f"📬 Сообщение {message_number} для пользователя {user_id} (опоздание: {delay_minutes} мин)")
            
            return [(m[0], m[1], m[2], m[3], m[4]) for m in messages]  # Возвращаем без scheduled_time
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
        """Отмена всех запланированных сообщений для пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                DELETE FROM scheduled_messages 
                WHERE user_id = ? AND is_sent = 0
            ''', (user_id,))
            
            conn.commit()
            logger.info(f"Отменены запланированные сообщения для пользователя {user_id}")
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
    
    def get_user_statistics(self):
        """Получение статистики пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            # Общее количество пользователей
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
            total_users = cursor.fetchone()[0]
            
            # Пользователи, которые начали разговор с ботом
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1 AND bot_started = 1')
            bot_started_users = cursor.fetchone()[0]
            
            # Пользователи за последние 24 часа
            yesterday = datetime.now() - timedelta(days=1)
            cursor.execute('''
                SELECT COUNT(*) FROM users 
                WHERE joined_at >= ? AND is_active = 1
            ''', (yesterday,))
            new_users_24h = cursor.fetchone()[0]
            
            # Количество отправленных сообщений
            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 1')
            sent_messages = cursor.fetchone()[0]
            
            # Количество отписавшихся
            cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 0')
            unsubscribed = cursor.fetchone()[0]
            
            # НОВАЯ СТАТИСТИКА: Оплатившие пользователи
            cursor.execute('SELECT COUNT(*) FROM users WHERE has_paid = 1')
            paid_users = cursor.fetchone()[0]
            
            return {
                'total_users': total_users,
                'bot_started_users': bot_started_users,
                'new_users_24h': new_users_24h,
                'sent_messages': sent_messages,
                'unsubscribed': unsubscribed,
                'paid_users': paid_users
            }
        finally:
            if conn:
                conn.close()
