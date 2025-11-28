"""
Базовый модуль для инициализации и управления соединением с БД
"""

import sqlite3
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class DatabaseBase:
    """Базовый класс для работы с БД"""

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
                project_dir = Path(__file__).parent.parent
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

            # НОВАЯ КОЛОНКА: payed_till
            if 'payed_till' not in columns:
                cursor.execute('ALTER TABLE users ADD COLUMN payed_till DATE DEFAULT NULL')
                logger.info("Добавлена колонка payed_till в users")

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

            # ========================================
            # 📊 ТАБЛИЦЫ ДЛЯ ОТСЛЕЖИВАНИЯ ВОРОНКИ
            # ========================================

            # Таблица отправленных сообщений
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_deliveries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    message_number INTEGER NOT NULL,
                    delivered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')

            # Таблица кликов по кнопкам
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS button_clicks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    message_number INTEGER NOT NULL,
                    button_id INTEGER,
                    button_type TEXT NOT NULL,
                    button_text TEXT,
                    clicked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')

            # ========================================
            # ОСТАЛЬНЫЕ ТАБЛИЦЫ (без изменений)
            # ========================================

            # НОВАЯ ТАБЛИЦА: Таблица настроек продления подписки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS renewal_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')

            # Инициализация настроек продления
            cursor.execute('''
                INSERT OR IGNORE INTO renewal_settings (key, value)
                VALUES ('renewal_message', ?)
            ''', ("⏰ <b>Ваша подписка истекает сегодня!</b>\n\n"
                 "💳 Чтобы продолжить получать эксклюзивные материалы, продлите подписку.\n\n"
                 "✨ Не упустите возможность оставаться в курсе всех новинок!",))

            cursor.execute('''
                INSERT OR IGNORE INTO renewal_settings (key, value)
                VALUES ('renewal_photo_url', '')
            ''')

            cursor.execute('''
                INSERT OR IGNORE INTO renewal_settings (key, value)
                VALUES ('renewal_video_url', '')
            ''')

            cursor.execute('''
                INSERT OR IGNORE INTO renewal_settings (key, value)
                VALUES ('renewal_button_text', 'Продлить подписку')
            ''')

            cursor.execute('''
                INSERT OR IGNORE INTO renewal_settings (key, value)
                VALUES ('renewal_button_url', '')
            ''')

            # Обновляем таблицу сообщений рассылки - добавляем поле для фото и видео
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS broadcast_messages (
                    message_number INTEGER PRIMARY KEY,
                    text TEXT NOT NULL,
                    delay_hours INTEGER DEFAULT 24,
                    photo_url TEXT DEFAULT NULL,
                    video_url TEXT DEFAULT NULL
                )
            ''')

            # Добавляем колонку photo_url если её нет (для существующих БД)
            cursor.execute("PRAGMA table_info(broadcast_messages)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'photo_url' not in columns:
                cursor.execute('ALTER TABLE broadcast_messages ADD COLUMN photo_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка photo_url в broadcast_messages")

            # Добавляем колонку video_url если её нет
            if 'video_url' not in columns:
                cursor.execute('ALTER TABLE broadcast_messages ADD COLUMN video_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка video_url в broadcast_messages")

            # Таблица кнопок для сообщений рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_number INTEGER,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1,
                    messages_count INTEGER DEFAULT 1,
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')

            # Миграция: добавляем messages_count для существующих баз
            try:
                cursor.execute("SELECT messages_count FROM message_buttons LIMIT 1")
            except sqlite3.OperationalError:
                # Колонка не существует - добавляем
                logger.info("🔄 Миграция: добавляем колонку messages_count в message_buttons")
                cursor.execute("ALTER TABLE message_buttons ADD COLUMN messages_count INTEGER DEFAULT 1")
                conn.commit()
                logger.info("✅ Миграция завершена")

            # ========================================
            # 🎬 ТАБЛИЦЫ ДЛЯ МЕДИА-АЛЬБОМОВ
            # ========================================

            # Таблица медиа-альбомов для сообщений рассылки
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS message_media_albums (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_number INTEGER NOT NULL,
                    media_type TEXT NOT NULL CHECK(media_type IN ('photo', 'video')),
                    media_url TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    FOREIGN KEY (message_number) REFERENCES broadcast_messages(message_number)
                )
            ''')

            # Таблица медиа-альбомов для массовых рассылок
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scheduled_broadcast_media (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    broadcast_id INTEGER NOT NULL,
                    media_type TEXT NOT NULL CHECK(media_type IN ('photo', 'video')),
                    media_url TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    FOREIGN KEY (broadcast_id) REFERENCES scheduled_broadcasts(id)
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
                    video_url TEXT DEFAULT NULL,
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
                    video_url TEXT DEFAULT NULL,
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

            # НОВЫЕ ТАБЛИЦЫ ДЛЯ РАССЫЛОК ОПЛАТИВШИХ ПОЛЬЗОВАТЕЛЕЙ

            # Таблица сообщений рассылки для оплативших
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paid_broadcast_messages (
                    message_number INTEGER PRIMARY KEY,
                    text TEXT NOT NULL,
                    delay_hours REAL DEFAULT 24,
                    photo_url TEXT DEFAULT NULL,
                    video_url TEXT DEFAULT NULL
                )
            ''')

            # Таблица кнопок для сообщений рассылки оплативших
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paid_message_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_number INTEGER,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1,
                    FOREIGN KEY (message_number) REFERENCES paid_broadcast_messages(message_number)
                )
            ''')

            # Таблица запланированных сообщений для оплативших
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paid_scheduled_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    message_number INTEGER,
                    scheduled_time TIMESTAMP,
                    is_sent INTEGER DEFAULT 0,
                    FOREIGN KEY (user_id) REFERENCES users(user_id),
                    FOREIGN KEY (message_number) REFERENCES paid_broadcast_messages(message_number)
                )
            ''')

            # Запланированные массовые рассылки для оплативших
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paid_scheduled_broadcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_text TEXT NOT NULL,
                    photo_url TEXT DEFAULT NULL,
                    video_url TEXT DEFAULT NULL,
                    scheduled_time TIMESTAMP NOT NULL,
                    is_sent INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Кнопки для запланированных рассылок оплативших
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS paid_scheduled_broadcast_buttons (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    broadcast_id INTEGER,
                    button_text TEXT NOT NULL,
                    button_url TEXT NOT NULL,
                    position INTEGER DEFAULT 1,
                    FOREIGN KEY (broadcast_id) REFERENCES paid_scheduled_broadcasts(id)
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

            # Добавляем URL видео для приветствия (опционально)
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value)
                VALUES ('welcome_video_url', '')
            ''')

            # Добавляем URL видео для прощания (опционально)
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value)
                VALUES ('goodbye_video_url', '')
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

            # Добавляем URL видео для сообщения об оплате
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value)
                VALUES ('payment_success_video_url', '')
            ''')

            # ✅ НОВОЕ: Инициализация настройки для включения/выключения сообщения подтверждения
            cursor.execute('''
                INSERT OR IGNORE INTO settings (key, value)
                VALUES ('success_message_enabled', '1')
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

            # ========================================
            # 📊 ИНДЕКСЫ ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ
            # ========================================

            # Основные индексы
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_active ON users(is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_bot_started ON users(bot_started)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_paid ON users(has_paid)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_messages_time ON scheduled_messages(scheduled_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_scheduled_messages_sent ON scheduled_messages(is_sent)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(payment_status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_paid_scheduled_messages_time ON paid_scheduled_messages(scheduled_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_paid_scheduled_messages_sent ON paid_scheduled_messages(is_sent)')

            # 📊 Индексы для воронки
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deliveries_user ON message_deliveries(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deliveries_message ON message_deliveries(message_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_deliveries_time ON message_deliveries(delivered_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_user ON button_clicks(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_message ON button_clicks(message_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_time ON button_clicks(clicked_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_clicks_type ON button_clicks(button_type)')

            # 🎬 Индексы для медиа-альбомов
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_media_albums_message ON message_media_albums(message_number)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_media_albums_position ON message_media_albums(message_number, position)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_broadcast_media_broadcast ON scheduled_broadcast_media(broadcast_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_broadcast_media_position ON scheduled_broadcast_media(broadcast_id, position)')

            # ========================================
            # 🎥 ДОБАВЛЕНИЕ КОЛОНОК video_url В СУЩЕСТВУЮЩИЕ ТАБЛИЦЫ
            # ========================================

            # Добавляем video_url в welcome_follow_messages если её нет
            cursor.execute("PRAGMA table_info(welcome_follow_messages)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'video_url' not in columns:
                cursor.execute('ALTER TABLE welcome_follow_messages ADD COLUMN video_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка video_url в welcome_follow_messages")

            # Добавляем video_url в scheduled_broadcasts если её нет
            cursor.execute("PRAGMA table_info(scheduled_broadcasts)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'video_url' not in columns:
                cursor.execute('ALTER TABLE scheduled_broadcasts ADD COLUMN video_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка video_url в scheduled_broadcasts")

            # Добавляем video_url в paid_broadcast_messages если её нет
            cursor.execute("PRAGMA table_info(paid_broadcast_messages)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'video_url' not in columns:
                cursor.execute('ALTER TABLE paid_broadcast_messages ADD COLUMN video_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка video_url в paid_broadcast_messages")

            # Добавляем video_url в paid_scheduled_broadcasts если её нет
            cursor.execute("PRAGMA table_info(paid_scheduled_broadcasts)")
            columns = [column[1] for column in cursor.fetchall()]
            if 'video_url' not in columns:
                cursor.execute('ALTER TABLE paid_scheduled_broadcasts ADD COLUMN video_url TEXT DEFAULT NULL')
                logger.info("Добавлена колонка video_url в paid_scheduled_broadcasts")

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

                # Добавляем статистику воронки
                cursor.execute('SELECT COUNT(*) FROM message_deliveries')
                info['message_deliveries_count'] = cursor.fetchone()[0]

                cursor.execute('SELECT COUNT(*) FROM button_clicks')
                info['button_clicks_count'] = cursor.fetchone()[0]

                # Добавляем статистику медиа-альбомов
                cursor.execute('SELECT COUNT(*) FROM message_media_albums')
                info['media_albums_count'] = cursor.fetchone()[0]

                cursor.execute('SELECT COUNT(*) FROM scheduled_broadcast_media')
                info['scheduled_broadcast_media_count'] = cursor.fetchone()[0]
            except:
                info['users_count'] = 'N/A'
                info['scheduled_messages_count'] = 'N/A'
                info['payments_count'] = 'N/A'
                info['message_deliveries_count'] = 'N/A'
                info['button_clicks_count'] = 'N/A'
                info['media_albums_count'] = 'N/A'
                info['scheduled_broadcast_media_count'] = 'N/A'

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
