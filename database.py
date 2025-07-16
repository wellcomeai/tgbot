import sqlite3
import json
import os
import csv
import io
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path='/data/bot_database.db'):
        """Инициализация базы данных"""
        self.db_path = db_path
        
        # Создаем директорию если её нет
        db_dir = os.path.dirname(db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)
            logger.info(f"Создана директория для БД: {db_dir}")
        
        self.init_db()
        logger.info(f"База данных инициализирована: {db_path}")
    
    def init_db(self):
        """Создание таблиц в базе данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Включаем WAL режим для лучшей производительности
        cursor.execute('PRAGMA journal_mode=WAL')
        
        # Таблица пользователей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active INTEGER DEFAULT 1,
                bot_started INTEGER DEFAULT 0
            )
        ''')
        
        # Добавляем колонку bot_started если её нет (для существующих БД)
        cursor.execute("PRAGMA table_info(users)")
        columns = [column[1] for column in cursor.fetchall()]
        if 'bot_started' not in columns:
            cursor.execute('ALTER TABLE users ADD COLUMN bot_started INTEGER DEFAULT 0')
            logger.info("Добавлена колонка bot_started в users")
        
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
        
        # Остальные таблицы без изменений...
        
        # Таблица кнопок для сообщений
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
        
        # Таблица запланированных сообщений
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
        ''', ("🎉 <b>Добро пожаловать в наш закрытый канал!</b>\n\n"
             "Рад видеть вас среди наших подписчиков! 🚀\n\n"
             "В ближайшие дни вы будете получать полезные материалы от нашего бота.\n\n"
             "Если у вас есть вопросы - не стесняйтесь задавать!",))
        
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
        
        # Инициализация сообщений рассылки по умолчанию
        default_messages = [
            ("Сообщение 1: Основы работы с нашим сервисом 📚", 24, None),
            ("Сообщение 2: Продвинутые функции и возможности 🔧", 48, None),
            ("Сообщение 3: Лучшие практики и советы 💡", 72, None),
            ("Сообщение 4: Частые вопросы и ответы ❓", 96, None),
            ("Сообщение 5: Примеры успешных кейсов 📈", 120, None),
            ("Сообщение 6: Дополнительные ресурсы 📖", 144, None),
            ("Сообщение 7: Благодарность и обратная связь 🙏", 168, None)
        ]
        
        for i, (text, delay, photo) in enumerate(default_messages, 1):
            cursor.execute('''
                INSERT OR IGNORE INTO broadcast_messages (message_number, text, delay_hours, photo_url)
                VALUES (?, ?, ?, ?)
            ''', (i, text, delay, photo))
        
        conn.commit()
        conn.close()
        
        # Таблица кнопок для сообщений
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
        
        # Таблица запланированных сообщений
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
        ''', ("🎉 <b>Добро пожаловать в наш закрытый канал!</b>\n\n"
             "Рад видеть вас среди наших подписчиков! 🚀\n\n"
             "В ближайшие дни вы будете получать полезные материалы от нашего бота.\n\n"
             "Если у вас есть вопросы - не стесняйтесь задавать!",))
        
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
        
        # Инициализация сообщений рассылки по умолчанию
        default_messages = [
            ("Сообщение 1: Основы работы с нашим сервисом 📚", 24, None),
            ("Сообщение 2: Продвинутые функции и возможности 🔧", 48, None),
            ("Сообщение 3: Лучшие практики и советы 💡", 72, None),
            ("Сообщение 4: Частые вопросы и ответы ❓", 96, None),
            ("Сообщение 5: Примеры успешных кейсов 📈", 120, None),
            ("Сообщение 6: Дополнительные ресурсы 📖", 144, None),
            ("Сообщение 7: Благодарность и обратная связь 🙏", 168, None)
        ]
        
        for i, (text, delay, photo) in enumerate(default_messages, 1):
            cursor.execute('''
                INSERT OR IGNORE INTO broadcast_messages (message_number, text, delay_hours, photo_url)
                VALUES (?, ?, ?, ?)
            ''', (i, text, delay, photo))
        
        conn.commit()
        conn.close()
    
    def _get_connection(self):
        """Получить соединение с БД с retry логикой"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                conn = sqlite3.connect(self.db_path, timeout=30)
                return conn
            except sqlite3.OperationalError as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Не удалось подключиться к БД, попытка {attempt + 1}/{max_retries}: {e}")
                    continue
                else:
                    raise
    
    def add_user(self, user_id, username, first_name):
        """Добавление нового пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT OR REPLACE INTO users (user_id, username, first_name, is_active)
            VALUES (?, ?, ?, 1)
        ''', (user_id, username, first_name))
        
        conn.commit()
        conn.close()
        logger.info(f"Добавлен пользователь {user_id} (@{username})")
    
    def mark_user_started_bot(self, user_id):
        """Пометить пользователя как начавшего разговор с ботом"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET bot_started = 1 WHERE user_id = ?
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        logger.info(f"Пользователь {user_id} начал разговор с ботом")
    
    def get_users_with_bot_started(self):
        """Получить только пользователей, которые начали разговор с ботом"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT user_id, username, first_name, joined_at, is_active, bot_started FROM users WHERE is_active = 1 AND bot_started = 1')
        users = cursor.fetchall()
        
        conn.close()
        return users
    
    def deactivate_user(self, user_id):
        """Деактивация пользователя при отписке"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE users SET is_active = 0 WHERE user_id = ?
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        logger.info(f"Деактивирован пользователь {user_id}")
    
    def get_user(self, user_id):
        """Получение информации о пользователе"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT user_id, username, first_name, joined_at, is_active, bot_started FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        conn.close()
        return user
    
    def get_all_users(self):
        """Получение всех активных пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT user_id, username, first_name, joined_at, is_active, bot_started FROM users WHERE is_active = 1')
        users = cursor.fetchall()
        
        conn.close()
        return users
    
    def get_latest_users(self, limit=10):
        """Получение последних зарегистрированных пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user_id, username, first_name, joined_at, is_active, bot_started 
            FROM users 
            WHERE is_active = 1 
            ORDER BY joined_at DESC 
            LIMIT ?
        ''', (limit,))
        users = cursor.fetchall()
        
        conn.close()
        return users
    
    def export_users_to_csv(self):
        """Экспорт всех пользователей в CSV формат"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT user_id, username, first_name, joined_at, is_active, bot_started 
            FROM users 
            ORDER BY joined_at DESC
        ''')
        users = cursor.fetchall()
        
        # Создаем CSV в памяти
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Заголовки
        writer.writerow(['ID', 'Username', 'Имя', 'Дата регистрации', 'Статус', 'Разговор с ботом'])
        
        # Данные
        for user in users:
            user_id, username, first_name, joined_at, is_active, bot_started = user
            status = 'Активен' if is_active else 'Отписался'
            bot_status = 'Да' if bot_started else 'Нет'
            writer.writerow([user_id, username or '', first_name or '', joined_at, status, bot_status])
        
        conn.close()
        
        # Возвращаем CSV как строку
        csv_content = output.getvalue()
        output.close()
        
        return csv_content
    
    def get_welcome_message(self):
        """Получение приветственного сообщения и фото"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT value FROM settings WHERE key = "welcome_message"')
        message = cursor.fetchone()
        
        cursor.execute('SELECT value FROM settings WHERE key = "welcome_photo_url"')
        photo = cursor.fetchone()
        
        conn.close()
        return {
            'text': message[0] if message else "Добро пожаловать!",
            'photo': photo[0] if photo and photo[0] else None
        }
    
    def get_goodbye_message(self):
        """Получение прощального сообщения и фото"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT value FROM settings WHERE key = "goodbye_message"')
        message = cursor.fetchone()
        
        cursor.execute('SELECT value FROM settings WHERE key = "goodbye_photo_url"')
        photo = cursor.fetchone()
        
        conn.close()
        return {
            'text': message[0] if message else "До свидания!",
            'photo': photo[0] if photo and photo[0] else None
        }
    
    def set_welcome_message(self, message, photo_url=None):
        """Установка приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE settings SET value = ? WHERE key = "welcome_message"
        ''', (message,))
        
        if photo_url is not None:
            cursor.execute('''
                UPDATE settings SET value = ? WHERE key = "welcome_photo_url"
            ''', (photo_url,))
        
        conn.commit()
        conn.close()
    
    def set_goodbye_message(self, message, photo_url=None):
        """Установка прощального сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE settings SET value = ? WHERE key = "goodbye_message"
        ''', (message,))
        
        if photo_url is not None:
            cursor.execute('''
                UPDATE settings SET value = ? WHERE key = "goodbye_photo_url"
            ''', (photo_url,))
        
        conn.commit()
        conn.close()
    
    def get_broadcast_message(self, message_number):
        """Получение сообщения рассылки по номеру"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT text, delay_hours, photo_url FROM broadcast_messages 
            WHERE message_number = ?
        ''', (message_number,))
        result = cursor.fetchone()
        
        conn.close()
        return result
    
    def get_all_broadcast_messages(self):
        """Получение всех сообщений рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM broadcast_messages ORDER BY message_number')
        messages = cursor.fetchall()
        
        conn.close()
        return messages
    
    def add_broadcast_message(self, text, delay_hours, photo_url=None):
        """Добавление нового сообщения рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Находим следующий доступный номер сообщения
        cursor.execute('SELECT MAX(message_number) FROM broadcast_messages')
        max_number = cursor.fetchone()[0]
        next_number = (max_number or 0) + 1
        
        cursor.execute('''
            INSERT INTO broadcast_messages (message_number, text, delay_hours, photo_url)
            VALUES (?, ?, ?, ?)
        ''', (next_number, text, delay_hours, photo_url))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Добавлено сообщение рассылки #{next_number}")
        return next_number
    
    def delete_broadcast_message(self, message_number):
        """Удаление сообщения рассылки и всех его запланированных отправок"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
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
        conn.close()
        
        logger.info(f"Удалено сообщение рассылки #{message_number}")
    
    def update_broadcast_message(self, message_number, text=None, delay_hours=None, photo_url=None):
        """Обновление сообщения рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
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
        conn.close()
    
    def add_message_button(self, message_number, button_text, button_url, position=1):
        """Добавление кнопки к сообщению"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO message_buttons (message_number, button_text, button_url, position)
            VALUES (?, ?, ?, ?)
        ''', (message_number, button_text, button_url, position))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Добавлена кнопка к сообщению #{message_number}")
    
    def update_message_button(self, button_id, button_text=None, button_url=None):
        """Обновление кнопки сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
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
        conn.close()
    
    def delete_message_button(self, button_id):
        """Удаление кнопки сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM message_buttons WHERE id = ?', (button_id,))
        
        conn.commit()
        conn.close()
    
    def get_message_buttons(self, message_number):
        """Получение всех кнопок для конкретного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, button_text, button_url, position 
            FROM message_buttons 
            WHERE message_number = ? 
            ORDER BY position
        ''', (message_number,))
        
        buttons = cursor.fetchall()
        conn.close()
        return buttons
    
    def get_broadcast_status(self):
        """Получение текущего статуса рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT value FROM broadcast_settings WHERE key = "broadcast_enabled"')
        enabled = cursor.fetchone()
        
        cursor.execute('SELECT value FROM broadcast_settings WHERE key = "auto_resume_time"')
        resume_time = cursor.fetchone()
        
        conn.close()
        
        return {
            'enabled': enabled[0] == '1' if enabled else True,
            'auto_resume_time': resume_time[0] if resume_time and resume_time[0] else None
        }
    
    def set_broadcast_status(self, enabled, auto_resume_time=None):
        """Установка статуса рассылки"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE broadcast_settings SET value = ? WHERE key = "broadcast_enabled"
        ''', ('1' if enabled else '0',))
        
        if auto_resume_time is not None:
            cursor.execute('''
                UPDATE broadcast_settings SET value = ? WHERE key = "auto_resume_time"
            ''', (auto_resume_time,))
        
        conn.commit()
        conn.close()
    
    def schedule_message(self, user_id, message_number, scheduled_time):
        """Планирование отправки сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO scheduled_messages (user_id, message_number, scheduled_time)
            VALUES (?, ?, ?)
        ''', (user_id, message_number, scheduled_time))
        
        conn.commit()
        conn.close()
    
    def get_pending_messages(self):
        """Получение сообщений, готовых к отправке"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        current_time = datetime.now()
        cursor.execute('''
            SELECT sm.id, sm.user_id, sm.message_number, bm.text, bm.photo_url
            FROM scheduled_messages sm
            JOIN broadcast_messages bm ON sm.message_number = bm.message_number
            WHERE sm.is_sent = 0 AND sm.scheduled_time <= ?
        ''', (current_time,))
        
        messages = cursor.fetchall()
        conn.close()
        return messages
    
    def mark_message_sent(self, message_id):
        """Отметка сообщения как отправленного"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE scheduled_messages SET is_sent = 1 
            WHERE id = ?
        ''', (message_id,))
        
        conn.commit()
        conn.close()
    
    def cancel_user_messages(self, user_id):
        """Отмена всех запланированных сообщений для пользователя"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM scheduled_messages 
            WHERE user_id = ? AND is_sent = 0
        ''', (user_id,))
        
        conn.commit()
        conn.close()
        logger.info(f"Отменены запланированные сообщения для пользователя {user_id}")
    
    def get_user_statistics(self):
        """Получение статистики пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
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
        
        conn.close()
        return {
            'total_users': total_users,
            'bot_started_users': bot_started_users,
            'new_users_24h': new_users_24h,
            'sent_messages': sent_messages,
            'unsubscribed': unsubscribed
        }
