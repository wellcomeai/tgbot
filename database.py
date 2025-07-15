import sqlite3
import json
import os
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
                is_active INTEGER DEFAULT 1
            )
        ''')
        
        # Таблица сообщений рассылки
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS broadcast_messages (
                message_number INTEGER PRIMARY KEY,
                text TEXT NOT NULL,
                delay_hours INTEGER DEFAULT 24
            )
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
        
        # Таблица настроек
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
        
        # Инициализация сообщений рассылки по умолчанию
        default_messages = [
            ("Сообщение 1: Основы работы с нашим сервисом 📚", 24),
            ("Сообщение 2: Продвинутые функции и возможности 🔧", 48),
            ("Сообщение 3: Лучшие практики и советы 💡", 72),
            ("Сообщение 4: Частые вопросы и ответы ❓", 96),
            ("Сообщение 5: Примеры успешных кейсов 📈", 120),
            ("Сообщение 6: Дополнительные ресурсы 📖", 144),
            ("Сообщение 7: Благодарность и обратная связь 🙏", 168)
        ]
        
        for i, (text, delay) in enumerate(default_messages, 1):
            cursor.execute('''
                INSERT OR IGNORE INTO broadcast_messages (message_number, text, delay_hours)
                VALUES (?, ?, ?)
            ''', (i, text, delay))
        
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
            INSERT OR REPLACE INTO users (user_id, username, first_name)
            VALUES (?, ?, ?)
        ''', (user_id, username, first_name))
        
        conn.commit()
        conn.close()
        logger.info(f"Добавлен пользователь {user_id} (@{username})")
    
    def get_user(self, user_id):
        """Получение информации о пользователе"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM users WHERE user_id = ?', (user_id,))
        user = cursor.fetchone()
        
        conn.close()
        return user
    
    def get_all_users(self):
        """Получение всех активных пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM users WHERE is_active = 1')
        users = cursor.fetchall()
        
        conn.close()
        return users
    
    def get_welcome_message(self):
        """Получение приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('SELECT value FROM settings WHERE key = "welcome_message"')
        result = cursor.fetchone()
        
        conn.close()
        return result[0] if result else "Добро пожаловать!"
    
    def set_welcome_message(self, message):
        """Установка приветственного сообщения"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE settings SET value = ? WHERE key = "welcome_message"
        ''', (message,))
        
        conn.commit()
        conn.close()
    
    def get_broadcast_message(self, message_number):
        """Получение сообщения рассылки по номеру"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT text, delay_hours FROM broadcast_messages 
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
    
    def update_broadcast_message(self, message_number, text=None, delay_hours=None):
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
            SELECT sm.id, sm.user_id, sm.message_number, bm.text
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
    
    def get_user_statistics(self):
        """Получение статистики пользователей"""
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Общее количество пользователей
        cursor.execute('SELECT COUNT(*) FROM users WHERE is_active = 1')
        total_users = cursor.fetchone()[0]
        
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
        
        conn.close()
        return {
            'total_users': total_users,
            'new_users_24h': new_users_24h,
            'sent_messages': sent_messages
        }
