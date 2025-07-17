from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from datetime import datetime, timedelta
import logging
import io
import re
import asyncio

logger = logging.getLogger(__name__)

class AdminPanel:
    def __init__(self, db, admin_chat_id):
        self.db = db
        self.admin_chat_id = admin_chat_id
        self.waiting_for = {}  # Словарь для отслеживания ожидаемого ввода
        self.admin_messages = {}  # Словарь для хранения ID сообщений админа {user_id: [message_ids]}
    
    async def cleanup_previous_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
        """Удаление предыдущих сообщений админа для очистки чата"""
        if user_id not in self.admin_messages:
            return
        
        message_ids = self.admin_messages[user_id].copy()
        
        # Очищаем список, так как будем удалять сообщения
        self.admin_messages[user_id] = []
        
        # Удаляем сообщения с небольшой задержкой
        for message_id in message_ids:
            try:
                await asyncio.sleep(0.05)  # Небольшая задержка между удалениями
                await context.bot.delete_message(chat_id=user_id, message_id=message_id)
                logger.debug(f"🗑️ Удалено сообщение {message_id} для админа {user_id}")
            except BadRequest as e:
                # Сообщение уже удалено или недоступно - это нормально
                logger.debug(f"ℹ️ Не удалось удалить сообщение {message_id}: {e}")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при удалении сообщения {message_id}: {e}")
                # Продолжаем удалять остальные сообщения
    
    def track_admin_message(self, user_id: int, message_id: int):
        """Добавление ID сообщения в отслеживание"""
        if user_id not in self.admin_messages:
            self.admin_messages[user_id] = []
        
        self.admin_messages[user_id].append(message_id)
        
        # Ограничиваем количество отслеживаемых сообщений (последние 10)
        if len(self.admin_messages[user_id]) > 10:
            self.admin_messages[user_id] = self.admin_messages[user_id][-10:]
        
        logger.debug(f"📝 Отслеживаем сообщение {message_id} для админа {user_id}")
    
    async def safe_edit_or_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode='HTML'):
        """Безопасное редактирование или отправка сообщения с обработкой ошибок"""
        user_id = update.effective_user.id
        
        try:
            if update.callback_query:
                try:
                    sent_message = await update.callback_query.edit_message_text(
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode=parse_mode
                    )
                except BadRequest as e:
                    if "Message to edit not found" in str(e) or "Message is not modified" in str(e):
                        # Сообщение уже удалено или не изменилось, отправляем новое
                        sent_message = await context.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode
                        )
                    else:
                        raise
            else:
                sent_message = await update.message.reply_text(
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
            
            # Отслеживаем новое сообщение
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            
            return sent_message
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке/редактировании сообщения: {e}")
            # Попытка отправить новое сообщение в случае ошибки
            try:
                sent_message = await context.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                return sent_message
            except Exception as e2:
                logger.error(f"❌ Критическая ошибка при отправке сообщения: {e2}")
                return None
    
    def parse_delay_input(self, text):
        """Парсинг ввода задержки в различных форматах"""
        text = text.strip().lower()
        
        try:
            # Проверяем формат с минутами
            if 'м' in text or 'минут' in text:
                # Извлекаем число
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    minutes = float(match.group(1))
                    hours = minutes / 60
                    return hours, f"{int(minutes)} минут"
            
            # Проверяем формат с часами
            elif 'ч' in text or 'час' in text:
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} часов"
            
            # Проверяем просто число (считаем как часы)
            else:
                hours = float(text)
                if hours < 1:
                    minutes = int(hours * 60)
                    return hours, f"{minutes} минут"
                else:
                    return hours, f"{hours} часов"
                    
        except ValueError:
            return None, None
        
        return None, None
    
    def parse_hours_input(self, text):
        """Парсинг ввода часов для планирования рассылок"""
        text = text.strip().lower()
        
        try:
            # Убираем все пробелы и проверяем различные форматы
            text_clean = text.replace(' ', '')
            
            # Форматы: 1ч, 2ч, 3час, 4часа, 5часов
            if 'ч' in text_clean:
                match = re.search(r'(\d+(?:\.\d+)?)', text_clean)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} час(ов)"
            
            # Форматы: час, часа, часов с числом
            elif any(word in text for word in ['час', 'часа', 'часов']):
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} час(ов)"
            
            # Просто число - считаем как часы
            else:
                hours = float(text)
                return hours, f"{hours} час(ов)"
                    
        except ValueError:
            return None, None
        
        return None, None
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню админа"""
        user_id = update.effective_user.id
        
        # Очищаем предыдущие сообщения
        await self.cleanup_previous_messages(update, context, user_id)
        
        broadcast_status = self.db.get_broadcast_status()
        status_icon = "🟢" if broadcast_status['enabled'] else "🔴"
        
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("✉️ Управление рассылкой", callback_data="admin_broadcast")],
            [InlineKeyboardButton(f"{status_icon} Статус рассылки", callback_data="admin_broadcast_status")],
            [InlineKeyboardButton("👋 Приветственное сообщение", callback_data="admin_welcome")],
            [InlineKeyboardButton("😢 Прощальное сообщение", callback_data="admin_goodbye")],
            [InlineKeyboardButton("✅ Сообщение подтверждения", callback_data="admin_success_message")],
            [InlineKeyboardButton("📢 Отправить всем сообщение", callback_data="admin_send_all")],
            [InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="admin_scheduled_broadcasts")],
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "🔧 <b>Админ-панель</b>\n\nВыберите действие:"
        
        # Используем безопасный метод отправки/редактирования
        sent_message = await self.safe_edit_or_send_message(update, context, text, reply_markup)
        return sent_message
    
    async def show_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику"""
        user_id = update.effective_user.id
        
        stats = self.db.get_user_statistics()
        
        text = (
            "📊 <b>Статистика бота</b>\n\n"
            f"👥 Всего активных пользователей: {stats['total_users']}\n"
            f"💬 Начали разговор с ботом: {stats['bot_started_users']}\n"
            f"🆕 Новых за 24 часа: {stats['new_users_24h']}\n"
            f"✉️ Отправлено сообщений: {stats['sent_messages']}\n"
            f"🚪 Отписалось пользователей: {stats['unsubscribed']}\n\n"
            f"💡 <b>Массовая рассылка:</b> доступна для {stats['bot_started_users']} пользователей"
        )
        
        keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_broadcast_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статус рассылки"""
        user_id = update.effective_user.id
        
        broadcast_status = self.db.get_broadcast_status()
        
        status_text = "✅ Включена" if broadcast_status['enabled'] else "❌ Отключена"
        
        text = f"🔄 <b>Статус рассылки</b>\n\n<b>Текущий статус:</b> {status_text}\n"
        
        if not broadcast_status['enabled'] and broadcast_status['auto_resume_time']:
            resume_time = datetime.fromisoformat(broadcast_status['auto_resume_time'])
            text += f"<b>Автовозобновление:</b> {resume_time.strftime('%d.%m.%Y %H:%M')}\n"
        
        keyboard = []
        
        if broadcast_status['enabled']:
            keyboard.append([InlineKeyboardButton("🔴 Отключить рассылку", callback_data="disable_broadcast")])
        else:
            keyboard.append([InlineKeyboardButton("🟢 Включить рассылку", callback_data="enable_broadcast")])
            keyboard.append([InlineKeyboardButton("⏰ Установить таймер", callback_data="set_broadcast_timer")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_broadcast_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления рассылкой"""
        user_id = update.effective_user.id
        
        messages = self.db.get_all_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url in messages:
            # Получаем количество кнопок для сообщения
            buttons = self.db.get_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            photo_icon = "🖼" if photo_url else ""
            
            # Форматируем отображение времени
            if delay_hours < 1:
                delay_str = f"{int(delay_hours * 60)}м"
            else:
                delay_str = f"{delay_hours}ч"
            
            button_text = f"{photo_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после регистрации.\n"
            "🖼 - сообщение содержит фото\n"
            "🔘N - количество кнопок в сообщении"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню редактирования конкретного сообщения"""
        user_id = update.effective_user.id
        
        msg_data = self.db.get_broadcast_message(message_number)
        if not msg_data:
            await update.callback_query.answer("Сообщение не найдено!", show_alert=True)
            return
        
        text, delay_hours, photo_url = msg_data
        buttons = self.db.get_message_buttons(message_number)
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_delay_{message_number}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_photo_{message_number}")]
        ]
        
        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_photo_{message_number}")])
        
        # Кнопки для управления кнопками сообщения
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_buttons_{message_number}")])
        
        # Кнопка удаления сообщения
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_msg_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Формируем текст с информацией о кнопках
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        # Форматируем отображение задержки
        if delay_hours < 1:
            delay_str = f"{int(delay_hours * 60)} минут"
        else:
            delay_str = f"{delay_hours} часов"
        
        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после регистрации\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
            f"{buttons_info}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_message_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню управления кнопками сообщения"""
        user_id = update.effective_user.id
        
        buttons = self.db.get_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_button_{button_id}")])
        
        if len(buttons) < 3:  # Максимум 3 кнопки
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Кнопки сообщения {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки"""
        user_id = update.effective_user.id
        
        # Получаем информацию о кнопке
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT message_number, button_text, button_url 
            FROM message_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        message_number, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_buttons_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            "Выберите действие:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_success_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования сообщения подтверждения"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение подтверждения
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
        success_msg = cursor.fetchone()
        conn.close()
        
        if success_msg:
            current_message = success_msg[0]
        else:
            current_message = (
                "👋 <b>Добро пожаловать!</b>\n\n"
                "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                "📚 <b>Вы получите доступ к:</b>\n"
                "• Эксклюзивным материалам\n"
                "• Полезным советам и инструкциям\n"
                "• Актуальным новостям\n"
                "• Поддержке сообщества\n\n"
                "🙏 <b>Спасибо, что подписались!</b>\n\n"
                "💬 Если у вас есть вопросы - не стесняйтесь писать!"
            )
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_success_message_text")],
            [InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_success_message")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "✅ <b>Сообщение подтверждения</b>\n\n"
            "Это сообщение отправляется пользователям после успешной подписки.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)

    # ===== МЕТОДЫ ДЛЯ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ =====
    
    async def show_welcome_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования приветственного сообщения"""
        user_id = update.effective_user.id
        
        welcome_data = self.db.get_welcome_message()
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        # Управление кнопками
        keyboard.append([InlineKeyboardButton("⌨️ Управление механическими кнопками", callback_data="manage_welcome_buttons")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Добавляем информацию о кнопках
        buttons_info = ""
        if welcome_buttons:
            buttons_info = f"\n\n<b>⌨️ Механические кнопки ({len(welcome_buttons)}):</b>\n"
            for i, (button_id, button_text, position) in enumerate(welcome_buttons, 1):
                # Получаем количество последующих сообщений
                follow_messages = self.db.get_welcome_follow_messages(button_id)
                follow_count = len(follow_messages)
                buttons_info += f"{i}. {button_text} ({follow_count} сообщ.)\n"
            buttons_info += "\n💡 <b>Пользователи видят эти кнопки внизу экрана</b>"
        else:
            buttons_info += (
                "\n\n<b>⚠️ Механические кнопки не настроены</b>\n"
                "Пользователи видят стандартные кнопки:\n"
                "• ✅ Согласиться на получение уведомлений\n"
                "• 📋 Что я буду получать?\n"
                "• ℹ️ Подробнее о боте\n\n"
                "💡 <b>Рекомендация:</b> Добавьте свои кнопки для полного контроля!"
            )
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_welcome_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками приветственного сообщения"""
        user_id = update.effective_user.id
        
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = []
        
        for button_id, button_text, position in welcome_buttons:
            # Получаем количество последующих сообщений
            follow_messages = self.db.get_welcome_follow_messages(button_id)
            follow_count = len(follow_messages)
            
            button_display = f"⌨️ {button_text} ({follow_count} сообщ.)"
            keyboard.append([InlineKeyboardButton(button_display, callback_data=f"edit_welcome_button_{button_id}")])
        
        if len(welcome_buttons) < 5:  # Максимум 5 механических кнопок
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_welcome_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_welcome")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Механические кнопки приветствия</b>\n\n"
            f"Текущие кнопки: {len(welcome_buttons)}/5\n\n"
            "💡 <b>Что это:</b>\n"
            "• Кнопки отображаются внизу экрана у пользователя\n"
            "• При нажатии автоматически подписывается на рассылку\n"
            "• Можно настроить последующие сообщения\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_welcome_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки приветственного сообщения"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT button_text 
            FROM welcome_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        button_text = button_data[0]
        follow_messages = self.db.get_welcome_follow_messages(button_id)
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_button_text_{button_id}")],
            [InlineKeyboardButton("📬 Управление сообщениями", callback_data=f"manage_welcome_follow_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_welcome_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Информация о последующих сообщениях
        messages_info = ""
        if follow_messages:
            messages_info = f"\n<b>Последующие сообщения ({len(follow_messages)}):</b>\n"
            for msg_id, msg_num, text, photo_url in follow_messages:
                short_text = text[:30] + "..." if len(text) > 30 else text
                photo_icon = "🖼" if photo_url else ""
                messages_info += f"{msg_num}. {photo_icon}{short_text}\n"
        
        text = (
            f"⌨️ <b>Редактирование механической кнопки</b>\n\n"
            f"<b>Текст кнопки:</b> {button_text}\n\n"
            f"💡 <b>Как работает:</b>\n"
            f"• Пользователь видит эту кнопку внизу экрана\n"
            f"• При нажатии автоматически подписывается\n"
            f"• Получает все настроенные последующие сообщения"
            f"{messages_info}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_welcome_follow_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню управления последующими сообщениями"""
        user_id = update.effective_user.id
        
        follow_messages = self.db.get_welcome_follow_messages(button_id)
        
        keyboard = []
        
        for msg_id, msg_num, text, photo_url in follow_messages:
            short_text = text[:20] + "..." if len(text) > 20 else text
            photo_icon = "🖼" if photo_url else ""
            button_display = f"{photo_icon} Сообщение {msg_num}: {short_text}"
            keyboard.append([InlineKeyboardButton(button_display, callback_data=f"edit_welcome_follow_{msg_id}")])
        
        if len(follow_messages) < 3:  # Максимум 3 сообщения
            keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data=f"add_welcome_follow_{button_id}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_welcome_button_{button_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"📬 <b>Последующие сообщения</b>\n\n"
            f"Текущие сообщения: {len(follow_messages)}/3\n\n"
            "Эти сообщения отправятся после нажатия кнопки:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_welcome_follow_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_id):
        """Показать меню редактирования последующего сообщения"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT welcome_button_id, message_number, text, photo_url 
            FROM welcome_follow_messages 
            WHERE id = ?
        ''', (message_id,))
        message_data = cursor.fetchone()
        conn.close()
        
        if not message_data:
            await update.callback_query.answer("Сообщение не найдено!", show_alert=True)
            return
        
        button_id, msg_num, text, photo_url = message_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_follow_text_{message_id}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_welcome_follow_photo_{message_id}")]
        ]
        
        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_welcome_follow_photo_{message_id}")])
        
        keyboard.extend([
            [InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_welcome_follow_{message_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_welcome_follow_{button_id}")]
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"📬 <b>Сообщение {msg_num}</b>\n\n"
            f"<b>Текст:</b>\n{text}\n\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    # ===== МЕТОДЫ ДЛЯ ПРОЩАЛЬНОГО СООБЩЕНИЯ =====
    
    async def show_goodbye_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования прощального сообщения"""
        user_id = update.effective_user.id
        
        goodbye_data = self.db.get_goodbye_message()
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        # Управление кнопками
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data="manage_goodbye_buttons")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Добавляем информацию о кнопках
        buttons_info = ""
        if goodbye_buttons:
            buttons_info = f"\n<b>Кнопки ({len(goodbye_buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(goodbye_buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_goodbye_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками прощального сообщения"""
        user_id = update.effective_user.id
        
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = []
        
        for button_id, button_text, button_url, position in goodbye_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_goodbye_button_{button_id}")])
        
        if len(goodbye_buttons) < 3:  # Максимум 3 кнопки
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_goodbye_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_goodbye")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Кнопки прощального сообщения</b>\n\n"
            f"Текущие кнопки: {len(goodbye_buttons)}/3\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_goodbye_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки прощального сообщения"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT button_text, button_url 
            FROM goodbye_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_goodbye_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_goodbye_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_goodbye_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_goodbye_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            "Выберите действие:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    # ===== МЕТОДЫ ДЛЯ ЗАПЛАНИРОВАННЫХ РАССЫЛОК =====
    
    async def show_scheduled_broadcasts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать запланированные рассылки"""
        user_id = update.effective_user.id
        
        broadcasts = self.db.get_scheduled_broadcasts(include_sent=False)
        
        keyboard = []
        
        if broadcasts:
            for broadcast_id, message_text, photo_url, scheduled_time, is_sent, created_at in broadcasts:
                scheduled_dt = datetime.fromisoformat(scheduled_time)
                time_str = scheduled_dt.strftime("%d.%m %H:%M")
                
                # Получаем количество кнопок
                buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
                button_icon = f"🔘{len(buttons)}" if buttons else ""
                photo_icon = "🖼" if photo_url else ""
                
                short_text = message_text[:20] + "..." if len(message_text) > 20 else message_text
                button_display = f"{photo_icon}{button_icon} {time_str}: {short_text}"
                keyboard.append([InlineKeyboardButton(button_display, callback_data=f"edit_scheduled_broadcast_{broadcast_id}")])
        
        keyboard.append([InlineKeyboardButton("➕ Запланировать рассылку", callback_data="add_scheduled_broadcast")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "⏰ <b>Запланированные рассылки</b>\n\n"
            f"Активных рассылок: {len(broadcasts)}\n\n"
            "🖼 - сообщение с фото\n"
            "🔘N - количество кнопок\n\n"
            "Выберите рассылку для редактирования:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_scheduled_broadcast_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
        """Показать меню редактирования запланированной рассылки"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT message_text, photo_url, scheduled_time, created_at
            FROM scheduled_broadcasts 
            WHERE id = ? AND is_sent = 0
        ''', (broadcast_id,))
        broadcast_data = cursor.fetchone()
        conn.close()
        
        if not broadcast_data:
            await update.callback_query.answer("Рассылка не найдена или уже отправлена!", show_alert=True)
            return
        
        message_text, photo_url, scheduled_time, created_at = broadcast_data
        buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
        
        scheduled_dt = datetime.fromisoformat(scheduled_time)
        current_dt = datetime.now()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_scheduled_text_{broadcast_id}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_scheduled_photo_{broadcast_id}")]
        ]
        
        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_scheduled_photo_{broadcast_id}")])
        
        keyboard.extend([
            [InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_scheduled_buttons_{broadcast_id}")],
            [InlineKeyboardButton("⏰ Изменить время", callback_data=f"edit_scheduled_time_{broadcast_id}")],
            [InlineKeyboardButton("🗑 Отменить рассылку", callback_data=f"delete_scheduled_broadcast_{broadcast_id}")],
            [InlineKeyboardButton("« Назад", callback_data="admin_scheduled_broadcasts")]
        ])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Информация о кнопках
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        # Вычисляем время до отправки
        time_diff = scheduled_dt - current_dt
        if time_diff.total_seconds() > 0:
            if time_diff.days > 0:
                time_until = f"{time_diff.days} дн. {time_diff.seconds // 3600} ч."
            else:
                time_until = f"{time_diff.seconds // 3600} ч. {(time_diff.seconds % 3600) // 60} мин."
        else:
            time_until = "Готово к отправке"
        
        text = (
            f"⏰ <b>Редактирование рассылки #{broadcast_id}</b>\n\n"
            f"<b>Текст:</b>\n{message_text}\n\n"
            f"<b>Запланировано на:</b> {scheduled_dt.strftime('%d.%m.%Y %H:%M')}\n"
            f"<b>До отправки:</b> {time_until}\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
            f"{buttons_info}"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_scheduled_broadcast_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
        """Показать меню управления кнопками запланированной рассылки"""
        user_id = update.effective_user.id
        
        buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_scheduled_button_{button_id}")])
        
        if len(buttons) < 3:  # Максимум 3 кнопки
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_scheduled_button_{broadcast_id}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_scheduled_broadcast_{broadcast_id}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Кнопки рассылки</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "Выберите кнопку для редактирования:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_scheduled_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки запланированной рассылки"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
            SELECT broadcast_id, button_text, button_url 
            FROM scheduled_broadcast_buttons 
            WHERE id = ?
        ''', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("Кнопка не найдена!", show_alert=True)
            return
        
        broadcast_id, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_scheduled_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_scheduled_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_scheduled_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_scheduled_buttons_{broadcast_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            "Выберите действие:"
        )
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    # ===== НОВАЯ УЛУЧШЕННАЯ СИСТЕМА МАССОВЫХ РАССЫЛОК =====
    
    async def show_send_all_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню отправки массового сообщения"""
        user_id = update.effective_user.id
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        text = (
            "📢 <b>Массовая рассылка</b>\n\n"
            "💡 <b>Возможности:</b>\n"
            "• Отправка текста и фото\n"
            "• Добавление кнопок\n"
            "• Отправка сразу или по расписанию\n"
            "• Предварительный просмотр\n\n"
            "📝 Начните с отправки текста сообщения:"
        )
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Устанавливаем состояние ожидания
        self.waiting_for[user_id] = {
            "type": "send_all_text",
            "message_text": None,
            "photo_data": None,
            "buttons": [],
            "scheduled_hours": None
        }
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_broadcast_options(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать варианты отправки рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            await update.message.reply_text("❌ Данные рассылки не найдены. Начните сначала.")
            return
        
        broadcast_data = self.waiting_for[user_id]
        message_text = broadcast_data.get("message_text", "")
        photo_data = broadcast_data.get("photo_data")
        buttons = broadcast_data.get("buttons", [])
        
        # Формируем краткое описание
        text = "🎯 <b>Настройка рассылки</b>\n\n"
        text += f"📝 <b>Текст:</b> {message_text[:50]}{'...' if len(message_text) > 50 else ''}\n"
        
        if photo_data:
            text += "🖼 <b>Фото:</b> Есть\n"
        
        if buttons:
            text += f"🔘 <b>Кнопки:</b> {len(buttons)}\n"
        
        text += "\n⚡ <b>Выберите вариант отправки:</b>"
        
        keyboard = [
            [InlineKeyboardButton("🚀 Отправить сразу", callback_data="send_immediately")],
            [InlineKeyboardButton("⏰ Запланировать", callback_data="schedule_broadcast")],
            [InlineKeyboardButton("🔘 Добавить кнопку", callback_data="add_broadcast_button")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_back")]
        ]
        
        # Если есть кнопки, добавляем опцию их удаления
        if buttons:
            keyboard.insert(-1, [InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="remove_last_button")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.message.reply_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def show_broadcast_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_data):
        """Показать предварительный просмотр рассылки"""
        user_id = update.effective_user.id
        
        message_text = broadcast_data.get("message_text", "")
        photo_data = broadcast_data.get("photo_data")
        buttons = broadcast_data.get("buttons", [])
        scheduled_hours = broadcast_data.get("scheduled_hours")
        
        # Формируем превью
        preview_text = "📋 <b>Предварительный просмотр рассылки</b>\n\n"
        
        if scheduled_hours:
            scheduled_time = datetime.now() + timedelta(hours=scheduled_hours)
            preview_text += f"⏰ <b>Запланировано на:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
            preview_text += f"⌛ <b>Через:</b> {scheduled_hours} час(ов)\n\n"
        else:
            preview_text += "🚀 <b>Отправка:</b> Немедленно\n\n"
        
        # Добавляем информацию о пользователях
        users_count = len(self.db.get_users_with_bot_started())
        preview_text += f"👥 <b>Получателей:</b> {users_count} пользователей\n\n"
        
        preview_text += "📝 <b>Текст сообщения:</b>\n"
        preview_text += f"<code>{message_text}</code>\n\n"
        
        if photo_data:
            preview_text += "🖼 <b>Фото:</b> Есть\n\n"
        
        if buttons:
            preview_text += f"🔘 <b>Кнопки ({len(buttons)}):</b>\n"
            for i, button in enumerate(buttons, 1):
                preview_text += f"{i}. {button['text']} → {button['url']}\n"
            preview_text += "\n"
        
        preview_text += "✅ Подтвердите отправку:"
        
        keyboard = [
            [InlineKeyboardButton("✅ Отправить", callback_data="confirm_broadcast")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="edit_broadcast")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.message.reply_text(
            text=preview_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def execute_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_data):
        """Выполнить отправку рассылки"""
        user_id = update.effective_user.id
        
        message_text = broadcast_data.get("message_text", "")
        photo_data = broadcast_data.get("photo_data")
        buttons = broadcast_data.get("buttons", [])
        scheduled_hours = broadcast_data.get("scheduled_hours")
        
        try:
            if scheduled_hours:
                # Запланированная рассылка
                scheduled_time = datetime.now() + timedelta(hours=scheduled_hours)
                broadcast_id = self.db.add_scheduled_broadcast(message_text, scheduled_time, photo_data)
                
                # Добавляем кнопки если есть
                for i, button in enumerate(buttons, 1):
                    self.db.add_scheduled_broadcast_button(broadcast_id, button["text"], button["url"], i)
                
                await update.callback_query.answer("✅ Рассылка запланирована!")
                
                result_text = (
                    f"⏰ <b>Рассылка запланирована!</b>\n\n"
                    f"📅 <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                    f"⌛ <b>Через:</b> {scheduled_hours} час(ов)\n"
                    f"📨 <b>ID рассылки:</b> #{broadcast_id}"
                )
                
            else:
                # Немедленная рассылка
                users_with_bot = self.db.get_users_with_bot_started()
                
                if not users_with_bot:
                    await update.callback_query.answer("❌ Нет пользователей для рассылки!", show_alert=True)
                    return
                
                # Создаем клавиатуру если есть кнопки
                reply_markup = None
                if buttons:
                    keyboard = []
                    for button in buttons:
                        keyboard.append([InlineKeyboardButton(button["text"], url=button["url"])])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                
                sent_count = 0
                failed_count = 0
                
                await update.callback_query.answer("🚀 Начинаем рассылку...")
                
                for user in users_with_bot:
                    user_id_to_send = user[0]
                    try:
                        await asyncio.sleep(0.1)  # Небольшая задержка
                        
                        if photo_data:
                            await context.bot.send_photo(
                                chat_id=user_id_to_send,
                                photo=photo_data,
                                caption=message_text,
                                parse_mode='HTML',
                                reply_markup=reply_markup
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=user_id_to_send,
                                text=message_text,
                                parse_mode='HTML',
                                reply_markup=reply_markup
                            )
                        sent_count += 1
                        
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"❌ Не удалось отправить рассылку пользователю {user_id_to_send}: {e}")
                
                result_text = (
                    f"✅ <b>Рассылка завершена!</b>\n\n"
                    f"📤 <b>Успешно отправлено:</b> {sent_count}\n"
                    f"❌ <b>Ошибок:</b> {failed_count}"
                )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
            
            # Отправляем результат
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=result_text,
                parse_mode='HTML'
            )
            
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            
            # Возвращаемся в главное меню через 3 секунды
            await asyncio.sleep(3)
            await self.show_main_menu_from_context(update, context)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при выполнении рассылки: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке рассылки!", show_alert=True)
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ =====
    
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать список пользователей"""
        user_id = update.effective_user.id
        
        users = self.db.get_latest_users(10)  # Показываем последних 10
        
        if not users:
            text = "👥 <b>Список пользователей</b>\n\nПользователей пока нет."
        else:
            text = "👥 <b>Список пользователей</b>\n\n<b>Последние 10 регистраций:</b>\n\n"
            for user in users:
                user_id_db, username, first_name, joined_at, is_active, bot_started = user
                username_str = f"@{username}" if username else "без username"
                # Форматируем дату
                join_date = datetime.fromisoformat(joined_at).strftime("%d.%m.%Y %H:%M")
                bot_status = "💬" if bot_started else "❌"
                text += f"• {first_name} ({username_str}) {bot_status}\n  ID: {user_id_db}, {join_date}\n\n"
            
            text += "\n💬 - может получать рассылки\n❌ - нужно написать боту /start"
        
        keyboard = [
            [InlineKeyboardButton("📊 Скачать CSV", callback_data="download_csv")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    async def send_csv_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить CSV файл с пользователями"""
        user_id = update.effective_user.id
        
        try:
            csv_content = self.db.export_users_to_csv()
            
            # Создаем файл в памяти
            csv_file = io.BytesIO()
            csv_file.write(csv_content.encode('utf-8'))
            csv_file.seek(0)
            
            # Отправляем файл
            sent_message = await context.bot.send_document(
                chat_id=update.callback_query.from_user.id,
                document=csv_file,
                filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="📊 Список всех пользователей бота"
            )
            
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            
            await update.callback_query.answer("CSV файл отправлен!")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке CSV: {e}")
            await update.callback_query.answer("Ошибка при создании файла!", show_alert=True)
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, input_type, **kwargs):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        self.waiting_for[user_id] = {"type": input_type, **kwargs}
        
        texts = {
            "welcome": "✏️ Отправьте новое приветственное сообщение:",
            "goodbye": "✏️ Отправьте новое прощальное сообщение:",
            "success_message": "✏️ Отправьте новое сообщение подтверждения (отправляется после успешной подписки):",
            "broadcast_text": f"✏️ Отправьте новый текст для сообщения {kwargs.get('message_number')}:",
            "broadcast_delay": self._get_delay_text(kwargs.get('message_number')),
            "broadcast_photo": f"🖼 Отправьте фото для сообщения {kwargs.get('message_number')} или ссылку на фото:",
            "welcome_photo": "🖼 Отправьте фото для приветственного сообщения или ссылку на фото:",
            "goodbye_photo": "🖼 Отправьте фото для прощального сообщения или ссылку на фото:",
            "add_message": "✏️ Отправьте текст нового сообщения:",
            "add_button": f"✏️ Отправьте текст для новой кнопки сообщения {kwargs.get('message_number')}:",
            "edit_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_button_url": "🔗 Отправьте новый URL для кнопки:",
            "broadcast_timer": "⏰ Отправьте количество часов, через которое возобновить рассылку:",
            "schedule_hours": "⏰ Через сколько часов отправить рассылку?\n\nПримеры: 1, 2.5, 24",
            "add_broadcast_button_text": "✏️ Отправьте текст для кнопки:",
            "add_broadcast_button_url": "🔗 Отправьте URL для кнопки:",
            
            # Новые типы для приветственного сообщения
            "add_welcome_button": "✏️ Отправьте текст для новой кнопки приветственного сообщения:",
            "edit_welcome_button_text": "✏️ Отправьте новый текст для кнопки:",
            "add_welcome_follow": f"✏️ Отправьте текст сообщения, которое будет отправлено после нажатия кнопки:",
            "edit_welcome_follow_text": "✏️ Отправьте новый текст сообщения:",
            "edit_welcome_follow_photo": "🖼 Отправьте фото или ссылку на фото:",
            
            # Новые типы для прощального сообщения
            "add_goodbye_button": "✏️ Отправьте текст для новой кнопки прощального сообщения:",
            "edit_goodbye_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_goodbye_button_url": "🔗 Отправьте новый URL для кнопки:",
            
            # Новые типы для запланированных рассылок
            "add_scheduled_broadcast": "📢 Отправьте текст сообщения для запланированной рассылки (можно с фото):",
            "schedule_broadcast_time": self._get_schedule_time_text(),
            "edit_scheduled_text": f"✏️ Отправьте новый текст для рассылки:",
            "edit_scheduled_photo": f"🖼 Отправьте фото или ссылку на фото:",
            "edit_scheduled_time": f"⏰ Отправьте новое время в часах:",
            "add_scheduled_button": f"✏️ Отправьте текст для новой кнопки:",
            "edit_scheduled_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_scheduled_button_url": "🔗 Отправьте новый URL для кнопки:",
        }
        
        text = texts.get(input_type, "Отправьте необходимые данные:")
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        sent_message = await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
        
        if sent_message:
            self.track_admin_message(user_id, sent_message.message_id)
    
    def _get_delay_text(self, message_number):
        """Получить текст для ввода задержки"""
        return (
            f"⏰ Отправьте новую задержку для сообщения {message_number}:\n\n"
            f"📝 <b>Форматы ввода:</b>\n"
            f"• <code>30м</code> или <code>30 минут</code> - для минут\n"
            f"• <code>2ч</code> или <code>2 часа</code> - для часов\n"
            f"• <code>1.5</code> - для 1.5 часов\n"
            f"• <code>0.05</code> - для 3 минут\n\n"
            f"💡 Примеры: <code>3м</code>, <code>30 минут</code>, <code>2ч</code>, <code>1.5</code>"
        )
    
    def _get_schedule_time_text(self):
        """Получить текст для ввода времени планирования"""
        return (
            "⏰ Через сколько часов отправить рассылку?\n\n"
            "📝 <b>Форматы ввода:</b>\n"
            "• <code>1ч</code> или <code>1 час</code> - через 1 час\n"
            "• <code>24ч</code> или <code>24 часа</code> - через 24 часа\n"
            "• <code>0.5</code> - через 30 минут\n"
            "• <code>48</code> - через 48 часов\n\n"
            "💡 Примеры: <code>2ч</code>, <code>12 часов</code>, <code>0.5</code>"
        )
    
    # ===== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ДЛЯ ВОЗВРАТА К НУЖНЫМ ЭКРАНАМ =====
    
    async def show_main_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню из контекста сообщения"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_main_menu(fake_update, context)
    
    async def show_broadcast_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню рассылки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_broadcast_menu(fake_update, context)
    
    async def show_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать редактирование сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_message_edit(fake_update, context, message_number)
    
    async def show_message_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать кнопки сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_message_buttons(fake_update, context, message_number)
    
    async def show_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать редактирование кнопки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_button_edit(fake_update, context, button_id)
    
    async def show_welcome_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать управление кнопками приветствия из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_welcome_buttons_management(fake_update, context)
    
    async def show_welcome_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать редактирование кнопки приветствия из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_welcome_button_edit(fake_update, context, button_id)
    
    async def show_welcome_follow_messages_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать последующие сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_welcome_follow_messages(fake_update, context, button_id)
    
    async def show_welcome_follow_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_id):
        """Показать редактирование последующего сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_welcome_follow_message_edit(fake_update, context, message_id)
    
    async def show_welcome_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования приветственного сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_welcome_edit(fake_update, context)
    
    async def show_goodbye_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования прощального сообщения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_goodbye_edit(fake_update, context)
    
    async def show_goodbye_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать управление кнопками прощания из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_goodbye_buttons_management(fake_update, context)
    
    async def show_goodbye_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать редактирование кнопки прощания из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_goodbye_button_edit(fake_update, context, button_id)
    
    async def show_scheduled_broadcasts_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать запланированные рассылки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_scheduled_broadcasts(fake_update, context)
    
    async def show_scheduled_broadcast_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
        """Показать редактирование рассылки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_scheduled_broadcast_edit(fake_update, context, broadcast_id)
    
    async def show_scheduled_broadcast_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, broadcast_id):
        """Показать кнопки рассылки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_scheduled_broadcast_buttons(fake_update, context, broadcast_id)
    
    async def show_scheduled_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать редактирование кнопки рассылки из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_scheduled_button_edit(fake_update, context, button_id)
    
    async def show_success_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать редактирование сообщения подтверждения из контекста"""
        class FakeQuery:
            def __init__(self, message):
                self.message = message
                self.from_user = message.from_user
                
            async def edit_message_text(self, text, reply_markup, parse_mode):
                return await self.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        
        fake_update = type('FakeUpdate', (), {})()
        fake_update.callback_query = FakeQuery(update.message)
        fake_update.effective_user = update.effective_user
        
        await self.show_success_message_edit(fake_update, context)
    
    # ===== ОБРАБОТЧИКИ CALLBACK ЗАПРОСОВ =====
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик всех callback запросов админ-панели"""
        query = update.callback_query
        data = query.data
        user_id = query.from_user.id
        
        if data == "admin_back":
            await self.show_main_menu(update, context)
        elif data == "admin_stats":
            await self.show_statistics(update, context)
        elif data == "admin_broadcast":
            await self.show_broadcast_menu(update, context)
        elif data == "admin_broadcast_status":
            await self.show_broadcast_status(update, context)
        elif data == "admin_welcome":
            await self.show_welcome_edit(update, context)
        elif data == "admin_goodbye":
            await self.show_goodbye_edit(update, context)
        elif data == "admin_success_message":
            await self.show_success_message_edit(update, context)
        elif data == "admin_users":
            await self.show_users_list(update, context)
        elif data == "admin_scheduled_broadcasts":
            await self.show_scheduled_broadcasts(update, context)
        elif data == "admin_send_all":
            await self.show_send_all_menu(update, context)
        elif data == "download_csv":
            await self.send_csv_file(update, context)
        elif data == "enable_broadcast":
            self.db.set_broadcast_status(True, None)
            await query.answer("Рассылка включена!")
            await self.show_broadcast_status(update, context)
        elif data == "disable_broadcast":
            self.db.set_broadcast_status(False, None)
            await query.answer("Рассылка отключена!")
            await self.show_broadcast_status(update, context)
        elif data == "set_broadcast_timer":
            await self.request_text_input(update, context, "broadcast_timer")
        elif data == "add_message":
            await self.request_text_input(update, context, "add_message")
        elif data == "edit_welcome_text":
            await self.request_text_input(update, context, "welcome")
        elif data == "edit_goodbye_text":
            await self.request_text_input(update, context, "goodbye")
        elif data == "edit_success_message_text":
            await self.request_text_input(update, context, "success_message")
        elif data == "reset_success_message":
            # Сбрасываем на сообщение по умолчанию
            default_success_message = (
                "👋 <b>Добро пожаловать!</b>\n\n"
                "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                "📚 <b>Вы получите доступ к:</b>\n"
                "• Эксклюзивным материалам\n"
                "• Полезным советам и инструкциям\n"
                "• Актуальным новостям\n"
                "• Поддержке сообщества\n\n"
                "🙏 <b>Спасибо, что подписались!</b>\n\n"
                "💬 Если у вас есть вопросы - не стесняйтесь писать!"
            )
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', 
                          ('success_message', default_success_message))
            conn.commit()
            conn.close()
            await query.answer("Сообщение сброшено на значение по умолчанию!")
            await self.show_success_message_edit(update, context)
        elif data == "edit_welcome_photo":
            await self.request_text_input(update, context, "welcome_photo")
        elif data == "edit_goodbye_photo":
            await self.request_text_input(update, context, "goodbye_photo")
        elif data == "remove_welcome_photo":
            self.db.set_welcome_message(self.db.get_welcome_message()['text'], photo_url="")
            await query.answer("Фото удалено!")
            await self.show_welcome_edit(update, context)
        elif data == "remove_goodbye_photo":
            self.db.set_goodbye_message(self.db.get_goodbye_message()['text'], photo_url="")
            await query.answer("Фото удалено!")
            await self.show_goodbye_edit(update, context)
        
        # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ УЛУЧШЕННОЙ РАССЫЛКИ =====
        elif data == "send_immediately":
            if user_id in self.waiting_for:
                broadcast_data = self.waiting_for[user_id]
                await self.show_broadcast_preview(update, context, broadcast_data)
        elif data == "schedule_broadcast":
            await self.request_text_input(update, context, "schedule_hours")
        elif data == "add_broadcast_button":
            await self.request_text_input(update, context, "add_broadcast_button_text")
        elif data == "remove_last_button":
            if user_id in self.waiting_for and "buttons" in self.waiting_for[user_id]:
                self.waiting_for[user_id]["buttons"].pop()
                await query.answer("Кнопка удалена!")
                await self.show_broadcast_options(update, context)
        elif data == "confirm_broadcast":
            if user_id in self.waiting_for:
                broadcast_data = self.waiting_for[user_id]
                await self.execute_broadcast(update, context, broadcast_data)
        elif data == "edit_broadcast":
            await self.show_broadcast_options(update, context)
        
        # ===== ОБРАБОТЧИКИ ДЛЯ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ =====
        elif data == "manage_welcome_buttons":
            await self.show_welcome_buttons_management(update, context)
        elif data == "add_welcome_button":
            await self.request_text_input(update, context, "add_welcome_button")
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА КНОПОК ПРИВЕТСТВИЯ
        elif data.startswith("edit_welcome_button_text_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_welcome_button_text", button_id=button_id)
        elif data.startswith("edit_welcome_button_") and not data.startswith("edit_welcome_button_text_"):
            button_id = int(data.split("_")[3])
            await self.show_welcome_button_edit(update, context, button_id)
        elif data.startswith("delete_welcome_button_"):
            button_id = int(data.split("_")[3])
            self.db.delete_welcome_button(button_id)
            await query.answer("Кнопка удалена!")
            await self.show_welcome_buttons_management(update, context)
        elif data.startswith("manage_welcome_follow_"):
            button_id = int(data.split("_")[3])
            await self.show_welcome_follow_messages(update, context, button_id)
        elif data.startswith("add_welcome_follow_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "add_welcome_follow", welcome_button_id=button_id)
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА ПОСЛЕДУЮЩИХ СООБЩЕНИЙ ПРИВЕТСТВИЯ
        elif data.startswith("edit_welcome_follow_text_"):
            message_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_welcome_follow_text", message_id=message_id)
        elif data.startswith("edit_welcome_follow_photo_"):
            message_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_welcome_follow_photo", message_id=message_id)
        elif data.startswith("edit_welcome_follow_") and not data.startswith("edit_welcome_follow_text_") and not data.startswith("edit_welcome_follow_photo_"):
            message_id = int(data.split("_")[3])
            await self.show_welcome_follow_message_edit(update, context, message_id)
        elif data.startswith("remove_welcome_follow_photo_"):
            message_id = int(data.split("_")[4])
            self.db.update_welcome_follow_message(message_id, photo_url="")
            await query.answer("Фото удалено!")
            await self.show_welcome_follow_message_edit(update, context, message_id)
        elif data.startswith("delete_welcome_follow_"):
            message_id = int(data.split("_")[3])
            # Получаем button_id для возврата
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT welcome_button_id FROM welcome_follow_messages WHERE id = ?', (message_id,))
            result = cursor.fetchone()
            conn.close()
            
            self.db.delete_welcome_follow_message(message_id)
            await query.answer("Сообщение удалено!")
            
            if result:
                button_id = result[0]
                await self.show_welcome_follow_messages(update, context, button_id)
        
        # ===== ОБРАБОТЧИКИ ДЛЯ ПРОЩАЛЬНОГО СООБЩЕНИЯ =====
        elif data == "manage_goodbye_buttons":
            await self.show_goodbye_buttons_management(update, context)
        elif data == "add_goodbye_button":
            await self.request_text_input(update, context, "add_goodbye_button")
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА КНОПОК ПРОЩАНИЯ
        elif data.startswith("edit_goodbye_button_text_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_goodbye_button_text", button_id=button_id)
        elif data.startswith("edit_goodbye_button_url_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_goodbye_button_url", button_id=button_id)
        elif data.startswith("edit_goodbye_button_") and not data.startswith("edit_goodbye_button_text_") and not data.startswith("edit_goodbye_button_url_"):
            button_id = int(data.split("_")[3])
            await self.show_goodbye_button_edit(update, context, button_id)
        elif data.startswith("delete_goodbye_button_"):
            button_id = int(data.split("_")[3])
            self.db.delete_goodbye_button(button_id)
            await query.answer("Кнопка удалена!")
            await self.show_goodbye_buttons_management(update, context)
        
        # ===== ОБРАБОТЧИКИ ДЛЯ ЗАПЛАНИРОВАННЫХ РАССЫЛОК =====
        elif data == "add_scheduled_broadcast":
            await self.request_text_input(update, context, "add_scheduled_broadcast")
        elif data.startswith("edit_scheduled_broadcast_"):
            broadcast_id = int(data.split("_")[3])
            await self.show_scheduled_broadcast_edit(update, context, broadcast_id)
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА РЕДАКТИРОВАНИЯ ЗАПЛАНИРОВАННЫХ РАССЫЛОК
        elif data.startswith("edit_scheduled_text_"):
            broadcast_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_scheduled_text", broadcast_id=broadcast_id)
        elif data.startswith("edit_scheduled_photo_"):
            broadcast_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_scheduled_photo", broadcast_id=broadcast_id)
        elif data.startswith("edit_scheduled_time_"):
            broadcast_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_scheduled_time", broadcast_id=broadcast_id)
        elif data.startswith("remove_scheduled_photo_"):
            broadcast_id = int(data.split("_")[3])
            # Обновляем рассылку, убирая фото
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE scheduled_broadcasts SET photo_url = NULL WHERE id = ?', (broadcast_id,))
            conn.commit()
            conn.close()
            await query.answer("Фото удалено!")
            await self.show_scheduled_broadcast_edit(update, context, broadcast_id)
        elif data.startswith("delete_scheduled_broadcast_"):
            broadcast_id = int(data.split("_")[3])
            self.db.delete_scheduled_broadcast(broadcast_id)
            await query.answer("Рассылка отменена!")
            await self.show_scheduled_broadcasts(update, context)
        elif data.startswith("manage_scheduled_buttons_"):
            broadcast_id = int(data.split("_")[3])
            await self.show_scheduled_broadcast_buttons(update, context, broadcast_id)
        elif data.startswith("add_scheduled_button_"):
            broadcast_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "add_scheduled_button", broadcast_id=broadcast_id)
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА КНОПОК ЗАПЛАНИРОВАННЫХ РАССЫЛОК
        elif data.startswith("edit_scheduled_button_text_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_scheduled_button_text", button_id=button_id)
        elif data.startswith("edit_scheduled_button_url_"):
            button_id = int(data.split("_")[4])
            await self.request_text_input(update, context, "edit_scheduled_button_url", button_id=button_id)
        elif data.startswith("edit_scheduled_button_") and not data.startswith("edit_scheduled_button_text_") and not data.startswith("edit_scheduled_button_url_"):
            button_id = int(data.split("_")[3])
            await self.show_scheduled_button_edit(update, context, button_id)
        elif data.startswith("delete_scheduled_button_"):
            button_id = int(data.split("_")[3])
            # Получаем broadcast_id для возврата
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT broadcast_id FROM scheduled_broadcast_buttons WHERE id = ?', (button_id,))
            result = cursor.fetchone()
            conn.close()
            
            self.db.delete_scheduled_broadcast_button(button_id)
            await query.answer("Кнопка удалена!")
            
            if result:
                broadcast_id = result[0]
                await self.show_scheduled_broadcast_buttons(update, context, broadcast_id)
        
        # ===== ОБРАБОТЧИКИ ДЛЯ ОСНОВНЫХ СООБЩЕНИЙ РАССЫЛКИ =====
        elif data.startswith("edit_msg_"):
            message_number = int(data.split("_")[2])
            await self.show_message_edit(update, context, message_number)
        elif data.startswith("delete_msg_"):
            message_number = int(data.split("_")[2])
            # Подтверждение удаления
            keyboard = [
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{message_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_msg_{message_number}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            sent_message = await query.edit_message_text(
                text=f"⚠️ Вы уверены, что хотите удалить сообщение {message_number}?\n\nЭто также отменит все запланированные отправки этого сообщения.",
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
        elif data.startswith("confirm_delete_"):
            message_number = int(data.split("_")[2])
            self.db.delete_broadcast_message(message_number)
            await query.answer("Сообщение удалено!")
            await self.show_broadcast_menu(update, context)
        elif data.startswith("manage_buttons_"):
            message_number = int(data.split("_")[2])
            await self.show_message_buttons(update, context, message_number)
        elif data.startswith("add_button_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "add_button", message_number=message_number)
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА КНОПОК - сначала проверяем полные паттерны
        elif data.startswith("edit_button_text_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_text", button_id=button_id)
        elif data.startswith("edit_button_url_"):
            button_id = int(data.split("_")[3])
            await self.request_text_input(update, context, "edit_button_url", button_id=button_id)
        elif data.startswith("edit_button_") and not data.startswith("edit_button_text_") and not data.startswith("edit_button_url_"):
            button_id = int(data.split("_")[2])
            await self.show_button_edit(update, context, button_id)
        elif data.startswith("delete_button_"):
            button_id = int(data.split("_")[2])
            # Получаем message_number для возврата
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT message_number FROM message_buttons WHERE id = ?', (button_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                message_number = result[0]
                self.db.delete_message_button(button_id)
                await query.answer("Кнопка удалена!")
                await self.show_message_buttons(update, context, message_number)
        
        # ИСПРАВЛЕННАЯ ОБРАБОТКА ТЕКСТА И ЗАДЕРЖКИ
        elif data.startswith("edit_text_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_text", message_number=message_number)
        elif data.startswith("edit_delay_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_delay", message_number=message_number)
        elif data.startswith("edit_photo_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_photo", message_number=message_number)
        elif data.startswith("remove_photo_"):
            message_number = int(data.split("_")[2])
            self.db.update_broadcast_message(message_number, photo_url="")
            await query.answer("Фото удалено!")
            await self.show_message_edit(update, context, message_number)
        elif data == "admin_cancel":
            user_id = query.from_user.id
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений и фото от админа"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            return
        
        waiting_data = self.waiting_for[user_id]
        input_type = waiting_data["type"]
        
        # Обработка фото
        if update.message.photo:
            photo_file_id = update.message.photo[-1].file_id
            
            if input_type == "send_all_text":
                # Для массовой рассылки с фото
                text = update.message.caption or ""
                if not text:
                    sent_message = await update.message.reply_text("❌ Отправьте фото с подписью (текстом сообщения)")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    return
                
                self.waiting_for[user_id]["message_text"] = text
                self.waiting_for[user_id]["photo_data"] = photo_file_id
                
                sent_message = await update.message.reply_text("✅ Сообщение с фото получено!")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                
                await self.show_broadcast_options(update, context)
                return
            
            elif input_type in ["broadcast_photo", "welcome_photo", "goodbye_photo", "edit_welcome_follow_photo", "edit_scheduled_photo"]:
                if input_type == "broadcast_photo":
                    message_number = waiting_data["message_number"]
                    self.db.update_broadcast_message(message_number, photo_url=photo_file_id)
                    sent_message = await update.message.reply_text(f"✅ Фото для сообщения {message_number} обновлено!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_message_edit_from_context(update, context, message_number)
                    
                elif input_type == "welcome_photo":
                    welcome_text = self.db.get_welcome_message()['text']
                    self.db.set_welcome_message(welcome_text, photo_url=photo_file_id)
                    sent_message = await update.message.reply_text("✅ Фото для приветственного сообщения обновлено!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_welcome_edit_from_context(update, context)
                    
                elif input_type == "goodbye_photo":
                    goodbye_text = self.db.get_goodbye_message()['text']
                    self.db.set_goodbye_message(goodbye_text, photo_url=photo_file_id)
                    sent_message = await update.message.reply_text("✅ Фото для прощального сообщения обновлено!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_goodbye_edit_from_context(update, context)
                
                elif input_type == "edit_welcome_follow_photo":
                    message_id = waiting_data["message_id"]
                    self.db.update_welcome_follow_message(message_id, photo_url=photo_file_id)
                    sent_message = await update.message.reply_text("✅ Фото обновлено!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_welcome_follow_message_edit_from_context(update, context, message_id)
                
                elif input_type == "edit_scheduled_photo":
                    broadcast_id = waiting_data["broadcast_id"]
                    conn = self.db._get_connection()
                    cursor = conn.cursor()
                    cursor.execute('UPDATE scheduled_broadcasts SET photo_url = ? WHERE id = ?', (photo_file_id, broadcast_id))
                    conn.commit()
                    conn.close()
                    sent_message = await update.message.reply_text("✅ Фото для рассылки обновлено!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_scheduled_broadcast_edit_from_context(update, context, broadcast_id)
                
                return
            
        # Обработка текста
        text = update.message.text if update.message.text else update.message.caption
        
        # ===== ОБРАБОТКА ФОТО И ССЫЛОК НА ФОТО =====
        if input_type in ["broadcast_photo", "welcome_photo", "goodbye_photo", "edit_welcome_follow_photo", "edit_scheduled_photo"]:
            # Если отправили текст вместо фото - проверяем, может это ссылка
            if text and (text.startswith("http://") or text.startswith("https://")):
                if input_type == "broadcast_photo":
                    message_number = waiting_data["message_number"]
                    self.db.update_broadcast_message(message_number, photo_url=text)
                    sent_message = await update.message.reply_text(f"✅ Ссылка на фото для сообщения {message_number} сохранена!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_message_edit_from_context(update, context, message_number)
                elif input_type == "welcome_photo":
                    welcome_text = self.db.get_welcome_message()['text']
                    self.db.set_welcome_message(welcome_text, photo_url=text)
                    sent_message = await update.message.reply_text("✅ Ссылка на фото для приветственного сообщения сохранена!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_welcome_edit_from_context(update, context)
                elif input_type == "goodbye_photo":
                    goodbye_text = self.db.get_goodbye_message()['text']
                    self.db.set_goodbye_message(goodbye_text, photo_url=text)
                    sent_message = await update.message.reply_text("✅ Ссылка на фото для прощального сообщения сохранена!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_goodbye_edit_from_context(update, context)
            else:
                sent_message = await update.message.reply_text("❌ Пожалуйста, отправьте фото или ссылку на фото")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            return
        
        # ===== ОСТАЛЬНАЯ ОБРАБОТКА ТЕКСТА =====
        
        if input_type == "send_all_text":
            # Начало создания массовой рассылки
            self.waiting_for[user_id]["message_text"] = text
            
            sent_message = await update.message.reply_text("✅ Текст сообщения получен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            
            await self.show_broadcast_options(update, context)
            
        elif input_type == "schedule_hours":
            # Планирование времени рассылки
            try:
                hours = float(text)
                if hours <= 0:
                    sent_message = await update.message.reply_text("❌ Количество часов должно быть больше 0")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    return
                
                self.waiting_for[user_id]["scheduled_hours"] = hours
                broadcast_data = self.waiting_for[user_id]
                await self.show_broadcast_preview(update, context, broadcast_data)
                
            except ValueError:
                sent_message = await update.message.reply_text("❌ Введите корректное число часов")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "add_broadcast_button_text":
            # Сохраняем текст кнопки и запрашиваем URL
            self.waiting_for[user_id]["button_text"] = text
            self.waiting_for[user_id]["type"] = "add_broadcast_button_url"
            
            sent_message = await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "add_broadcast_button_url":
            # Добавляем кнопку к рассылке
            if not (text.startswith("http://") or text.startswith("https://")):
                sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                return
            
            button_text = self.waiting_for[user_id]["button_text"]
            
            if "buttons" not in self.waiting_for[user_id]:
                self.waiting_for[user_id]["buttons"] = []
            
            self.waiting_for[user_id]["buttons"].append({
                "text": button_text,
                "url": text
            })
            
            # Возвращаемся к вариантам рассылки
            self.waiting_for[user_id]["type"] = "send_all_text"
            
            sent_message = await update.message.reply_text("✅ Кнопка добавлена!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            
            await self.show_broadcast_options(update, context)
        
        elif input_type == "broadcast_timer":
            try:
                hours = float(text)
                if hours < 1:
                    raise ValueError("Время должно быть больше 0")
                
                resume_time = datetime.now() + timedelta(hours=hours)
                self.db.set_broadcast_status(False, resume_time.isoformat())
                
                sent_message = await update.message.reply_text(
                    f"✅ Рассылка отключена на {hours} часов. Автовозобновление: {resume_time.strftime('%d.%m.%Y %H:%M')}"
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                
                del self.waiting_for[user_id]
                await asyncio.sleep(2)
                await self.show_main_menu_from_context(update, context)
            except ValueError:
                sent_message = await update.message.reply_text("❌ Пожалуйста, введите корректное число часов (больше 0)")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "welcome":
            self.db.set_welcome_message(text)
            sent_message = await update.message.reply_text("✅ Приветственное сообщение обновлено!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
            
        elif input_type == "goodbye":
            self.db.set_goodbye_message(text)
            sent_message = await update.message.reply_text("✅ Прощальное сообщение обновлено!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        elif input_type == "success_message":
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('success_message', text))
            conn.commit()
            conn.close()
            sent_message = await update.message.reply_text("✅ Сообщение подтверждения обновлено!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_success_message_edit_from_context(update, context)
            
        elif input_type == "broadcast_text":
            message_number = waiting_data["message_number"]
            self.db.update_broadcast_message(message_number, text=text)
            sent_message = await update.message.reply_text(f"✅ Текст сообщения {message_number} обновлён!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
            
        elif input_type == "broadcast_delay":
            message_number = waiting_data["message_number"]
            
            # Парсим новый формат ввода
            delay_hours, delay_display = self.parse_delay_input(text)
            
            if delay_hours is not None and delay_hours > 0:
                self.db.update_broadcast_message(message_number, delay_hours=delay_hours)
                sent_message = await update.message.reply_text(f"✅ Задержка для сообщения {message_number} установлена на {delay_display}!")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)
            else:
                sent_message = await update.message.reply_text(
                    "❌ Неверный формат! Примеры правильного ввода:\n\n"
                    "• <code>3м</code> или <code>3 минуты</code>\n"
                    "• <code>2ч</code> или <code>2 часа</code>\n"
                    "• <code>1.5</code> (для 1.5 часов)\n"
                    "• <code>0.05</code> (для 3 минут)",
                    parse_mode='HTML'
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "add_message":
            if waiting_data.get("step") == "text":
                # Сохраняем текст и запрашиваем задержку
                self.waiting_for[user_id]["text"] = text
                self.waiting_for[user_id]["step"] = "delay"
                sent_message = await update.message.reply_text(
                    "⏰ Теперь отправьте задержку:\n\n"
                    "📝 <b>Форматы ввода:</b>\n"
                    "• <code>30м</code> или <code>30 минут</code> - для минут\n"
                    "• <code>2ч</code> или <code>2 часа</code> - для часов\n"
                    "• <code>1.5</code> - для 1.5 часов\n"
                    "• <code>0.05</code> - для 3 минут",
                    parse_mode='HTML'
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            elif waiting_data.get("step") == "delay":
                # Парсим задержку
                delay_hours, delay_display = self.parse_delay_input(text)
                
                if delay_hours is not None and delay_hours > 0:
                    # Добавляем сообщение
                    message_text = waiting_data["text"]
                    new_number = self.db.add_broadcast_message(message_text, delay_hours)
                    
                    sent_message = await update.message.reply_text(f"✅ Сообщение {new_number} добавлено с задержкой {delay_display}!")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_broadcast_menu_from_context(update, context)
                else:
                    sent_message = await update.message.reply_text(
                        "❌ Неверный формат! Примеры правильного ввода:\n\n"
                        "• <code>3м</code> или <code>3 минуты</code>\n"
                        "• <code>2ч</code> или <code>2 часа</code>\n"
                        "• <code>1.5</code> (для 1.5 часов)\n"
                        "• <code>0.05</code> (для 3 минут)",
                        parse_mode='HTML'
                    )
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
            else:
                # Новое сообщение - сначала текст
                self.waiting_for[user_id]["step"] = "text"
                await self.handle_message(update, context)  # Повторная обработка
        
        elif input_type == "add_button":
            if waiting_data.get("step") == "text":
                # Сохраняем текст и запрашиваем URL
                self.waiting_for[user_id]["button_text"] = text
                self.waiting_for[user_id]["step"] = "url"
                sent_message = await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            elif waiting_data.get("step") == "url":
                # Проверяем URL
                if not (text.startswith("http://") or text.startswith("https://")):
                    sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    return
                
                # Добавляем кнопку
                message_number = waiting_data["message_number"]
                button_text = waiting_data["button_text"]
                
                # Определяем позицию
                existing_buttons = self.db.get_message_buttons(message_number)
                position = len(existing_buttons) + 1
                
                self.db.add_message_button(message_number, button_text, text, position)
                sent_message = await update.message.reply_text("✅ Кнопка добавлена!")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                del self.waiting_for[user_id]
                await self.show_message_buttons_from_context(update, context, message_number)
            else:
                # Новая кнопка - сначала текст
                self.waiting_for[user_id]["step"] = "text"
                await self.handle_message(update, context)  # Повторная обработка
        
        elif input_type == "edit_button_text":
            button_id = waiting_data["button_id"]
            # Получаем message_number для возврата
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT message_number FROM message_buttons WHERE id = ?', (button_id,))
            result = cursor.fetchone()
            conn.close()
            
            self.db.update_message_button(button_id, button_text=text)
            sent_message = await update.message.reply_text("✅ Текст кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            
            if result:
                await self.show_button_edit_from_context(update, context, button_id)
        
        elif input_type == "edit_button_url":
            if not (text.startswith("http://") or text.startswith("https://")):
                sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                return
            
            button_id = waiting_data["button_id"]
            self.db.update_message_button(button_id, button_url=text)
            sent_message = await update.message.reply_text("✅ URL кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_button_edit_from_context(update, context, button_id)
        
        # ===== ОБРАБОТКА ДЛЯ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ =====
        elif input_type == "add_welcome_button":
            # Определяем позицию
            existing_buttons = self.db.get_welcome_buttons()
            position = len(existing_buttons) + 1
            
            button_id = self.db.add_welcome_button(text, position)
            sent_message = await update.message.reply_text("✅ Механическая кнопка добавлена!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_welcome_buttons_management_from_context(update, context)
        
        elif input_type == "edit_welcome_button_text":
            button_id = waiting_data["button_id"]
            self.db.update_welcome_button(button_id, button_text=text)
            sent_message = await update.message.reply_text("✅ Текст кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_welcome_button_edit_from_context(update, context, button_id)
        
        elif input_type == "add_welcome_follow":
            welcome_button_id = waiting_data["welcome_button_id"]
            photo_url = update.message.photo[-1].file_id if update.message.photo else None
            
            message_number = self.db.add_welcome_follow_message(welcome_button_id, text, photo_url)
            sent_message = await update.message.reply_text(f"✅ Сообщение {message_number} добавлено!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_welcome_follow_messages_from_context(update, context, welcome_button_id)
        
        elif input_type == "edit_welcome_follow_text":
            message_id = waiting_data["message_id"]
            self.db.update_welcome_follow_message(message_id, text=text)
            sent_message = await update.message.reply_text("✅ Текст сообщения обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_welcome_follow_message_edit_from_context(update, context, message_id)
        
        # ===== ОБРАБОТКА ДЛЯ ПРОЩАЛЬНОГО СООБЩЕНИЯ =====
        elif input_type == "add_goodbye_button":
            if "step" not in waiting_data:
                # Сохраняем текст и запрашиваем URL
                self.waiting_for[user_id]["button_text"] = text
                self.waiting_for[user_id]["step"] = "url"
                sent_message = await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            elif waiting_data["step"] == "url":
                # Проверяем URL
                if not (text.startswith("http://") or text.startswith("https://")):
                    sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    return
                
                # Добавляем кнопку
                button_text = waiting_data["button_text"]
                existing_buttons = self.db.get_goodbye_buttons()
                position = len(existing_buttons) + 1
                
                self.db.add_goodbye_button(button_text, text, position)
                sent_message = await update.message.reply_text("✅ Кнопка добавлена!")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                del self.waiting_for[user_id]
                await self.show_goodbye_buttons_management_from_context(update, context)
        
        elif input_type == "edit_goodbye_button_text":
            button_id = waiting_data["button_id"]
            self.db.update_goodbye_button(button_id, button_text=text)
            sent_message = await update.message.reply_text("✅ Текст кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_goodbye_button_edit_from_context(update, context, button_id)
        
        elif input_type == "edit_goodbye_button_url":
            if not (text.startswith("http://") or text.startswith("https://")):
                sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                return
            
            button_id = waiting_data["button_id"]
            self.db.update_goodbye_button(button_id, button_url=text)
            sent_message = await update.message.reply_text("✅ URL кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_goodbye_button_edit_from_context(update, context, button_id)
        
        # ===== ОБРАБОТКА ДЛЯ ЗАПЛАНИРОВАННЫХ РАССЫЛОК =====
        elif input_type == "add_scheduled_broadcast":
            if "step" not in waiting_data:
                # Сохраняем текст и фото, запрашиваем время
                self.waiting_for[user_id]["message_text"] = text
                if update.message.photo:
                    self.waiting_for[user_id]["photo_id"] = update.message.photo[-1].file_id
                self.waiting_for[user_id]["step"] = "time"
                sent_message = await update.message.reply_text(self._get_schedule_time_text(), parse_mode='HTML')
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            elif waiting_data["step"] == "time":
                # Парсим время
                hours, hours_display = self.parse_hours_input(text)
                
                if hours is not None and hours > 0:
                    # Создаем рассылку
                    scheduled_time = datetime.now() + timedelta(hours=hours)
                    message_text = waiting_data["message_text"]
                    photo_id = waiting_data.get("photo_id")
                    
                    broadcast_id = self.db.add_scheduled_broadcast(message_text, scheduled_time, photo_id)
                    
                    sent_message = await update.message.reply_text(
                        f"✅ Рассылка запланирована!\n\n"
                        f"📅 Время отправки: {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                        f"⏰ Через: {hours_display}"
                    )
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    del self.waiting_for[user_id]
                    await self.show_scheduled_broadcasts_from_context(update, context)
                else:
                    sent_message = await update.message.reply_text(
                        "❌ Неверный формат! Примеры:\n\n"
                        "• <code>2ч</code> или <code>2 часа</code>\n"
                        "• <code>24</code> (для 24 часов)\n"
                        "• <code>0.5</code> (для 30 минут)",
                        parse_mode='HTML'
                    )
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "edit_scheduled_text":
            broadcast_id = waiting_data["broadcast_id"]
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE scheduled_broadcasts SET message_text = ? WHERE id = ?', (text, broadcast_id))
            conn.commit()
            conn.close()
            sent_message = await update.message.reply_text("✅ Текст рассылки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_scheduled_broadcast_edit_from_context(update, context, broadcast_id)
        
        elif input_type == "edit_scheduled_time":
            broadcast_id = waiting_data["broadcast_id"]
            hours, hours_display = self.parse_hours_input(text)
            
            if hours is not None and hours > 0:
                new_scheduled_time = datetime.now() + timedelta(hours=hours)
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute('UPDATE scheduled_broadcasts SET scheduled_time = ? WHERE id = ?', (new_scheduled_time, broadcast_id))
                conn.commit()
                conn.close()
                
                sent_message = await update.message.reply_text(
                    f"✅ Время рассылки обновлено!\n\n"
                    f"📅 Новое время: {new_scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                    f"⏰ Через: {hours_display}"
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                del self.waiting_for[user_id]
                await self.show_scheduled_broadcast_edit_from_context(update, context, broadcast_id)
            else:
                sent_message = await update.message.reply_text(
                    "❌ Неверный формат! Примеры:\n\n"
                    "• <code>2ч</code> или <code>2 часа</code>\n"
                    "• <code>24</code> (для 24 часов)\n"
                    "• <code>0.5</code> (для 30 минут)",
                    parse_mode='HTML'
                )
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
        
        elif input_type == "add_scheduled_button":
            if "step" not in waiting_data:
                # Сохраняем текст и запрашиваем URL
                self.waiting_for[user_id]["button_text"] = text
                self.waiting_for[user_id]["step"] = "url"
                sent_message = await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
            elif waiting_data["step"] == "url":
                # Проверяем URL
                if not (text.startswith("http://") or text.startswith("https://")):
                    sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                    if sent_message:
                        self.track_admin_message(user_id, sent_message.message_id)
                    return
                
                # Добавляем кнопку
                broadcast_id = waiting_data["broadcast_id"]
                button_text = waiting_data["button_text"]
                existing_buttons = self.db.get_scheduled_broadcast_buttons(broadcast_id)
                position = len(existing_buttons) + 1
                
                self.db.add_scheduled_broadcast_button(broadcast_id, button_text, text, position)
                sent_message = await update.message.reply_text("✅ Кнопка добавлена!")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                del self.waiting_for[user_id]
                await self.show_scheduled_broadcast_buttons_from_context(update, context, broadcast_id)
        
        elif input_type == "edit_scheduled_button_text":
            button_id = waiting_data["button_id"]
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE scheduled_broadcast_buttons SET button_text = ? WHERE id = ?', (text, button_id))
            conn.commit()
            conn.close()
            sent_message = await update.message.reply_text("✅ Текст кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_scheduled_button_edit_from_context(update, context, button_id)
        
        elif input_type == "edit_scheduled_button_url":
            if not (text.startswith("http://") or text.startswith("https://")):
                sent_message = await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                if sent_message:
                    self.track_admin_message(user_id, sent_message.message_id)
                return
            
            button_id = waiting_data["button_id"]
            conn = self.db._get_connection()
            cursor = conn.cursor()
            cursor.execute('UPDATE scheduled_broadcast_buttons SET button_url = ? WHERE id = ?', (text, button_id))
            conn.commit()
            conn.close()
            sent_message = await update.message.reply_text("✅ URL кнопки обновлен!")
            if sent_message:
                self.track_admin_message(user_id, sent_message.message_id)
            del self.waiting_for[user_id]
            await self.show_scheduled_button_edit_from_context(update, context, button_id)
