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
        self.broadcast_drafts = {}  # Словарь для хранения черновиков массовых рассылок
    
    async def safe_edit_or_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode='HTML'):
        """Безопасное редактирование или отправка сообщения с обработкой ошибок"""
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
                            chat_id=update.effective_user.id,
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
            
            return sent_message
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке/редактировании сообщения: {e}")
            # Попытка отправить новое сообщение в случае ошибки
            try:
                sent_message = await context.bot.send_message(
                    chat_id=update.effective_user.id,
                    text=text,
                    reply_markup=reply_markup,
                    parse_mode=parse_mode
                )
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
    
    async def show_broadcast_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статус рассылки"""
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
    
    async def show_broadcast_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления рассылкой"""
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
    
    # ===== ИСПРАВЛЕННАЯ СИСТЕМА МАССОВЫХ РАССЫЛОК =====
    
    async def show_send_all_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню массовой рассылки с отдельными пунктами"""
        user_id = update.effective_user.id
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        draft = self.broadcast_drafts[user_id]
        
        # Формируем информацию о текущем состоянии
        text = "📢 <b>Массовая рассылка</b>\n\n"
        
        # Текст
        if draft["message_text"]:
            text += f"📝 <b>Текст:</b> {draft['message_text'][:50]}{'...' if len(draft['message_text']) > 50 else ''}\n"
        else:
            text += "📝 <b>Текст:</b> <i>Не задан</i>\n"
        
        # Фото
        if draft["photo_data"]:
            text += "🖼 <b>Фото:</b> Есть\n"
        else:
            text += "🖼 <b>Фото:</b> Нет\n"
        
        # Кнопки
        if draft["buttons"]:
            text += f"🔘 <b>Кнопки:</b> {len(draft['buttons'])}\n"
        else:
            text += "🔘 <b>Кнопки:</b> Нет\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            text += f"⏰ <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
        else:
            text += "⏰ <b>Время отправки:</b> <i>Сразу</i>\n"
        
        # Получаем количество пользователей
        users_count = len(self.db.get_users_with_bot_started())
        text += f"\n👥 <b>Получателей:</b> {users_count} пользователей\n"
        
        text += "\n<b>Выберите действие:</b>"
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="mass_edit_text")],
            [InlineKeyboardButton("🖼 Добавить фото", callback_data="mass_add_photo")],
            [InlineKeyboardButton("⏰ Время отправки", callback_data="mass_set_time")],
            [InlineKeyboardButton("🔘 Добавить кнопку", callback_data="mass_add_button")],
        ]
        
        # Кнопка удаления фото (если есть)
        if draft["photo_data"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить фото", callback_data="mass_remove_photo")])
        
        # Кнопка удаления последней кнопки (если есть)
        if draft["buttons"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="mass_remove_button")])
        
        keyboard.append([InlineKeyboardButton("📋 Предпросмотр", callback_data="mass_preview")])
        
        # Кнопка отправки (только если есть текст)
        if draft["message_text"]:
            keyboard.append([InlineKeyboardButton("🚀 Отправить сейчас", callback_data="mass_send_now")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Используем безопасный метод отправки/редактирования
        sent_message = await self.safe_edit_or_send_message(update, context, text, reply_markup)
        return sent_message
    
    # ===== ИСПРАВЛЕННЫЕ МЕТОДЫ ДЛЯ КОНТЕКСТНЫХ ПЕРЕХОДОВ =====
    
    async def send_new_menu_message(self, context: ContextTypes.DEFAULT_TYPE, user_id: int, text: str, reply_markup=None):
        """Отправить новое сообщение с меню (вместо редактирования)"""
        try:
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            return sent_message
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке нового сообщения меню: {e}")
            return None
    
    async def show_send_all_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение вместо редактирования"""
        user_id = update.effective_user.id
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        draft = self.broadcast_drafts[user_id]
        
        # Формируем информацию о текущем состоянии
        text = "📢 <b>Массовая рассылка</b>\n\n"
        
        # Текст
        if draft["message_text"]:
            text += f"📝 <b>Текст:</b> {draft['message_text'][:50]}{'...' if len(draft['message_text']) > 50 else ''}\n"
        else:
            text += "📝 <b>Текст:</b> <i>Не задан</i>\n"
        
        # Фото
        if draft["photo_data"]:
            text += "🖼 <b>Фото:</b> Есть\n"
        else:
            text += "🖼 <b>Фото:</b> Нет\n"
        
        # Кнопки
        if draft["buttons"]:
            text += f"🔘 <b>Кнопки:</b> {len(draft['buttons'])}\n"
        else:
            text += "🔘 <b>Кнопки:</b> Нет\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            text += f"⏰ <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
        else:
            text += "⏰ <b>Время отправки:</b> <i>Сразу</i>\n"
        
        # Получаем количество пользователей
        users_count = len(self.db.get_users_with_bot_started())
        text += f"\n👥 <b>Получателей:</b> {users_count} пользователей\n"
        
        text += "\n<b>Выберите действие:</b>"
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="mass_edit_text")],
            [InlineKeyboardButton("🖼 Добавить фото", callback_data="mass_add_photo")],
            [InlineKeyboardButton("⏰ Время отправки", callback_data="mass_set_time")],
            [InlineKeyboardButton("🔘 Добавить кнопку", callback_data="mass_add_button")],
        ]
        
        # Кнопка удаления фото (если есть)
        if draft["photo_data"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить фото", callback_data="mass_remove_photo")])
        
        # Кнопка удаления последней кнопки (если есть)
        if draft["buttons"]:
            keyboard.append([InlineKeyboardButton("🗑 Удалить последнюю кнопку", callback_data="mass_remove_button")])
        
        keyboard.append([InlineKeyboardButton("📋 Предпросмотр", callback_data="mass_preview")])
        
        # Кнопка отправки (только если есть текст)
        if draft["message_text"]:
            keyboard.append([InlineKeyboardButton("🚀 Отправить сейчас", callback_data="mass_send_now")])
        
        keyboard.append([InlineKeyboardButton("❌ Отмена", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # ИСПРАВЛЕНИЕ: Отправляем НОВОЕ сообщение вместо редактирования
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ ОБРАБОТКИ ВВОДА =====
    
    async def handle_mass_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста для массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        # Инициализируем черновик если его нет
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        self.broadcast_drafts[user_id]["message_text"] = text
        
        await update.message.reply_text("✅ Текст сообщения сохранен!")
        del self.waiting_for[user_id]
        
        # Возвращаемся в меню (теперь БЕЗОПАСНО)
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода фото для массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
            return
        
        if user_id not in self.broadcast_drafts:
            self.broadcast_drafts[user_id] = {
                "message_text": "",
                "photo_data": None,
                "buttons": [],
                "scheduled_hours": None,
                "created_at": datetime.now()
            }
        
        self.broadcast_drafts[user_id]["photo_data"] = text
        
        await update.message.reply_text("✅ Фото добавлено!")
        del self.waiting_for[user_id]
        
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_time_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода времени для массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Если пустой текст - отправляем сейчас
        if not text.strip():
            self.broadcast_drafts[user_id]["scheduled_hours"] = None
            await update.message.reply_text("✅ Рассылка будет отправлена сейчас!")
        else:
            try:
                hours = float(text.strip())
                if hours <= 0:
                    await update.message.reply_text("❌ Количество часов должно быть больше 0")
                    return
                
                if hours > 8760:  # Больше года
                    await update.message.reply_text("❌ Слишком большое количество часов")
                    return
                
                self.broadcast_drafts[user_id]["scheduled_hours"] = hours
                scheduled_time = datetime.now() + timedelta(hours=hours)
                await update.message.reply_text(f"✅ Рассылка запланирована на {scheduled_time.strftime('%d.%m.%Y %H:%M')}!")
                
            except ValueError:
                await update.message.reply_text("❌ Введите корректное число часов")
                return
        
        del self.waiting_for[user_id]
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_mass_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста кнопки для массовой рассылки"""
        user_id = update.effective_user.id
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Сохраняем текст кнопки и переходим к вводу URL
        self.waiting_for[user_id]["button_text"] = text
        self.waiting_for[user_id]["type"] = "mass_button_url"
        
        await update.message.reply_text("🔗 Теперь отправьте URL для кнопки:")
    
    async def handle_mass_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода URL кнопки для массовой рассылки"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        if user_id not in self.broadcast_drafts:
            await update.message.reply_text("❌ Черновик не найден!")
            return
        
        # Проверяем лимит кнопок
        if len(self.broadcast_drafts[user_id]["buttons"]) >= 10:
            await update.message.reply_text("❌ Максимум 10 кнопок на сообщение.")
            return
        
        button_text = self.waiting_for[user_id]["button_text"]
        
        # Добавляем кнопку
        self.broadcast_drafts[user_id]["buttons"].append({
            "text": button_text,
            "url": text
        })
        
        await update.message.reply_text("✅ Кнопка добавлена!")
        del self.waiting_for[user_id]
        
        await self.show_send_all_menu_from_context(update, context)
    
    async def handle_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_data: dict):
        """Обработка загруженного фото"""
        user_id = update.effective_user.id
        input_type = waiting_data["type"]
        
        if input_type == "mass_photo":
            # Фото для массовой рассылки
            photo_file_id = update.message.photo[-1].file_id
            
            if user_id not in self.broadcast_drafts:
                self.broadcast_drafts[user_id] = {
                    "message_text": "",
                    "photo_data": None,
                    "buttons": [],
                    "scheduled_hours": None,
                    "created_at": datetime.now()
                }
            
            self.broadcast_drafts[user_id]["photo_data"] = photo_file_id
            
            await update.message.reply_text("✅ Фото добавлено!")
            del self.waiting_for[user_id]
            
            await self.show_send_all_menu_from_context(update, context)
        
        # Обработка для других типов фото...
        else:
            await self.show_error_message(update, context, "❌ Неожиданное фото.")
    
    # ===== МЕТОДЫ ОБРАБОТКИ CALLBACK ЗАПРОСОВ =====
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик всех callback запросов админ-панели"""
        query = update.callback_query
        data = query.data
        user_id = query.from_user.id
        
        await query.answer()
        
        try:
            # Основные команды
            if data == "admin_back":
                await self.show_main_menu(update, context)
            elif data == "admin_stats":
                await self.show_statistics(update, context)
            elif data == "admin_broadcast":
                await self.show_broadcast_menu(update, context)
            elif data == "admin_broadcast_status":
                await self.show_broadcast_status(update, context)
            elif data == "admin_send_all":
                await self.show_send_all_menu(update, context)
            elif data == "enable_broadcast":
                self.db.set_broadcast_status(True, None)
                await self.show_broadcast_status(update, context)
            elif data == "disable_broadcast":
                self.db.set_broadcast_status(False, None)
                await self.show_broadcast_status(update, context)
            elif data == "set_broadcast_timer":
                await self.request_text_input(update, context, "broadcast_timer")
            
            # ===== ОБРАБОТЧИКИ ДЛЯ МАССОВОЙ РАССЫЛКИ =====
            elif data == "mass_edit_text":
                await self.request_text_input(update, context, "mass_text")
            elif data == "mass_add_photo":
                await self.request_text_input(update, context, "mass_photo")
            elif data == "mass_set_time":
                await self.request_text_input(update, context, "mass_time")
            elif data == "mass_add_button":
                await self.request_text_input(update, context, "mass_button_text")
            elif data == "mass_remove_photo":
                if user_id in self.broadcast_drafts:
                    self.broadcast_drafts[user_id]["photo_data"] = None
                    await self.show_send_all_menu(update, context)
            elif data == "mass_remove_button":
                if user_id in self.broadcast_drafts and self.broadcast_drafts[user_id]["buttons"]:
                    self.broadcast_drafts[user_id]["buttons"].pop()
                    await self.show_send_all_menu(update, context)
            elif data == "mass_preview":
                await self.show_mass_broadcast_preview(update, context)
            elif data == "mass_send_now":
                if user_id in self.broadcast_drafts:
                    # Убираем время планирования для немедленной отправки
                    self.broadcast_drafts[user_id]["scheduled_hours"] = None
                    await self.show_mass_broadcast_preview(update, context)
            elif data == "mass_confirm_send":
                await self.execute_mass_broadcast(update, context)
            
            else:
                await self.show_error_message(update, context, "❌ Неизвестная команда.")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке callback {data}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка. Попробуйте еще раз.")
    
    async def show_error_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, error_text: str):
        """Показать сообщение об ошибке и вернуться в главное меню"""
        user_id = update.effective_user.id
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        # Отправляем сообщение об ошибке
        await context.bot.send_message(
            chat_id=user_id,
            text=error_text,
            parse_mode='HTML'
        )
        
        # Показываем главное меню через 2 секунды
        await asyncio.sleep(2)
        await self.show_main_menu_safe(update, context)
    
    async def show_main_menu_safe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасный показ главного меню"""
        user_id = update.effective_user.id
        
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
        
        await context.bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, input_type, **kwargs):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        self.waiting_for[user_id] = {"type": input_type, "created_at": datetime.now(), **kwargs}
        
        texts = {
            "mass_text": "✏️ Отправьте текст для массовой рассылки:",
            "mass_photo": "🖼 Отправьте фото для массовой рассылки или ссылку на фото:",
            "mass_time": "⏰ Через сколько часов отправить рассылку?\n\nПримеры: 1, 2.5, 24\n\nОставьте пустым для отправки сейчас:",
            "mass_button_text": "✏️ Отправьте текст для кнопки:",
            "mass_button_url": "🔗 Отправьте URL для кнопки:",
            "broadcast_timer": "⏰ Отправьте количество часов, через которое возобновить рассылку:",
        }
        
        text = texts.get(input_type, "Отправьте необходимые данные:")
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_send_all")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений и фото от админа"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            return
        
        waiting_data = self.waiting_for[user_id]
        input_type = waiting_data["type"]
        
        # Проверяем, что состояние валидно
        if not self.validate_waiting_state(waiting_data):
            await self.show_error_message(update, context, "❌ Некорректное состояние. Начните сначала.")
            return
        
        try:
            # Обработка фото
            if update.message.photo:
                await self.handle_photo_input(update, context, waiting_data)
                return
            
            # Обработка текста
            text = update.message.text if update.message.text else update.message.caption
            if not text:
                await self.show_error_message(update, context, "❌ Пустое сообщение. Попробуйте еще раз.")
                return
            
            # ===== ОБРАБОТКА НОВЫХ ТИПОВ ДЛЯ МАССОВОЙ РАССЫЛКИ =====
            if input_type == "mass_text":
                await self.handle_mass_text_input(update, context, text)
            elif input_type == "mass_photo":
                await self.handle_mass_photo_input(update, context, text)
            elif input_type == "mass_time":
                await self.handle_mass_time_input(update, context, text)
            elif input_type == "mass_button_text":
                await self.handle_mass_button_text_input(update, context, text)
            elif input_type == "mass_button_url":
                await self.handle_mass_button_url_input(update, context, text)
            elif input_type == "broadcast_timer":
                await self.handle_broadcast_timer(update, context, text)
            else:
                await self.show_error_message(update, context, "❌ Неизвестный тип ввода.")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке сообщения от админа {user_id}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка при обработке вашего сообщения.")
    
    async def handle_broadcast_timer(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка установки таймера рассылки"""
        user_id = update.effective_user.id
        
        try:
            hours = float(text)
            if hours < 1:
                raise ValueError("Время должно быть больше 0")
            
            resume_time = datetime.now() + timedelta(hours=hours)
            self.db.set_broadcast_status(False, resume_time.isoformat())
            
            await update.message.reply_text(
                f"✅ Рассылка отключена на {hours} часов. Автовозобновление: {resume_time.strftime('%d.%m.%Y %H:%M')}"
            )
            
            del self.waiting_for[user_id]
            await asyncio.sleep(2)
            await self.show_main_menu_safe(update, context)
            
        except ValueError:
            await update.message.reply_text("❌ Пожалуйста, введите корректное число часов (больше 0)")
    
    def validate_waiting_state(self, waiting_data: dict) -> bool:
        """Проверить, что состояние ожидания валидно"""
        if not waiting_data or "type" not in waiting_data:
            return False
        
        # Проверяем, что состояние не слишком старое (30 минут)
        created_at = waiting_data.get("created_at")
        if created_at and (datetime.now() - created_at).total_seconds() > 1800:
            return False
        
        return True
    
    # ===== ЗАГЛУШКИ ДЛЯ ОТСУТСТВУЮЩИХ МЕТОДОВ =====
    
    async def show_mass_broadcast_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предварительный просмотр массовой рассылки"""
        await update.callback_query.answer("🚧 Функция в разработке")
    
    async def execute_mass_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выполнить массовую рассылку"""
        await update.callback_query.answer("🚧 Функция в разработке")
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ =====
    
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать список пользователей"""
        users = self.db.get_latest_users(10)
        
        if not users:
            text = "👥 <b>Список пользователей</b>\n\nПользователей пока нет."
        else:
            text = "👥 <b>Список пользователей</b>\n\n<b>Последние 10 регистраций:</b>\n\n"
            for user in users:
                user_id_db, username, first_name, joined_at, is_active, bot_started = user
                username_str = f"@{username}" if username else "без username"
                join_date = datetime.fromisoformat(joined_at).strftime("%d.%m.%Y %H:%M")
                bot_status = "💬" if bot_started else "❌"
                text += f"• {first_name} ({username_str}) {bot_status}\n  ID: {user_id_db}, {join_date}\n\n"
            
            text += "\n💬 - может получать рассылки\n❌ - нужно написать боту /start"
        
        keyboard = [
            [InlineKeyboardButton("📊 Скачать CSV", callback_data="download_csv")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def send_csv_file(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить CSV файл с пользователями"""
        try:
            csv_content = self.db.export_users_to_csv()
            
            csv_file = io.BytesIO()
            csv_file.write(csv_content.encode('utf-8'))
            csv_file.seek(0)
            
            await context.bot.send_document(
                chat_id=update.callback_query.from_user.id,
                document=csv_file,
                filename=f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                caption="📊 Список всех пользователей бота"
            )
            
            await update.callback_query.answer("CSV файл отправлен!")
            
        except Exception as e:
            logger.error(f"Ошибка при отправке CSV: {e}")
            await update.callback_query.answer("Ошибка при создании файла!", show_alert=True)
    
    # ===== ИНИЦИАЛИЗАЦИЯ =====
    
    async def initialize_admin_panel(self):
        """Инициализация админ-панели"""
        logger.info("🔧 Инициализация админ-панели...")
        
        # Проверяем статус рассылки
        broadcast_status = self.db.get_broadcast_status()
        if broadcast_status['auto_resume_time']:
            resume_time = datetime.fromisoformat(broadcast_status['auto_resume_time'])
            if datetime.now() >= resume_time:
                self.db.set_broadcast_status(True, None)
                logger.info("✅ Рассылка автоматически возобновлена")
        
        logger.info("✅ Админ-панель инициализирована")
    
    def get_admin_stats(self) -> dict:
        """Получить статистику админ-панели"""
        return {
            "waiting_states": len(self.waiting_for),
            "broadcast_drafts": len(self.broadcast_drafts),
            "broadcast_enabled": self.db.get_broadcast_status()['enabled'],
            "total_users": len(self.db.get_users_with_bot_started()),
            "total_broadcast_messages": len(self.db.get_all_broadcast_messages()),
            "scheduled_broadcasts": len(self.db.get_scheduled_broadcasts())
        }
    
    def __del__(self):
        """Деструктор для очистки ресурсов"""
        try:
            # Очищаем все состояния ожидания
            if hasattr(self, 'waiting_for'):
                self.waiting_for.clear()
            
            # Очищаем черновики рассылок
            if hasattr(self, 'broadcast_drafts'):
                self.broadcast_drafts.clear()
                
            logger.debug("🧹 Админ-панель очищена")
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке админ-панели: {e}")
