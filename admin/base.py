"""
Базовый класс для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import ContextTypes
from telegram.error import BadRequest, TimedOut
from datetime import datetime
import logging
import asyncio
import io

logger = logging.getLogger(__name__)


class AdminBaseMixin:
    """Базовый миксин для админ-панели"""
    
    def __init__(self, db, admin_chat_id):
        self.db = db
        self.admin_chat_id = admin_chat_id
        self.waiting_for = {}  # Словарь для отслеживания ожидания ввода
        self.broadcast_drafts = {}  # Черновики массовых рассылок
    
    async def cleanup_old_waiting_states(self):
        """Очистка старых состояний ожидания ввода"""
        while True:
            try:
                current_time = datetime.now()
                to_delete = []
                
                for user_id, data in self.waiting_for.items():
                    if 'created_at' in data:
                        age = current_time - data['created_at']
                        if age.total_seconds() > 1800:  # 30 минут
                            to_delete.append(user_id)
                
                for user_id in to_delete:
                    del self.waiting_for[user_id]
                    logger.info(f"🧹 Очищено устаревшее состояние ожидания для пользователя {user_id}")
                
                # Очистка старых черновиков
                to_delete_drafts = []
                for user_id, draft in self.broadcast_drafts.items():
                    if 'created_at' in draft:
                        age = current_time - draft['created_at']
                        if age.total_seconds() > 7200:  # 2 часа
                            to_delete_drafts.append(user_id)
                
                for user_id in to_delete_drafts:
                    del self.broadcast_drafts[user_id]
                    logger.info(f"🧹 Очищен устаревший черновик для пользователя {user_id}")
                
                await asyncio.sleep(1800)  # Проверяем каждые 30 минут
                
            except Exception as e:
                logger.error(f"❌ Ошибка при очистке состояний: {e}")
                await asyncio.sleep(1800)
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню админ-панели"""
        stats = self.db.get_user_statistics()
        
        text = (
            "🛠 <b>Админ-панель</b>\n\n"
            f"👥 <b>Пользователи:</b> {stats['total_users']}\n"
            f"🚀 <b>Начали разговор:</b> {stats['bot_started_users']}\n"
            f"💰 <b>Оплатили:</b> {stats['paid_users']}\n"
            f"📊 <b>Новых за 24ч:</b> {stats['new_users_24h']}\n"
            f"📨 <b>Отправлено:</b> {stats['sent_messages']}\n"
            f"❌ <b>Отписались:</b> {stats['unsubscribed']}\n\n"
            "Выберите раздел:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("👥 Пользователи", callback_data="admin_users")],
            [InlineKeyboardButton("✉️ Рассылка", callback_data="admin_broadcast")],
            [InlineKeyboardButton("💰 Рассылка для оплативших", callback_data="admin_paid_broadcast")],
            [InlineKeyboardButton("🔄 Статус рассылки", callback_data="admin_broadcast_status")],
            [InlineKeyboardButton("📢 Массовая рассылка", callback_data="admin_send_all")],
            [InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="admin_scheduled_broadcasts")],
            [InlineKeyboardButton("👋 Приветствие", callback_data="admin_welcome")],
            [InlineKeyboardButton("👋 Прощание", callback_data="admin_goodbye")],
            [InlineKeyboardButton("✅ Подтверждение", callback_data="admin_success_message")],
            [InlineKeyboardButton("💳 Сообщение об оплате", callback_data="admin_payment_message")],
            [InlineKeyboardButton("💰 Сообщения продления", callback_data="admin_renewal")],
            [InlineKeyboardButton("📈 Статистика платежей", callback_data="admin_payment_stats")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def safe_edit_or_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                      text: str, reply_markup=None):
        """Безопасное редактирование или отправка сообщения"""
        try:
            if update.callback_query and update.callback_query.message:
                # Пытаемся отредактировать существующее сообщение
                try:
                    await update.callback_query.edit_message_text(
                        text=text,
                        reply_markup=reply_markup,
                        parse_mode='HTML'
                    )
                    return
                except BadRequest as e:
                    # Если не удалось отредактировать, отправляем новое
                    logger.debug(f"Не удалось отредактировать сообщение: {e}")
            
            # Отправляем новое сообщение
            if update.callback_query:
                user_id = update.callback_query.from_user.id
            else:
                user_id = update.effective_user.id
            
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
        except TimedOut as e:
            logger.warning(f"⚠️ Timeout при отправке сообщения: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения админу: {e}")
    
    async def show_main_menu_safe(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасное отображение главного меню из любого контекста"""
        try:
            await self.show_main_menu(update, context)
        except Exception as e:
            logger.error(f"❌ Ошибка при показе главного меню: {e}")
            # Отправляем минимальное сообщение об ошибке
            user_id = self.admin_chat_id
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="❌ Произошла ошибка. Попробуйте команду /start",
                    reply_markup=ReplyKeyboardRemove()
                )
            except Exception as e2:
                logger.error(f"❌ Критическая ошибка при отправке сообщения об ошибке: {e2}")
    
    async def send_new_menu_message(self, context: ContextTypes.DEFAULT_TYPE, user_id: int, 
                                  text: str, reply_markup=None):
        """Отправка нового сообщения меню"""
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке нового сообщения меню: {e}")
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                               input_type: str, **kwargs):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        
        # Сохраняем состояние ожидания ввода
        self.waiting_for[user_id] = {
            "type": input_type,
            "created_at": datetime.now(),
            **kwargs
        }
        
        # Определяем текст запроса в зависимости от типа
        prompt_texts = {
            "welcome": "✏️ Отправьте новый текст приветственного сообщения:",
            "welcome_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "goodbye": "✏️ Отправьте новый текст прощального сообщения:",
            "goodbye_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "success_message": "✏️ Отправьте новый текст сообщения подтверждения:",
            "broadcast_timer": "⏰ На сколько часов отключить рассылку?\n\nПример: <code>2.5</code> (на 2,5 часа)",
            "renewal_text": "✏️ Отправьте текст сообщения о продлении подписки:",
            "renewal_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "renewal_button_text": "📝 Отправьте текст кнопки продления:",
            "renewal_button_url": "🔗 Отправьте URL для кнопки продления:"
        }
        
        prompt_text = prompt_texts.get(input_type, "✏️ Отправьте текст:")
        
        # Добавляем кнопку отмены в зависимости от контекста
        cancel_callback_data = "admin_back"
        if input_type.startswith("renewal"):
            cancel_callback_data = "admin_renewal"
        
        cancel_markup = InlineKeyboardMarkup([[
            InlineKeyboardButton("❌ Отмена", callback_data=cancel_callback_data)
        ]])
        
        await self.safe_edit_or_send_message(update, context, prompt_text, cancel_markup)
    
    async def show_error_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, error_text: str):
        """Показать сообщение об ошибке"""
        try:
            keyboard = [[InlineKeyboardButton("« Назад в меню", callback_data="admin_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, error_text, reply_markup)
        except Exception as e:
            logger.error(f"❌ Ошибка при показе сообщения об ошибке: {e}")
    
    async def handle_photo_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, waiting_data: dict):
        """Обработка загрузки фото"""
        user_id = update.effective_user.id
        input_type = waiting_data["type"]
        
        try:
            # ✅ НОВОЕ: Получаем file_id напрямую, без конвертации в URL
            photo = update.message.photo[-1]
            photo_file_id = photo.file_id
            
            logger.info(f"📸 Получен file_id фото: {photo_file_id}")
            
            # Обрабатываем в зависимости от типа
            if input_type == "welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, photo_file_id)
                await update.message.reply_text("✅ Фото приветствия обновлено!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)
                
            elif input_type == "goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, photo_file_id)
                await update.message.reply_text("✅ Фото прощания обновлено!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)
                
            elif input_type == "renewal_photo":
                self.db.set_renewal_message(photo_url=photo_file_id)
                await update.message.reply_text("✅ Фото сообщения продления обновлено!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)
                
            else:
                await update.message.reply_text("❌ Неизвестный тип ввода фото.")
                del self.waiting_for[user_id]
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке фото: {e}")
            await update.message.reply_text("❌ Ошибка при обработке фото. Попробуйте еще раз.")
    
    async def handle_photo_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                   url: str, input_type: str, **kwargs):
        """Обработка ввода URL фото (для обратной совместимости)"""
        user_id = update.effective_user.id
        
        try:
            if input_type == "welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, url)
                await update.message.reply_text("✅ Ссылка на фото приветствия сохранена!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)
                
            elif input_type == "goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, url)
                await update.message.reply_text("✅ Ссылка на фото прощания сохранена!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)
                
            elif input_type == "renewal_photo":
                self.db.set_renewal_message(photo_url=url)
                await update.message.reply_text("✅ Ссылка на фото сообщения продления сохранена!")
                del self.waiting_for[user_id]
                await self.show_renewal_edit_from_context(update, context)
                
            else:
                await update.message.reply_text("❌ Неизвестный тип ввода URL фото.")
                del self.waiting_for[user_id]
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке URL фото: {e}")
            await update.message.reply_text("❌ Ошибка при сохранении URL фото. Попробуйте еще раз.")
    
    def validate_waiting_state(self, waiting_data: dict) -> bool:
        """Валидация состояния ожидания ввода"""
        if not waiting_data:
            return False
        
        # Проверяем возраст состояния
        if 'created_at' in waiting_data:
            age = datetime.now() - waiting_data['created_at']
            if age.total_seconds() > 1800:  # 30 минут
                return False
        
        return True
    
    def format_delay_display(self, delay_hours: float) -> str:
        """Форматирование отображения задержки"""
        if delay_hours < 1:
            minutes = int(delay_hours * 60)
            return f"{minutes}м"
        elif delay_hours == int(delay_hours):
            return f"{int(delay_hours)}ч"
        else:
            return f"{delay_hours}ч"
    
    def format_delay_display_full(self, delay_hours: float) -> str:
        """Полное форматирование отображения задержки"""
        if delay_hours < 1:
            minutes = int(delay_hours * 60)
            return f"{minutes} минут"
        elif delay_hours == 1:
            return "1 час"
        elif delay_hours == int(delay_hours):
            return f"{int(delay_hours)} часов"
        else:
            return f"{delay_hours} часов"
    
    def parse_delay_input(self, text: str) -> tuple:
        """Парсинг ввода задержки от пользователя"""
        try:
            text = text.strip().lower()
            
            # Удаляем лишние пробелы
            text = ' '.join(text.split())
            
            # Парсинг различных форматов
            if 'м' in text or 'мин' in text:
                # Минуты
                number_str = text.replace('м', '').replace('мин', '').replace('инут', '').replace('ы', '').replace('а', '').strip()
                minutes = float(number_str)
                hours = minutes / 60
                return hours, self.format_delay_display(hours)
                
            elif 'ч' in text or 'час' in text:
                # Часы
                number_str = text.replace('ч', '').replace('час', '').replace('ов', '').replace('а', '').strip()
                hours = float(number_str)
                return hours, self.format_delay_display(hours)
                
            else:
                # Просто число - считаем часами
                hours = float(text)
                return hours, self.format_delay_display(hours)
                
        except ValueError:
            return None, None
