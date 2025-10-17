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
        """Запросить ввод текста от админа - УНИВЕРСАЛЬНЫЙ МЕТОД"""
        user_id = update.callback_query.from_user.id
        
        # Сохраняем состояние ожидания ввода
        self.waiting_for[user_id] = {
            "type": input_type,
            "created_at": datetime.now(),
            **kwargs
        }
        
        # Получаем номер сообщения если есть
        message_number = kwargs.get('message_number', '')
        button_id = kwargs.get('button_id', '')
        
        # ПОЛНЫЙ словарь всех типов ввода
        prompt_texts = {
            # === БАЗОВЫЕ СООБЩЕНИЯ ===
            "welcome": "✏️ Отправьте новый текст приветственного сообщения:",
            "welcome_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "goodbye": "✏️ Отправьте новый текст прощального сообщения:",
            "goodbye_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "success_message": "✏️ Отправьте новый текст сообщения подтверждения:",
            
            # === РАССЫЛКИ - ОСНОВНАЯ ВОРОНКА ===
            "broadcast_text": f"✏️ Отправьте новый текст для сообщения {message_number}:",
            "broadcast_delay": self._get_delay_text(message_number) if message_number else "⏰ Отправьте задержку:",
            "broadcast_photo": f"🖼 Отправьте фото для сообщения {message_number} напрямую или ссылку (http://...):" if message_number else "🖼 Отправьте фото:",
            "broadcast_timer": "⏰ На сколько часов отключить рассылку?\n\nПример: <code>2.5</code> (на 2,5 часа)",
            
            # === КНОПКИ ОСНОВНОЙ ВОРОНКИ ===
            "add_button": f"✏️ Отправьте текст для новой кнопки сообщения {message_number}:\n\n💡 После этого мы попросим URL для кнопки.",
            "edit_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_button_url": "🔗 Отправьте новый URL для кнопки:",
            
            # === МАССОВЫЕ РАССЫЛКИ ===
            "mass_text": "✏️ Отправьте текст для массовой рассылки:",
            "mass_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "mass_time": "⏰ Через сколько часов отправить рассылку?\n\nПримеры: 1, 2.5, 24\n\nОставьте пустым для отправки сейчас:",
            "mass_button_text": "✏️ Отправьте текст для кнопки:",
            "mass_button_url": "🔗 Отправьте URL для кнопки:",
            
            # === ПЛАТЕЖИ ===
            "payment_message_text": "✏️ Отправьте новый текст сообщения после оплаты:\n\n💡 Можно использовать переменную {amount} - она будет заменена на сумму платежа.",
            "payment_message_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            
            # === ПРОДЛЕНИЕ ===
            "renewal_text": "✏️ Отправьте текст сообщения о продлении подписки:",
            "renewal_photo": "🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "renewal_button_text": "📝 Отправьте текст кнопки продления:",
            "renewal_button_url": "🔗 Отправьте URL для кнопки продления:",
            
            # === КНОПКИ ПРИВЕТСТВИЯ ===
            "add_welcome_button": "⌨️ Отправьте текст для новой кнопки приветствия:",
            "edit_welcome_button_text": "📝 Отправьте новый текст для кнопки:",
            
            # === КНОПКИ ПРОЩАНИЯ ===
            "add_goodbye_button": "🔘 Отправьте текст для новой кнопки прощания:\n\n💡 После этого мы попросим URL для кнопки.",
            "edit_goodbye_button_text": "📝 Отправьте новый текст для кнопки:",
            "edit_goodbye_button_url": "🔗 Отправьте новый URL для кнопки:",
            
            # === ПЛАТНЫЕ РАССЫЛКИ - ВОРОНКА ===
            "paid_broadcast_text": f"💰 ✏️ Отправьте новый текст для сообщения оплативших {message_number}:",
            "paid_broadcast_delay": f"💰 ⏰ Отправьте новую задержку для сообщения оплативших {message_number} после оплаты:\n\n" + 
                                   "📝 <b>Форматы ввода:</b>\n" +
                                   "• <code>30м</code> или <code>30 минут</code> - для минут\n" +
                                   "• <code>2ч</code> или <code>2 часа</code> - для часов\n" +
                                   "• <code>1.5</code> - для 1.5 часов\n" +
                                   "• <code>0</code> - для мгновенной отправки\n\n" +
                                   "💡 Примеры: <code>3м</code>, <code>30 минут</code>, <code>2ч</code>, <code>0</code>",
            "paid_broadcast_photo": f"💰 🖼 Отправьте фото для сообщения оплативших {message_number} напрямую или ссылку (http://...):",
            
            # === КНОПКИ ПЛАТНОЙ ВОРОНКИ ===
            "add_paid_button": f"💰 ✏️ Отправьте текст для кнопки платного сообщения {message_number}:\n\n💡 После этого мы попросим URL.",
            
            # === МАССОВЫЕ ПЛАТНЫЕ РАССЫЛКИ ===
            "paid_mass_text": "💰 ✏️ Отправьте текст для массовой рассылки оплативших:",
            "paid_mass_photo": "💰 🖼 Отправьте фото напрямую или ссылку на фото (http://...):",
            "paid_mass_time": "💰 ⏰ Через сколько часов отправить рассылку оплативших?\n\nПримеры: 1, 2.5, 24\n\nОставьте пустым для отправки сейчас:",
            "paid_mass_button_text": "💰 ✏️ Отправьте текст для кнопки:",
            "paid_mass_button_url": "💰 🔗 Отправьте URL для кнопки:",
        }
        
        # Получаем текст подсказки
        prompt_text = prompt_texts.get(input_type, "✏️ Отправьте необходимые данные:")
        
        # Определяем кнопку отмены в зависимости от контекста
        if input_type.startswith("renewal"):
            cancel_callback_data = "admin_renewal"
        elif input_type.startswith("paid_"):
            cancel_callback_data = "admin_paid_broadcast"
        elif input_type.startswith("mass_"):
            cancel_callback_data = "admin_send_all"
        elif input_type.startswith("broadcast_"):
            cancel_callback_data = "admin_broadcast"
        elif input_type.startswith("goodbye"):
            cancel_callback_data = "admin_goodbye"
        elif input_type.startswith("welcome"):
            cancel_callback_data = "admin_welcome"
        elif input_type == "payment_message_text" or input_type == "payment_message_photo":
            cancel_callback_data = "admin_payment_message"
        else:
            cancel_callback_data = "admin_back"
        
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
