"""
Базовый класс админ-панели с общей функциональностью
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from datetime import datetime, timedelta
import logging
import asyncio

from .statistics import StatisticsMixin
from .broadcasts import BroadcastsMixin
from .mass_broadcasts import MassBroadcastsMixin
from .messages import MessagesMixin
from .buttons import ButtonsMixin
from .handlers import HandlersMixin
from .utils import UtilsMixin
from .mixins.menu_mixin import MenuMixin
from .mixins.input_mixin import InputMixin
from .mixins.navigation_mixin import NavigationMixin
from .paid_broadcasts import PaidBroadcastsMixin
from .paid_buttons import PaidButtonsMixin  
from .paid_mass_broadcasts import PaidMassBroadcastsMixin

logger = logging.getLogger(__name__)


class AdminPanel(
    StatisticsMixin,
    BroadcastsMixin,
    MassBroadcastsMixin,
    MessagesMixin,
    ButtonsMixin,
    HandlersMixin,
    UtilsMixin,
    MenuMixin,
    InputMixin,
    NavigationMixin
):
    """
    Главный класс админ-панели, объединяющий всю функциональность
    """
    
    def __init__(self, db, admin_chat_id):
        """Инициализация админ-панели"""
        self.db = db
        self.admin_chat_id = admin_chat_id
        self.waiting_for = {}  # Словарь для отслеживания ожидаемого ввода
        self.broadcast_drafts = {}  # Словарь для хранения черновиков массовых рассылок
        
        logger.info("🔧 Админ-панель инициализирована")
    
    async def safe_edit_or_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, 
                                      text: str, reply_markup=None, parse_mode='HTML'):
        """Безопасная отправка/редактирование сообщения"""
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
            error_msg = str(e)
            if 'Event loop is closed' in error_msg:
                logger.warning(f"⚠️ Event loop закрыт при отправке сообщения админу: {error_msg}")
                return None
            elif 'RuntimeError' in error_msg and 'closed' in error_msg:
                logger.warning(f"⚠️ Runtime ошибка с закрытым ресурсом: {error_msg}")
                return None
            else:
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
                if 'Event loop is closed' not in str(e2):
                    logger.error(f"❌ Критическая ошибка при отправке сообщения: {e2}")
                return None
    
    async def send_new_menu_message(self, context: ContextTypes.DEFAULT_TYPE, user_id: int, 
                                  text: str, reply_markup=None):
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
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке нового сообщения меню: {e}")
            return None
    
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
            [InlineKeyboardButton("💰 Сообщение после оплаты", callback_data="admin_payment_message")],
            [InlineKeyboardButton("📊 Статистика платежей", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("📢 Отправить всем сообщение", callback_data="admin_send_all")],
            [InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="admin_scheduled_broadcasts")],
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "🔧 <b>Админ-панель</b>\n\nВыберите действие:"
        
        sent_message = await self.safe_edit_or_send_message(update, context, text, reply_markup)
        return sent_message
    
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
            [InlineKeyboardButton("💰 Сообщение после оплаты", callback_data="admin_payment_message")],
            [InlineKeyboardButton("📊 Статистика платежей", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("📢 Отправить всем сообщение", callback_data="admin_send_all")],
            [InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="admin_scheduled_broadcasts")],
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "🔧 <b>Админ-панель</b>\n\nВыберите действие:"
        
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке безопасного меню: {e}")
    
    async def show_error_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, error_text: str):
        """Показать сообщение об ошибке и вернуться в главное меню"""
        user_id = update.effective_user.id
        
        # Очищаем состояние ожидания
        if user_id in self.waiting_for:
            del self.waiting_for[user_id]
        
        # Отправляем сообщение об ошибке
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=error_text,
                parse_mode='HTML'
            )
            
            # Показываем главное меню через 2 секунды
            await asyncio.sleep(2)
            await self.show_main_menu_safe(update, context)
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке сообщения об ошибке: {e}")
    
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
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при очистке админ-панели: {e}")
