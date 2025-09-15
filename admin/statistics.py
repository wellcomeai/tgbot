"""
Функциональность статистики для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging
import io

logger = logging.getLogger(__name__)


class StatisticsMixin:
    """Миксин для работы со статистикой"""
    
    async def show_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать расширенную статистику"""
        stats = self.db.get_user_statistics()
        payment_stats = self.db.get_payment_statistics()
        
        text = (
            "📊 <b>Статистика бота</b>\n\n"
            f"👥 Всего активных пользователей: {stats['total_users']}\n"
            f"💬 Начали разговор с ботом: {stats['bot_started_users']}\n"
            f"🆕 Новых за 24 часа: {stats['new_users_24h']}\n"
            f"✉️ Отправлено сообщений: {stats['sent_messages']}\n"
            f"🚪 Отписалось пользователей: {stats['unsubscribed']}\n"
            f"💰 Оплатило пользователей: {stats['paid_users']}\n\n"
        )
        
        if payment_stats:
            conversion_rate = payment_stats['conversion_rate']
            avg_amount = payment_stats['avg_amount']
            text += (
                f"💸 <b>Платежи:</b>\n"
                f"• Конверсия: {conversion_rate}%\n"
                f"• Средний чек: {avg_amount} руб.\n\n"
            )
        
        text += f"💡 <b>Массовая рассылка:</b> доступна для {stats['bot_started_users']} пользователей"
        
        keyboard = [
            [InlineKeyboardButton("📊 Детали платежей", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_payment_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику платежей"""
        stats = self.db.get_payment_statistics()
        
        if not stats:
            text = "❌ <b>Ошибка при получении статистики платежей</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            return
        
        text = (
            "📊 <b>Статистика платежей</b>\n\n"
            f"💰 <b>Общие показатели:</b>\n"
            f"• Всего платежей: {stats['total_payments']}\n"
            f"• Всего пользователей: {stats['total_users']}\n"
            f"• Оплатило: {stats['paid_users']}\n"
            f"• Конверсия: {stats['conversion_rate']}%\n"
            f"• Средний чек: {stats['avg_amount']} руб.\n\n"
        )
        
        # UTM источники
        if stats['utm_sources']:
            text += "🔗 <b>По источникам:</b>\n"
            for utm_source, count in stats['utm_sources']:
                text += f"• {utm_source}: {count} платежей\n"
            text += "\n"
        
        # Последние платежи
        if stats['recent_payments']:
            text += "📋 <b>Последние платежи:</b>\n"
            for user_id, first_name, username, amount, created_at in stats['recent_payments'][:5]:
                username_str = f"@{username}" if username else "без username"
                date_str = datetime.fromisoformat(created_at).strftime("%d.%m %H:%M")
                text += f"• {first_name} ({username_str}): {amount} руб. - {date_str}\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать список пользователей"""
        users = self.db.get_latest_users(10)
        
        if not users:
            text = "👥 <b>Список пользователей</b>\n\nПользователей пока нет."
        else:
            text = "👥 <b>Список пользователей</b>\n\n<b>Последние 10 регистраций:</b>\n\n"
            for user in users:
                if len(user) >= 8:  # Новый формат с полями has_paid и paid_at
                    user_id_db, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at = user
                    paid_icon = "💰" if has_paid else ""
                else:  # Старый формат
                    user_id_db, username, first_name, joined_at, is_active, bot_started = user
                    paid_icon = ""
                
                username_str = f"@{username}" if username else "без username"
                join_date = datetime.fromisoformat(joined_at).strftime("%d.%m.%Y %H:%M")
                bot_status = "💬" if bot_started else "❌"
                text += f"• {first_name} ({username_str}) {bot_status}{paid_icon}\n  ID: {user_id_db}, {join_date}\n\n"
            
            text += "\n💬 - может получать рассылки\n❌ - нужно написать боту /start\n💰 - оплатил"
        
        keyboard = [
            [InlineKeyboardButton("📊 Скачать CSV", callback_data="download_csv")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
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
            if 'Event loop is closed' not in str(e):
                logger.error(f"Ошибка при отправке CSV: {e}")
            await update.callback_query.answer("Ошибка при создании файла!", show_alert=True)
