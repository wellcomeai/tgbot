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
            [InlineKeyboardButton("🔄 Статистика воронки", callback_data="admin_funnel_stats")],
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
    
    async def show_funnel_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику воронки рассылки"""
        try:
            # Получаем данные воронки
            funnel_data = self.db.get_funnel_data()
            
            if not funnel_data:
                text = (
                    "📊 <b>ВОРОНКА РАССЫЛКИ</b>\n\n"
                    "⚠️ Пока нет данных по воронке.\n\n"
                    "Данные появятся после того, как:\n"
                    "• Будут отправлены сообщения рассылки\n"
                    "• Пользователи начнут нажимать на кнопки\n\n"
                    "💡 Воронка показывает, на каком этапе пользователи теряют интерес к вашему контенту."
                )
            else:
                text = "📊 <b>ВОРОНКА РАССЫЛКИ</b>\n\n"
                
                # Находим сообщение с максимальным отвалом
                biggest_drop = self.db.get_biggest_drop_message()
                
                for msg_data in funnel_data:
                    message_number = msg_data['message_number']
                    message_text = msg_data['message_text']
                    delivered = msg_data['delivered']
                    clicked_callback = msg_data['clicked_callback']
                    clicked_url = msg_data['clicked_url']
                    conversion_rate = msg_data['conversion_rate']
                    dropped = msg_data['dropped']
                    drop_rate = msg_data['drop_rate']
                    
                    # Заголовок сообщения
                    text += f"<b>Сообщение {message_number}</b>\n"
                    text += f"<i>{message_text}</i>\n"
                    
                    if delivered == 0:
                        text += "└─ ⏳ Еще не отправлялось\n\n"
                        continue
                    
                    # Статистика
                    text += f"├─ 📬 Получили: <b>{delivered}</b> чел.\n"
                    text += f"├─ 🔘 Нажали кнопку: <b>{clicked_callback}</b> ({conversion_rate}%)\n"
                    
                    # Предупреждение о большом отвале
                    if drop_rate >= 30:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate}%) ⚠️ <b>БОЛЬШОЙ ОТВАЛ!</b>\n\n"
                    elif drop_rate >= 20:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate}%) ⚠️\n\n"
                    else:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate}%)\n\n"
                
                # Добавляем рекомендацию если есть проблемное сообщение
                if biggest_drop and biggest_drop['drop_rate'] >= 20:
                    text += (
                        f"⚠️ <b>ПРОБЛЕМА:</b> Самый большой отвал после "
                        f"<b>Сообщения {biggest_drop['message_number']}</b> ({biggest_drop['drop_rate']}%)\n\n"
                        f"💡 <b>Рекомендация:</b> Проверьте текст и предложение в этом сообщении."
                    )
            
            # Создаем кнопки для детализации
            keyboard = []
            
            if funnel_data:
                # Добавляем кнопки для сообщений с данными
                for msg_data in funnel_data[:5]:  # Показываем первые 5
                    if msg_data['delivered'] > 0:
                        message_number = msg_data['message_number']
                        keyboard.append([
                            InlineKeyboardButton(
                                f"📝 Детали сообщения {message_number}",
                                callback_data=f"admin_msg_detail_{message_number}"
                            )
                        ])
            
            keyboard.append([InlineKeyboardButton("🔄 Обновить", callback_data="admin_funnel_stats")])
            keyboard.append([InlineKeyboardButton("« Назад к статистике", callback_data="admin_stats")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при показе статистики воронки: {e}")
            text = "❌ <b>Ошибка при загрузке статистики воронки</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_message_details(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number: int):
        """Показать детальную статистику по конкретному сообщению"""
        try:
            # Получаем детализацию
            details = self.db.get_message_details(message_number)
            
            if not details:
                text = f"❌ <b>Сообщение {message_number} не найдено</b>"
                keyboard = [[InlineKeyboardButton("« Назад к воронке", callback_data="admin_funnel_stats")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                await self.safe_edit_or_send_message(update, context, text, reply_markup)
                return
            
            message_text = details['message_text']
            delivered = details['delivered']
            clicked_callback = details['clicked_callback_count']
            clicked_url = details['clicked_url_count']
            not_clicked = details['not_clicked']
            avg_reaction_time = details['avg_reaction_time_seconds']
            button_details = details['button_details']
            
            # Формируем текст
            text = f"📝 <b>СООБЩЕНИЕ {message_number} - Детальная статистика</b>\n\n"
            text += f"<i>{message_text[:100]}{'...' if len(message_text) > 100 else ''}</i>\n\n"
            
            if delivered == 0:
                text += "⏳ Это сообщение еще не отправлялось пользователям.\n"
            else:
                text += f"📬 <b>Отправлено:</b> {delivered} пользователям\n"
                
                # Среднее время реакции
                if avg_reaction_time > 0:
                    if avg_reaction_time < 60:
                        time_str = f"{int(avg_reaction_time)} сек"
                    elif avg_reaction_time < 3600:
                        minutes = int(avg_reaction_time / 60)
                        seconds = int(avg_reaction_time % 60)
                        time_str = f"{minutes} мин {seconds} сек"
                    else:
                        hours = int(avg_reaction_time / 3600)
                        minutes = int((avg_reaction_time % 3600) / 60)
                        time_str = f"{hours} ч {minutes} мин"
                    
                    text += f"⏰ <b>Среднее время реакции:</b> {time_str}\n\n"
                else:
                    text += f"⏰ <b>Среднее время реакции:</b> Нет данных\n\n"
                
                # Статистика по кнопкам
                if button_details:
                    total_clicks = sum([btn['click_count'] for btn in button_details])
                    text += f"🔘 <b>Кнопки (всего кликов: {total_clicks}):</b>\n"
                    
                    for btn in button_details:
                        button_text = btn['button_text']
                        button_type = btn['button_type']
                        click_count = btn['click_count']
                        percentage = btn['percentage']
                        
                        # Иконка в зависимости от типа кнопки
                        icon = "📩" if button_type == "callback" else "🔗"
                        
                        text += f"{icon} <b>{button_text}</b> → {click_count} кликов ({percentage}%)\n"
                    
                    text += "\n"
                else:
                    text += "🔘 <b>Кнопки:</b> Нет кнопок в этом сообщении\n\n"
                
                # Не нажали ничего
                if not_clicked > 0:
                    not_clicked_percent = round((not_clicked / delivered * 100), 2)
                    text += f"❌ <b>Не нажали ничего:</b> {not_clicked} чел. ({not_clicked_percent}%)\n\n"
                
                # Рекомендации
                if not_clicked_percent >= 30:
                    text += (
                        "💡 <b>Рекомендация:</b> Большой процент пользователей не нажимает на кнопки. "
                        "Возможно, стоит:\n"
                        "• Сделать призыв к действию более заметным\n"
                        "• Упростить текст кнопки\n"
                        "• Добавить больше ценности в предложение"
                    )
                elif clicked_callback / delivered >= 0.7:  # Конверсия >= 70%
                    text += "✅ <b>Отлично!</b> Высокая вовлеченность пользователей."
            
            # Кнопки навигации
            keyboard = [
                [InlineKeyboardButton("🔄 Обновить", callback_data=f"admin_msg_detail_{message_number}")],
                [InlineKeyboardButton("« Назад к воронке", callback_data="admin_funnel_stats")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при показе деталей сообщения {message_number}: {e}")
            text = "❌ <b>Ошибка при загрузке деталей сообщения</b>"
            keyboard = [[InlineKeyboardButton("« Назад к воронке", callback_data="admin_funnel_stats")]]
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
