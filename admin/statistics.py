"""
Функциональность статистики для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging
import io
import html

logger = logging.getLogger(__name__)


class StatisticsMixin:
    """Миксин для работы со статистикой"""
    
    async def show_dashboard(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показать главный дашборд со сводкой
        """
        try:
            stats = self.db.get_user_statistics()
            funnel_summary = self.db.get_biggest_drop_summary()
            
            # ===== СВОДКА ЗА СЕГОДНЯ =====
            text = "📊 <b>СВОДКА ЗА СЕГОДНЯ</b>\n\n"
            
            new_today = stats['new_users_today']
            change_percent = stats['new_users_change_percent']
            
            # Иконка изменения
            if change_percent > 0:
                change_icon = "📈"
                change_text = f"+{change_percent}%"
            elif change_percent < 0:
                change_icon = "📉"
                change_text = f"{change_percent}%"
            else:
                change_icon = "➡️"
                change_text = "0%"
            
            text += f"🆕 Новых: <b>{new_today}</b> {change_icon} ({change_text} к вчера)\n"
            text += f"💬 Начали разговор: <b>{stats['bot_started_today']}</b>\n"
            text += f"💰 Оплатили: <b>{stats['paid_today']}</b>\n"
            text += f"🚪 Отписались: <b>{stats['unsubscribed_today']}</b>\n\n"
            
            # ===== ДИНАМИКА =====
            text += "📅 <b>ДИНАМИКА</b>\n\n"
            text += f"За 7 дней: <b>{stats['new_users_7d']}</b> новых\n"
            text += f"За 30 дней: <b>{stats['new_users_30d']}</b> новых\n\n"
            
            # ===== ОБЩИЕ ПОКАЗАТЕЛИ =====
            text += "📈 <b>ОБЩИЕ ПОКАЗАТЕЛИ</b>\n\n"
            text += f"👥 Всего активных: <b>{stats['total_users']}</b>\n"
            text += f"💬 С ботом: <b>{stats['bot_started_users']}</b>\n"
            text += f"💰 Оплатили: <b>{stats['paid_users']}</b> ({stats['conversion_rate']}% конверсия)\n"
            text += f"✉️ Отправлено сообщений: <b>{stats['sent_messages']}</b>\n\n"
            
            # ===== ВОРОНКА (проблемы) =====
            if funnel_summary and funnel_summary['has_problems']:
                text += "⚠️ <b>ПРОБЛЕМЫ ВОРОНКИ</b>\n\n"
                text += (
                    f"Сообщение {funnel_summary['message_number']}: "
                    f"<b>{funnel_summary['drop_rate']}%</b> отвал\n"
                    f"<i>{html.escape(funnel_summary['message_text'])}</i>\n\n"
                    f"💡 Рекомендуем пересмотреть это сообщение"
                )
            elif funnel_summary and funnel_summary['total_messages_with_data'] > 0:
                text += "✅ <b>ВОРОНКА В НОРМЕ</b>\n\n"
                text += f"Проблем не обнаружено (проверено {funnel_summary['total_messages_with_data']} сообщений)"
            else:
                text += "💡 <b>ВОРОНКА</b>\n\n"
                text += "Данных пока нет. Начните рассылку для анализа."
            
            # Кнопки навигации
            keyboard = [
                [InlineKeyboardButton("🔄 Воронка (детали)", callback_data="admin_funnel_stats")],
                [InlineKeyboardButton("💰 Платежи (детали)", callback_data="admin_payment_stats")],
                [
                    InlineKeyboardButton("👥 Пользователи", callback_data="admin_users"),
                    InlineKeyboardButton("🗑️ Очистка", callback_data="admin_cleanup")
                ],
                [InlineKeyboardButton("🔄 Обновить", callback_data="admin_dashboard")],
                [InlineKeyboardButton("« Назад в меню", callback_data="admin_back")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при показе дашборда: {e}")
            text = "❌ <b>Ошибка при загрузке дашборда</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать расширенную статистику (старая версия, перенаправляет на дашборд)"""
        await self.show_dashboard(update, context)
    
    async def show_cleanup_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показать меню управления очисткой данных
        """
        try:
            # Получаем информацию о количестве данных
            conn = self.db._get_connection()
            cursor = conn.cursor()
            
            # Считаем записи в таблицах воронки
            cursor.execute('SELECT COUNT(*) FROM message_deliveries')
            deliveries_count = cursor.fetchone()[0]
            
            cursor.execute('SELECT COUNT(*) FROM button_clicks')
            clicks_count = cursor.fetchone()[0]
            
            # Считаем запланированные сообщения
            cursor.execute('SELECT COUNT(*) FROM scheduled_messages WHERE is_sent = 1')
            sent_messages_count = cursor.fetchone()[0]
            
            conn.close()
            
            text = "🗑️ <b>ОЧИСТКА ДАННЫХ</b>\n\n"
            text += "Управление хранением данных в базе.\n\n"
            
            # Воронка
            text += "📊 <b>Данные воронки</b>\n"
            text += f"├─ Отправок: <b>{deliveries_count}</b>\n"
            text += f"└─ Кликов: <b>{clicks_count}</b>\n\n"
            
            if deliveries_count > 0 or clicks_count > 0:
                text += "Рекомендуется хранить данные воронки за последние 30-90 дней.\n\n"
            
            # Запланированные сообщения
            text += "📅 <b>Отправленные сообщения</b>\n"
            text += f"└─ Записей: <b>{sent_messages_count}</b>\n\n"
            
            if sent_messages_count > 0:
                text += "Рекомендуется хранить записи за последние 7-30 дней.\n\n"
            
            text += "⚠️ <b>Важно:</b> Очистка необратима!\nВосстановить данные будет невозможно."
            
            # Кнопки управления
            keyboard = []

            # Очистка воронки
            if deliveries_count > 0 or clicks_count > 0:
                keyboard.append([InlineKeyboardButton("📊 Очистить воронку", callback_data="admin_cleanup_funnel_menu")])

            # Очистка сообщений
            if sent_messages_count > 0:
                keyboard.append([InlineKeyboardButton("📅 Очистить сообщения", callback_data="admin_cleanup_messages_menu")])

            # Полная очистка воронки
            if deliveries_count > 0 or clicks_count > 0:
                keyboard.append([InlineKeyboardButton("🗑️ Очистить ВСЮ статистику", callback_data="admin_cleanup_all")])

            keyboard.append([InlineKeyboardButton("« Назад к сводке", callback_data="admin_dashboard")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при показе меню очистки: {e}")
            text = "❌ <b>Ошибка при загрузке меню очистки</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_dashboard")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_cleanup_funnel_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показать меню выбора периода очистки воронки
        """
        text = (
            "📊 <b>ОЧИСТКА ДАННЫХ ВОРОНКИ</b>\n\n"
            "Выберите период, данные старше которого будут удалены:\n\n"
            "⚠️ <b>Будут удалены:</b>\n"
            "• Записи об отправке сообщений\n"
            "• Записи о кликах по кнопкам\n\n"
            "<b>Это необратимо!</b>"
        )
        
        keyboard = [
            [InlineKeyboardButton("🗑️ Старше 30 дней", callback_data="admin_cleanup_funnel_30")],
            [InlineKeyboardButton("🗑️ Старше 60 дней", callback_data="admin_cleanup_funnel_60")],
            [InlineKeyboardButton("🗑️ Старше 90 дней", callback_data="admin_cleanup_funnel_90")],
            [InlineKeyboardButton("« Отмена", callback_data="admin_cleanup")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_cleanup_messages_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Показать меню выбора периода очистки отправленных сообщений
        """
        text = (
            "📅 <b>ОЧИСТКА ОТПРАВЛЕННЫХ СООБЩЕНИЙ</b>\n\n"
            "Выберите период, данные старше которого будут удалены:\n\n"
            "⚠️ <b>Будут удалены:</b>\n"
            "• Записи об отправленных сообщениях из таблицы scheduled_messages\n\n"
            "<b>Это необратимо!</b>"
        )
        
        keyboard = [
            [InlineKeyboardButton("🗑️ Старше 7 дней", callback_data="admin_cleanup_messages_7")],
            [InlineKeyboardButton("🗑️ Старше 14 дней", callback_data="admin_cleanup_messages_14")],
            [InlineKeyboardButton("🗑️ Старше 30 дней", callback_data="admin_cleanup_messages_30")],
            [InlineKeyboardButton("« Отмена", callback_data="admin_cleanup")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def perform_cleanup_funnel(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int):
        """
        Выполнить очистку данных воронки
        """
        try:
            await update.callback_query.answer("⏳ Выполняется очистка...", show_alert=False)
            
            # Выполняем очистку
            deliveries_deleted, clicks_deleted = self.db.cleanup_old_funnel_data(days_old=days)
            
            text = (
                f"✅ <b>ОЧИСТКА ЗАВЕРШЕНА</b>\n\n"
                f"📊 Удалено данных воронки старше {days} дней:\n"
                f"├─ Отправок: <b>{deliveries_deleted}</b>\n"
                f"└─ Кликов: <b>{clicks_deleted}</b>\n\n"
                f"💾 Место в базе данных освобождено."
            )
            
            keyboard = [
                [InlineKeyboardButton("« Назад к очистке", callback_data="admin_cleanup")],
                [InlineKeyboardButton("« К сводке", callback_data="admin_dashboard")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке воронки: {e}")
            text = f"❌ <b>Ошибка при очистке</b>\n\n{str(e)}"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_cleanup")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def perform_cleanup_messages(self, update: Update, context: ContextTypes.DEFAULT_TYPE, days: int):
        """
        Выполнить очистку старых отправленных сообщений
        """
        try:
            await update.callback_query.answer("⏳ Выполняется очистка...", show_alert=False)
            
            # Выполняем очистку
            deleted_count = self.db.cleanup_old_scheduled_messages(days_old=days)
            
            text = (
                f"✅ <b>ОЧИСТКА ЗАВЕРШЕНА</b>\n\n"
                f"📅 Удалено записей старше {days} дней:\n"
                f"└─ Отправленных сообщений: <b>{deleted_count}</b>\n\n"
                f"💾 Место в базе данных освобождено."
            )
            
            keyboard = [
                [InlineKeyboardButton("« Назад к очистке", callback_data="admin_cleanup")],
                [InlineKeyboardButton("« К сводке", callback_data="admin_dashboard")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при очистке сообщений: {e}")
            text = f"❌ <b>Ошибка при очистке</b>\n\n{str(e)}"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_cleanup")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_payment_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику платежей"""
        stats = self.db.get_payment_statistics()
        
        if not stats:
            text = "❌ <b>Ошибка при получении статистики платежей</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_dashboard")]]
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
                text += f"• {html.escape(str(utm_source))}: {count} платежей\n"
            text += "\n"
        
        # Последние платежи
        if stats['recent_payments']:
            text += "📋 <b>Последние платежи:</b>\n"
            for user_id, first_name, username, amount, created_at in stats['recent_payments'][:5]:
                username_str = f"@{username}" if username else "без username"
                date_str = datetime.fromisoformat(created_at).strftime("%d.%m %H:%M")
                text += f"• {html.escape(str(first_name))} ({html.escape(username_str)}): {amount} руб. - {date_str}\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("« Назад к сводке", callback_data="admin_dashboard")]
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
                    
                    # Заголовок сообщения - экранируем HTML для безопасности
                    text += f"<b>Сообщение {message_number}:</b> {html.escape(message_text)}\n"
                    
                    if delivered == 0:
                        text += "└─ ⏳ Еще не отправлялось\n\n"
                        continue
                    
                    # ОБНОВЛЕНО: Статистика по кликнувшим + total кликов
                    text += f"├─ 📬 Получили: <b>{delivered}</b> чел.\n"
                    text += f"├─ ✅ Кликнули кнопки: <b>{msg_data['clicked_any_button']}</b> чел.\n"
                    text += f"│  ├─ 📩 Callback: {clicked_callback} чел. ({msg_data.get('total_callback_clicks', 0)} кликов)\n"
                    text += f"│  └─ 🔗 URL: {clicked_url} чел. ({msg_data.get('total_url_clicks', 0)} кликов)\n"
                    
                    # Предупреждение о большом отвале
                    if drop_rate >= 30:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate:.1f}%) ⚠️ <b>БОЛЬШОЙ ОТВАЛ!</b>\n\n"
                    elif drop_rate >= 20:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate:.1f}%) ⚠️\n\n"
                    else:
                        text += f"└─ 📉 Отвалилось: <b>{dropped}</b> ({drop_rate:.1f}%)\n\n"
                
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
            keyboard.append([InlineKeyboardButton("« Назад к сводке", callback_data="admin_dashboard")])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
            
        except Exception as e:
            logger.error(f"❌ Ошибка при показе статистики воронки: {e}")
            text = "❌ <b>Ошибка при загрузке статистики воронки</b>"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_dashboard")]]
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
            
            # Обрезаем текст сообщения если он слишком длинный и экранируем HTML
            display_text = message_text[:100] + '...' if len(message_text) > 100 else message_text
            text += f"{html.escape(display_text)}\n\n"
            
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
                        
                        # Экранируем текст кнопки
                        text += f"{icon} <b>{html.escape(button_text)}</b> → {click_count} кликов ({percentage}%)\n"
                    
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
                # Экранируем пользовательские данные
                text += f"• {html.escape(str(first_name))} ({html.escape(username_str)}) {bot_status}{paid_icon}\n  ID: {user_id_db}, {join_date}\n\n"
            
            text += "\n💬 - может получать рассылки\n❌ - нужно написать боту /start\n💰 - оплатил"
        
        keyboard = [
            [InlineKeyboardButton("📊 Скачать CSV", callback_data="download_csv")],
            [InlineKeyboardButton("« Назад к сводке", callback_data="admin_dashboard")]
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

    async def show_cleanup_all_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать подтверждение полной очистки статистики"""
        text = (
            "⚠️ <b>ВНИМАНИЕ: ПОЛНАЯ ОЧИСТКА</b>\n\n"
            "Вы собираетесь удалить ВСЕ данные статистики воронки:\n"
            "• Все записи об отправке сообщений\n"
            "• Все записи о кликах по кнопкам\n\n"
            "❌ <b>Это действие НЕОБРАТИМО!</b>\n\n"
            "Вы уверены?"
        )

        keyboard = [
            [InlineKeyboardButton("🗑️ Да, удалить ВСЁ", callback_data="admin_cleanup_all_confirm")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_cleanup")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await self.safe_edit_or_send_message(update, context, text, reply_markup)

    async def perform_cleanup_all(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Выполнить полную очистку статистики воронки"""
        try:
            await update.callback_query.answer("⏳ Выполняется полная очистка...", show_alert=False)

            deliveries_deleted, clicks_deleted = self.db.cleanup_all_funnel_data()

            text = (
                f"✅ <b>ПОЛНАЯ ОЧИСТКА ЗАВЕРШЕНА</b>\n\n"
                f"🗑️ Удалено:\n"
                f"├─ Отправок: <b>{deliveries_deleted}</b>\n"
                f"└─ Кликов: <b>{clicks_deleted}</b>\n\n"
                f"💾 База данных очищена."
            )

            keyboard = [
                [InlineKeyboardButton("« Назад к очистке", callback_data="admin_cleanup")],
                [InlineKeyboardButton("« К сводке", callback_data="admin_dashboard")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            await self.safe_edit_or_send_message(update, context, text, reply_markup)

        except Exception as e:
            logger.error(f"❌ Ошибка при полной очистке: {e}")
            text = f"❌ <b>Ошибка при очистке</b>\n\n{str(e)}"
            keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_cleanup")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await self.safe_edit_or_send_message(update, context, text, reply_markup)
