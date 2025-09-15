from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from telegram.error import BadRequest
from datetime import datetime, timedelta
import logging
import io
import re
import asyncio
import utm_utils

logger = logging.getLogger(__name__)

class AdminPanel:
    def __init__(self, db, admin_chat_id):
        self.db = db
        self.admin_chat_id = admin_chat_id
        self.waiting_for = {}  # Словарь для отслеживания ожидаемого ввода
        self.broadcast_drafts = {}  # Словарь для хранения черновиков массовых рассылок
    
    async def safe_edit_or_send_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, reply_markup=None, parse_mode='HTML'):
        """ИСПРАВЛЕННАЯ безопасная отправка/редактирование сообщения"""
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
            # ИСПРАВЛЕНО: Более точная обработка ошибок event loop
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
            [InlineKeyboardButton("💰 Сообщение после оплаты", callback_data="admin_payment_message")],
            [InlineKeyboardButton("📊 Статистика платежей", callback_data="admin_payment_stats")],
            [InlineKeyboardButton("📢 Отправить всем сообщение", callback_data="admin_send_all")],
            [InlineKeyboardButton("⏰ Запланированные рассылки", callback_data="admin_scheduled_broadcasts")],
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "🔧 <b>Админ-панель</b>\n\nВыберите действие:"
        
        # Используем безопасный метод отправки/редактирования
        sent_message = await self.safe_edit_or_send_message(update, context, text, reply_markup)
        return sent_message
    
    # ===== НОВЫЕ МЕТОДЫ ДЛЯ РАБОТЫ С ПЛАТЕЖАМИ =====
    
    async def show_payment_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования сообщения после оплаты"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение об оплате
        payment_message_data = self.db.get_payment_success_message()
        
        if payment_message_data and payment_message_data['text']:
            current_message = payment_message_data['text']
            current_photo = payment_message_data['photo_url']
        else:
            current_message = (
                "🎉 <b>Спасибо за покупку!</b>\n\n"
                "💰 Ваш платеж успешно обработан!\n\n"
                "✅ Вы получили полный доступ ко всем материалам.\n\n"
                "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                "🙏 Благодарим за доверие!"
            )
            current_photo = None
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_payment_message_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_payment_message_photo")]
        ]
        
        if current_photo:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_payment_message_photo")])
        
        keyboard.append([InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_payment_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "💰 <b>Сообщение после оплаты</b>\n\n"
            "Это сообщение отправляется пользователям после успешной оплаты.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}\n\n"
            f"<b>Фото:</b> {'Есть' if current_photo else 'Нет'}\n\n"
            "💡 <i>В тексте можно использовать переменную {amount} - она будет заменена на сумму платежа.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
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
    
    async def handle_payment_message_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, input_type: str):
        """Обработка ввода для сообщения после оплаты"""
        user_id = update.effective_user.id
        
        if input_type == "payment_message_text":
            if len(text) > 4096:
                await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                return
            
            # Получаем текущее фото
            current_data = self.db.get_payment_success_message()
            current_photo = current_data['photo_url'] if current_data else None
            
            # Сохраняем новый текст
            self.db.set_payment_success_message(text, current_photo)
            
            await update.message.reply_text("✅ Сообщение после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
            
        elif input_type == "payment_message_photo":
            if not (text.startswith("http://") or text.startswith("https://")):
                await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
                return
            
            # Получаем текущий текст
            current_data = self.db.get_payment_success_message()
            current_text = current_data['text'] if current_data else None
            
            if not current_text:
                current_text = (
                    "🎉 <b>Спасибо за покупку!</b>\n\n"
                    "💰 Ваш платеж успешно обработан!\n\n"
                    "✅ Вы получили полный доступ ко всем материалам.\n\n"
                    "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                    "🙏 Благодарим за доверие!"
                )
            
            # Сохраняем новое фото
            self.db.set_payment_success_message(current_text, text)
            
            await update.message.reply_text("✅ Фото для сообщения после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
    
    async def show_payment_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        
        # Получаем текущее сообщение об оплате
        payment_message_data = self.db.get_payment_success_message()
        
        if payment_message_data and payment_message_data['text']:
            current_message = payment_message_data['text']
            current_photo = payment_message_data['photo_url']
        else:
            current_message = (
                "🎉 <b>Спасибо за покупку!</b>\n\n"
                "💰 Ваш платеж успешно обработан!\n\n"
                "✅ Вы получили полный доступ ко всем материалам.\n\n"
                "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                "🙏 Благодарим за доверие!"
            )
            current_photo = None
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_payment_message_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_payment_message_photo")]
        ]
        
        if current_photo:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_payment_message_photo")])
        
        keyboard.append([InlineKeyboardButton("🔄 Сбросить по умолчанию", callback_data="reset_payment_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "💰 <b>Сообщение после оплаты</b>\n\n"
            "Это сообщение отправляется пользователям после успешной оплаты.\n\n"
            f"<b>Текущий текст:</b>\n{current_message}\n\n"
            f"<b>Фото:</b> {'Есть' if current_photo else 'Нет'}\n\n"
            "💡 <i>В тексте можно использовать переменную {amount} - она будет заменена на сумму платежа.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # ===== ОБНОВЛЕННЫЕ МЕТОДЫ СТАТИСТИКИ =====
    
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
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
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
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # ===== СИСТЕМА МАССОВЫХ РАССЫЛОК =====
    
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
        text += "\n💡 <i>Все ссылки автоматически получат UTM метки для отслеживания.</i>\n"
        
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
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_mass_broadcast_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предварительный просмотр массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.broadcast_drafts[user_id]
        
        # Валидация
        if not draft["message_text"]:
            await update.callback_query.answer("❌ Сначала добавьте текст сообщения!", show_alert=True)
            return
        
        # Формируем превью
        preview_text = "📋 <b>Предварительный просмотр рассылки</b>\n\n"
        
        # Время отправки
        if draft["scheduled_hours"]:
            scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
            preview_text += f"⏰ <b>Запланировано на:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
            preview_text += f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n\n"
        else:
            preview_text += "🚀 <b>Отправка:</b> Немедленно\n\n"
        
        # Получатели
        users_count = len(self.db.get_users_with_bot_started())
        preview_text += f"👥 <b>Получателей:</b> {users_count} пользователей\n\n"
        
        # Фото
        if draft["photo_data"]:
            preview_text += "🖼 <b>Фото:</b> Есть\n\n"
        
        # Текст сообщения
        preview_text += "📝 <b>Текст сообщения:</b>\n"
        preview_text += f"<code>{draft['message_text']}</code>\n\n"
        
        # Кнопки
        if draft["buttons"]:
            preview_text += f"🔘 <b>Кнопки ({len(draft['buttons'])}):</b>\n"
            for i, button in enumerate(draft["buttons"], 1):
                preview_text += f"{i}. {button['text']} → {button['url']}\n"
            preview_text += "\n"
        
        preview_text += "🔗 <i>Все ссылки получат UTM метки для отслеживания конверсий.</i>\n\n"
        preview_text += "✅ Подтвердите отправку:"
        
        keyboard = [
            [InlineKeyboardButton("✅ Отправить", callback_data="mass_confirm_send")],
            [InlineKeyboardButton("✏️ Редактировать", callback_data="admin_send_all")],
            [InlineKeyboardButton("❌ Отмена", callback_data="admin_back")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем превью
        try:
            sent_message = await context.bot.send_message(
                chat_id=user_id,
                text=preview_text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
            
            # Если есть фото, отправляем его для предпросмотра
            if draft["photo_data"]:
                try:
                    await context.bot.send_photo(
                        chat_id=user_id,
                        photo=draft["photo_data"],
                        caption="📸 <b>Предпросмотр фото:</b>",
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if 'Event loop is closed' not in str(e):
                        logger.error(f"❌ Ошибка при отправке предпросмотра фото: {e}")
        except Exception as e:
            # ИСПРАВЛЕНО: Проверяем на event loop ошибки
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при отправке предпросмотра: {e}")
    
    async def execute_mass_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия выполнения массовой рассылки"""
        user_id = update.effective_user.id
        
        if user_id not in self.broadcast_drafts:
            await update.callback_query.answer("❌ Черновик не найден!", show_alert=True)
            return
        
        draft = self.broadcast_drafts[user_id]
        
        # Валидация
        if not draft["message_text"]:
            await update.callback_query.answer("❌ Сначала добавьте текст сообщения!", show_alert=True)
            return
        
        try:
            if draft["scheduled_hours"]:
                # Запланированная рассылка
                scheduled_time = datetime.now() + timedelta(hours=draft["scheduled_hours"])
                broadcast_id = self.db.add_scheduled_broadcast(
                    draft["message_text"], 
                    scheduled_time, 
                    draft["photo_data"]
                )
                
                # Добавляем кнопки если есть
                for i, button in enumerate(draft["buttons"], 1):
                    self.db.add_scheduled_broadcast_button(
                        broadcast_id, 
                        button["text"], 
                        button["url"], 
                        i
                    )
                
                await update.callback_query.answer("✅ Рассылка запланирована!")
                
                result_text = (
                    f"⏰ <b>Рассылка запланирована!</b>\n\n"
                    f"📅 <b>Время отправки:</b> {scheduled_time.strftime('%d.%m.%Y %H:%M')}\n"
                    f"⌛ <b>Через:</b> {draft['scheduled_hours']} час(ов)\n"
                    f"📨 <b>ID рассылки:</b> #{broadcast_id}\n\n"
                    f"🔗 <i>Все ссылки получат UTM метки для отслеживания.</i>"
                )
                
            else:
                # Немедленная рассылка
                users_with_bot = self.db.get_users_with_bot_started()
                
                if not users_with_bot:
                    await update.callback_query.answer("❌ Нет пользователей для рассылки!", show_alert=True)
                    return
                
                # Создаем клавиатуру если есть кнопки
                reply_markup = None
                if draft["buttons"]:
                    keyboard = []
                    for button in draft["buttons"]:
                        keyboard.append([InlineKeyboardButton(button["text"], url=button["url"])])
                    reply_markup = InlineKeyboardMarkup(keyboard)
                
                sent_count = 0
                failed_count = 0
                
                await update.callback_query.answer("🚀 Начинаем рассылку...")
                
                # Отправляем прогресс для больших рассылок
                progress_message = None
                if len(users_with_bot) > 50:
                    try:
                        progress_message = await context.bot.send_message(
                            chat_id=user_id,
                            text="🚀 <b>Рассылка начата...</b>\n\n📊 Прогресс: 0%",
                            parse_mode='HTML'
                        )
                    except Exception as e:
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Ошибка при отправке сообщения прогресса: {e}")
                
                for i, user in enumerate(users_with_bot):
                    user_id_to_send = user[0]
                    try:
                        await asyncio.sleep(0.1)  # Небольшая задержка
                        
                        # Обрабатываем текст и кнопки с UTM метками
                        processed_text = utm_utils.process_text_links(draft["message_text"], user_id_to_send)
                        
                        processed_reply_markup = reply_markup
                        if draft["buttons"]:
                            keyboard = []
                            for button in draft["buttons"]:
                                processed_url = utm_utils.add_utm_to_url(button["url"], user_id_to_send)
                                keyboard.append([InlineKeyboardButton(button["text"], url=processed_url)])
                            processed_reply_markup = InlineKeyboardMarkup(keyboard)
                        
                        if draft["photo_data"]:
                            await context.bot.send_photo(
                                chat_id=user_id_to_send,
                                photo=draft["photo_data"],
                                caption=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        else:
                            await context.bot.send_message(
                                chat_id=user_id_to_send,
                                text=processed_text,
                                parse_mode='HTML',
                                reply_markup=processed_reply_markup
                            )
                        sent_count += 1
                        
                        # Обновляем прогресс каждые 25 пользователей
                        if progress_message and i % 25 == 0:
                            progress = int((i / len(users_with_bot)) * 100)
                            try:
                                await progress_message.edit_text(
                                    f"🚀 <b>Рассылка в процессе...</b>\n\n"
                                    f"📊 Прогресс: {progress}%\n"
                                    f"✅ Отправлено: {sent_count}/{len(users_with_bot)}",
                                    parse_mode='HTML'
                                )
                            except Exception as e:
                                # ИСПРАВЛЕНО: Игнорируем ошибки event loop при обновлении прогресса
                                if 'Event loop is closed' not in str(e):
                                    logger.warning(f"⚠️ Ошибка при обновлении прогресса: {e}")
                        
                    except Exception as e:
                        failed_count += 1
                        if 'Event loop is closed' not in str(e):
                            logger.error(f"❌ Не удалось отправить рассылку пользователю {user_id_to_send}: {e}")
                
                result_text = (
                    f"✅ <b>Рассылка завершена!</b>\n\n"
                    f"📤 <b>Успешно отправлено:</b> {sent_count}\n"
                    f"❌ <b>Ошибок:</b> {failed_count}\n\n"
                    f"🔗 <i>Все ссылки содержат UTM метки для отслеживания конверсий.</i>"
                )
            
            # Очищаем черновик
            if user_id in self.broadcast_drafts:
                del self.broadcast_drafts[user_id]
            
            # Отправляем результат
            try:
                await context.bot.send_message(
                    chat_id=user_id,
                    text=result_text,
                    parse_mode='HTML'
                )
                
                # Возвращаемся в главное меню через 3 секунды
                await asyncio.sleep(3)
                await self.show_main_menu_safe(update, context)
            except Exception as e:
                if 'Event loop is closed' not in str(e):
                    logger.error(f"❌ Ошибка при отправке результата рассылки: {e}")
            
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при выполнении рассылки: {e}")
            await update.callback_query.answer("❌ Ошибка при отправке рассылки!", show_alert=True)
    
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
    
    # ===== ОСТАЛЬНЫЕ МЕТОДЫ =====
    
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
    
    # ===== ИСПРАВЛЕННЫЙ МЕТОД request_text_input =====
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, input_type, **kwargs):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        
        # ✅ ИСПРАВЛЕНИЕ: Добавляем правильную инициализацию для кнопок
        if input_type == "add_button":
            self.waiting_for[user_id] = {
                "type": input_type, 
                "created_at": datetime.now(), 
                "step": "text",  # ✅ ДОБАВЛЕНО: устанавливаем начальный шаг
                **kwargs
            }
        elif input_type == "add_goodbye_button":
            self.waiting_for[user_id] = {
                "type": input_type, 
                "created_at": datetime.now(), 
                "step": "text",  # ✅ ДОБАВЛЕНО: устанавливаем начальный шаг для кнопок прощания
                **kwargs
            }
        else:
            self.waiting_for[user_id] = {"type": input_type, "created_at": datetime.now(), **kwargs}
        
        texts = {
            "welcome": "✏️ Отправьте новое приветственное сообщение:",
            "goodbye": "✏️ Отправьте новое прощальное сообщение:",
            "success_message": "✏️ Отправьте новое сообщение подтверждения:",
            "broadcast_text": f"✏️ Отправьте новый текст для сообщения {kwargs.get('message_number')}:",
            "broadcast_delay": self._get_delay_text(kwargs.get('message_number')),
            "broadcast_photo": f"🖼 Отправьте фото для сообщения {kwargs.get('message_number')} или ссылку на фото:",
            "welcome_photo": "🖼 Отправьте фото для приветственного сообщения или ссылку на фото:",
            "goodbye_photo": "🖼 Отправьте фото для прощального сообщения или ссылку на фото:",
            "edit_button_text": "✏️ Отправьте новый текст для кнопки:",
            "edit_button_url": "🔗 Отправьте новый URL для кнопки:",
            "broadcast_timer": "⏰ Отправьте количество часов, через которое возобновить рассылку:",
            "add_message": "✏️ Отправьте текст нового сообщения:",
            # ✅ ИСПРАВЛЕНО: Правильный текст для кнопок
            "add_button": f"✏️ Отправьте текст для новой кнопки сообщения {kwargs.get('message_number')}:\n\n💡 После этого мы попросим URL для кнопки.",
            # Новые типы для массовой рассылки
            "mass_text": "✏️ Отправьте текст для массовой рассылки:",
            "mass_photo": "🖼 Отправьте фото для массовой рассылки или ссылку на фото:",
            "mass_time": "⏰ Через сколько часов отправить рассылку?\n\nПримеры: 1, 2.5, 24\n\nОставьте пустым для отправки сейчас:",
            "mass_button_text": "✏️ Отправьте текст для кнопки:",
            "mass_button_url": "🔗 Отправьте URL для кнопки:",
            # Новые типы для платежей
            "payment_message_text": "✏️ Отправьте новый текст сообщения после оплаты:\n\n💡 Можно использовать переменную {amount} - она будет заменена на сумму платежа.",
            "payment_message_photo": "🖼 Отправьте фото для сообщения после оплаты или ссылку на фото:",
            # НОВЫЕ ТИПЫ ДЛЯ КНОПОК ПРИВЕТСТВИЯ
            "add_welcome_button": "⌨️ Отправьте текст для новой кнопки приветствия:",
            "edit_welcome_button_text": "📝 Отправьте новый текст для кнопки:",
            # НОВЫЕ ТИПЫ ДЛЯ КНОПОК ПРОЩАНИЯ
            "add_goodbye_button": "🔘 Отправьте текст для новой кнопки прощания:\n\n💡 После этого мы попросим URL для кнопки.",
            "edit_goodbye_button_text": "📝 Отправьте новый текст для кнопки:",
            "edit_goodbye_button_url": "🔗 Отправьте новый URL для кнопки:",
        }
        
        text = texts.get(input_type, "Отправьте необходимые данные:")
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_send_all")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
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
            if 'Event loop is closed' not in str(e):
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
        text += "\n💡 <i>Все ссылки автоматически получат UTM метки для отслеживания.</i>\n"
        
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
    
    async def show_welcome_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        welcome_data = self.db.get_welcome_message()
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        keyboard.append([InlineKeyboardButton("⌨️ Управление кнопками", callback_data="manage_welcome_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if welcome_buttons:
            buttons_info = f"\n\n<b>⌨️ Кнопки ({len(welcome_buttons)}):</b>\n"
            for i, (button_id, button_text, position) in enumerate(welcome_buttons, 1):
                buttons_info += f"{i}. {button_text}\n"
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_goodbye_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        goodbye_data = self.db.get_goodbye_message()
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        keyboard.append([InlineKeyboardButton("🔘 Управление инлайн кнопками", callback_data="manage_goodbye_buttons")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if goodbye_buttons:
            buttons_info = f"\n\n<b>🔘 Инлайн кнопки ({len(goodbye_buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(goodbye_buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_success_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
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
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_message_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        
        msg_data = self.db.get_broadcast_message(message_number)
        if not msg_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Сообщение не найдено!")
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
        
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        if delay_hours < 1:
            delay_str = f"{int(delay_hours * 60)} минут"
        else:
            delay_str = f"{delay_hours} часов"
        
        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после регистрации\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
            f"{buttons_info}\n\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_message_buttons(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню управления кнопками сообщения"""
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
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """Показать меню редактирования кнопки"""
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
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_welcome_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования приветственного сообщения"""
        welcome_data = self.db.get_welcome_message()
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        # Управление кнопками
        keyboard.append([InlineKeyboardButton("⌨️ Управление кнопками", callback_data="manage_welcome_buttons")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Добавляем информацию о кнопках
        buttons_info = ""
        if welcome_buttons:
            buttons_info = f"\n\n<b>⌨️ Кнопки ({len(welcome_buttons)}):</b>\n"
            for i, (button_id, button_text, position) in enumerate(welcome_buttons, 1):
                buttons_info += f"{i}. {button_text}\n"
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    # ===== НОВЫЙ ИСПРАВЛЕННЫЙ МЕТОД show_welcome_buttons_management =====
    async def show_welcome_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками приветственного сообщения"""
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = []
        
        # Показать существующие кнопки для редактирования
        for button_id, button_text, position in welcome_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_welcome_button_{button_id}")])
        
        # Кнопка добавления (лимит 5 кнопок)
        if len(welcome_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_welcome_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_welcome")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Механические кнопки приветствия</b>\n\n"
            f"Текущие кнопки: {len(welcome_buttons)}/5\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # ===== НОВЫЙ МЕТОД show_welcome_button_edit =====
    async def show_welcome_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать меню редактирования конкретной кнопки"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, position FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_id, button_text, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_button_text_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_welcome_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст кнопки:</b> {button_text}\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # ===== НОВЫЙ МЕТОД show_welcome_button_delete_confirm =====
    async def show_welcome_button_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать подтверждение удаления кнопки"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT button_text FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_text = button_data[0]
        
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_welcome_button_{button_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⚠️ <b>Подтверждение удаления</b>\n\n"
            f"Вы уверены, что хотите удалить кнопку:\n"
            f'<b>"{button_text}"</b>?'
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # ===== НОВЫЕ HELPER-МЕТОДЫ =====
    async def show_welcome_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасное отображение управления кнопками из контекста"""
        user_id = update.effective_user.id
        
        welcome_buttons = self.db.get_welcome_buttons()
        
        keyboard = []
        
        for button_id, button_text, position in welcome_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_welcome_button_{button_id}")])
        
        if len(welcome_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_welcome_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_welcome")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Механические кнопки приветствия</b>\n\n"
            f"Текущие кнопки: {len(welcome_buttons)}/5\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_welcome_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Безопасное отображение редактирования кнопки из контекста"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, position FROM welcome_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        button_id, button_text, position = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_welcome_button_text_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_welcome_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data="manage_welcome_buttons")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⌨️ <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст кнопки:</b> {button_text}\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    # ===== НОВЫЕ МЕТОДЫ ОБРАБОТКИ ВВОДА =====
    async def handle_add_welcome_button_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления новой кнопки приветствия"""
        user_id = update.effective_user.id
        
        if len(text) > 30:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 30 символов.")
            return
        
        # Проверяем уникальность
        existing_button = self.db.get_welcome_button_by_text(text)
        if existing_button:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Добавляем кнопку
        button_id = self.db.add_welcome_button(text)
        
        await update.message.reply_text(f"✅ Кнопка '{text}' добавлена!")
        del self.waiting_for[user_id]
        
        # Показываем обновленный список
        await self.show_welcome_buttons_management_from_context(update, context)
    
    async def handle_edit_welcome_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения текста кнопки"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if len(text) > 30:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 30 символов.")
            return
        
        # Проверяем уникальность (исключая текущую кнопку)
        existing_button = self.db.get_welcome_button_by_text(text)
        if existing_button and existing_button[0] != button_id:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Обновляем кнопку
        self.db.update_welcome_button(button_id, button_text=text)
        
        await update.message.reply_text(f"✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_welcome_button_edit_from_context(update, context, button_id)
    
    async def handle_add_goodbye_button_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления новой кнопки прощания (в два этапа)"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Шаг 1: Получаем текст кнопки
            if len(text) > 64:
                await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
                return
            
            # Сохраняем текст и переходим к URL
            self.waiting_for[user_id]["button_text"] = text
            self.waiting_for[user_id]["step"] = "url"
            
            await update.message.reply_text(
                f"✅ Текст кнопки сохранен: <b>{text}</b>\n\n"
                f"🔗 Теперь отправьте URL для кнопки:\n\n"
                f"💡 Пример: https://example.com\n"
                f"🎯 UTM метки будут добавлены автоматически!",
                parse_mode='HTML'
            )
            
        elif current_step == "url":
            # Шаг 2: Получаем URL кнопки
            if not (text.startswith("http://") or text.startswith("https://")):
                await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                return
            
            if len(text) > 256:
                await update.message.reply_text("❌ URL слишком длинный.")
                return
            
            button_text = waiting_data["button_text"]
            
            # Проверяем уникальность текста
            existing_button = self.db.get_goodbye_button_by_text(button_text)
            if existing_button:
                await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
                del self.waiting_for[user_id]
                return
            
            # Добавляем кнопку
            button_id = self.db.add_goodbye_button(button_text, text)
            
            await update.message.reply_text(
                f"✅ Кнопка успешно добавлена!\n\n"
                f"📝 <b>Текст:</b> {button_text}\n"
                f"🔗 <b>URL:</b> {text}\n\n"
                f"🎯 <b>UTM метки будут добавлены автоматически при отправке!</b>",
                parse_mode='HTML'
            )
            
            del self.waiting_for[user_id]
            
            # Показываем обновленный список
            await self.show_goodbye_buttons_management_from_context(update, context)
        
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить кнопку заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    async def handle_edit_goodbye_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения текста кнопки прощания"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        # Проверяем уникальность (исключая текущую кнопку)
        existing_button = self.db.get_goodbye_button_by_text(text)
        if existing_button and existing_button[0] != button_id:
            await update.message.reply_text("❌ Кнопка с таким текстом уже существует!")
            return
        
        # Обновляем кнопку
        self.db.update_goodbye_button(button_id, button_text=text)
        
        await update.message.reply_text(f"✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_goodbye_button_edit_from_context(update, context, button_id)
    
    async def handle_edit_goodbye_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка изменения URL кнопки прощания"""
        user_id = update.effective_user.id
        button_id = self.waiting_for[user_id]["button_id"]
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        # Обновляем URL кнопки
        self.db.update_goodbye_button(button_id, button_url=text)
        
        await update.message.reply_text("✅ URL кнопки обновлен!")
        del self.waiting_for[user_id]
        
        # Показываем меню редактирования кнопки
        await self.show_goodbye_button_edit_from_context(update, context, button_id)
    
    async def show_goodbye_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования прощального сообщения"""
        goodbye_data = self.db.get_goodbye_message()
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        # Управление кнопками
        keyboard.append([InlineKeyboardButton("🔘 Управление инлайн кнопками", callback_data="manage_goodbye_buttons")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Добавляем информацию о кнопках
        buttons_info = ""
        if goodbye_buttons:
            buttons_info = f"\n\n<b>🔘 Инлайн кнопки ({len(goodbye_buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(goodbye_buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
            f"{buttons_info}"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_success_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования сообщения подтверждения"""
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
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
    
    async def show_scheduled_broadcasts(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать запланированные рассылки"""
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
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "⏰ <b>Запланированные рассылки</b>\n\n"
            f"Активных рассылок: {len(broadcasts)}\n\n"
            "🖼 - сообщение с фото\n"
            "🔘N - количество кнопок\n\n"
            "💡 <i>Все ссылки получат UTM метки для отслеживания.</i>\n\n"
            "Выберите рассылку для редактирования:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    # ===== МЕТОДЫ ДЛЯ УПРАВЛЕНИЯ КНОПКАМИ ПРОЩАНИЯ =====
    
    async def show_goodbye_buttons_management(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления кнопками прощального сообщения"""
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = []
        
        # Показать существующие кнопки для редактирования
        for button_id, button_text, button_url, position in goodbye_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_goodbye_button_{button_id}")])
        
        # Кнопка добавления (лимит 5 кнопок)
        if len(goodbye_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_goodbye_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_goodbye")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Инлайн кнопки прощания</b>\n\n"
            f"Текущие кнопки: {len(goodbye_buttons)}/5\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_goodbye_button_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать меню редактирования конкретной кнопки прощания"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, button_url, position FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_id, button_text, button_url, position = button_data
        
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
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_goodbye_button_delete_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Показать подтверждение удаления кнопки прощания"""
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT button_text FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await update.callback_query.answer("❌ Кнопка не найдена!", show_alert=True)
            return
        
        button_text = button_data[0]
        
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_goodbye_button_{button_id}")],
            [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_goodbye_button_{button_id}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"⚠️ <b>Подтверждение удаления</b>\n\n"
            f"Вы уверены, что хотите удалить кнопку:\n"
            f'<b>"{button_text}"</b>?'
        )
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_goodbye_buttons_management_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Безопасное отображение управления кнопками прощания из контекста"""
        user_id = update.effective_user.id
        
        goodbye_buttons = self.db.get_goodbye_buttons()
        
        keyboard = []
        
        for button_id, button_text, button_url, position in goodbye_buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_goodbye_button_{button_id}")])
        
        if len(goodbye_buttons) < 5:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data="add_goodbye_button")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_goodbye")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            f"🔘 <b>Инлайн кнопки прощания</b>\n\n"
            f"Текущие кнопки: {len(goodbye_buttons)}/5\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
    async def show_goodbye_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id: int):
        """Безопасное отображение редактирования кнопки прощания из контекста"""
        user_id = update.effective_user.id
        
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id, button_text, button_url, position FROM goodbye_buttons WHERE id = ?', (button_id,))
        button_data = cursor.fetchone()
        conn.close()
        
        if not button_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        button_id, button_text, button_url, position = button_data
        
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
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, text, reply_markup)
    
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
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при очистке админ-панели: {e}")
    
    async def show_button_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, button_id):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
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
            await context.bot.send_message(chat_id=user_id, text="❌ Кнопка не найдена!")
            return
        
        message_number, button_text, button_url = button_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_button_text_{button_id}")],
            [InlineKeyboardButton("🔗 Изменить URL", callback_data=f"edit_button_url_{button_id}")],
            [InlineKeyboardButton("🗑 Удалить кнопку", callback_data=f"delete_button_{button_id}")],
            [InlineKeyboardButton("« Назад", callback_data=f"manage_buttons_{message_number}")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"🔘 <b>Редактирование кнопки</b>\n\n"
            f"<b>Текст:</b> {button_text}\n"
            f"<b>URL:</b> {button_url}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите действие:"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_broadcast_menu_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        
        messages = self.db.get_all_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours, photo_url in messages:
            buttons = self.db.get_message_buttons(msg_num)
            button_icon = f"🔘{len(buttons)}" if buttons else ""
            photo_icon = "🖼" if photo_url else ""
            
            if delay_hours < 1:
                delay_str = f"{int(delay_hours * 60)}м"
            else:
                delay_str = f"{delay_hours}ч"
            
            button_text = f"{photo_icon}{button_icon} Сообщение {msg_num} ({delay_str})"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("➕ Добавить сообщение", callback_data="add_message")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка после регистрации.\n"
            "🖼 - сообщение содержит фото\n"
            "🔘N - количество кнопок в сообщении\n\n"
            "💡 <i>Все ссылки в сообщениях автоматически получают UTM метки для отслеживания конверсий.</i>"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_message_buttons_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """ИСПРАВЛЕННАЯ версия - отправляем НОВОЕ сообщение"""
        user_id = update.effective_user.id
        
        buttons = self.db.get_message_buttons(message_number)
        
        keyboard = []
        
        for button_id, button_text, button_url, position in buttons:
            keyboard.append([InlineKeyboardButton(f"📝 {button_text}", callback_data=f"edit_button_{button_id}")])
        
        if len(buttons) < 3:
            keyboard.append([InlineKeyboardButton("➕ Добавить кнопку", callback_data=f"add_button_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data=f"edit_msg_{message_number}")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"🔘 <b>Кнопки сообщения {message_number}</b>\n\n"
            f"Текущие кнопки: {len(buttons)}/3\n\n"
            "💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            "Выберите кнопку для редактирования или добавьте новую:"
        )
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    # ===== ОБРАБОТЧИКИ CALLBACK ЗАПРОСОВ =====
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ обработка всех callback запросов админ-панели"""
        query = update.callback_query
        data = query.data
        user_id = query.from_user.id
        
        try:
            await query.answer()
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.warning(f"⚠️ Ошибка при ответе на callback: {e}")
        
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
            elif data == "admin_users":
                await self.show_users_list(update, context)
            elif data == "admin_send_all":
                await self.show_send_all_menu(update, context)
            elif data == "admin_welcome":
                await self.show_welcome_edit(update, context)
            elif data == "admin_goodbye":
                await self.show_goodbye_edit(update, context)
            elif data == "admin_success_message":
                await self.show_success_message_edit(update, context)
            elif data == "admin_scheduled_broadcasts":
                await self.show_scheduled_broadcasts(update, context)
            elif data == "download_csv":
                await self.send_csv_file(update, context)
            elif data == "enable_broadcast":
                self.db.set_broadcast_status(True, None)
                await self.show_broadcast_status(update, context)
            elif data == "disable_broadcast":
                self.db.set_broadcast_status(False, None)
                await self.show_broadcast_status(update, context)
            elif data == "set_broadcast_timer":
                await self.request_text_input(update, context, "broadcast_timer")
            
            # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ ПЛАТЕЖЕЙ =====
            elif data == "admin_payment_message":
                await self.show_payment_message_edit(update, context)
            elif data == "admin_payment_stats":
                await self.show_payment_statistics(update, context)
            elif data == "edit_payment_message_text":
                await self.request_text_input(update, context, "payment_message_text")
            elif data == "edit_payment_message_photo":
                await self.request_text_input(update, context, "payment_message_photo")
            elif data == "remove_payment_message_photo":
                # Получаем текущий текст и удаляем фото
                current_data = self.db.get_payment_success_message()
                current_text = current_data['text'] if current_data else None
                if not current_text:
                    current_text = (
                        "🎉 <b>Спасибо за покупку!</b>\n\n"
                        "💰 Ваш платеж успешно обработан!\n\n"
                        "✅ Вы получили полный доступ ко всем материалам.\n\n"
                        "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                        "🙏 Благодарим за доверие!"
                    )
                self.db.set_payment_success_message(current_text, "")
                await self.show_payment_message_edit(update, context)
            elif data == "reset_payment_message":
                default_payment_message = (
                    "🎉 <b>Спасибо за покупку!</b>\n\n"
                    "💰 Ваш платеж на сумму {amount} успешно обработан!\n\n"
                    "✅ Вы получили полный доступ ко всем материалам.\n\n"
                    "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                    "🙏 Благодарим за доверие!"
                )
                self.db.set_payment_success_message(default_payment_message, "")
                await self.show_payment_message_edit(update, context)
            
            # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ МАССОВОЙ РАССЫЛКИ =====
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
                    self.broadcast_drafts[user_id]["scheduled_hours"] = None
                    await self.show_mass_broadcast_preview(update, context)
            elif data == "mass_confirm_send":
                await self.execute_mass_broadcast(update, context)
            
            # ===== ОБРАБОТЧИКИ ДЛЯ ОСНОВНЫХ СООБЩЕНИЙ РАССЫЛКИ =====
            elif data.startswith("edit_msg_"):
                message_number = int(data.split("_")[2])
                await self.show_message_edit(update, context, message_number)
            elif data.startswith("manage_buttons_"):
                message_number = int(data.split("_")[2])
                await self.show_message_buttons(update, context, message_number)
            elif data.startswith("edit_button_") and not data.startswith("edit_button_text_") and not data.startswith("edit_button_url_"):
                button_id = int(data.split("_")[2])
                await self.show_button_edit(update, context, button_id)
            elif data.startswith("edit_button_text_"):
                button_id = int(data.split("_")[3])
                await self.request_text_input(update, context, "edit_button_text", button_id=button_id)
            elif data.startswith("edit_button_url_"):
                button_id = int(data.split("_")[3])
                await self.request_text_input(update, context, "edit_button_url", button_id=button_id)
            elif data.startswith("delete_button_"):
                button_id = int(data.split("_")[2])
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT message_number FROM message_buttons WHERE id = ?', (button_id,))
                result = cursor.fetchone()
                conn.close()
                
                if result:
                    message_number = result[0]
                    self.db.delete_message_button(button_id)
                    await self.show_message_buttons(update, context, message_number)
            
            # ===== ОБРАБОТЧИКИ ДЛЯ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ =====
            elif data == "manage_welcome_buttons":
                await self.show_welcome_buttons_management(update, context)
            elif data == "add_welcome_button":
                await self.request_text_input(update, context, "add_welcome_button")
            elif data.startswith("edit_welcome_button_") and not data.startswith("edit_welcome_button_text_"):
                button_id = int(data.split("_")[3])
                await self.show_welcome_button_edit(update, context, button_id)
            elif data.startswith("edit_welcome_button_text_"):
                button_id = int(data.split("_")[4])
                await self.request_text_input(update, context, "edit_welcome_button_text", button_id=button_id)
            elif data.startswith("delete_welcome_button_"):
                button_id = int(data.split("_")[3])
                await self.show_welcome_button_delete_confirm(update, context, button_id)
            elif data.startswith("confirm_delete_welcome_button_"):
                button_id = int(data.split("_")[4])
                self.db.delete_welcome_button(button_id)
                await query.answer("✅ Кнопка удалена!")
                await self.show_welcome_buttons_management(update, context)
            elif data == "edit_welcome_text":
                await self.request_text_input(update, context, "welcome")
            elif data == "edit_welcome_photo":
                await self.request_text_input(update, context, "welcome_photo")
            elif data == "remove_welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, photo_url="")
                await self.show_welcome_edit(update, context)
            
            # ===== ОБРАБОТЧИКИ ДЛЯ ПРОЩАЛЬНОГО СООБЩЕНИЯ =====
            elif data == "manage_goodbye_buttons":
                await self.show_goodbye_buttons_management(update, context)
            elif data == "add_goodbye_button":
                await self.request_text_input(update, context, "add_goodbye_button")
            elif data.startswith("edit_goodbye_button_") and not data.startswith("edit_goodbye_button_text_") and not data.startswith("edit_goodbye_button_url_"):
                button_id = int(data.split("_")[3])
                await self.show_goodbye_button_edit(update, context, button_id)
            elif data.startswith("edit_goodbye_button_text_"):
                button_id = int(data.split("_")[4])
                await self.request_text_input(update, context, "edit_goodbye_button_text", button_id=button_id)
            elif data.startswith("edit_goodbye_button_url_"):
                button_id = int(data.split("_")[4])
                await self.request_text_input(update, context, "edit_goodbye_button_url", button_id=button_id)
            elif data.startswith("delete_goodbye_button_"):
                button_id = int(data.split("_")[3])
                await self.show_goodbye_button_delete_confirm(update, context, button_id)
            elif data.startswith("confirm_delete_goodbye_button_"):
                button_id = int(data.split("_")[4])
                self.db.delete_goodbye_button(button_id)
                await query.answer("✅ Кнопка удалена!")
                await self.show_goodbye_buttons_management(update, context)
            elif data == "edit_goodbye_text":
                await self.request_text_input(update, context, "goodbye")
            elif data == "edit_goodbye_photo":
                await self.request_text_input(update, context, "goodbye_photo")
            elif data == "remove_goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, photo_url="")
                await self.show_goodbye_edit(update, context)
            
            # ===== ОБРАБОТЧИКИ ДЛЯ СООБЩЕНИЯ ПОДТВЕРЖДЕНИЯ =====
            elif data == "edit_success_message_text":
                await self.request_text_input(update, context, "success_message")
            elif data == "reset_success_message":
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
                await self.show_success_message_edit(update, context)
            
            # ===== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ =====
            elif await self.handle_additional_callbacks(update, context):
                pass
            
            else:
                await self.show_error_message(update, context, "❌ Неизвестная команда.")
                
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при обработке callback {data}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка. Попробуйте еще раз.")
    
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
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """ИСПРАВЛЕННАЯ обработка текстовых сообщений и фото от админа"""
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
            
            # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ КНОПОК ПРИВЕТСТВИЯ =====
            if input_type == "add_welcome_button":
                await self.handle_add_welcome_button_input(update, context, text)
            elif input_type == "edit_welcome_button_text":
                await self.handle_edit_welcome_button_text_input(update, context, text)
            
            # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ КНОПОК ПРОЩАНИЯ =====
            elif input_type == "add_goodbye_button":
                await self.handle_add_goodbye_button_input(update, context, text)
            elif input_type == "edit_goodbye_button_text":
                await self.handle_edit_goodbye_button_text_input(update, context, text)
            elif input_type == "edit_goodbye_button_url":
                await self.handle_edit_goodbye_button_url_input(update, context, text)
                
            # ===== НОВЫЕ ОБРАБОТЧИКИ ДЛЯ ПЛАТЕЖЕЙ =====
            elif input_type == "payment_message_text":
                await self.handle_payment_message_input(update, context, text, "payment_message_text")
            elif input_type == "payment_message_photo":
                await self.handle_payment_message_input(update, context, text, "payment_message_photo")
            
            # ===== ОБРАБОТКА НОВЫХ ТИПОВ ДЛЯ МАССОВОЙ РАССЫЛКИ =====
            elif input_type == "mass_text":
                await self.handle_mass_text_input(update, context, text)
            elif input_type == "mass_photo":
                await self.handle_mass_photo_input(update, context, text)
            elif input_type == "mass_time":
                await self.handle_mass_time_input(update, context, text)
            elif input_type == "mass_button_text":
                await self.handle_mass_button_text_input(update, context, text)
            elif input_type == "mass_button_url":
                await self.handle_mass_button_url_input(update, context, text)
            
            # ===== ОБРАБОТКА ОСТАЛЬНЫХ ТИПОВ =====
            elif input_type == "broadcast_timer":
                await self.handle_broadcast_timer(update, context, text)
            elif input_type == "add_message":
                await self.handle_add_message(update, context, text)
            elif input_type == "add_button":
                await self.handle_add_button(update, context, text)
            
            # Обработка для базовых настроек
            elif input_type == "welcome":
                if len(text) > 4096:
                    await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                    return
                self.db.set_welcome_message(text)
                await update.message.reply_text("✅ Приветственное сообщение обновлено!")
                del self.waiting_for[user_id]
                await self.show_welcome_edit_from_context(update, context)
                
            elif input_type == "goodbye":
                if len(text) > 4096:
                    await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                    return
                self.db.set_goodbye_message(text)
                await update.message.reply_text("✅ Прощальное сообщение обновлено!")
                del self.waiting_for[user_id]
                await self.show_goodbye_edit_from_context(update, context)
            
            elif input_type == "success_message":
                if len(text) > 4096:
                    await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                    return
                conn = self.db._get_connection()
                cursor = conn.cursor()
                cursor.execute('INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)', ('success_message', text))
                conn.commit()
                conn.close()
                await update.message.reply_text("✅ Сообщение подтверждения обновлено!")
                del self.waiting_for[user_id]
                await self.show_success_message_edit_from_context(update, context)
            
            # Обработка для базовых сообщений рассылки
            elif input_type == "broadcast_text":
                message_number = waiting_data["message_number"]
                if len(text) > 4096:
                    await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
                    return
                self.db.update_broadcast_message(message_number, text=text)
                await update.message.reply_text(f"✅ Текст сообщения {message_number} обновлён!")
                del self.waiting_for[user_id]
                await self.show_message_edit_from_context(update, context, message_number)
            
            elif input_type == "broadcast_delay":
                message_number = waiting_data["message_number"]
                
                delay_hours, delay_display = self.parse_delay_input(text)
                
                if delay_hours is not None and delay_hours > 0:
                    self.db.update_broadcast_message(message_number, delay_hours=delay_hours)
                    await update.message.reply_text(f"✅ Задержка для сообщения {message_number} установлена на {delay_display}!")
                    del self.waiting_for[user_id]
                    await self.show_message_edit_from_context(update, context, message_number)
                else:
                    await update.message.reply_text(
                        "❌ Неверный формат! Примеры правильного ввода:\n\n"
                        "• <code>3м</code> или <code>3 минуты</code>\n"
                        "• <code>2ч</code> или <code>2 часа</code>\n"
                        "• <code>1.5</code> (для 1.5 часов)\n"
                        "• <code>0.05</code> (для 3 минут)",
                        parse_mode='HTML'
                    )
            
            # Обработка URL-ссылок на фото
            elif input_type in ["broadcast_photo", "welcome_photo", "goodbye_photo", "payment_message_photo"]:
                if text.startswith("http://") or text.startswith("https://"):
                    await self.handle_photo_url_input(update, context, text, input_type, **waiting_data)
                else:
                    await update.message.reply_text("❌ Отправьте фото или ссылку на фото (начинающуюся с http:// или https://)")
            
            elif input_type == "edit_button_text":
                button_id = waiting_data["button_id"]
                if len(text) > 64:
                    await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
                    return
                    
                self.db.update_message_button(button_id, button_text=text)
                await update.message.reply_text("✅ Текст кнопки обновлен!")
                del self.waiting_for[user_id]
                await self.show_button_edit_from_context(update, context, button_id)
            
            elif input_type == "edit_button_url":
                if not (text.startswith("http://") or text.startswith("https://")):
                    await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                    return
                
                if len(text) > 256:
                    await update.message.reply_text("❌ URL слишком длинный.")
                    return
                
                button_id = waiting_data["button_id"]
                self.db.update_message_button(button_id, button_url=text)
                await update.message.reply_text("✅ URL кнопки обновлен!")
                del self.waiting_for[user_id]
                await self.show_button_edit_from_context(update, context, button_id)
            
            else:
                await self.show_error_message(update, context, "❌ Неизвестный тип ввода.")
                
        except Exception as e:
            if 'Event loop is closed' not in str(e):
                logger.error(f"❌ Ошибка при обработке сообщения от админа {user_id}: {e}")
            await self.show_error_message(update, context, "❌ Произошла ошибка при обработке вашего сообщения.")
    
    # ===== ОБРАБОТЧИКИ ДЛЯ МАССОВОЙ РАССЫЛКИ =====
    
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
        
        # Возвращаемся в меню
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
        
        elif input_type == "payment_message_photo":
            # Фото для сообщения об оплате
            photo_file_id = update.message.photo[-1].file_id
            
            # Получаем текущий текст
            current_data = self.db.get_payment_success_message()
            current_text = current_data['text'] if current_data else None
            
            if not current_text:
                current_text = (
                    "🎉 <b>Спасибо за покупку!</b>\n\n"
                    "💰 Ваш платеж успешно обработан!\n\n"
                    "✅ Вы получили полный доступ ко всем материалам.\n\n"
                    "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                    "🙏 Благодарим за доверие!"
                )
            
            self.db.set_payment_success_message(current_text, photo_file_id)
            await update.message.reply_text("✅ Фото для сообщения после оплаты обновлено!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
        
        elif input_type == "broadcast_photo":
            # Фото для базового сообщения рассылки
            message_number = waiting_data["message_number"]
            photo_file_id = update.message.photo[-1].file_id
            
            self.db.update_broadcast_message(message_number, photo_url=photo_file_id)
            await update.message.reply_text(f"✅ Фото для сообщения {message_number} обновлено!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        
        elif input_type == "welcome_photo":
            # Фото для приветственного сообщения
            photo_file_id = update.message.photo[-1].file_id
            welcome_text = self.db.get_welcome_message()['text']
            self.db.set_welcome_message(welcome_text, photo_url=photo_file_id)
            await update.message.reply_text("✅ Фото для приветственного сообщения обновлено!")
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
        
        elif input_type == "goodbye_photo":
            # Фото для прощального сообщения
            photo_file_id = update.message.photo[-1].file_id
            goodbye_text = self.db.get_goodbye_message()['text']
            self.db.set_goodbye_message(goodbye_text, photo_url=photo_file_id)
            await update.message.reply_text("✅ Фото для прощального сообщения обновлено!")
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        else:
            await self.show_error_message(update, context, "❌ Неожиданное фото.")
    
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
    
    async def handle_photo_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, url: str, input_type: str, **kwargs):
        """Обработка URL-ссылок на фото"""
        user_id = update.effective_user.id
        
        if input_type == "broadcast_photo":
            message_number = kwargs.get("message_number")
            self.db.update_broadcast_message(message_number, photo_url=url)
            await update.message.reply_text(f"✅ Ссылка на фото для сообщения {message_number} сохранена!")
            del self.waiting_for[user_id]
            await self.show_message_edit_from_context(update, context, message_number)
        
        elif input_type == "welcome_photo":
            welcome_text = self.db.get_welcome_message()['text']
            self.db.set_welcome_message(welcome_text, photo_url=url)
            await update.message.reply_text("✅ Ссылка на фото для приветственного сообщения сохранена!")
            del self.waiting_for[user_id]
            await self.show_welcome_edit_from_context(update, context)
        
        elif input_type == "goodbye_photo":
            goodbye_text = self.db.get_goodbye_message()['text']
            self.db.set_goodbye_message(goodbye_text, photo_url=url)
            await update.message.reply_text("✅ Ссылка на фото для прощального сообщения сохранена!")
            del self.waiting_for[user_id]
            await self.show_goodbye_edit_from_context(update, context)
        
        elif input_type == "payment_message_photo":
            # Получаем текущий текст
            current_data = self.db.get_payment_success_message()
            current_text = current_data['text'] if current_data else None
            
            if not current_text:
                current_text = (
                    "🎉 <b>Спасибо за покупку!</b>\n\n"
                    "💰 Ваш платеж успешно обработан!\n\n"
                    "✅ Вы получили полный доступ ко всем материалам.\n\n"
                    "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                    "🙏 Благодарим за доверие!"
                )
            
            self.db.set_payment_success_message(current_text, url)
            await update.message.reply_text("✅ Ссылка на фото для сообщения после оплаты сохранена!")
            del self.waiting_for[user_id]
            await self.show_payment_message_edit_from_context(update, context)
        
        elif input_type == "mass_photo":
            if user_id not in self.broadcast_drafts:
                self.broadcast_drafts[user_id] = {
                    "message_text": "",
                    "photo_data": None,
                    "buttons": [],
                    "scheduled_hours": None,
                    "created_at": datetime.now()
                }
            
            self.broadcast_drafts[user_id]["photo_data"] = url
            await update.message.reply_text("✅ Ссылка на фото сохранена!")
            del self.waiting_for[user_id]
            await self.show_send_all_menu_from_context(update, context)
    
    # ===== МЕТОДЫ ДЛЯ ОБРАБОТКИ ОСТАЛЬНЫХ CALLBACK =====
    
    async def handle_additional_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дополнительные обработчики callback для полноты функционала"""
        query = update.callback_query
        data = query.data
        user_id = query.from_user.id
        
        # Обработка для базовых сообщений рассылки
        if data.startswith("edit_text_"):
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
            await self.show_message_edit(update, context, message_number)
        elif data.startswith("add_button_"):
            message_number = int(data.split("_")[2])
            # Проверяем лимит кнопок
            existing_buttons = self.db.get_message_buttons(message_number)
            if len(existing_buttons) >= 3:
                await query.answer("❌ Максимум 3 кнопки на сообщение!", show_alert=True)
                return False
            # ✅ ИСПРАВЛЕНО: правильный вызов с message_number
            await self.request_text_input(update, context, "add_button", message_number=message_number)
        elif data.startswith("delete_msg_"):
            message_number = int(data.split("_")[2])
            # Подтверждение удаления
            keyboard = [
                [InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete_{message_number}")],
                [InlineKeyboardButton("❌ Отмена", callback_data=f"edit_msg_{message_number}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(
                update, context,
                f"⚠️ Вы уверены, что хотите удалить сообщение {message_number}?\n\nЭто также отменит все запланированные отправки этого сообщения.",
                reply_markup
            )
        elif data.startswith("confirm_delete_"):
            message_number = int(data.split("_")[2])
            self.db.delete_broadcast_message(message_number)
            await self.show_broadcast_menu(update, context)
        elif data == "add_message":
            # ✅ ИСПРАВЛЕНО: правильная инициализация для добавления сообщения
            user_id = update.callback_query.from_user.id
            self.waiting_for[user_id] = {
                "type": "add_message", 
                "created_at": datetime.now(), 
                "step": "text"  # ✅ ДОБАВЛЕНО: устанавливаем начальный шаг
            }
            
            await self.safe_edit_or_send_message(
                update, context,
                "✏️ Отправьте текст нового сообщения:\n\n💡 После этого мы попросим задержку для отправки.",
                InlineKeyboardMarkup([[InlineKeyboardButton("❌ Отмена", callback_data="admin_broadcast")]])
            )
        else:
            return False  # Не обработано
        
        return True  # Обработано
        
        return True  # Обработано
    
    async def handle_add_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления нового сообщения"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Сохраняем текст и запрашиваем задержку
            self.waiting_for[user_id]["text"] = text
            self.waiting_for[user_id]["step"] = "delay"
            await update.message.reply_text(
                "⏰ Теперь отправьте задержку:\n\n"
                "📝 <b>Форматы ввода:</b>\n"
                "• <code>30м</code> или <code>30 минут</code> - для минут\n"
                "• <code>2ч</code> или <code>2 часа</code> - для часов\n"
                "• <code>1.5</code> - для 1.5 часов\n"
                "• <code>0.05</code> - для 3 минут",
                parse_mode='HTML'
            )
        elif current_step == "delay":
            # Парсим задержку
            delay_hours, delay_display = self.parse_delay_input(text)
            
            if delay_hours is not None and delay_hours > 0:
                # Добавляем сообщение
                message_text = waiting_data["text"]
                new_number = self.db.add_broadcast_message(message_text, delay_hours)
                
                await update.message.reply_text(f"✅ Сообщение {new_number} добавлено с задержкой {delay_display}!")
                del self.waiting_for[user_id]
                await self.show_broadcast_menu_from_context(update, context)
            else:
                await update.message.reply_text(
                    "❌ Неверный формат! Примеры правильного ввода:\n\n"
                    "• <code>3м</code> или <code>3 минуты</code>\n"
                    "• <code>2ч</code> или <code>2 часа</code>\n"
                    "• <code>1.5</code> (для 1.5 часов)\n"
                    "• <code>0.05</code> (для 3 минут)",
                    parse_mode='HTML'
                )
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить сообщение заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    # ===== ИСПРАВЛЕННЫЙ МЕТОД handle_add_button =====
    
    async def handle_add_button(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка добавления кнопки к сообщению"""
        user_id = update.effective_user.id
        waiting_data = self.waiting_for[user_id]
        
        # ✅ ИСПРАВЛЕНИЕ: Упрощенная и правильная логика
        current_step = waiting_data.get("step", "text")
        
        if current_step == "text":
            # Шаг 1: Получаем текст кнопки
            if len(text) > 64:
                await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
                return
            
            # Сохраняем текст и переходим к URL
            self.waiting_for[user_id]["button_text"] = text
            self.waiting_for[user_id]["step"] = "url"
            
            await update.message.reply_text(
                f"✅ Текст кнопки сохранен: <b>{text}</b>\n\n"
                f"🔗 Теперь отправьте URL для кнопки:\n\n"
                f"💡 Пример: https://example.com\n"
                f"🎯 UTM метки будут добавлены автоматически!",
                parse_mode='HTML'
            )
            
        elif current_step == "url":
            # Шаг 2: Получаем URL кнопки
            if not (text.startswith("http://") or text.startswith("https://")):
                await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
                return
            
            if len(text) > 256:
                await update.message.reply_text("❌ URL слишком длинный.")
                return
            
            # Добавляем кнопку в базу данных
            message_number = waiting_data["message_number"]
            button_text = waiting_data["button_text"]
            
            # Определяем позицию
            existing_buttons = self.db.get_message_buttons(message_number)
            position = len(existing_buttons) + 1
            
            # Сохраняем кнопку в БД
            self.db.add_message_button(message_number, button_text, text, position)
            
            await update.message.reply_text(
                f"✅ Кнопка успешно добавлена!\n\n"
                f"📝 <b>Текст:</b> {button_text}\n"
                f"🔗 <b>URL:</b> {text}\n\n"
                f"🎯 <b>UTM метки будут добавлены автоматически при отправке!</b>",
                parse_mode='HTML'
            )
            
            # Очищаем состояние ожидания
            del self.waiting_for[user_id]
            
            # Возвращаемся к меню управления кнопками
            await self.show_message_buttons_from_context(update, context, message_number)
            
        else:
            # Неожиданное состояние - сбрасываем
            logger.error(f"❌ Неожиданное состояние step='{current_step}' для пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте добавить кнопку заново."
            )
            
            # Очищаем состояние
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
    
    def validate_waiting_state(self, waiting_data: dict) -> bool:
        """Проверить, что состояние ожидания валидно"""
        if not waiting_data or "type" not in waiting_data:
            return False
        
        # Проверяем, что состояние не слишком старое (30 минут)
        created_at = waiting_data.get("created_at")
        if created_at and (datetime.now() - created_at).total_seconds() > 1800:
            return False
        
        return True
    
    # ===== МЕТОДЫ ДЛЯ РЕДАКТИРОВАНИЯ СООБЩЕНИЙ =====
    
    async def show_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню редактирования конкретного сообщения"""
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
        
        keyboard.append([InlineKeyboardButton("🔘 Управление кнопками", callback_data=f"manage_buttons_{message_number}")])
        keyboard.append([InlineKeyboardButton("🗑 Удалить сообщение", callback_data=f"delete_msg_{message_number}")])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        buttons_info = ""
        if buttons:
            buttons_info = f"\n<b>Кнопки ({len(buttons)}):</b>\n"
            for i, (button_id, button_text, button_url, position) in enumerate(buttons, 1):
                buttons_info += f"{i}. {button_text} → {button_url}\n"
        
        if delay_hours < 1:
            delay_str = f"{int(delay_hours * 60)} минут"
        else:
            delay_str = f"{delay_hours} часов"
        
        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_str} после регистрации\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
            f"{buttons_info}\n\n"
            f"💡 <i>Все ссылки автоматически получают UTM метки для отслеживания.</i>"
        )
        
        await self.safe_edit_or_send_message(update, context, message_text, reply_markup)
