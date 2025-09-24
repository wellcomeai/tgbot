"""
Функциональность управления сообщениями продления подписки для админ-панели
"""

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class RenewalMixin:
    """Миксин для работы с сообщениями продления подписки"""
    
    async def show_renewal_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления сообщениями продления"""
        renewal_data = self.db.get_renewal_message()
        
        if not renewal_data:
            await self.show_error_message(update, context, "❌ Ошибка при получении настроек продления")
            return
        
        # Формируем информацию о текущих настройках
        text_info = renewal_data.get('text', 'Не настроено')[:100] + '...' if len(renewal_data.get('text', '')) > 100 else renewal_data.get('text', 'Не настроено')
        photo_info = "Есть" if renewal_data.get('photo_url') else "Нет"
        button_text_info = renewal_data.get('button_text', 'Не настроено')
        button_url_info = renewal_data.get('button_url', 'Не настроено')
        
        text = (
            "💰 <b>Сообщения продления подписки</b>\n\n"
            "⏰ <b>Отправляется:</b> В день истечения подписки в 12:00 МСК\n\n"
            "<b>Текущие настройки:</b>\n\n"
            f"📝 <b>Текст:</b> {text_info}\n\n"
            f"🖼 <b>Фото:</b> {photo_info}\n\n"
            f"🔘 <b>Кнопка:</b> {button_text_info}\n"
            f"🔗 <b>URL кнопки:</b> {button_url_info[:50]}{'...' if len(button_url_info) > 50 else ''}\n\n"
            "💡 <i>Все ссылки автоматически получают UTM метки для отслеживания конверсий.</i>\n\n"
            "Выберите что настроить:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="renewal_edit_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="renewal_edit_photo")],
            [InlineKeyboardButton("🔘 Настроить кнопку", callback_data="renewal_edit_button")],
        ]
        
        # Дополнительные действия
        if renewal_data.get('photo_url'):
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="renewal_remove_photo")])
        
        if renewal_data.get('button_text') and renewal_data.get('button_url'):
            keyboard.append([InlineKeyboardButton("🗑 Удалить кнопку", callback_data="renewal_remove_button")])
        
        keyboard.extend([
            [InlineKeyboardButton("📋 Предпросмотр", callback_data="renewal_preview")],
            [InlineKeyboardButton("🔄 Сбросить к стандартному", callback_data="renewal_reset")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_renewal_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать детальное меню редактирования продления"""
        renewal_data = self.db.get_renewal_message()
        
        if not renewal_data:
            await self.show_error_message(update, context, "❌ Ошибка при получении настроек продления")
            return
        
        text = (
            f"💰 <b>Редактирование сообщения продления</b>\n\n"
            f"<b>Текущий текст сообщения:</b>\n"
            f"{renewal_data.get('text', 'Не настроено')}\n\n"
            f"<b>Фото:</b> {'Установлено' if renewal_data.get('photo_url') else 'Не установлено'}\n\n"
            f"<b>Кнопка:</b>\n"
            f"Текст: {renewal_data.get('button_text', 'Не настроено')}\n"
            f"URL: {renewal_data.get('button_url', 'Не настроено')}\n\n"
            f"💡 <i>UTM метки добавляются автоматически при отправке.</i>\n\n"
            f"Выберите что изменить:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Текст сообщения", callback_data="renewal_edit_text")],
            [InlineKeyboardButton("🖼 Фото", callback_data="renewal_edit_photo")],
            [InlineKeyboardButton("📝 Текст кнопки", callback_data="renewal_edit_button_text")],
            [InlineKeyboardButton("🔗 URL кнопки", callback_data="renewal_edit_button_url")],
            [InlineKeyboardButton("« Назад", callback_data="admin_renewal")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_renewal_edit_from_context(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Отправить НОВОЕ сообщение меню редактирования продления"""
        user_id = update.effective_user.id
        
        renewal_data = self.db.get_renewal_message()
        
        if not renewal_data:
            await context.bot.send_message(chat_id=user_id, text="❌ Ошибка при получении настроек продления")
            return
        
        message_text = (
            "💰 <b>Сообщения продления подписки</b>\n\n"
            "⏰ <b>Отправляется:</b> В день истечения подписки в 12:00 МСК\n\n"
            "<b>Текущие настройки:</b>\n\n"
            f"📝 <b>Текст:</b> {renewal_data.get('text', 'Не настроено')[:100]}{'...' if len(renewal_data.get('text', '')) > 100 else ''}\n\n"
            f"🖼 <b>Фото:</b> {'Есть' if renewal_data.get('photo_url') else 'Нет'}\n\n"
            f"🔘 <b>Кнопка:</b> {renewal_data.get('button_text', 'Не настроено')}\n"
            f"🔗 <b>URL кнопки:</b> {renewal_data.get('button_url', 'Не настроено')[:50]}{'...' if len(renewal_data.get('button_url', '')) > 50 else ''}\n\n"
            "💡 <i>Все ссылки автоматически получают UTM метки для отслеживания конверсий.</i>\n\n"
            "Выберите что настроить:"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="renewal_edit_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="renewal_edit_photo")],
            [InlineKeyboardButton("🔘 Настроить кнопку", callback_data="renewal_edit_button")],
        ]
        
        # Дополнительные действия
        if renewal_data.get('photo_url'):
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="renewal_remove_photo")])
        
        if renewal_data.get('button_text') and renewal_data.get('button_url'):
            keyboard.append([InlineKeyboardButton("🗑 Удалить кнопку", callback_data="renewal_remove_button")])
        
        keyboard.extend([
            [InlineKeyboardButton("📋 Предпросмотр", callback_data="renewal_preview")],
            [InlineKeyboardButton("🔄 Сбросить к стандартному", callback_data="renewal_reset")],
            [InlineKeyboardButton("« Назад", callback_data="admin_back")]
        ])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.send_new_menu_message(context, user_id, message_text, reply_markup)
    
    async def show_renewal_button_setup(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню настройки кнопки продления"""
        renewal_data = self.db.get_renewal_message()
        
        text = (
            "🔘 <b>Настройка кнопки продления</b>\n\n"
            f"<b>Текущий текст кнопки:</b> {renewal_data.get('button_text', 'Не настроено')}\n"
            f"<b>Текущий URL:</b> {renewal_data.get('button_url', 'Не настроено')}\n\n"
            "💡 <i>Кнопка будет добавлена под сообщением о продлении с автоматическими UTM метками.</i>\n\n"
            "Что хотите изменить?"
        )
        
        keyboard = [
            [InlineKeyboardButton("📝 Текст кнопки", callback_data="renewal_edit_button_text")],
            [InlineKeyboardButton("🔗 URL кнопки", callback_data="renewal_edit_button_url")],
            [InlineKeyboardButton("« Назад", callback_data="admin_renewal")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await self.safe_edit_or_send_message(update, context, text, reply_markup)
    
    async def show_renewal_preview(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать предпросмотр сообщения продления"""
        renewal_data = self.db.get_renewal_message()
        user_id = update.callback_query.from_user.id
        
        if not renewal_data or not renewal_data.get('text'):
            await update.callback_query.answer("❌ Сначала настройте текст сообщения!", show_alert=True)
            return
        
        # Формируем превью
        preview_text = (
            "📋 <b>Предпросмотр сообщения продления</b>\n\n"
            "Так будет выглядеть сообщение для пользователей:\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        )
        
        # Отправляем превью
        await context.bot.send_message(
            chat_id=user_id,
            text=preview_text,
            parse_mode='HTML'
        )
        
        # Отправляем само сообщение как превью
        try:
            # Создаем клавиатуру если есть кнопка
            reply_markup = None
            if renewal_data.get('button_text') and renewal_data.get('button_url'):
                keyboard = [[InlineKeyboardButton(
                    renewal_data['button_text'], 
                    url=renewal_data['button_url']
                )]]
                reply_markup = InlineKeyboardMarkup(keyboard)
            
            if renewal_data.get('photo_url'):
                # Отправляем с фото
                await context.bot.send_photo(
                    chat_id=user_id,
                    photo=renewal_data['photo_url'],
                    caption=renewal_data['text'],
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                # Отправляем только текст
                await context.bot.send_message(
                    chat_id=user_id,
                    text=renewal_data['text'],
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
                
            # Отправляем заключительное сообщение
            finish_text = (
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "☝️ <b>Так выглядит ваше сообщение о продлении</b>\n\n"
                "💡 При отправке пользователям все ссылки получат UTM метки для отслеживания конверсий."
            )
            
            keyboard = [[InlineKeyboardButton("« Вернуться к настройкам", callback_data="admin_renewal")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await context.bot.send_message(
                chat_id=user_id,
                text=finish_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке предпросмотра: {e}")
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Ошибка при создании предпросмотра. Проверьте настройки.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("« Назад", callback_data="admin_renewal")
                ]])
            )
    
    # === ОБРАБОТЧИКИ ВВОДА ===
    
    async def handle_renewal_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста сообщения продления"""
        user_id = update.effective_user.id
        
        if len(text) > 4096:
            await update.message.reply_text("❌ Текст слишком длинный. Максимум 4096 символов.")
            return
        
        self.db.set_renewal_message(text=text)
        await update.message.reply_text("✅ Текст сообщения продления обновлен!")
        del self.waiting_for[user_id]
        await self.show_renewal_edit_from_context(update, context)
    
    async def handle_renewal_button_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода текста кнопки продления"""
        user_id = update.effective_user.id
        
        if len(text) > 64:
            await update.message.reply_text("❌ Текст кнопки слишком длинный. Максимум 64 символа.")
            return
        
        self.db.set_renewal_message(button_text=text)
        await update.message.reply_text("✅ Текст кнопки обновлен!")
        del self.waiting_for[user_id]
        await self.show_renewal_edit_from_context(update, context)
    
    async def handle_renewal_button_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
        """Обработка ввода URL кнопки продления"""
        user_id = update.effective_user.id
        
        if not (text.startswith("http://") or text.startswith("https://")):
            await update.message.reply_text("❌ URL должен начинаться с http:// или https://")
            return
        
        if len(text) > 256:
            await update.message.reply_text("❌ URL слишком длинный.")
            return
        
        self.db.set_renewal_message(button_url=text)
        await update.message.reply_text("✅ URL кнопки обновлен!")
        del self.waiting_for[user_id]
        await self.show_renewal_edit_from_context(update, context)
    
    # === ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ===
    
    async def handle_renewal_callbacks(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Дополнительные обработчики callback для продления"""
        query = update.callback_query
        data = query.data
        
        if data == "renewal_edit_text":
            await self.request_text_input(update, context, "renewal_text")
        
        elif data == "renewal_edit_photo":
            await self.request_text_input(update, context, "renewal_photo")
        
        elif data == "renewal_edit_button":
            await self.show_renewal_button_setup(update, context)
        
        elif data == "renewal_edit_button_text":
            await self.request_text_input(update, context, "renewal_button_text")
        
        elif data == "renewal_edit_button_url":
            await self.request_text_input(update, context, "renewal_button_url")
        
        elif data == "renewal_remove_photo":
            self.db.set_renewal_message(photo_url="")
            await update.callback_query.answer("✅ Фото удалено!")
            await self.show_renewal_menu(update, context)
        
        elif data == "renewal_remove_button":
            self.db.set_renewal_message(button_text="", button_url="")
            await update.callback_query.answer("✅ Кнопка удалена!")
            await self.show_renewal_menu(update, context)
        
        elif data == "renewal_preview":
            await self.show_renewal_preview(update, context)
        
        elif data == "renewal_reset":
            # Подтверждение сброса
            keyboard = [
                [InlineKeyboardButton("✅ Да, сбросить", callback_data="renewal_confirm_reset")],
                [InlineKeyboardButton("❌ Отмена", callback_data="admin_renewal")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await self.safe_edit_or_send_message(
                update, context,
                "⚠️ <b>Подтверждение сброса</b>\n\n"
                "Вы уверены, что хотите сбросить настройки сообщения продления к стандартным?\n\n"
                "Это действие нельзя отменить.",
                reply_markup
            )
        
        elif data == "renewal_confirm_reset":
            # Сброс к стандартным настройкам
            default_message = (
                "⏰ <b>Ваша подписка истекает сегодня!</b>\n\n"
                "💳 Чтобы продолжить получать эксклюзивные материалы, продлите подписку.\n\n"
                "✨ Не упустите возможность оставаться в курсе всех новинок!"
            )
            
            self.db.set_renewal_message(
                text=default_message,
                photo_url="",
                button_text="Продлить подписку",
                button_url=""
            )
            
            await update.callback_query.answer("✅ Настройки сброшены к стандартным!")
            await self.show_renewal_menu(update, context)
        
        else:
            return False  # Не обработано
        
        return True  # Обработано
