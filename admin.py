from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
import logging

logger = logging.getLogger(__name__)

class AdminPanel:
    def __init__(self, db, admin_chat_id):
        self.db = db
        self.admin_chat_id = admin_chat_id
        self.waiting_for = {}  # Словарь для отслеживания ожидаемого ввода
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать главное меню админа"""
        keyboard = [
            [InlineKeyboardButton("📊 Статистика", callback_data="admin_stats")],
            [InlineKeyboardButton("✉️ Управление рассылкой", callback_data="admin_broadcast")],
            [InlineKeyboardButton("👋 Приветственное сообщение", callback_data="admin_welcome")],
            [InlineKeyboardButton("📢 Отправить всем сообщение", callback_data="admin_send_all")],
            [InlineKeyboardButton("👥 Список пользователей", callback_data="admin_users")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = "🔧 <b>Админ-панель</b>\n\nВыберите действие:"
        
        if update.callback_query:
            await update.callback_query.edit_message_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                text=text,
                reply_markup=reply_markup,
                parse_mode='HTML'
            )
    
    async def show_statistics(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать статистику"""
        stats = self.db.get_user_statistics()
        
        text = (
            "📊 <b>Статистика бота</b>\n\n"
            f"👥 Всего пользователей: {stats['total_users']}\n"
            f"🆕 Новых за 24 часа: {stats['new_users_24h']}\n"
            f"✉️ Отправлено сообщений: {stats['sent_messages']}"
        )
        
        keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_broadcast_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню управления рассылкой"""
        messages = self.db.get_all_broadcast_messages()
        
        keyboard = []
        for msg_num, text, delay_hours in messages:
            button_text = f"Сообщение {msg_num} ({delay_hours}ч)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка в часах после регистрации."
        )
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_message_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE, message_number):
        """Показать меню редактирования конкретного сообщения"""
        msg_data = self.db.get_broadcast_message(message_number)
        if not msg_data:
            await update.callback_query.answer("Сообщение не найдено!", show_alert=True)
            return
        
        text, delay_hours = msg_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_delay_{message_number}")],
            [InlineKeyboardButton("« Назад", callback_data="admin_broadcast")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_hours} часов после регистрации"
        )
        
        await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def request_text_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE, input_type, message_number=None):
        """Запросить ввод текста от админа"""
        user_id = update.callback_query.from_user.id
        
        if input_type == "welcome":
            self.waiting_for[user_id] = {"type": "welcome"}
            text = "✏️ Отправьте новое приветственное сообщение:"
        elif input_type == "broadcast_text":
            self.waiting_for[user_id] = {"type": "broadcast_text", "message_number": message_number}
            text = f"✏️ Отправьте новый текст для сообщения {message_number}:"
        elif input_type == "broadcast_delay":
            self.waiting_for[user_id] = {"type": "broadcast_delay", "message_number": message_number}
            text = f"⏰ Отправьте новую задержку в часах для сообщения {message_number} (только число):"
        elif input_type == "send_all":
            self.waiting_for[user_id] = {"type": "send_all"}
            text = "📢 Отправьте сообщение, которое будет разослано всем пользователям:"
        
        keyboard = [[InlineKeyboardButton("❌ Отмена", callback_data="admin_cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_users_list(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать список пользователей"""
        users = self.db.get_all_users()
        
        if not users:
            text = "👥 <b>Список пользователей</b>\n\nПользователей пока нет."
        else:
            text = "👥 <b>Список пользователей</b>\n\n"
            for user in users[:20]:  # Показываем первых 20
                user_id, username, first_name, joined_at, is_active = user
                username_str = f"@{username}" if username else "без username"
                text += f"• {first_name} ({username_str}) - ID: {user_id}\n"
            
            if len(users) > 20:
                text += f"\n... и ещё {len(users) - 20} пользователей"
        
        keyboard = [[InlineKeyboardButton("« Назад", callback_data="admin_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text=text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик всех callback запросов админ-панели"""
        query = update.callback_query
        data = query.data
        
        if data == "admin_back":
            await self.show_main_menu(update, context)
        elif data == "admin_stats":
            await self.show_statistics(update, context)
        elif data == "admin_broadcast":
            await self.show_broadcast_menu(update, context)
        elif data == "admin_welcome":
            await self.request_text_input(update, context, "welcome")
        elif data == "admin_send_all":
            await self.request_text_input(update, context, "send_all")
        elif data == "admin_users":
            await self.show_users_list(update, context)
        elif data.startswith("edit_msg_"):
            message_number = int(data.split("_")[2])
            await self.show_message_edit(update, context, message_number)
        elif data.startswith("edit_text_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_text", message_number)
        elif data.startswith("edit_delay_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_delay", message_number)
        elif data == "admin_cancel":
            user_id = query.from_user.id
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений от админа"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            return
        
        waiting_data = self.waiting_for[user_id]
        text = update.message.text
        
        if waiting_data["type"] == "welcome":
            self.db.set_welcome_message(text)
            await update.message.reply_text("✅ Приветственное сообщение обновлено!")
            del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
            
        elif waiting_data["type"] == "broadcast_text":
            message_number = waiting_data["message_number"]
            self.db.update_broadcast_message(message_number, text=text)
            await update.message.reply_text(f"✅ Текст сообщения {message_number} обновлён!")
            del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
            
        elif waiting_data["type"] == "broadcast_delay":
            try:
                delay_hours = int(text)
                if delay_hours < 1:
                    raise ValueError("Задержка должна быть больше 0")
                
                message_number = waiting_data["message_number"]
                self.db.update_broadcast_message(message_number, delay_hours=delay_hours)
                await update.message.reply_text(f"✅ Задержка для сообщения {message_number} установлена на {delay_hours} часов!")
                del self.waiting_for[user_id]
                await self.show_main_menu(update, context)
            except ValueError:
                await update.message.reply_text("❌ Пожалуйста, введите корректное число часов (больше 0)")
                
        elif waiting_data["type"] == "send_all":
            # Отправка сообщения всем пользователям
            users = self.db.get_all_users()
            sent_count = 0
            failed_count = 0
            
            for user in users:
                user_id_to_send = user[0]
                try:
                    await context.bot.send_message(
                        chat_id=user_id_to_send,
                        text=text,
                        parse_mode='HTML'
                    )
                    sent_count += 1
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Не удалось отправить сообщение пользователю {user_id_to_send}: {e}")
            
            await update.message.reply_text(
                f"✅ Рассылка завершена!\n"
                f"Успешно отправлено: {sent_count}\n"
                f"Не удалось отправить: {failed_count}"
            )
            del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
