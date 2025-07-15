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
            [InlineKeyboardButton("😢 Прощальное сообщение", callback_data="admin_goodbye")],
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
            f"👥 Всего активных пользователей: {stats['total_users']}\n"
            f"🆕 Новых за 24 часа: {stats['new_users_24h']}\n"
            f"✉️ Отправлено сообщений: {stats['sent_messages']}\n"
            f"🚪 Отписалось пользователей: {stats['unsubscribed']}"
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
        for msg_num, text, delay_hours, photo_url in messages:
            photo_icon = "🖼" if photo_url else ""
            button_text = f"{photo_icon} Сообщение {msg_num} ({delay_hours}ч)"
            keyboard.append([InlineKeyboardButton(button_text, callback_data=f"edit_msg_{msg_num}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        text = (
            "✉️ <b>Управление рассылкой</b>\n\n"
            "Выберите сообщение для редактирования.\n"
            "В скобках указана задержка в часах после регистрации.\n"
            "🖼 - сообщение содержит фото"
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
        
        text, delay_hours, photo_url = msg_data
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data=f"edit_text_{message_number}")],
            [InlineKeyboardButton("⏰ Изменить задержку", callback_data=f"edit_delay_{message_number}")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data=f"edit_photo_{message_number}")]
        ]
        
        if photo_url:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data=f"remove_photo_{message_number}")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_broadcast")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            f"📝 <b>Сообщение {message_number}</b>\n\n"
            f"<b>Текущий текст:</b>\n{text}\n\n"
            f"<b>Задержка:</b> {delay_hours} часов после регистрации\n"
            f"<b>Фото:</b> {'Есть' if photo_url else 'Нет'}"
        )
        
        await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_welcome_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования приветственного сообщения"""
        welcome_data = self.db.get_welcome_message()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_welcome_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_welcome_photo")]
        ]
        
        if welcome_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_welcome_photo")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "👋 <b>Приветственное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{welcome_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if welcome_data['photo'] else 'Нет'}"
        )
        
        await update.callback_query.edit_message_text(
            text=message_text,
            reply_markup=reply_markup,
            parse_mode='HTML'
        )
    
    async def show_goodbye_edit(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Показать меню редактирования прощального сообщения"""
        goodbye_data = self.db.get_goodbye_message()
        
        keyboard = [
            [InlineKeyboardButton("📝 Изменить текст", callback_data="edit_goodbye_text")],
            [InlineKeyboardButton("🖼 Изменить фото", callback_data="edit_goodbye_photo")]
        ]
        
        if goodbye_data['photo']:
            keyboard.append([InlineKeyboardButton("❌ Удалить фото", callback_data="remove_goodbye_photo")])
        
        keyboard.append([InlineKeyboardButton("« Назад", callback_data="admin_back")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        message_text = (
            "😢 <b>Прощальное сообщение</b>\n\n"
            f"<b>Текущий текст:</b>\n{goodbye_data['text']}\n\n"
            f"<b>Фото:</b> {'Есть' if goodbye_data['photo'] else 'Нет'}"
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
        elif input_type == "goodbye":
            self.waiting_for[user_id] = {"type": "goodbye"}
            text = "✏️ Отправьте новое прощальное сообщение:"
        elif input_type == "broadcast_text":
            self.waiting_for[user_id] = {"type": "broadcast_text", "message_number": message_number}
            text = f"✏️ Отправьте новый текст для сообщения {message_number}:"
        elif input_type == "broadcast_delay":
            self.waiting_for[user_id] = {"type": "broadcast_delay", "message_number": message_number}
            text = f"⏰ Отправьте новую задержку в часах для сообщения {message_number} (только число):"
        elif input_type == "broadcast_photo":
            self.waiting_for[user_id] = {"type": "broadcast_photo", "message_number": message_number}
            text = f"🖼 Отправьте фото для сообщения {message_number} или ссылку на фото:"
        elif input_type == "welcome_photo":
            self.waiting_for[user_id] = {"type": "welcome_photo"}
            text = "🖼 Отправьте фото для приветственного сообщения или ссылку на фото:"
        elif input_type == "goodbye_photo":
            self.waiting_for[user_id] = {"type": "goodbye_photo"}
            text = "🖼 Отправьте фото для прощального сообщения или ссылку на фото:"
        elif input_type == "send_all":
            self.waiting_for[user_id] = {"type": "send_all"}
            text = "📢 Отправьте сообщение, которое будет разослано всем пользователям (можно с фото):"
        
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
            await self.show_welcome_edit(update, context)
        elif data == "admin_goodbye":
            await self.show_goodbye_edit(update, context)
        elif data == "edit_welcome_text":
            await self.request_text_input(update, context, "welcome")
        elif data == "edit_goodbye_text":
            await self.request_text_input(update, context, "goodbye")
        elif data == "edit_welcome_photo":
            await self.request_text_input(update, context, "welcome_photo")
        elif data == "edit_goodbye_photo":
            await self.request_text_input(update, context, "goodbye_photo")
        elif data == "remove_welcome_photo":
            self.db.set_welcome_message(self.db.get_welcome_message()['text'], photo_url="")
            await query.answer("Фото удалено!")
            await self.show_welcome_edit(update, context)
        elif data == "remove_goodbye_photo":
            self.db.set_goodbye_message(self.db.get_goodbye_message()['text'], photo_url="")
            await query.answer("Фото удалено!")
            await self.show_goodbye_edit(update, context)
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
        elif data.startswith("edit_photo_"):
            message_number = int(data.split("_")[2])
            await self.request_text_input(update, context, "broadcast_photo", message_number)
        elif data.startswith("remove_photo_"):
            message_number = int(data.split("_")[2])
            self.db.update_broadcast_message(message_number, photo_url="")
            await query.answer("Фото удалено!")
            await self.show_message_edit(update, context, message_number)
        elif data == "admin_cancel":
            user_id = query.from_user.id
            if user_id in self.waiting_for:
                del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
    
    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработчик текстовых сообщений и фото от админа"""
        user_id = update.effective_user.id
        
        if user_id not in self.waiting_for:
            return
        
        waiting_data = self.waiting_for[user_id]
        
        # Обработка фото
        if update.message.photo and waiting_data["type"] in ["broadcast_photo", "welcome_photo", "goodbye_photo", "send_all"]:
            photo_file_id = update.message.photo[-1].file_id  # Берем фото в лучшем качестве
            
            if waiting_data["type"] == "broadcast_photo":
                message_number = waiting_data["message_number"]
                self.db.update_broadcast_message(message_number, photo_url=photo_file_id)
                await update.message.reply_text(f"✅ Фото для сообщения {message_number} обновлено!")
                del self.waiting_for[user_id]
                await self.show_main_menu(update, context)
                
            elif waiting_data["type"] == "welcome_photo":
                welcome_text = self.db.get_welcome_message()['text']
                self.db.set_welcome_message(welcome_text, photo_url=photo_file_id)
                await update.message.reply_text("✅ Фото для приветственного сообщения обновлено!")
                del self.waiting_for[user_id]
                await self.show_main_menu(update, context)
                
            elif waiting_data["type"] == "goodbye_photo":
                goodbye_text = self.db.get_goodbye_message()['text']
                self.db.set_goodbye_message(goodbye_text, photo_url=photo_file_id)
                await update.message.reply_text("✅ Фото для прощального сообщения обновлено!")
                del self.waiting_for[user_id]
                await self.show_main_menu(update, context)
            
            return
        
        # Обработка текста
        text = update.message.text if update.message.text else update.message.caption
        
        if waiting_data["type"] == "welcome":
            self.db.set_welcome_message(text)
            await update.message.reply_text("✅ Приветственное сообщение обновлено!")
            del self.waiting_for[user_id]
            await self.show_main_menu(update, context)
            
        elif waiting_data["type"] == "goodbye":
            self.db.set_goodbye_message(text)
            await update.message.reply_text("✅ Прощальное сообщение обновлено!")
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
        
        elif waiting_data["type"] in ["broadcast_photo", "welcome_photo", "goodbye_photo"]:
            # Если отправили текст вместо фото - проверяем, может это ссылка
            if text and (text.startswith("http://") or text.startswith("https://")):
                if waiting_data["type"] == "broadcast_photo":
                    message_number = waiting_data["message_number"]
                    self.db.update_broadcast_message(message_number, photo_url=text)
                    await update.message.reply_text(f"✅ Ссылка на фото для сообщения {message_number} сохранена!")
                elif waiting_data["type"] == "welcome_photo":
                    welcome_text = self.db.get_welcome_message()['text']
                    self.db.set_welcome_message(welcome_text, photo_url=text)
                    await update.message.reply_text("✅ Ссылка на фото для приветственного сообщения сохранена!")
                elif waiting_data["type"] == "goodbye_photo":
                    goodbye_text = self.db.get_goodbye_message()['text']
                    self.db.set_goodbye_message(goodbye_text, photo_url=text)
                    await update.message.reply_text("✅ Ссылка на фото для прощального сообщения сохранена!")
                
                del self.waiting_for[user_id]
                await self.show_main_menu(update, context)
            else:
                await update.message.reply_text("❌ Пожалуйста, отправьте фото или ссылку на фото")
                
        elif waiting_data["type"] == "send_all":
            # Отправка сообщения всем пользователям
            users = self.db.get_all_users()
            sent_count = 0
            failed_count = 0
            
            photo_file_id = None
            if update.message.photo:
                photo_file_id = update.message.photo[-1].file_id
            
            for user in users:
                user_id_to_send = user[0]
                try:
                    if photo_file_id:
                        await context.bot.send_photo(
                            chat_id=user_id_to_send,
                            photo=photo_file_id,
                            caption=text,
                            parse_mode='HTML'
                        )
                    else:
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
