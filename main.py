import logging
import os
import sys
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated, Message, Chat, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ChatJoinRequestHandler, MessageHandler, filters, ChatMemberHandler
from telegram.error import Forbidden, BadRequest
from database import Database
from admin import AdminPanel
from scheduler import MessageScheduler
from datetime import datetime

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Конфигурация
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_CHAT_ID = int(os.environ.get('ADMIN_CHAT_ID', '0'))
CHANNEL_ID = os.environ.get('CHANNEL_ID')

# Проверка наличия обязательных переменных
if not BOT_TOKEN:
    logger.error("BOT_TOKEN не установлен!")
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

if ADMIN_CHAT_ID == 0:
    logger.error("ADMIN_CHAT_ID не установлен!")
    raise ValueError("ADMIN_CHAT_ID не установлен в переменных окружения")

if not CHANNEL_ID:
    logger.error("CHANNEL_ID не установлен!")
    raise ValueError("CHANNEL_ID не установлен в переменных окружения")

logger.info(f"Бот запускается с ADMIN_CHAT_ID: {ADMIN_CHAT_ID}, CHANNEL_ID: {CHANNEL_ID}")

# Создаем директорию для данных если её нет
os.makedirs('/data', exist_ok=True)

# Инициализация компонентов
db = Database('/data/bot_database.db')
admin_panel = AdminPanel(db, ADMIN_CHAT_ID)
scheduler = MessageScheduler(db)

# Константы для callback данных (для совместимости с админ-панелью)
CALLBACK_USER_CONSENT = "user_consent"
CALLBACK_START_NOTIFICATIONS = "start_notifications"
CALLBACK_BOT_INFO = "bot_info"
CALLBACK_WHAT_WILL_RECEIVE = "what_will_receive"
CALLBACK_SETTINGS = "settings"
CALLBACK_DECLINE = "decline"

class CallbackHandler:
    """Класс для обработки различных запросов от пользователей"""
    
    def __init__(self, db, scheduler):
        self.db = db
        self.scheduler = scheduler
    
    async def execute_start_logic(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, telegram_user) -> bool:
        """
        Выполнение логики команды /start
        """
        try:
            logger.info(f"🚀 Выполняем логику /start для пользователя {user_id}")
            
            # Шаг 1: Помечаем пользователя как начавшего разговор с ботом
            mark_success = db.mark_user_started_bot(user_id)
            if not mark_success:
                logger.error(f"❌ Не удалось пометить пользователя {user_id} как начавшего разговор")
                return False
            
            # Небольшая задержка для обеспечения консистентности БД
            await asyncio.sleep(0.1)
            
            # Шаг 2: Планируем сообщения рассылки
            schedule_success = await scheduler.schedule_user_messages(context, user_id)
            if not schedule_success:
                logger.error(f"❌ Не удалось запланировать сообщения для пользователя {user_id}")
                return False
            
            logger.info(f"✅ Логика /start успешно выполнена для пользователя {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при выполнении логики /start для пользователя {user_id}: {e}")
            return False
    
    async def handle_welcome_button_callback(self, user_id: int, callback_data: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """
        Обработка нажатия на кнопку приветственного сообщения
        """
        try:
            logger.info(f"🔘 Пользователь {user_id} нажал кнопку приветствия: {callback_data}")
            
            # Сначала выполняем стандартную логику /start
            start_success = await self.execute_start_logic(user_id, context, None)
            if not start_success:
                logger.error(f"❌ Не удалось выполнить логику /start для пользователя {user_id}")
                return False
            
            # Находим кнопку по callback_data
            welcome_buttons = self.db.get_welcome_buttons()
            button_id = None
            
            for btn_id, btn_text, btn_callback, position in welcome_buttons:
                if btn_callback == callback_data:
                    button_id = btn_id
                    break
            
            if not button_id:
                logger.warning(f"⚠️ Кнопка с callback_data '{callback_data}' не найдена")
                return True  # Возвращаем True, так как основная логика /start выполнена
            
            # Получаем последующие сообщения для этой кнопки
            follow_messages = self.db.get_welcome_follow_messages(button_id)
            
            if not follow_messages:
                logger.info(f"ℹ️ Нет последующих сообщений для кнопки {button_id}")
                return True
            
            # Отправляем все последующие сообщения
            for msg_id, msg_num, text, photo_url in follow_messages:
                try:
                    if photo_url:
                        await context.bot.send_photo(
                            chat_id=user_id,
                            photo=photo_url,
                            caption=text,
                            parse_mode='HTML'
                        )
                    else:
                        await context.bot.send_message(
                            chat_id=user_id,
                            text=text,
                            parse_mode='HTML'
                        )
                    
                    logger.info(f"✅ Отправлено последующее сообщение {msg_num} пользователю {user_id}")
                    
                    # Небольшая пауза между сообщениями
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при отправке последующего сообщения {msg_num} пользователю {user_id}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке кнопки приветствия для пользователя {user_id}: {e}")
            return False

# Создаем глобальный экземпляр callback handler
callback_handler = CallbackHandler(db, scheduler)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start - теперь работает с реальными командами"""
    user = update.effective_user
    
    # Проверяем, является ли пользователь админом
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
        return
    
    # Для обычных пользователей выполняем логику подписки
    logger.info(f"📋 Обработка команды /start для пользователя {user.id}")
    success = await callback_handler.execute_start_logic(user.id, context, user)
    
    if success:
        # Отправляем приветственное сообщение /start и убираем клавиатуру
        await update.message.reply_text(
            "👋 <b>Команда /start выполнена!</b>\n\n"
            "🚀 <b>Добро пожаловать!</b>\n\n"
            "Теперь вы полноценный участник нашего сообщества!\n\n"
            "📚 <b>Вы получите доступ к:</b>\n"
            "• Эксклюзивным материалам\n"
            "• Полезным советам и инструкциям\n"
            "• Актуальным новостям\n"
            "• Поддержке сообщества\n\n"
            "🙏 <b>Спасибо, что подписались!</b>\n\n"
            "Если вы хотите получать материалы от нашего канала, "
            "пожалуйста, подайте заявку на вступление в наш канал.\n\n"
            "💬 Если у вас есть вопросы - не стесняйтесь писать!",
            parse_mode='HTML',
            reply_markup=ReplyKeyboardRemove()
        )
        logger.info(f"✅ Команда /start успешно выполнена для пользователя {user.id}")
    else:
        await update.message.reply_text(
            "❌ Произошла ошибка при подписке на уведомления. "
            "Попробуйте еще раз или обратитесь к администратору.",
            reply_markup=ReplyKeyboardRemove()
        )

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик заявок на вступление в канал с кнопками из админ-панели"""
    chat_join_request = update.chat_join_request
    user = chat_join_request.from_user
    
    try:
        # Одобряем заявку
        await chat_join_request.approve()
        logger.info(f"Одобрена заявка от пользователя {user.id} (@{user.username})")
        
        # Добавляем пользователя в базу данных
        db.add_user(user.id, user.username, user.first_name)
        
        # Получаем приветственное сообщение и кнопки
        welcome_data = db.get_welcome_message()
        welcome_buttons = db.get_welcome_buttons()
        
        # Создаем клавиатуру
        reply_markup = None
        
        if welcome_buttons:
            # Если админ настроил кнопки - используем ТОЛЬКО их
            keyboard = []
            for button_id, button_text, callback_data, position in welcome_buttons:
                keyboard.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.info(f"✅ Используем {len(welcome_buttons)} кнопок, настроенных админом")
        else:
            # Если админ не настроил кнопки, используем стандартную клавиатуру
            keyboard = [
                [KeyboardButton("✅ Согласиться на получение уведомлений")],
                [KeyboardButton("📋 Что я буду получать?")],
                [KeyboardButton("ℹ️ Подробнее о боте")]
            ]
            reply_markup = ReplyKeyboardMarkup(
                keyboard, 
                resize_keyboard=True,
                one_time_keyboard=True,
                input_field_placeholder="Выберите действие..."
            )
            logger.info("✅ Используем стандартные кнопки (админ не настроил свои)")
        
        # Отправляем приветственное сообщение
        try:
            # Используем текст сообщения как есть, без дополнительных подсказок
            welcome_text = welcome_data['text']
            
            if welcome_data['photo']:
                await context.bot.send_photo(
                    chat_id=user.id,
                    photo=welcome_data['photo'],
                    caption=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            
            logger.info(f"✅ Приветственное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить приветственное сообщение пользователю {user.id}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке приветственного сообщения пользователю {user.id}: {e}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке заявки от {user.id}: {e}")

async def handle_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик изменений статуса участника канала"""
    if update.my_chat_member:
        return
    
    if not update.chat_member:
        return
    
    # Проверяем, что это наш канал
    if str(update.chat_member.chat.id) != str(CHANNEL_ID) and update.chat_member.chat.username != CHANNEL_ID.replace('@', ''):
        return
    
    old_status = update.chat_member.old_chat_member.status
    new_status = update.chat_member.new_chat_member.status
    user = update.chat_member.new_chat_member.user
    
    logger.info(f"Изменение статуса пользователя {user.id}: {old_status} -> {new_status}")
    
    # Если пользователь покинул канал
    if old_status in ["member", "administrator", "creator"] and new_status in ["left", "kicked"]:
        logger.info(f"Пользователь {user.id} (@{user.username}) покинул канал")
        
        # Деактивируем пользователя
        db.deactivate_user(user.id)
        
        # Отменяем запланированные сообщения
        db.cancel_user_messages(user.id)
        
        # Получаем прощальное сообщение и кнопки
        goodbye_data = db.get_goodbye_message()
        goodbye_buttons = db.get_goodbye_buttons()
        
        # Создаем инлайн-клавиатуру если есть кнопки
        reply_markup = None
        if goodbye_buttons:
            keyboard = []
            for button_id, button_text, button_url, position in goodbye_buttons:
                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем прощальное сообщение
        try:
            if goodbye_data['photo']:
                await context.bot.send_photo(
                    chat_id=user.id,
                    photo=goodbye_data['photo'],
                    caption=goodbye_data['text'],
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=goodbye_data['text'],
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            
            logger.info(f"✅ Прощальное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки"""
    query = update.callback_query
    user_id = query.from_user.id
    callback_data = query.data
    
    # Проверяем, является ли пользователь админом
    if user_id == ADMIN_CHAT_ID:
        await query.answer()
        await admin_panel.handle_callback(update, context)
        return
    
    # Проверяем, является ли это кнопкой приветственного сообщения
    welcome_buttons = db.get_welcome_buttons()
    welcome_callbacks = [btn_callback for _, _, btn_callback, _ in welcome_buttons]
    
    if callback_data in welcome_callbacks:
        await query.answer("⏳ Обрабатываем ваш запрос...")
        
        try:
            # Обрабатываем нажатие на кнопку приветствия
            success = await callback_handler.handle_welcome_button_callback(
                user_id, callback_data, context
            )
            
            if success:
                # Убираем клавиатуру и отправляем подтверждение
                await query.edit_message_reply_markup(reply_markup=None)
                
                await context.bot.send_message(
                    chat_id=user_id,
                    text="👋 <b>Добро пожаловать!</b>\n\n"
                    "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                    "📚 <b>Вы получите доступ к:</b>\n"
                    "• Эксклюзивным материалам\n"
                    "• Полезным советам и инструкциям\n"
                    "• Актуальным новостям\n"
                    "• Поддержке сообщества\n\n"
                    "🙏 <b>Спасибо, что подписались!</b>\n\n"
                    "💬 Если у вас есть вопросы - не стесняйтесь писать!",
                    parse_mode='HTML'
                )
                logger.info(f"✅ Пользователь {user_id} успешно подписан через кнопку приветствия")
            else:
                await context.bot.send_message(
                    chat_id=user_id,
                    text="❌ Произошла ошибка при подписке на уведомления. "
                    "Попробуйте еще раз или обратитесь к администратору."
                )
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке кнопки приветствия от пользователя {user_id}: {e}")
            await context.bot.send_message(
                chat_id=user_id,
                text="❌ Произошла техническая ошибка. Попробуйте позже."
            )
        
        return
    
    # Для остальных callback данных (старые кнопки)
    if callback_data in [CALLBACK_USER_CONSENT, CALLBACK_START_NOTIFICATIONS]:
        await query.answer(
            "Пожалуйста, используйте кнопки клавиатуры для взаимодействия с ботом.",
            show_alert=True
        )
        
        # Отправляем новое сообщение с обычными кнопками
        keyboard = [
            [KeyboardButton("✅ Согласиться на получение уведомлений")],
            [KeyboardButton("📋 Что я буду получать?")],
            [KeyboardButton("ℹ️ Подробнее о боте")]
        ]
        reply_markup = ReplyKeyboardMarkup(
            keyboard, 
            resize_keyboard=True,
            one_time_keyboard=True,
            input_field_placeholder="Выберите действие..."
        )
        
        await context.bot.send_message(
            chat_id=user_id,
            text="👇 <b>Выберите действие:</b>\n\n"
            "Что вас интересует?",
            parse_mode='HTML',
            reply_markup=reply_markup
        )
        
    elif callback_data in [CALLBACK_BOT_INFO, CALLBACK_WHAT_WILL_RECEIVE, CALLBACK_DECLINE]:
        await query.answer(
            "Пожалуйста, используйте кнопки клавиатуры для взаимодействия с ботом.",
            show_alert=True
        )
        
    else:
        # Для неизвестных callback данных
        await query.answer(
            "Эта кнопка больше не активна. Используйте кнопки клавиатуры.",
            show_alert=True
        )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений и нажатий на обычные кнопки"""
    user_id = update.effective_user.id
    message_text = update.message.text
    
    # Проверяем, является ли пользователь админом
    if user_id == ADMIN_CHAT_ID:
        await admin_panel.handle_message(update, context)
        return
    
    # Обрабатываем нажатия на обычные кнопки
    if message_text == "✅ Согласиться на получение уведомлений":
        await handle_consent_button(update, context)
        return
    
    elif message_text == "📋 Что я буду получать?":
        await handle_what_will_receive_button(update, context)
        return
    
    elif message_text == "ℹ️ Подробнее о боте":
        await handle_bot_info_button(update, context)
        return
    
    elif message_text == "🔙 Назад к выбору":
        await handle_back_to_menu(update, context)
        return
    
    # Если это обычное сообщение от пользователя
    else:
        success = await callback_handler.execute_start_logic(user_id, context, update.effective_user)
        
        if success:
            # Убираем клавиатуру
            await update.message.reply_text(
                "👋 <b>Спасибо за сообщение!</b>\n\n"
                "Теперь вы будете получать все важные уведомления от нашего бота.\n\n"
                "🚀 <b>Добро пожаловать!</b>\n\n"
                "Теперь вы полноценный участник нашего сообщества!\n\n"
                "📚 <b>Вы получите доступ к:</b>\n"
                "• Эксклюзивным материалам\n"
                "• Полезным советам и инструкциям\n"
                "• Актуальным новостям\n"
                "• Поддержке сообщества\n\n"
                "Если у вас есть вопросы - обращайтесь к администратору канала.\n\n"
                "💬 Не стесняйтесь писать!",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору.",
                reply_markup=ReplyKeyboardRemove()
            )

async def handle_consent_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на кнопку согласия - ПРОСТОЕ РЕШЕНИЕ"""
    user_id = update.effective_user.id
    
    try:
        logger.info(f"🔘 Пользователь {user_id} нажал кнопку согласия")
        
        # Убеждаемся, что пользователь существует и активен
        user_exists = db.ensure_user_exists_and_active(
            user_id, 
            update.effective_user.username, 
            update.effective_user.first_name
        )
        
        if not user_exists:
            logger.error(f"❌ Не удалось обеспечить существование пользователя {user_id}")
            await update.message.reply_text(
                "❌ Произошла ошибка. Попробуйте позже.",
                reply_markup=ReplyKeyboardRemove()
            )
            return
        
        # Проверяем, есть ли уже запланированные сообщения
        existing_messages = db.get_user_scheduled_messages(user_id)
        if existing_messages:
            logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
            await update.message.reply_text(
                "✅ <b>Вы уже подписаны на уведомления!</b>\n\n"
                "📬 Вы будете получать все важные сообщения от нашего бота.\n\n"
                "🔔 Если вы не получаете сообщения, проверьте:\n"
                "• Не заблокировали ли вы бота\n"
                "• Настройки уведомлений в Telegram\n\n"
                "💡 Если проблемы продолжаются, обратитесь к администратору канала.\n\n"
                "🙏 Спасибо, что остаетесь с нами!",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
            return
        
        # ПРОСТОЕ РЕШЕНИЕ: Просто выполняем логику start()
        success = await callback_handler.execute_start_logic(user_id, context, update.effective_user)
        
        if success:
            await update.message.reply_text(
                "👋 <b>Добро пожаловать!</b>\n\n"
                "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                "📚 <b>Вы получите доступ к:</b>\n"
                "• Эксклюзивным материалам\n"
                "• Полезным советам и инструкциям\n"
                "• Актуальным новостям\n"
                "• Поддержке сообщества\n\n"
                "🙏 <b>Спасибо, что подписались!</b>\n\n"
                "💬 Если у вас есть вопросы - не стесняйтесь писать!",
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
            logger.info(f"✅ Пользователь {user_id} успешно подписан")
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору.",
                reply_markup=ReplyKeyboardRemove()
            )
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке кнопки согласия от пользователя {user_id}: {e}")
        await update.message.reply_text(
            "❌ Произошла техническая ошибка. Попробуйте позже.",
            reply_markup=ReplyKeyboardRemove()
        )

async def handle_bot_info_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на кнопку информации о боте"""
    user_id = update.effective_user.id
    
    info_message = (
        "🤖 <b>Информация о боте</b>\n\n"
        "📋 <b>Что делает бот:</b>\n"
        "• Отправляет полезные материалы\n"
        "• Предоставляет эксклюзивный контент\n"
        "• Уведомляет о новостях канала\n"
        "• Помогает в обучении\n\n"
        "⏰ <b>Как часто приходят сообщения:</b>\n"
        "• Первое сообщение через 3 минуты\n"
        "• Затем с интервалом 4-24 часа\n"
        "• Всего 7 полезных сообщений\n\n"
        "🛡️ <b>Конфиденциальность:</b>\n"
        "• Мы не передаем ваши данные третьим лицам\n"
        "• Вы можете отписаться в любое время\n\n"
        "❓ Готовы начать получать полезные материалы?"
    )
    
    keyboard = [
        [KeyboardButton("✅ Согласиться на получение уведомлений")],
        [KeyboardButton("🔙 Назад к выбору")]
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard, 
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await update.message.reply_text(
        info_message,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def handle_what_will_receive_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на кнопку о содержимом"""
    user_id = update.effective_user.id
    
    # Получаем все сообщения рассылки для показа
    messages = db.get_all_broadcast_messages()
    
    content_message = (
        "📋 <b>Что вы будете получать:</b>\n\n"
        "🎯 <b>Серия из полезных сообщений:</b>\n\n"
    )
    
    for i, (message_number, text, delay_hours, photo_url) in enumerate(messages, 1):
        if delay_hours < 1:
            delay_text = f"{int(delay_hours * 60)} минут"
        else:
            delay_text = f"{int(delay_hours)} час(ов)"
        
        # Показываем краткое описание каждого сообщения
        short_text = text[:50] + "..." if len(text) > 50 else text
        content_message += f"{i}. {short_text} (через {delay_text})\n"
    
    content_message += (
        "\n💡 <b>Каждое сообщение содержит:</b>\n"
        "• Практические советы\n"
        "• Полезные инструкции\n"
        "• Эксклюзивные материалы\n"
        "• Ответы на частые вопросы\n\n"
        "🔔 <b>Все сообщения бесплатны!</b>\n\n"
        "Готовы начать обучение?"
    )
    
    keyboard = [
        [KeyboardButton("✅ Согласиться на получение уведомлений")],
        [KeyboardButton("🔙 Назад к выбору")]
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard, 
        resize_keyboard=True,
        one_time_keyboard=True
    )
    
    await update.message.reply_text(
        content_message,
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def handle_back_to_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопки возврата в меню"""
    keyboard = [
        [KeyboardButton("✅ Согласиться на получение уведомлений")],
        [KeyboardButton("📋 Что я буду получать?")],
        [KeyboardButton("ℹ️ Подробнее о боте")]
    ]
    reply_markup = ReplyKeyboardMarkup(
        keyboard, 
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )
    
    await update.message.reply_text(
        "👇 <b>Выберите действие:</b>\n\n"
        "Что вас интересует?",
        parse_mode='HTML',
        reply_markup=reply_markup
    )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")

async def post_init(application: Application) -> None:
    """Инициализация после запуска"""
    logger.info("Бот с поддержкой кнопок приветствия и прощания успешно запущен и готов к работе!")

def main():
    """Главная функция запуска бота"""
    # Создаём приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчик инициализации
    application.post_init = post_init
    
    # Регистрируем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, message_handler))
    
    # Добавляем обработчик ошибок
    application.add_error_handler(error_handler)
    
    # Запускаем фоновую задачу для рассылки
    application.job_queue.run_repeating(
        scheduler.send_scheduled_messages,
        interval=60,  # каждые 60 секунд
        first=10  # первый запуск через 10 секунд
    )
    
    # Запускаем фоновую задачу для запланированных массовых рассылок
    application.job_queue.run_repeating(
        scheduler.send_scheduled_broadcasts,
        interval=120,  # каждые 2 минуты
        first=20  # первый запуск через 20 секунд
    )
    
    logger.info("Запуск бота с поддержкой кнопок приветствия и запланированных рассылок...")
    
    # Запускаем бота
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == '__main__':
    main()
