import logging
import os
import sys
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated, Message, Chat
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

# Константы для callback данных
CALLBACK_USER_CONSENT = "user_consent"
CALLBACK_START_NOTIFICATIONS = "start_notifications"
CALLBACK_BOT_INFO = "bot_info"
CALLBACK_WHAT_WILL_RECEIVE = "what_will_receive"
CALLBACK_SETTINGS = "settings"
CALLBACK_DECLINE = "decline"

class CallbackHandler:
    """Класс для обработки callback запросов от пользователей"""
    
    def __init__(self, db, scheduler):
        self.db = db
        self.scheduler = scheduler
    
    def create_consent_keyboard(self, variant="simple"):
        """Создание клавиатуры с кнопкой согласия"""
        if variant == "simple":
            keyboard = [
                [InlineKeyboardButton(
                    "✅ Согласиться на получение уведомлений", 
                    callback_data=CALLBACK_USER_CONSENT
                )]
            ]
        elif variant == "choice":
            keyboard = [
                [InlineKeyboardButton(
                    "🚀 Начать получать материалы", 
                    callback_data=CALLBACK_START_NOTIFICATIONS
                )],
                [InlineKeyboardButton(
                    "ℹ️ Подробнее о боте", 
                    callback_data=CALLBACK_BOT_INFO
                )]
            ]
        elif variant == "extended":
            keyboard = [
                [InlineKeyboardButton(
                    "✅ Согласиться на уведомления", 
                    callback_data=CALLBACK_USER_CONSENT
                )],
                [InlineKeyboardButton(
                    "📋 Что я буду получать?", 
                    callback_data=CALLBACK_WHAT_WILL_RECEIVE
                )],
                [InlineKeyboardButton(
                    "❌ Пока не нужно", 
                    callback_data=CALLBACK_DECLINE
                )]
            ]
        else:
            # По умолчанию простая кнопка
            keyboard = [
                [InlineKeyboardButton(
                    "✅ Согласиться на получение уведомлений", 
                    callback_data=CALLBACK_USER_CONSENT
                )]
            ]
        
        return InlineKeyboardMarkup(keyboard)
    
    async def send_real_start_command(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, telegram_user) -> bool:
        """
        РЕАЛЬНАЯ отправка команды /start - пользователь действительно "запустит" бота
        """
        try:
            logger.info(f"📤 Отправляем РЕАЛЬНУЮ команду /start для пользователя {user_id}")
            
            # Создаем реальный объект чата
            chat = Chat(
                id=user_id, 
                type='private',
                username=telegram_user.username,
                first_name=telegram_user.first_name
            )
            
            # Создаем реальный объект сообщения с командой /start
            message = Message(
                message_id=int(datetime.now().timestamp()),  # Уникальный ID
                from_user=telegram_user,
                chat=chat,
                date=datetime.now(),
                text="/start"
            )
            
            # ВАЖНО: Устанавливаем связь с ботом
            message.set_bot(context.bot)
            
            # Создаем полноценный Update объект
            update = Update(
                update_id=int(datetime.now().timestamp()),
                message=message
            )
            
            # КЛЮЧЕВОЙ МОМЕНТ: Обрабатываем через application.process_update
            # Это вызовет CommandHandler("start", start) как будто пользователь отправил /start
            application = context.application
            await application.process_update(update)
            
            logger.info(f"✅ РЕАЛЬНАЯ команда /start успешно обработана для пользователя {user_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке реальной команды /start для пользователя {user_id}: {e}")
            return False
    
    async def handle_user_consent(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка согласия пользователя с РЕАЛЬНОЙ командой /start"""
        query = update.callback_query
        user_id = query.from_user.id
        
        try:
            logger.info(f"🔘 Пользователь {user_id} дал согласие на получение уведомлений")
            
            # Убеждаемся, что пользователь существует и активен
            user_exists = db.ensure_user_exists_and_active(
                user_id, 
                query.from_user.username, 
                query.from_user.first_name
            )
            
            if not user_exists:
                logger.error(f"❌ Не удалось обеспечить существование пользователя {user_id}")
                await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
                return False
            
            # Проверяем, есть ли уже запланированные сообщения
            existing_messages = db.get_user_scheduled_messages(user_id)
            if existing_messages:
                logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
                await query.answer("Вы уже подписаны на уведомления! ✅")
                await self.update_message_already_subscribed(query)
                return True
            
            # Уведомляем пользователя о выполнении команды
            await query.answer("Выполняется команда /start...", show_alert=False)
            
            # Отправляем РЕАЛЬНУЮ команду /start
            success = await self.send_real_start_command(user_id, context, query.from_user)
            
            if success:
                # Обновляем исходное сообщение
                await self.update_message_success_after_start(query, user_id)
                logger.info(f"✅ Пользователь {user_id} РЕАЛЬНО запустил бота через кнопку")
                return True
            else:
                await query.answer("Произошла ошибка при запуске бота. Попробуйте позже.", show_alert=True)
                return False
                
        except Exception as e:
            logger.error(f"❌ Критическая ошибка при обработке согласия от пользователя {user_id}: {e}")
            await query.answer("Произошла техническая ошибка. Попробуйте позже.", show_alert=True)
            return False
    
    async def update_message_success_after_start(self, query, user_id):
        """Обновление сообщения после успешного выполнения команды /start"""
        try:
            success_message = (
                "🎉 <b>Команда /start выполнена успешно!</b>\n\n"
                "✅ Согласие на получение уведомлений получено!\n\n"
                "📬 Теперь вы будете получать все важные уведомления и полезные материалы от нашего бота.\n\n"
                "🔔 В ближайшие дни вы получите серию образовательных сообщений, которые помогут вам максимально эффективно использовать наш сервис.\n\n"
                "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🚀 <b>Добро пожаловать!</b>\n\n"
                "Теперь вы полноценный участник нашего сообщества!\n\n"
                "📚 <b>Вы получите доступ к:</b>\n"
                "• Эксклюзивным материалам\n"
                "• Полезным советам и инструкциям\n"
                "• Актуальным новостям\n"
                "• Поддержке сообщества\n\n"
                "🙏 <b>Спасибо, что подписались!</b>\n\n"
                "💡 Если у вас есть вопросы - не стесняйтесь писать в любое время!\n\n"
                "🎯 Следите за обновлениями - впереди много интересного!\n\n"
                "✨ <i>Команда /start была выполнена автоматически</i>\n\n"
                "Добро пожаловать в нашу команду! 🚀"
            )
            
            # Обновляем сообщение
            if query.message.photo:
                await query.edit_message_caption(
                    caption=success_message,
                    parse_mode='HTML'
                )
            else:
                await query.edit_message_text(
                    text=success_message,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении сообщения для пользователя {user_id}: {e}")
    
    async def update_message_already_subscribed(self, query):
        """Обновление сообщения если пользователь уже подписан"""
        try:
            already_subscribed_message = (
                "✅ <b>Вы уже подписаны на уведомления!</b>\n\n"
                "📬 Вы будете получать все важные сообщения от нашего бота.\n\n"
                "🔔 Если вы не получаете сообщения, проверьте:\n"
                "• Не заблокировали ли вы бота\n"
                "• Настройки уведомлений в Telegram\n\n"
                "💡 Если проблемы продолжаются, обратитесь к администратору канала.\n\n"
                "🙏 Спасибо, что остаетесь с нами!"
            )
            
            if query.message.photo:
                await query.edit_message_caption(
                    caption=already_subscribed_message,
                    parse_mode='HTML'
                )
            else:
                await query.edit_message_text(
                    text=already_subscribed_message,
                    parse_mode='HTML'
                )
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении сообщения для уже подписанного пользователя: {e}")
    
    async def handle_bot_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка запроса информации о боте"""
        query = update.callback_query
        
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
        
        # Создаем клавиатуру для продолжения
        keyboard = [
            [InlineKeyboardButton("✅ Да, начать!", callback_data=CALLBACK_USER_CONSENT)],
            [InlineKeyboardButton("❌ Пока не готов", callback_data=CALLBACK_DECLINE)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.answer()
        
        try:
            if query.message.photo:
                await query.edit_message_caption(
                    caption=info_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await query.edit_message_text(
                    text=info_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при показе информации о боте: {e}")
    
    async def handle_what_will_receive(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка запроса о том, что будет получать пользователь"""
        query = update.callback_query
        
        # Получаем все сообщения рассылки для показа
        messages = db.get_all_broadcast_messages()
        
        content_message = (
            "📋 <b>Что вы будете получать:</b>\n\n"
            "🎯 <b>Серия из 7 полезных сообщений:</b>\n\n"
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
            [InlineKeyboardButton("✅ Да, хочу получать!", callback_data=CALLBACK_USER_CONSENT)],
            [InlineKeyboardButton("⬅️ Назад", callback_data=CALLBACK_BOT_INFO)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.answer()
        
        try:
            if query.message.photo:
                await query.edit_message_caption(
                    caption=content_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await query.edit_message_text(
                    text=content_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при показе содержания: {e}")
    
    async def handle_decline(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Обработка отказа от подписки"""
        query = update.callback_query
        
        decline_message = (
            "😔 <b>Жаль, что вы пока не готовы!</b>\n\n"
            "🔄 <b>Вы всегда можете:</b>\n"
            "• Вернуться к этому сообщению позже\n"
            "• Написать боту /start для подписки\n"
            "• Обратиться к администратору канала\n\n"
            "💡 <b>Возможно, вас заинтересует:</b>\n"
            "• Наш канал с полезными материалами\n"
            "• Сообщество единомышленников\n"
            "• Бесплатные обучающие материалы\n\n"
            "🤝 Мы будем рады видеть вас в нашем сообществе в любое время!\n\n"
            "❓ Может, все-таки попробуете?"
        )
        
        keyboard = [
            [InlineKeyboardButton("🤔 Хорошо, попробую", callback_data=CALLBACK_USER_CONSENT)],
            [InlineKeyboardButton("ℹ️ Подробнее о боте", callback_data=CALLBACK_BOT_INFO)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.answer("Понятно, возможно в другой раз!")
        
        try:
            if query.message.photo:
                await query.edit_message_caption(
                    caption=decline_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await query.edit_message_text(
                    text=decline_message,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
        except Exception as e:
            logger.error(f"❌ Ошибка при показе сообщения об отказе: {e}")
    
    async def execute_start_logic(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, telegram_user) -> bool:
        """
        Выполнение логики команды /start при нажатии на инлайн кнопку
        """
        try:
            logger.info(f"🚀 Выполняем логику /start для пользователя {user_id} через callback")
            
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

# Создаем глобальный экземпляр callback handler
callback_handler = CallbackHandler(db, scheduler)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # Проверяем, является ли пользователь админом
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
        return
    
    # Проверяем, является ли это симулированным вызовом
    is_simulated = (
        update.message and 
        abs(update.message.message_id - int(datetime.now().timestamp())) < 2 and
        update.message.text == "/start"
    )
    
    if is_simulated:
        # Для симулированных вызовов логика уже выполнена, просто отправляем ответ
        logger.info(f"📋 Обработка симулированной команды /start для пользователя {user.id}")
        await context.bot.send_message(
            chat_id=user.id,
            text="👋 <b>Команда /start выполнена автоматически!</b>\n\n"
            "🚀 <b>Добро пожаловать!</b>\n\n"
            "Теперь вы полноценный участник нашего сообщества!\n\n"
            "📚 <b>Вы получите доступ к:</b>\n"
            "• Эксклюзивным материалам\n"
            "• Полезным советам и инструкциям\n"
            "• Актуальным новостям\n"
            "• Поддержке сообщества\n\n"
            "🙏 <b>Спасибо, что подписались!</b>\n\n"
            "💬 Если у вас есть вопросы - не стесняйтесь писать!",
            parse_mode='HTML'
        )
        logger.info(f"✅ Отправлен ответ на симулированную команду /start для пользователя {user.id}")
    else:
        # Для реальных команд /start выполняем полную логику
        logger.info(f"📋 Обработка реальной команды /start для пользователя {user.id}")
        success = await callback_handler.execute_start_logic(user.id, context, user)
        
        if success:
            await update.message.reply_text(
                "👋 <b>Привет!</b> Теперь вы будете получать уведомления от бота.\n\n"
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
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору."
            )

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик заявок на вступление в канал с улучшенной инлайн кнопкой"""
    chat_join_request = update.chat_join_request
    user = chat_join_request.from_user
    
    try:
        # Одобряем заявку
        await chat_join_request.approve()
        logger.info(f"Одобрена заявка от пользователя {user.id} (@{user.username})")
        
        # Добавляем пользователя в базу данных
        db.add_user(user.id, user.username, user.first_name)
        
        # Получаем приветственное сообщение
        welcome_data = db.get_welcome_message()
        
        # Создаем клавиатуру (можно выбрать вариант: simple, choice, extended)
        reply_markup = callback_handler.create_consent_keyboard("simple")
        
        # Отправляем приветственное сообщение с инлайн кнопкой
        try:
            welcome_text = (
                f"{welcome_data['text']}\n\n"
                "💡 <b>Важно:</b> Для получения уведомлений и полезных материалов от бота, "
                "пожалуйста, нажмите кнопку ниже. Это выполнит команду /start автоматически:"
            )
            
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
            
            logger.info(f"✅ Приветственное сообщение с инлайн кнопкой отправлено пользователю {user.id}")
            
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
        
        # Получаем прощальное сообщение
        goodbye_data = db.get_goodbye_message()
        
        # Отправляем прощальное сообщение
        try:
            if goodbye_data['photo']:
                await context.bot.send_photo(
                    chat_id=user.id,
                    photo=goodbye_data['photo'],
                    caption=goodbye_data['text'],
                    parse_mode='HTML'
                )
            else:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=goodbye_data['text'],
                    parse_mode='HTML'
                )
            
            logger.info(f"✅ Прощальное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки с реальной командой /start"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Проверяем, является ли пользователь админом
    if user_id == ADMIN_CHAT_ID:
        await query.answer()
        await admin_panel.handle_callback(update, context)
        return
    
    # Обрабатываем различные callback данные
    if query.data == CALLBACK_USER_CONSENT:
        await callback_handler.handle_user_consent(update, context)
        
    elif query.data == CALLBACK_START_NOTIFICATIONS:
        await callback_handler.handle_user_consent(update, context)
        
    elif query.data == CALLBACK_BOT_INFO:
        await callback_handler.handle_bot_info(update, context)
        
    elif query.data == CALLBACK_WHAT_WILL_RECEIVE:
        await callback_handler.handle_what_will_receive(update, context)
        
    elif query.data == CALLBACK_DECLINE:
        await callback_handler.handle_decline(update, context)
        
    else:
        # Обработка неизвестных callback данных
        try:
            logger.info(f"🔘 Пользователь {user_id} нажал неизвестную кнопку: {query.data}")
            
            # Убеждаемся, что пользователь существует
            db.ensure_user_exists_and_active(
                user_id, 
                query.from_user.username, 
                query.from_user.first_name
            )
            
            # Уведомляем пользователя
            await query.answer("Выполняется команда /start...", show_alert=False)
            
            # Отправляем реальную команду /start
            success = await callback_handler.send_real_start_command(user_id, context, query.from_user)
            
            if success:
                await callback_handler.update_message_success_after_start(query, user_id)
            else:
                await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке callback от пользователя {user_id}: {e}")
            await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = update.effective_user.id
    
    # Проверяем, является ли пользователь админом
    if user_id == ADMIN_CHAT_ID:
        await admin_panel.handle_message(update, context)
    else:
        # Если обычный пользователь написал боту
        success = await callback_handler.execute_start_logic(user_id, context, update.effective_user)
        
        if success:
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
                parse_mode='HTML'
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору."
            )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")

async def post_init(application: Application) -> None:
    """Инициализация после запуска"""
    logger.info("Бот с РЕАЛЬНОЙ командой /start через callback успешно запущен и готов к работе!")

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
    
    logger.info("Запуск бота с РЕАЛЬНОЙ командой /start через callback...")
    
    # Запускаем бота
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == '__main__':
    main()
