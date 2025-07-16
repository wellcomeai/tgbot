import logging
import os
import sys
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ChatJoinRequestHandler, MessageHandler, filters, ChatMemberHandler
from database import Database
from admin import AdminPanel
from scheduler import MessageScheduler
import asyncio

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

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start - только для админа"""
    user = update.effective_user
    
    # Проверяем, является ли пользователь админом
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
    else:
        # Для обычных пользователей помечаем как начавших разговор с ботом
        db.mark_user_started_bot(user.id)
        
        # Если у пользователя еще нет запланированных сообщений, планируем их
        existing_messages = db.get_user_scheduled_messages(user.id)
        if not existing_messages:
            await scheduler.schedule_user_messages(context, user.id)
        
        await update.message.reply_text(
            "👋 Привет! Теперь вы будете получать уведомления от бота.\n\n"
            "Если вы хотите получать материалы от нашего канала, "
            "пожалуйста, подайте заявку на вступление в наш канал."
        )

async def handle_join_request(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик заявок на вступление в канал"""
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
        
        # Создаем inline кнопку для согласия на получение уведомлений
        keyboard = [
            [InlineKeyboardButton("✅ Согласиться на получение уведомлений", callback_data="user_consent")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем приветственное сообщение в личку
        try:
            welcome_text = welcome_data['text'] + "\n\n💡 <b>Важно:</b> Для получения уведомлений и полезных материалов от бота, пожалуйста, нажмите кнопку ниже и дайте согласие:"
            
            if welcome_data['photo']:
                # Отправляем с фото
                await context.bot.send_photo(
                    chat_id=user.id,
                    photo=welcome_data['photo'],
                    caption=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                # Отправляем только текст
                await context.bot.send_message(
                    chat_id=user.id,
                    text=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            
            # ❌ УБИРАЕМ ОТСЮДА планирование сообщений!
            # Теперь сообщения будут планироваться только после нажатия кнопки согласия
            
        except Exception as e:
            logger.error(f"Не удалось отправить приветственное сообщение пользователю {user.id}: {e}")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке заявки от {user.id}: {e}")

async def handle_member_update(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик изменений статуса участника канала"""
    if update.my_chat_member:
        return  # Игнорируем изменения статуса самого бота
    
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
        except Exception as e:
            logger.error(f"Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # Проверяем, является ли пользователь админом
    if user_id == ADMIN_CHAT_ID:
        await query.answer()
        # Передаём обработку админ-панели
        await admin_panel.handle_callback(update, context)
    else:
        # Обработка согласия пользователя
        if query.data == "user_consent":
            # Помечаем пользователя как начавшего разговор с ботом
            db.mark_user_started_bot(user_id)
            
            # ✅ ТЕПЕРЬ ПЛАНИРУЕМ СООБЩЕНИЯ ПОСЛЕ ПОЛУЧЕНИЯ СОГЛАСИЯ
            await scheduler.schedule_user_messages(context, user_id)
            
            # Отправляем заготовленное сообщение согласия
            await query.answer("Согласие получено! ✅")
            
            # Отправляем новое сообщение вместо редактирования
            await query.message.reply_text(
                "🎉 <b>Отлично! Согласие получено!</b>\n\n"
                "📬 Теперь вы будете получать все важные уведомления и полезные материалы от нашего бота.\n\n"
                "🔔 В ближайшие дни вы получите серию образовательных сообщений, которые помогут вам максимально эффективно использовать наш сервис.\n\n"
                "💡 Если у вас возникнут вопросы - не стесняйтесь писать в любое время!\n\n"
                "Добро пожаловать в нашу команду! 🚀",
                parse_mode='HTML'
            )
            
            # Отправляем дополнительное сообщение благодарности
            await asyncio.sleep(1)  # Небольшая задержка для плавности
            await context.bot.send_message(
                chat_id=user_id,
                text="🙏 <b>Спасибо, что подписались!</b>\n\n"
                     "Мы очень рады видеть вас среди наших подписчиков.\n\n"
                     "📋 Первое полезное сообщение придет уже через <b>3 минуты</b>!\n\n"
                     "🎯 Следите за обновлениями - впереди много интересного!",
                parse_mode='HTML'
            )
        else:
            # Если обычный пользователь нажал другую кнопку
            db.mark_user_started_bot(user_id)
            # Планируем сообщения и для этого случая
            await scheduler.schedule_user_messages(context, user_id)
            await query.answer("Спасибо! Теперь вы будете получать уведомления от бота.")

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = update.effective_user.id
    
    # Проверяем, является ли пользователь админом и ожидается ли от него ввод
    if user_id == ADMIN_CHAT_ID:
        await admin_panel.handle_message(update, context)
    else:
        # Если обычный пользователь написал боту, помечаем его как начавшего разговор
        db.mark_user_started_bot(user_id)
        
        # Если у пользователя еще нет запланированных сообщений, планируем их
        existing_messages = db.get_user_scheduled_messages(user_id)
        if not existing_messages:
            await scheduler.schedule_user_messages(context, user_id)
        
        # Отправляем дружелюбный ответ
        await update.message.reply_text(
            "👋 Спасибо за сообщение!\n\n"
            "Теперь вы будете получать все важные уведомления от нашего бота.\n\n"
            "Если у вас есть вопросы - обращайтесь к администратору канала."
        )

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")

async def post_init(application: Application) -> None:
    """Инициализация после запуска"""
    logger.info("Бот успешно запущен и готов к работе!")

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
    
    logger.info("Запуск бота...")
    
    # Запускаем бота
    application.run_polling(
        drop_pending_updates=True,
        allowed_updates=Update.ALL_TYPES
    )

if __name__ == '__main__':
    main()
