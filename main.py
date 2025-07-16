import logging
import os
import sys
import asyncio
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ChatJoinRequestHandler, MessageHandler, filters, ChatMemberHandler
from telegram.error import Forbidden, BadRequest
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

async def subscribe_user_to_notifications(user_id: int, context: ContextTypes.DEFAULT_TYPE, 
                                        source: str = "unknown") -> bool:
    """
    Унифицированная функция для подписки пользователя на уведомления
    
    Args:
        user_id: ID пользователя
        context: Контекст телеграм бота
        source: Источник подписки ('start', 'message', 'callback', 'join_request')
    
    Returns:
        bool: True если подписка успешна, False иначе
    """
    try:
        logger.info(f"🔄 Подписка пользователя {user_id} на уведомления (источник: {source})")
        
        # Проверяем, существует ли пользователь в БД
        user_info = db.get_user(user_id)
        if not user_info:
            logger.error(f"❌ Пользователь {user_id} не найден в базе данных")
            return False
        
        # Проверяем, есть ли уже запланированные сообщения ДО изменения статуса
        existing_messages = db.get_user_scheduled_messages(user_id)
        if existing_messages:
            logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
            # Все равно помечаем как начавшего разговор (может быть полезно для статистики)
            db.mark_user_started_bot(user_id)
            return True
        
        # Помечаем пользователя как начавшего разговор с ботом
        if not db.mark_user_started_bot(user_id):
            logger.error(f"❌ Не удалось пометить пользователя {user_id} как начавшего разговор")
            return False
        
        # Небольшая задержка для обеспечения консистентности БД
        await asyncio.sleep(0.1)
        
        # Повторно проверяем, есть ли уже запланированные сообщения (на случай race condition)
        existing_messages = db.get_user_scheduled_messages(user_id)
        if existing_messages:
            logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений (повторная проверка)")
            return True
        
        # Планируем сообщения
        success = await scheduler.schedule_user_messages(context, user_id)
        
        if success:
            logger.info(f"✅ Пользователь {user_id} успешно подписан на уведомления (источник: {source})")
            return True
        else:
            logger.error(f"❌ Не удалось запланировать сообщения для пользователя {user_id} (источник: {source})")
            return False
            
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при подписке пользователя {user_id}: {e} (источник: {source})")
        return False

async def send_additional_messages(context: ContextTypes.DEFAULT_TYPE, user_id: int, delay_text: str):
    """Отправка дополнительных сообщений после согласия"""
    try:
        logger.info(f"📤 Отправка дополнительных сообщений пользователю {user_id}")
        
        # Отправляем дополнительное сообщение благодарности
        await asyncio.sleep(1)
        await context.bot.send_message(
            chat_id=user_id,
            text=f"🙏 <b>Спасибо, что подписались!</b>\n\n"
                 f"Мы очень рады видеть вас среди наших подписчиков.\n\n"
                 f"📋 Первое полезное сообщение придет через {delay_text}!\n\n"
                 f"🎯 Следите за обновлениями - впереди много интересного!",
            parse_mode='HTML'
        )
        
        # Эмулируем команду /start - отправляем второе сообщение
        await asyncio.sleep(2)
        await context.bot.send_message(
            chat_id=user_id,
            text="🚀 <b>Добро пожаловать!</b>\n\n"
                 "Теперь вы полноценный участник нашего сообщества!\n\n"
                 "📚 Вы получите доступ к:\n"
                 "• Эксклюзивным материалам\n"
                 "• Полезным советам и инструкциям\n"
                 "• Актуальным новостям\n"
                 "• Поддержке сообщества\n\n"
                 "💬 Если у вас есть вопросы - не стесняйтесь писать!",
            parse_mode='HTML'
        )
        
        logger.info(f"✅ Дополнительные сообщения отправлены пользователю {user_id}")
        
    except Forbidden as send_error:
        logger.warning(f"⚠️ Не удалось отправить дополнительные сообщения пользователю {user_id}: {send_error}")
        logger.info(f"💡 Пользователь {user_id} получил основное сообщение. Для получения дополнительных материалов нужно написать боту /start")
        
    except Exception as send_error:
        logger.error(f"❌ Неожиданная ошибка при отправке дополнительных сообщений пользователю {user_id}: {send_error}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # Проверяем, является ли пользователь админом
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
    else:
        # Для обычных пользователей подписываем на уведомления
        success = await subscribe_user_to_notifications(user.id, context, "start")
        
        if success:
            await update.message.reply_text(
                "👋 Привет! Теперь вы будете получать уведомления от бота.\n\n"
                "Если вы хотите получать материалы от нашего канала, "
                "пожалуйста, подайте заявку на вступление в наш канал."
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору."
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
            
            logger.info(f"✅ Приветственное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить приветственное сообщение пользователю {user.id}: {e}")
            logger.info(f"💡 Пользователь {user.id} должен будет написать боту /start для получения сообщений")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке приветственного сообщения пользователю {user.id}: {e}")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке заявки от {user.id}: {e}")

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
            
            logger.info(f"✅ Прощальное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: пользователь заблокировал бота")
            
        except Exception as e:
            logger.error(f"❌ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")

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
            try:
                logger.info(f"🔘 Пользователь {user_id} нажал кнопку согласия")
                
                # 🔧 ИСПРАВЛЕНИЕ: Проверяем существующие сообщения ДО подписки
                existing_messages = db.get_user_scheduled_messages(user_id)
                if existing_messages:
                    logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
                    await query.answer("Вы уже подписаны на уведомления! ✅")
                    return
                
                # Подписываем на уведомления
                success = await subscribe_user_to_notifications(user_id, context, "callback_consent")
                
                if success:
                    # Отправляем заготовленное сообщение согласия
                    await query.answer("Согласие получено! ✅")
                    
                    # Получаем информацию о первом сообщении
                    first_message = db.get_broadcast_message(1)
                    if first_message:
                        delay_hours = first_message[1]  # delay_hours
                        if delay_hours < 1:
                            delay_text = f"<b>{int(delay_hours * 60)} минут</b>"
                        else:
                            delay_text = f"<b>{int(delay_hours)} час(ов)</b>"
                    else:
                        delay_text = "<b>скоро</b>"
                    
                    consent_text = ("🎉 <b>Отлично! Согласие получено!</b>\n\n"
                                   "📬 Теперь вы будете получать все важные уведомления и полезные материалы от нашего бота.\n\n"
                                   "🔔 В ближайшие дни вы получите серию образовательных сообщений, которые помогут вам максимально эффективно использовать наш сервис.\n\n"
                                   f"📋 Первое полезное сообщение придет через {delay_text}!\n\n"
                                   "💡 Если у вас возникнут вопросы - не стесняйтесь писать в любое время!\n\n"
                                   "🎯 Следите за обновлениями - впереди много интересного!\n\n"
                                   "Добро пожаловать в нашу команду! 🚀")
                    
                    # Проверяем, есть ли фото в исходном сообщении
                    if query.message.photo:
                        # Если есть фото, редактируем caption
                        await query.edit_message_caption(
                            caption=consent_text,
                            parse_mode='HTML'
                        )
                    else:
                        # Если нет фото, редактируем текст
                        await query.edit_message_text(
                            text=consent_text,
                            parse_mode='HTML'
                        )
                    
                    logger.info(f"✅ Основное сообщение согласия обновлено для пользователя {user_id}")
                    
                    # Отправляем дополнительные сообщения
                    await send_additional_messages(context, user_id, delay_text)
                        
                else:
                    logger.error(f"❌ Не удалось запланировать сообщения для пользователя {user_id}")
                    await query.answer("Ошибка при планировании сообщений. Попробуйте позже.", show_alert=True)
                    
            except Exception as e:
                logger.error(f"❌ Критическая ошибка при обработке согласия от пользователя {user_id}: {e}")
                await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
        else:
            # Если обычный пользователь нажал другую кнопку
            try:
                logger.info(f"🔘 Пользователь {user_id} нажал кнопку: {query.data}")
                
                # 🔧 ИСПРАВЛЕНИЕ: Проверяем существующие сообщения ДО подписки
                existing_messages = db.get_user_scheduled_messages(user_id)
                if existing_messages:
                    logger.info(f"ℹ️ Пользователь {user_id} уже имеет {len(existing_messages)} запланированных сообщений")
                    await query.answer("Вы уже подписаны на уведомления! ✅")
                    return
                
                success = await subscribe_user_to_notifications(user_id, context, "callback_other")
                
                if success:
                    await query.answer("Спасибо! Теперь вы будете получать уведомления от бота.")
                    logger.info(f"✅ Пользователь {user_id} успешно подписался на уведомления")
                else:
                    await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)
                    logger.error(f"❌ Не удалось запланировать сообщения для пользователя {user_id}")
                    
            except Exception as e:
                logger.error(f"❌ Ошибка при обработке callback от пользователя {user_id}: {e}")
                await query.answer("Произошла ошибка. Попробуйте позже.", show_alert=True)

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = update.effective_user.id
    
    # Проверяем, является ли пользователь админом и ожидается ли от него ввод
    if user_id == ADMIN_CHAT_ID:
        await admin_panel.handle_message(update, context)
    else:
        # Если обычный пользователь написал боту, подписываем на уведомления
        success = await subscribe_user_to_notifications(user_id, context, "message")
        
        if success:
            await update.message.reply_text(
                "👋 Спасибо за сообщение!\n\n"
                "Теперь вы будете получать все важные уведомления от нашего бота.\n\n"
                "Если у вас есть вопросы - обращайтесь к администратору канала."
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
