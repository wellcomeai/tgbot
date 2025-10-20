import logging
import os
import sys
import asyncio
import json
from pathlib import Path
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated, Message, Chat, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ChatJoinRequestHandler, MessageHandler, filters, ChatMemberHandler
from telegram.error import Forbidden, BadRequest
from database import Database
from admin import AdminPanel
from scheduler import MessageScheduler
from aiohttp import web, ClientSession
import threading
import pytz

# Настройка логирования для Render
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Отключаем избыточные логи для чистоты
logging.getLogger('aiohttp.access').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

# ===== КОНФИГУРАЦИЯ ДЛЯ RENDER =====
BOT_TOKEN = os.environ.get('BOT_TOKEN')

# ОБНОВЛЕНО: Поддержка нескольких администраторов через запятую
admin_ids_str = os.environ.get('ADMIN_CHAT_ID', '0')
if ',' in admin_ids_str:
    ADMIN_CHAT_IDS = [int(id.strip()) for id in admin_ids_str.split(',')]
else:
    ADMIN_CHAT_IDS = [int(admin_ids_str)]

# Для обратной совместимости (если AdminPanel требует одно значение)
ADMIN_CHAT_ID = ADMIN_CHAT_IDS[0]

# Функция-помощник для проверки прав админа
def is_admin(user_id: int) -> bool:
    """Проверяет, является ли пользователь администратором"""
    return user_id in ADMIN_CHAT_IDS

CHANNEL_ID = os.environ.get('CHANNEL_ID')

# Настройки для Render
RENDER_PORT = int(os.environ.get('PORT', '10000'))
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
USE_WEBHOOK = os.environ.get('USE_WEBHOOK', 'true').lower() == 'true'

# ИСПРАВЛЕНО: Настройка для Render Disk - правильный путь
RENDER_DISK_PATH = os.environ.get('RENDER_DISK_PATH', '/data')

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не установлен!")
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

if not ADMIN_CHAT_IDS or ADMIN_CHAT_IDS[0] == 0:
    logger.error("❌ ADMIN_CHAT_ID не установлен!")
    raise ValueError("ADMIN_CHAT_ID не установлен в переменных окружения")

if not CHANNEL_ID:
    logger.error("❌ CHANNEL_ID не установлен!")
    raise ValueError("CHANNEL_ID не установлен в переменных окружения")

if USE_WEBHOOK and not WEBHOOK_URL:
    logger.error("❌ WEBHOOK_URL не установлен для режима webhook!")
    raise ValueError("WEBHOOK_URL обязателен для режима webhook на Render")

# Логирование конфигурации
logger.info(f"🚀 Конфигурация для Render:")
logger.info(f"   🌐 aiohttp порт: {RENDER_PORT}")
logger.info(f"   📱 Webhook URL: {WEBHOOK_URL}")
logger.info(f"   💾 Render Disk: {RENDER_DISK_PATH}")
logger.info(f"   👤 Admin IDs: {ADMIN_CHAT_IDS}")
logger.info(f"   📢 Channel: {CHANNEL_ID}")

# ===== ИНИЦИАЛИЗАЦИЯ КОМПОНЕНТОВ =====
# Создаём базу данных с учётом Render Disk
try:
    if RENDER_DISK_PATH:
        logger.info(f"🗄️ Используем Render Disk для базы данных: {RENDER_DISK_PATH}")
        os.environ['RENDER_DISK_PATH'] = RENDER_DISK_PATH  # Устанавливаем переменную для Database
        db = Database()
    else:
        logger.warning("⚠️ RENDER_DISK_PATH не настроен, используем локальное хранилище")
        db = Database()
    
    # Выводим информацию о базе данных
    db_info = db.get_database_info()
    logger.info(f"📊 База данных: {db_info}")
    
except Exception as e:
    logger.error(f"❌ Критическая ошибка инициализации базы данных: {e}")
    raise

# Инициализируем остальные компоненты
admin_panel = AdminPanel(db, ADMIN_CHAT_ID)
scheduler = MessageScheduler(db)

# Глобальные переменные для интеграции
bot_application = None
bot_instance = None

# ===== ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ ПЕРСОНАЛИЗАЦИИ =====
def personalize_message(text: str, user) -> str:
    """
    Заменяет переменные в тексте на данные пользователя
    
    Доступные переменные:
    - {username} - никнейм пользователя (@username) или имя
    - {first_name} - имя пользователя
    - {last_name} - фамилия пользователя
    """
    if not text:
        return text
    
    # Заменяем {username}
    username_value = f"@{user.username}" if user.username else (user.first_name or "друг")
    text = text.replace('{username}', username_value)
    
    # Заменяем {first_name}
    first_name_value = user.first_name or "друг"
    text = text.replace('{first_name}', first_name_value)
    
    # Заменяем {last_name}
    last_name_value = user.last_name or ""
    text = text.replace('{last_name}', last_name_value)
    
    return text

# ===== AIOHTTP ПРИЛОЖЕНИЕ ДЛЯ WEBHOOK'ОВ =====
app = web.Application()

async def telegram_webhook(request):
    """Обработка Telegram webhook через aiohttp"""
    try:
        # Получаем данные от Telegram
        update_data = await request.json()
        
        if not update_data:
            logger.warning("⚠️ Получен пустой Telegram webhook")
            return web.json_response({'ok': False}, status=400)
        
        logger.debug(f"📱 Получен Telegram update: {update_data.get('update_id')}")
        
        # Создаем Update объект и обрабатываем его асинхронно
        update = Update.de_json(update_data, bot_instance)
        
        # Обрабатываем update напрямую в текущем event loop
        if bot_application:
            await bot_application.process_update(update)
        
        logger.debug(f"✅ Update {update_data.get('update_id')} обработан успешно")
        
        return web.json_response({'ok': True})
        
    except Exception as e:
        logger.error(f"❌ Ошибка в Telegram webhook: {e}", exc_info=True)
        return web.json_response({'error': str(e)}, status=500)

async def payment_webhook(request):
    """Обработка Payment webhook"""
    try:
        # Получаем платежные данные
        payment_data = await request.json()
        
        if not payment_data:
            logger.warning("⚠️ Получен пустой payment webhook")
            return web.json_response({'error': 'Empty payload'}, status=400)
        
        logger.info(f"💰 Получен payment webhook: {payment_data}")
        
        # Валидация обязательных полей
        required_fields = ['user_id', 'payment_status', 'amount', 'payed_till']
        for field in required_fields:
            if field not in payment_data:
                logger.error(f"❌ Отсутствует обязательное поле: {field}")
                return web.json_response({'error': f'Missing required field: {field}'}, status=400)
        
        user_id = payment_data.get('user_id')
        payment_status = payment_data.get('payment_status')
        amount = payment_data.get('amount')
        
        # Валидация типов данных
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            logger.error(f"❌ Неверный формат user_id: {user_id}")
            return web.json_response({'error': 'Invalid user_id format'}, status=400)
        
        # Валидация формата payed_till
        payed_till = payment_data.get('payed_till')
        try:
            from datetime import datetime
            # Проверяем формат даты (ожидаем YYYY-MM-DD)
            datetime.strptime(payed_till, '%Y-%m-%d')
        except (ValueError, TypeError):
            logger.error(f"❌ Неверный формат payed_till: {payed_till}. Ожидается YYYY-MM-DD")
            return web.json_response({'error': 'Invalid payed_till format, expected YYYY-MM-DD'}, status=400)
        
        if payment_status not in ['success', 'failed', 'pending']:
            logger.error(f"❌ Неверный payment_status: {payment_status}")
            return web.json_response({'error': 'Invalid payment_status'}, status=400)
        
        # Проверяем, существует ли пользователь
        user = db.get_user(user_id)
        if not user:
            logger.error(f"❌ Пользователь {user_id} не найден")
            return web.json_response({'error': 'User not found'}, status=404)
        
        # Обрабатываем только успешные платежи
        if payment_status == 'success':
            success = await handle_successful_payment(user_id, amount, payment_data)
            
            if success:
                logger.info(f"✅ Успешно обработан платеж для пользователя {user_id}")
                return web.json_response({
                    'status': 'success',
                    'message': 'Payment processed successfully',
                    'user_id': user_id
                })
            else:
                logger.error(f"❌ Ошибка при обработке платежа для пользователя {user_id}")
                return web.json_response({'error': 'Payment processing failed'}, status=500)
        else:
            # Логируем неуспешные платежи
            db.log_payment(user_id, amount, payment_status, payment_data.get('utm_source'), payment_data.get('utm_id'))
            logger.info(f"📝 Зафиксирован неуспешный платеж: {payment_status} для пользователя {user_id}")
            return web.json_response({
                'status': 'logged',
                'message': f'Payment status {payment_status} logged'
            })
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в payment webhook: {e}", exc_info=True)
        return web.json_response({'error': 'Internal server error'}, status=500)

async def health_check(request):
    """Health check endpoint с подробной диагностикой"""
    try:
        # Получаем информацию о базе данных
        db_info = db.get_database_info()
        
        health_data = {
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'service': 'telegram_bot',
            'telegram_webhook': f'/bot{BOT_TOKEN}',
            'payment_webhook': '/webhook/payment',
            'test_expired_subscriptions': '/test/expired-subscriptions',
            'test_setup_user': '/test/setup-user',
            'bot_running': bot_instance is not None,
            'aiohttp_port': RENDER_PORT,
            'database': db_info,
            'render_disk_configured': RENDER_DISK_PATH is not None,
            'render_disk_path': RENDER_DISK_PATH,
            'webhook_url': WEBHOOK_URL
        }
        
        return web.json_response(health_data)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в health check: {e}")
        return web.json_response({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }, status=500)

async def test_expired_subscriptions(request):
    """Тестовый эндпоинт для проверки истекших подписок"""
    try:
        if bot_application and bot_application.job_queue:
            # Создаем простой контекст
            class SimpleContext:
                def __init__(self, bot):
                    self.bot = bot
                    
            context = SimpleContext(bot_instance)
            await scheduler.check_expired_subscriptions(context)
            
            return web.json_response({
                'status': 'success',
                'message': 'Expired subscriptions check completed'
            })
        else:
            return web.json_response({
                'status': 'error', 
                'message': 'Bot not ready'
            }, status=503)
            
    except Exception as e:
        logger.error(f"❌ Ошибка в test_expired_subscriptions: {e}")
        return web.json_response({
            'status': 'error',
            'error': str(e)
        }, status=500)

async def setup_test_user(request):
    """Тестовый эндпоинт для настройки тестового пользователя"""
    try:
        data = await request.json()
        user_id = data.get('user_id')
        
        if not user_id:
            return web.json_response({
                'status': 'error',
                'message': 'user_id required'
            }, status=400)
        
        user_id = int(user_id)
        
        # Получаем сегодняшнюю дату в формате YYYY-MM-DD
        from datetime import date
        today = date.today().strftime('%Y-%m-%d')
        
        # Устанавливаем пользователя как оплатившего с истекающей сегодня подпиской
        success = db.mark_user_paid(user_id, "999", "success", today)
        
        if success:
            logger.info(f"🧪 Установлены тестовые данные для пользователя {user_id}: подписка до {today}")
            return web.json_response({
                'status': 'success',
                'message': f'User {user_id} set as paid with subscription expiring today ({today})',
                'user_id': user_id,
                'payed_till': today
            })
        else:
            return web.json_response({
                'status': 'error',
                'message': 'Failed to update user'
            }, status=500)
            
    except Exception as e:
        logger.error(f"❌ Ошибка в setup_test_user: {e}")
        return web.json_response({
            'status': 'error',
            'error': str(e)
        }, status=500)

async def handle_successful_payment(user_id: int, amount: str, webhook_data: dict) -> bool:
    """Асинхронная обработка успешного платежа"""
    try:
        # Получаем payed_till из webhook данных
        payed_till = webhook_data.get('payed_till')
        logger.info(f"💰 Обрабатываем успешный платеж для пользователя {user_id}, подписка до {payed_till}")
        
        # Получаем UTM данные
        utm_source = webhook_data.get('utm_source', '')
        utm_id = webhook_data.get('utm_id', '')
        
        # Отмечаем пользователя как оплатившего
        success = db.mark_user_paid(user_id, amount, 'success', payed_till)
        if not success:
            logger.error(f"❌ Не удалось отметить пользователя {user_id} как оплатившего")
            return False
        
        # Логируем платеж
        db.log_payment(user_id, amount, 'success', utm_source, utm_id)
        
        # Отменяем оставшиеся запланированные сообщения (обычной рассылки)
        cancelled_count = db.cancel_remaining_messages(user_id)
        logger.info(f"🚫 Отменено {cancelled_count} запланированных сообщений обычной рассылки для пользователя {user_id}")
        
        # ИСПРАВЛЕНО: Планируем сообщения для оплативших пользователей
        if bot_application and bot_application.job_queue:
            # Создаем простой контекст без create_context
            class SimpleContext:
                def __init__(self, bot):
                    self.bot = bot
                    
            context = SimpleContext(bot_instance)
            paid_schedule_success = await scheduler.schedule_paid_user_messages(context, user_id)
            
            if paid_schedule_success:
                logger.info(f"✅ Запланированы сообщения для оплатившего пользователя {user_id}")
            else:
                logger.warning(f"⚠️ Не удалось запланировать сообщения для оплатившего пользователя {user_id}")
        else:
            logger.warning("⚠️ Bot application не доступен для планирования платных сообщений")
        
        # Отправляем уведомление об успешной оплате
        if bot_instance:
            await send_payment_success_notification(user_id, amount)
        else:
            logger.warning("⚠️ Bot не инициализирован, не удалось отправить уведомление")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке успешного платежа для пользователя {user_id}: {e}")
        return False

async def send_payment_success_notification(user_id: int, amount: str):
    """Отправка уведомления об успешной оплате"""
    try:
        # Получаем настроенное сообщение
        message_data = db.get_payment_success_message()
        
        if not message_data or not message_data.get('text'):
            # Сообщение по умолчанию
            message_text = (
                "🎉 <b>Спасибо за покупку!</b>\n\n"
                f"💰 Платеж на сумму {amount} руб. успешно обработан.\n\n"
                "✅ Вы получили полный доступ ко всем материалам!\n\n"
                "📚 Если у вас есть вопросы - обращайтесь к нашей поддержке.\n\n"
                "🙏 Благодарим за доверие!"
            )
            photo_url = None
        else:
            message_text = message_data.get('text', '').replace('{amount}', str(amount))
            photo_url = message_data.get('photo_url')
        
        # Отправляем сообщение
        if photo_url:
            await bot_instance.send_photo(
                chat_id=user_id,
                photo=photo_url,
                caption=message_text,
                parse_mode='HTML'
            )
        else:
            await bot_instance.send_message(
                chat_id=user_id,
                text=message_text,
                parse_mode='HTML'
            )
        
        logger.info(f"✅ Отправлено уведомление об оплате пользователю {user_id}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке уведомления об оплате пользователю {user_id}: {e}")

# ===== НАСТРОЙКА МАРШРУТОВ =====
app.router.add_post(f'/bot{BOT_TOKEN}', telegram_webhook)
app.router.add_post('/webhook/payment', payment_webhook)
app.router.add_get('/health', health_check)
app.router.add_post('/test/expired-subscriptions', test_expired_subscriptions)
app.router.add_post('/test/setup-user', setup_test_user)

# ===== КОНСТАНТЫ ДЛЯ CALLBACK ДАННЫХ =====
CALLBACK_USER_CONSENT = "user_consent"
CALLBACK_START_NOTIFICATIONS = "start_notifications"
CALLBACK_BOT_INFO = "bot_info"
CALLBACK_WHAT_WILL_RECEIVE = "what_will_receive"
CALLBACK_SETTINGS = "settings"
CALLBACK_DECLINE = "decline"

# ===== CALLBACK HANDLER =====
class CallbackHandler:
    """Класс для обработки различных запросов от пользователей"""
    
    def __init__(self, db, scheduler):
        self.db = db
        self.scheduler = scheduler
    
    async def execute_start_logic(self, user_id: int, context: ContextTypes.DEFAULT_TYPE, telegram_user) -> bool:
        """Выполнение логики команды /start"""
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
    
    async def handle_welcome_button_press(self, user_id: int, button_text: str, context: ContextTypes.DEFAULT_TYPE) -> bool:
        """Обработка нажатия на механическую кнопку приветственного сообщения"""
        try:
            logger.info(f"⌨️ Пользователь {user_id} нажал механическую кнопку: {button_text}")
            
            # Сначала выполняем стандартную логику /start
            start_success = await self.execute_start_logic(user_id, context, None)
            if not start_success:
                logger.error(f"❌ Не удалось выполнить логику /start для пользователя {user_id}")
                return False
            
            # Находим кнопку по тексту
            button_data = self.db.get_welcome_button_by_text(button_text)
            
            if not button_data:
                logger.warning(f"⚠️ Кнопка с текстом '{button_text}' не найдена")
                return True  # Возвращаем True, так как основная логика /start выполнена
            
            button_id = button_data[0]
            
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
            logger.error(f"❌ Ошибка при обработке механической кнопки для пользователя {user_id}: {e}")
            return False

# Создаем глобальный экземпляр callback handler
callback_handler = CallbackHandler(db, scheduler)

# ===== TELEGRAM BOT HANDLERS =====

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    # ОБНОВЛЕНО: Проверяем, является ли пользователь админом
    if is_admin(user.id):
        await admin_panel.show_main_menu(update, context)
        return
    
    # Для обычных пользователей выполняем логику подписки
    logger.info(f"📋 Обработка команды /start для пользователя {user.id}")
    success = await callback_handler.execute_start_logic(user.id, context, user)
    
    if success:
        # ✅ ИСПРАВЛЕНИЕ: Проверяем статус включения сообщения подтверждения
        if not db.is_success_message_enabled():
            logger.info(f"ℹ️ Сообщение подтверждения выключено, ничего не отправляем пользователю {user.id}")
            return  # Просто выходим, ничего не отправляем
        
        # Получаем настраиваемое сообщение подтверждения из базы данных
        try:
            conn = db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
            success_msg = cursor.fetchone()
            
            if not success_msg:
                # Создаем настройку по умолчанию
                default_success_message = (
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
                    "💬 Если у вас есть вопросы - не стесняйтесь писать!"
                )
                cursor.execute('INSERT INTO settings (key, value) VALUES (?, ?)', ('success_message', default_success_message))
                conn.commit()
                success_text = default_success_message
            else:
                success_text = success_msg[0]
            
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Ошибка при получении сообщения подтверждения: {e}")
            success_text = (
                "👋 <b>Добро пожаловать!</b>\n\n"
                "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                "💬 Если у вас есть вопросы - не стесняйтесь писать!"
            )
        
        # Персонализируем текст
        success_text = personalize_message(success_text, user)
        
        # Отправляем настраиваемое приветственное сообщение и убираем клавиатуру
        await update.message.reply_text(
            success_text,
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
    """Обработчик заявок на вступление в канал"""
    chat_join_request = update.chat_join_request
    user = chat_join_request.from_user
    
    try:
        # Одобряем заявку
        await chat_join_request.approve()
        logger.info(f"✅ Одобрена заявка от пользователя {user.id} (@{user.username})")
        
        # Добавляем пользователя в базу данных
        db.add_user(user.id, user.username, user.first_name)
        
        # Получаем приветственное сообщение от админа
        welcome_data = db.get_welcome_message()
        welcome_buttons = db.get_welcome_buttons()
        
        # Персонализируем текст приветствия
        welcome_text = personalize_message(welcome_data['text'], user)
        
        # Создаем клавиатуру
        reply_markup = None
        
        # Сначала проверяем, есть ли кнопки, настроенные админом
        if welcome_buttons:
            # Используем механические кнопки (кнопки клавиатуры)
            keyboard = []
            for button_id, button_text, position in welcome_buttons:
                keyboard.append([KeyboardButton(button_text)])
            
            reply_markup = ReplyKeyboardMarkup(
                keyboard, 
                resize_keyboard=True,
                one_time_keyboard=True,
                input_field_placeholder="Выберите действие..."
            )
            logger.info(f"📱 Создана клавиатура с {len(welcome_buttons)} механическими кнопками")
        else:
            # Используем стандартные кнопки
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
            logger.info("📱 Создана стандартная клавиатура")
        
        # Отправляем приветственное сообщение с персонализацией
        try:
            if welcome_data['photo']:
                sent_message = await context.bot.send_photo(
                    chat_id=user.id,
                    photo=welcome_data['photo'],
                    caption=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                sent_message = await context.bot.send_message(
                    chat_id=user.id,
                    text=welcome_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            
            logger.info(f"✅ Персонализированное приветственное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить приветственное сообщение пользователю {user.id}: пользователь не начал диалог с ботом")
            
        except Exception as e:
            logger.error(f"❌ Не удалось отправить приветственное сообщение пользователю {user.id}: {e}")
        
        logger.info(f"✅ Пользователь {user.id} добавлен в базу данных с автоматическим приветственным сообщением")
        
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
        
        # Персонализируем текст прощания
        goodbye_text = personalize_message(goodbye_data['text'], user)
        
        # Создаем инлайн-клавиатуру если есть кнопки
        reply_markup = None
        if goodbye_buttons:
            keyboard = []
            for button_id, button_text, button_url, position in goodbye_buttons:
                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Отправляем прощальное сообщение с персонализацией
        try:
            if goodbye_data['photo']:
                await context.bot.send_photo(
                    chat_id=user.id,
                    photo=goodbye_data['photo'],
                    caption=goodbye_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            else:
                await context.bot.send_message(
                    chat_id=user.id,
                    text=goodbye_text,
                    parse_mode='HTML',
                    reply_markup=reply_markup
                )
            
            logger.info(f"✅ Персонализированное прощальное сообщение отправлено пользователю {user.id}")
            
        except Forbidden as e:
            logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")
            
        except Exception as e:
            logger.error(f"❌ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")

async def handle_next_message_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия кнопки 'Следующее сообщение'"""
    query = update.callback_query
    
    if query.data.startswith("next_msg_"):
        user_id = int(query.data.split("_")[2])
        
        # Проверяем права
        if query.from_user.id != user_id:
            await query.answer("❌ Это не ваша кнопка!")
            return
            
        await query.answer("📩 Отправляем следующее сообщение...")
        
        # 📊 НОВОЕ: Логируем клик по callback кнопке
        # Нужно определить, из какого сообщения был клик
        # Для этого получаем последнее отправленное сообщение пользователю
        try:
            # Получаем последнее отправленное (но не следующее) сообщение
            conn = db._get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                SELECT message_number 
                FROM scheduled_messages 
                WHERE user_id = ? AND is_sent = 1 
                ORDER BY id DESC 
                LIMIT 1
            ''', (user_id,))
            result = cursor.fetchone()
            conn.close()
            
            if result:
                current_message_number = result[0]
                
                # Логируем клик
                db.log_button_click(
                    user_id=user_id,
                    message_number=current_message_number,
                    button_id=None,  # Для callback кнопок следующего сообщения
                    button_type='callback',
                    button_text='Следующее сообщение'
                )
                
                logger.info(f"📊 Залогирован клик по callback кнопке в сообщении {current_message_number} от пользователя {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка при логировании клика по кнопке: {e}")
        
        # Отправляем следующее сообщение
        success = await scheduler.send_next_scheduled_message(context, user_id)
        
        if not success:
            await context.bot.send_message(
                chat_id=user_id,
                text="🎉 Это было последнее сообщение! Спасибо за внимание."
            )

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки"""
    query = update.callback_query
    user_id = query.from_user.id
    callback_data = query.data
    
    # ОБНОВЛЕНО: Проверяем, является ли пользователь админом
    if is_admin(user_id):
        await query.answer()
        await admin_panel.handle_callback(update, context)
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
    
    # ОБНОВЛЕНО: Проверяем, является ли пользователь админом
    if is_admin(user_id):
        await admin_panel.handle_message(update, context)
        return
    
    # Сначала проверяем, является ли это кнопкой, настроенной админом
    welcome_buttons = db.get_welcome_buttons()
    admin_button_texts = [button_text for _, button_text, _ in welcome_buttons]
    
    if message_text in admin_button_texts:
        # Обрабатываем нажатие на кнопку, настроенную админом
        try:
            success = await callback_handler.handle_welcome_button_press(
                user_id, message_text, context
            )
            
            if success:
                # ✅ ИСПРАВЛЕНИЕ: Проверяем статус включения сообщения подтверждения
                if not db.is_success_message_enabled():
                    logger.info(f"ℹ️ Сообщение подтверждения выключено, ничего не отправляем пользователю {user_id}")
                    return  # Просто выходим, ничего не отправляем
                
                # Получаем настраиваемое сообщение подтверждения из базы данных
                try:
                    conn = db._get_connection()
                    cursor = conn.cursor()
                    cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
                    success_msg = cursor.fetchone()
                    conn.close()
                    
                    if success_msg:
                        success_text = success_msg[0]
                    else:
                        success_text = (
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
                except Exception as e:
                    logger.error(f"❌ Ошибка при получении сообщения подтверждения: {e}")
                    success_text = (
                        "👋 <b>Добро пожаловать!</b>\n\n"
                        "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                        "💬 Если у вас есть вопросы - не стесняйтесь писать!"
                    )
                
                # Персонализируем текст подтверждения
                success_text = personalize_message(success_text, update.effective_user)
                
                # Убираем клавиатуру и отправляем подтверждение
                await update.message.reply_text(
                    success_text,
                    parse_mode='HTML',
                    reply_markup=ReplyKeyboardRemove()
                )
                logger.info(f"✅ Пользователь {user_id} успешно подписан через кнопку '{message_text}'")
            else:
                await update.message.reply_text(
                    "❌ Произошла ошибка при подписке на уведомления. "
                    "Попробуйте еще раз или обратитесь к администратору.",
                    reply_markup=ReplyKeyboardRemove()
                )
                
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке кнопки '{message_text}' от пользователя {user_id}: {e}")
            await update.message.reply_text(
                "❌ Произошла техническая ошибка. Попробуйте позже.",
                reply_markup=ReplyKeyboardRemove()
            )
        return
    
    # Затем обрабатываем стандартные кнопки
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
            # ✅ ИСПРАВЛЕНИЕ: Проверяем статус включения сообщения подтверждения
            if not db.is_success_message_enabled():
                logger.info(f"ℹ️ Сообщение подтверждения выключено, ничего не отправляем пользователю {user_id}")
                return  # Просто выходим, ничего не отправляем
            
            # Получаем настраиваемое сообщение подтверждения
            try:
                conn = db._get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
                success_msg = cursor.fetchone()
                conn.close()
                
                if success_msg:
                    success_text = success_msg[0]
                else:
                    success_text = (
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
                        "💬 Не стесняйтесь писать!"
                    )
            except Exception as e:
                logger.error(f"❌ Ошибка при получении сообщения подтверждения: {e}")
                success_text = (
                    "👋 <b>Спасибо за сообщение!</b>\n\n"
                    "💬 Если у вас есть вопросы - не стесняйтесь писать!"
                )
            
            # Персонализируем текст
            success_text = personalize_message(success_text, update.effective_user)
            
            # Убираем клавиатуру
            await update.message.reply_text(
                success_text,
                parse_mode='HTML',
                reply_markup=ReplyKeyboardRemove()
            )
        else:
            await update.message.reply_text(
                "❌ Произошла ошибка при подписке на уведомления. "
                "Попробуйте еще раз или обратитесь к администратору.",
                reply_markup=ReplyKeyboardRemove()
            )

# ===== ОБРАБОТЧИКИ КНОПОК =====

async def handle_consent_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатия на кнопку согласия"""
    user_id = update.effective_user.id
    user = update.effective_user
    
    try:
        logger.info(f"🔘 Пользователь {user_id} нажал кнопку согласия")
        
        # Убеждаемся, что пользователь существует и активен
        user_exists = db.ensure_user_exists_and_active(
            user_id, 
            user.username, 
            user.first_name
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
        
        # Выполняем логику start()
        success = await callback_handler.execute_start_logic(user_id, context, user)
        
        if success:
            # ✅ ИСПРАВЛЕНИЕ: Проверяем статус включения сообщения подтверждения
            if not db.is_success_message_enabled():
                logger.info(f"ℹ️ Сообщение подтверждения выключено, ничего не отправляем пользователю {user_id}")
                return  # Просто выходим, ничего не отправляем
            
            # Получаем настраиваемое сообщение подтверждения
            try:
                conn = db._get_connection()
                cursor = conn.cursor()
                cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
                success_msg = cursor.fetchone()
                conn.close()
                
                if success_msg:
                    success_text = success_msg[0]
                else:
                    success_text = (
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
            except Exception as e:
                logger.error(f"❌ Ошибка при получении сообщения подтверждения: {e}")
                success_text = (
                    "👋 <b>Добро пожаловать!</b>\n\n"
                    "🚀 Теперь вы полноценный участник нашего сообщества!\n\n"
                    "💬 Если у вас есть вопросы - не стесняйтесь писать!"
                )
            
            # Персонализируем текст
            success_text = personalize_message(success_text, user)
            
            await update.message.reply_text(
                success_text,
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
        "🔗 <b>Отслеживание:</b>\n"
        "• Все ссылки содержат UTM метки для анализа эффективности\n"
        "• Это помогает нам улучшать контент\n\n"
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
        "🔔 <b>Все сообщения бесплатны!</b>\n"
        "🔗 <b>Все ссылки отслеживаются для улучшения качества</b>\n\n"
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
    logger.error(f"❌ Exception while handling an update: {context.error}")

async def post_init(application: Application) -> None:
    """Инициализация после запуска"""
    global bot_application, bot_instance
    bot_application = application
    bot_instance = application.bot
    
    logger.info("🚀 Бот с интегрированными webhook'ами успешно запущен!")
    logger.info(f"📱 Telegram webhook: {WEBHOOK_URL}/bot{BOT_TOKEN}")
    logger.info(f"💰 Payment webhook: {WEBHOOK_URL}/webhook/payment")
    logger.info(f"🔍 Health check: {WEBHOOK_URL}/health")
    logger.info(f"🧪 Test expired subscriptions: {WEBHOOK_URL}/test/expired-subscriptions")
    
    # Выводим информацию о базе данных
    db_info = db.get_database_info()
    logger.info(f"📊 База данных готова: {db_info}")

async def run_telegram_bot():
    """Запуск Telegram бота в отдельной задаче"""
    global bot_application, bot_instance
    
    logger.info("🚀 Запуск Telegram бота для Render с Disk...")
    
    # Создаём Telegram приложение
    application = Application.builder().token(BOT_TOKEN).build()
    bot_instance = application.bot
    bot_application = application
    
    # Добавляем обработчик инициализации
    application.post_init = post_init
    
    # ===== РЕГИСТРИРУЕМ ВСЕ ОБРАБОТЧИКИ =====
    application.add_handler(CommandHandler("start", start))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    application.add_handler(ChatMemberHandler(handle_member_update, ChatMemberHandler.CHAT_MEMBER))
    application.add_handler(CallbackQueryHandler(handle_next_message_callback, pattern=r"^next_msg_"))
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, message_handler))
    
    # Добавляем обработчик ошибок
    application.add_error_handler(error_handler)
    
    # ===== ЗАПУСКАЕМ ФОНОВЫЕ ЗАДАЧИ =====
    # Запускаем фоновую задачу для рассылки
    application.job_queue.run_repeating(
        scheduler.send_scheduled_messages,
        interval=5,  # каждые 60 секунд
        first=1  # первый запуск через 10 секунд
    )
    
    # Запускаем фоновую задачу для запланированных массовых рассылок
    application.job_queue.run_repeating(
        scheduler.send_scheduled_broadcasts,
        interval=120,  # каждые 2 минуты
        first=20  # первый запуск через 20 секунд
    )
    
    # Запускаем фоновую задачу для рассылки платных сообщений
    application.job_queue.run_repeating(
        scheduler.send_scheduled_paid_messages,
        interval=60,  # каждые 60 секунд
        first=15  # первый запуск через 15 секунд
    )

    # Запускаем фоновую задачу для запланированных массовых рассылок оплативших
    application.job_queue.run_repeating(
        scheduler.send_scheduled_paid_broadcasts,
        interval=120,  # каждые 2 минуты
        first=25  # первый запуск через 25 секунд
    )
    
    # Запускаем фоновую задачу для проверки истекших подписок (ежедневно в 12:00 МСК)
    # Переводит пользователей с истекшими подписками обратно в обычную воронку БЕЗ планирования новых сообщений
    from datetime import time
    import pytz
    
    moscow_tz = pytz.timezone('Europe/Moscow')
    moscow_noon = time(12, 0, 0)  # 12:00 по Москве
    
    application.job_queue.run_daily(
        scheduler.check_expired_subscriptions,
        time=moscow_noon,
        name="check_expired_subscriptions"
    )
    
    if USE_WEBHOOK and WEBHOOK_URL:
        # Настраиваем Telegram webhook
        webhook_path = f"/bot{BOT_TOKEN}"
        webhook_url = f"{WEBHOOK_URL}{webhook_path}"
        
        logger.info(f"📡 Настройка Telegram webhook: {webhook_url}")
        
        # Запускаем только инициализацию бота
        await application.initialize()
        await application.start()
        
        # Устанавливаем webhook
        await application.bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )
        
        # Запускаем job queue
        application.job_queue.start()
        
        logger.info("✅ Telegram бот инициализирован в webhook режиме")
        
        # Держим бота активным
        while True:
            await asyncio.sleep(60)
    else:
        logger.warning("🔄 Запуск в режиме POLLING (не рекомендуется для Render)")
        logger.warning("⚠️ Убедитесь, что установлены USE_WEBHOOK=true и WEBHOOK_URL")
        
        await application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )

def main():
    """Главная функция запуска"""
    logger.info("🌐 Запуск в режиме WEBHOOK для продакшена на Render...")
    logger.info(f"📱 Telegram webhook endpoint: /bot{BOT_TOKEN}")
    logger.info(f"💰 Payment webhook endpoint: /webhook/payment")
    logger.info(f"🔍 Health check endpoint: /health")
    logger.info(f"🧪 Test expired subscriptions endpoint: /test/expired-subscriptions")
    logger.info(f"⚙️ Test setup user endpoint: /test/setup-user")
    
    async def init_and_run():
        """Инициализация и запуск всех сервисов"""
        # Запускаем Telegram бота в фоновой задаче
        bot_task = asyncio.create_task(run_telegram_bot())
        
        # Запускаем aiohttp сервер
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', RENDER_PORT)
        await site.start()
        
        logger.info(f"🌐 aiohttp сервер запущен на порту {RENDER_PORT}")
        
        # Ожидаем завершения задач
        await bot_task
    
    # Запускаем все в едином event loop
    asyncio.run(init_and_run())

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)
    finally:
        logger.info("👋 Бот завершен")
