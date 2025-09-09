import logging
import os
import sys
import asyncio
import json
import threading
import queue
import time
from pathlib import Path
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ChatJoinRequest, ChatMemberUpdated, Message, Chat, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove, Bot
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, ChatJoinRequestHandler, MessageHandler, filters, ChatMemberHandler
from telegram.error import Forbidden, BadRequest
from telegram.constants import ChatMemberStatus
from database import Database
from admin import AdminPanel
from scheduler import MessageScheduler
from flask import Flask, request, jsonify
import concurrent.futures
import utm_utils

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
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)
logging.getLogger('httpcore').setLevel(logging.WARNING)

# ===== КОНФИГУРАЦИЯ ДЛЯ RENDER =====
BOT_TOKEN = os.environ.get('BOT_TOKEN')
ADMIN_CHAT_ID = int(os.environ.get('ADMIN_CHAT_ID', '0'))
CHANNEL_ID = os.environ.get('CHANNEL_ID')

# Настройки для Render
RENDER_PORT = int(os.environ.get('PORT', '10000'))
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
USE_WEBHOOK = os.environ.get('USE_WEBHOOK', 'true').lower() == 'true'
RENDER_DISK_PATH = os.environ.get('RENDER_DISK_PATH', '/data')

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("❌ BOT_TOKEN не установлен!")
    raise ValueError("BOT_TOKEN не установлен в переменных окружения")

if ADMIN_CHAT_ID == 0:
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
logger.info(f"   🌐 Flask порт: {RENDER_PORT}")
logger.info(f"   📱 Webhook URL: {WEBHOOK_URL}")
logger.info(f"   💾 Render Disk: {RENDER_DISK_PATH}")
logger.info(f"   👤 Admin ID: {ADMIN_CHAT_ID}")
logger.info(f"   📢 Channel: {CHANNEL_ID}")

# ===== ИНИЦИАЛИЗАЦИЯ КОМПОНЕНТОВ =====
try:
    if RENDER_DISK_PATH:
        logger.info(f"🗄️ Используем Render Disk для базы данных: {RENDER_DISK_PATH}")
        os.environ['RENDER_DISK_PATH'] = RENDER_DISK_PATH
        db = Database()
    else:
        logger.warning("⚠️ RENDER_DISK_PATH не настроен, используем локальное хранилище")
        db = Database()
    
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

# ===== ОПТИМИЗИРОВАННАЯ WEBHOOK ОБРАБОТКА =====
webhook_loop = None
webhook_thread = None
webhook_queue = queue.Queue()

def init_webhook_processor():
    """Инициализация обработчика webhook в отдельном потоке с долгоживущим event loop"""
    global webhook_loop, webhook_thread
    
    def run_webhook_loop():
        global webhook_loop
        webhook_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(webhook_loop)
        logger.info("🔄 Webhook event loop инициализирован")
        webhook_loop.run_forever()
    
    webhook_thread = threading.Thread(target=run_webhook_loop, daemon=True)
    webhook_thread.start()
    
    # Ждем инициализации
    time.sleep(0.2)
    logger.info("✅ Webhook processor готов")

def process_webhook_updates():
    """Обработчик очереди webhook updates с оптимизированной производительностью"""
    processed_count = 0
    
    while True:
        try:
            if not webhook_queue.empty():
                update_data = webhook_queue.get(timeout=1)
                
                if webhook_loop and not webhook_loop.is_closed():
                    try:
                        update = Update.de_json(update_data, bot_instance)
                        
                        # Асинхронная обработка без ожидания результата
                        future = asyncio.run_coroutine_threadsafe(
                            bot_application.process_update(update), 
                            webhook_loop
                        )
                        
                        processed_count += 1
                        if processed_count % 100 == 0:
                            logger.info(f"📊 Обработано webhook updates: {processed_count}")
                            
                    except Exception as e:
                        logger.error(f"❌ Ошибка обработки update {update_data.get('update_id', 'unknown')}: {e}")
                else:
                    logger.error("❌ Webhook loop недоступен")
                    time.sleep(1)
            else:
                time.sleep(0.1)  # Короткая пауза если очередь пуста
                
        except queue.Empty:
            continue
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в process_webhook_updates: {e}")
            time.sleep(1)

# ===== FLASK ПРИЛОЖЕНИЕ С ОПТИМИЗИРОВАННЫМИ WEBHOOK'АМИ =====
flask_app = Flask(__name__)

@flask_app.route(f'/bot{BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    """🚀 ОПТИМИЗИРОВАННЫЙ Telegram webhook handler без создания event loop"""
    try:
        update_data = request.get_json()
        
        if not update_data:
            logger.warning("⚠️ Получен пустой Telegram webhook")
            return jsonify({'ok': False}), 400
        
        update_id = update_data.get('update_id', 'unknown')
        logger.debug(f"📱 Получен Telegram update: {update_id}")
        
        # Просто добавляем в очередь - без создания event loop
        webhook_queue.put(update_data)
        
        return jsonify({'ok': True})
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в Telegram webhook: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@flask_app.route('/webhook/payment', methods=['POST'])
def payment_webhook():
    """💰 УЛУЧШЕННАЯ обработка Payment webhook с validation"""
    try:
        payment_data = request.get_json()
        
        if not payment_data:
            logger.warning("⚠️ Получен пустой payment webhook")
            return jsonify({'error': 'Empty payload'}), 400
        
        logger.info(f"💰 Получен payment webhook: {payment_data}")
        
        # Валидация обязательных полей
        required_fields = ['user_id', 'payment_status', 'amount']
        for field in required_fields:
            if field not in payment_data:
                logger.error(f"❌ Отсутствует обязательное поле: {field}")
                return jsonify({'error': f'Missing required field: {field}'}), 400
        
        user_id = payment_data.get('user_id')
        payment_status = payment_data.get('payment_status')
        amount = payment_data.get('amount')
        
        # Валидация типов данных
        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            logger.error(f"❌ Неверный формат user_id: {user_id}")
            return jsonify({'error': 'Invalid user_id format'}), 400
        
        if payment_status not in ['success', 'failed', 'pending']:
            logger.error(f"❌ Неверный payment_status: {payment_status}")
            return jsonify({'error': 'Invalid payment_status'}), 400
        
        # Проверяем, существует ли пользователь
        user = db.get_user(user_id)
        if not user:
            logger.error(f"❌ Пользователь {user_id} не найден")
            return jsonify({'error': 'User not found'}), 404
        
        # Обрабатываем только успешные платежи
        if payment_status == 'success':
            # Добавляем в очередь webhook для асинхронной обработки
            payment_webhook_data = {
                'type': 'payment_success',
                'user_id': user_id,
                'amount': amount,
                'webhook_data': payment_data
            }
            webhook_queue.put(payment_webhook_data)
            
            logger.info(f"✅ Платеж добавлен в очередь обработки для пользователя {user_id}")
            return jsonify({
                'status': 'queued',
                'message': 'Payment queued for processing',
                'user_id': user_id
            }), 200
        else:
            # Логируем неуспешные платежи
            db.log_payment(user_id, amount, payment_status, payment_data.get('utm_source'), payment_data.get('utm_id'))
            logger.info(f"📝 Зафиксирован неуспешный платеж: {payment_status} для пользователя {user_id}")
            return jsonify({
                'status': 'logged',
                'message': f'Payment status {payment_status} logged'
            }), 200
    
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в payment webhook: {e}", exc_info=True)
        return jsonify({'error': 'Internal server error'}), 500

@flask_app.route('/health', methods=['GET'])
def health_check():
    """🏥 Health check endpoint с подробной диагностикой"""
    try:
        db_info = db.get_database_info()
        
        health_data = {
            'status': 'ok',
            'timestamp': datetime.now().isoformat(),
            'service': 'telegram_bot',
            'telegram_webhook': f'/bot{BOT_TOKEN}',
            'payment_webhook': '/webhook/payment',
            'bot_running': bot_instance is not None,
            'flask_port': RENDER_PORT,
            'database': db_info,
            'render_disk_configured': RENDER_DISK_PATH is not None,
            'render_disk_path': RENDER_DISK_PATH,
            'webhook_url': WEBHOOK_URL,
            'webhook_queue_size': webhook_queue.qsize(),
            'webhook_loop_running': webhook_loop is not None and not webhook_loop.is_closed() if webhook_loop else False
        }
        
        return jsonify(health_data)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в health check: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

# ===== ОБРАБОТКА ПЛАТЕЖЕЙ =====
async def handle_successful_payment(user_id: int, amount: str, webhook_data: dict) -> bool:
    """Асинхронная обработка успешного платежа"""
    try:
        logger.info(f"💰 Обрабатываем успешный платеж для пользователя {user_id}")
        
        utm_source = webhook_data.get('utm_source', '')
        utm_id = webhook_data.get('utm_id', '')
        
        # Отмечаем пользователя как оплатившего
        success = db.mark_user_paid(user_id, amount, 'success')
        if not success:
            logger.error(f"❌ Не удалось отметить пользователя {user_id} как оплатившего")
            return False
        
        # Логируем платеж
        db.log_payment(user_id, amount, 'success', utm_source, utm_id)
        
        # Отменяем оставшиеся запланированные сообщения
        cancelled_count = db.cancel_remaining_messages(user_id)
        logger.info(f"🚫 Отменено {cancelled_count} запланированных сообщений для пользователя {user_id}")
        
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
        message_data = db.get_payment_success_message()
        
        if not message_data or not message_data.get('text'):
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
            
            mark_success = db.mark_user_started_bot(user_id)
            if not mark_success:
                logger.error(f"❌ Не удалось пометить пользователя {user_id} как начавшего разговор")
                return False
            
            await asyncio.sleep(0.1)
            
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
            
            start_success = await self.execute_start_logic(user_id, context, None)
            if not start_success:
                logger.error(f"❌ Не удалось выполнить логику /start для пользователя {user_id}")
                return False
            
            button_data = self.db.get_welcome_button_by_text(button_text)
            
            if not button_data:
                logger.warning(f"⚠️ Кнопка с текстом '{button_text}' не найдена")
                return True
            
            button_id = button_data[0]
            follow_messages = self.db.get_welcome_follow_messages(button_id)
            
            if not follow_messages:
                logger.info(f"ℹ️ Нет последующих сообщений для кнопки {button_id}")
                return True
            
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
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ Ошибка при отправке последующего сообщения {msg_num} пользователю {user_id}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке механической кнопки для пользователя {user_id}: {e}")
            return False

callback_handler = CallbackHandler(db, scheduler)

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
def is_our_channel(chat) -> bool:
    """✅ УЛУЧШЕННАЯ проверка что это наш канал с диагностикой"""
    try:
        logger.debug(f"🔍 Checking if chat {chat.id} is our channel {CHANNEL_ID}")
        
        # Проверка по ID (числовому)
        if CHANNEL_ID.lstrip('-').isdigit():
            channel_id_int = int(CHANNEL_ID)
            if chat.id == channel_id_int:
                logger.debug(f"✅ Match by ID: {chat.id} == {channel_id_int}")
                return True
        
        # Проверка по username (если канал публичный)
        if chat.username and CHANNEL_ID.startswith('@'):
            expected_username = CHANNEL_ID[1:]
            if chat.username == expected_username:
                logger.debug(f"✅ Match by username: @{chat.username}")
                return True
        
        logger.debug(f"❌ No match found")
        logger.debug(f"   Chat ID: {chat.id} vs Expected: {CHANNEL_ID}")
        logger.debug(f"   Chat username: @{chat.username if chat.username else 'None'}")
        
        return False
    except Exception as e:
        logger.error(f"❌ Error in is_our_channel: {e}")
        return False

# ===== УЛУЧШЕННЫЕ ФУНКЦИИ ОТПРАВКИ СООБЩЕНИЙ =====
async def send_welcome_message(user, context: ContextTypes.DEFAULT_TYPE):
    """✅ УЛУЧШЕННАЯ отправка приветственного сообщения с диагностикой"""
    try:
        logger.info(f"📤 Preparing welcome message for user {user.id}")
        
        welcome_data = db.get_welcome_message()
        welcome_buttons = db.get_welcome_buttons()
        
        logger.debug(f"🔍 Welcome config for user {user.id}:")
        logger.debug(f"   Text length: {len(welcome_data.get('text', ''))}")
        logger.debug(f"   Has photo: {bool(welcome_data.get('photo'))}")
        logger.debug(f"   Mechanical buttons: {len(welcome_buttons) if welcome_buttons else 0}")
        
        reply_markup = None
        
        if welcome_buttons:
            keyboard = []
            for button_id, button_text, position in welcome_buttons:
                keyboard.append([KeyboardButton(button_text)])
            
            reply_markup = ReplyKeyboardMarkup(
                keyboard, 
                resize_keyboard=True,
                one_time_keyboard=True,
                input_field_placeholder="Выберите действие..."
            )
            logger.debug(f"⌨️ Created {len(keyboard)} mechanical buttons for user {user.id}")
        else:
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
            logger.debug("📱 Created standard keyboard for user {user.id}")
        
        # Отправляем приветственное сообщение
        if welcome_data.get('photo'):
            sent_message = await context.bot.send_photo(
                chat_id=user.id,
                photo=welcome_data['photo'],
                caption=welcome_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            sent_message = await context.bot.send_message(
                chat_id=user.id,
                text=welcome_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        
        logger.info(f"✅ Welcome message sent to user {user.id}, message_id: {sent_message.message_id}")
        
    except Forbidden:
        logger.warning(f"⚠️ User {user.id} blocked bot - welcome not delivered")
        raise
    except Exception as e:
        logger.error(f"❌ Error sending welcome to user {user.id}: {e}")
        raise

async def send_goodbye_message(user, context: ContextTypes.DEFAULT_TYPE):
    """✅ УЛУЧШЕННАЯ отправка прощального сообщения с диагностикой"""
    try:
        logger.info(f"📤 Preparing goodbye message for user {user.id}")
        
        goodbye_data = db.get_goodbye_message()
        if not goodbye_data:
            logger.error(f"❌ No goodbye message configured")
            return
            
        goodbye_buttons = db.get_goodbye_buttons()
        
        logger.debug(f"🔍 Goodbye config for user {user.id}:")
        logger.debug(f"   Text length: {len(goodbye_data.get('text', ''))}")
        logger.debug(f"   Has photo: {bool(goodbye_data.get('photo'))}")  
        logger.debug(f"   Buttons count: {len(goodbye_buttons) if goodbye_buttons else 0}")
        
        # Подготавливаем кнопки с UTM метками
        reply_markup = None
        if goodbye_buttons:
            keyboard = []
            for button_id, button_text, button_url, position in goodbye_buttons:
                processed_url = utm_utils.add_utm_to_url(button_url, user.id)
                keyboard.append([InlineKeyboardButton(button_text, url=processed_url)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            logger.debug(f"🔘 Prepared {len(keyboard)} goodbye buttons with UTM for user {user.id}")
        
        # Отправляем сообщение
        message_text = goodbye_data.get('text', 'До свидания!')
        photo_url = goodbye_data.get('photo')
        
        if photo_url:
            logger.debug(f"🖼️ Sending goodbye with photo to user {user.id}")
            sent_message = await context.bot.send_photo(
                chat_id=user.id,
                photo=photo_url, 
                caption=message_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            logger.debug(f"📝 Sending text goodbye to user {user.id}")
            sent_message = await context.bot.send_message(
                chat_id=user.id,
                text=message_text,
                parse_mode='HTML', 
                reply_markup=reply_markup
            )
            
        logger.info(f"✅ Goodbye message sent to user {user.id}, message_id: {sent_message.message_id}")
        
    except Forbidden as e:
        logger.info(f"ℹ️ User {user.id} has blocked bot - goodbye not delivered: {e}")
        raise
        
    except BadRequest as e:
        error_msg = str(e).lower()
        if "chat not found" in error_msg:
            logger.info(f"ℹ️ Chat with user {user.id} not found - expected after channel leave: {e}")
        elif "user is deactivated" in error_msg:
            logger.info(f"ℹ️ User {user.id} account is deactivated: {e}")
        elif "message text is empty" in error_msg:
            logger.error(f"❌ Empty goodbye message configured: {e}")
        else:
            logger.warning(f"⚠️ BadRequest sending goodbye to user {user.id}: {e}")
        raise
        
    except Exception as e:
        logger.error(f"❌ Unexpected error sending goodbye to user {user.id}: {e}", exc_info=True)
        raise

# ===== RETRY ФУНКЦИИ =====
async def send_welcome_with_retry(user, context: ContextTypes.DEFAULT_TYPE, max_retries=2):
    """Отправка приветствия с retry логикой"""
    for attempt in range(max_retries + 1):
        try:
            await send_welcome_message(user, context)
            return True
            
        except Forbidden as e:
            logger.warning(f"⚠️ User {user.id} blocked bot - cannot send welcome: {e}")
            return False
            
        except BadRequest as e:
            logger.warning(f"⚠️ BadRequest for welcome to user {user.id}: {e}")
            if attempt < max_retries:
                await asyncio.sleep(0.5)
                continue
            return False
            
        except Exception as e:
            logger.error(f"❌ Attempt {attempt + 1} failed for welcome to user {user.id}: {e}")
            if attempt < max_retries:
                await asyncio.sleep(1)
                continue
            return False
    
    return False

async def send_goodbye_with_retry(user, context: ContextTypes.DEFAULT_TYPE, max_retries=3):
    """Отправка прощания с расширенной retry логикой и диагностикой"""
    logger.info(f"👋 Attempting to send goodbye to user {user.id}")
    
    for attempt in range(max_retries + 1):
        try:
            logger.debug(f"🔍 Goodbye attempt {attempt + 1} for user {user.id}")
            
            # Проверяем, что пользователь существует в БД и активен
            user_info = db.get_user(user.id)
            if not user_info:
                logger.warning(f"⚠️ User {user.id} not found in database for goodbye")
                return False
                
            is_active = user_info[4] if len(user_info) > 4 else True
            logger.debug(f"🔍 User {user.id} active status: {is_active}")
            
            await send_goodbye_message(user, context)
            
            logger.info(f"✅ Goodbye message sent to user {user.id} on attempt {attempt + 1}")
            return True
            
        except Forbidden as e:
            logger.info(f"ℹ️ User {user.id} blocked bot (expected on channel leave): {e}")
            return False
            
        except BadRequest as e:
            error_msg = str(e).lower()
            if "chat not found" in error_msg or "user not found" in error_msg:
                logger.info(f"ℹ️ User {user.id} chat not accessible (expected): {e}")
                return False
            else:
                logger.warning(f"⚠️ BadRequest for goodbye to user {user.id}: {e}")
                if attempt < max_retries:
                    await asyncio.sleep(0.5)
                    continue
                return False
            
        except Exception as e:
            logger.error(f"❌ Attempt {attempt + 1} failed for goodbye to user {user.id}: {e}")
            if attempt < max_retries:
                await asyncio.sleep(1)
                continue
            return False
    
    logger.error(f"❌ All {max_retries + 1} attempts failed for goodbye to user {user.id}")
    return False

# ===== ОБРАБОТЧИКИ СОБЫТИЙ КАНАЛА =====
async def handle_user_joined(user, context: ContextTypes.DEFAULT_TYPE):
    """Обработка вступления в канал"""
    try:
        logger.info(f"👋 PROCESSING JOIN: User {user.id} (@{user.username})")
        
        success = db.add_user(user.id, user.username, user.first_name)
        if not success:
            logger.error(f"❌ Failed to add user {user.id} to database")
            return
            
        logger.info(f"✅ User {user.id} added to database")
        
        welcome_sent = await send_welcome_with_retry(user, context)
        if welcome_sent:
            logger.info(f"✅ Welcome message sent to user {user.id}")
        else:
            logger.warning(f"⚠️ Failed to send welcome message to user {user.id}")
            
    except Exception as e:
        logger.error(f"❌ Error handling user join for {user.id}: {e}", exc_info=True)

async def handle_user_left(user, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выхода из канала с улучшенной логикой"""
    try:
        logger.info(f"🚪 PROCESSING LEAVE: User {user.id} (@{user.username})")
        
        # ✅ СНАЧАЛА отправляем прощальное сообщение (пока пользователь еще не деактивирован)
        goodbye_sent = await send_goodbye_with_retry(user, context)
        
        # Небольшая задержка для завершения отправки
        await asyncio.sleep(0.5)
        
        # Затем деактивируем пользователя
        db.deactivate_user(user.id)
        logger.info(f"❌ User {user.id} deactivated in database")
        
        # Отменяем запланированные сообщения
        cancelled_count = db.cancel_user_messages(user.id)
        logger.info(f"🚫 Cancelled {cancelled_count} scheduled messages for user {user.id}")
        
        # Результат
        if goodbye_sent:
            logger.info(f"✅ User {user.id} left - goodbye message sent")
        else:
            logger.warning(f"⚠️ User {user.id} left - goodbye message failed")
            
    except Exception as e:
        logger.error(f"❌ Error handling user leave for {user.id}: {e}", exc_info=True)

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ УЛУЧШЕННЫЙ обработчик с детальной диагностикой и retry логикой"""
    
    if not update.chat_member:
        logger.warning("❌ No chat_member in update")
        return
        
    try:
        chat_member_update = update.chat_member
        old_status = chat_member_update.old_chat_member.status  
        new_status = chat_member_update.new_chat_member.status
        user = chat_member_update.new_chat_member.user
        chat = chat_member_update.chat
        
        logger.info(f"🔍 CHAT_MEMBER EVENT DETAILED:")
        logger.info(f"   User: {user.id} (@{user.username}) {user.first_name}")
        logger.info(f"   Status: {old_status} → {new_status}")
        logger.info(f"   Chat: {chat.id} ({chat.title})")
        logger.info(f"   Is bot: {user.is_bot}")
        logger.info(f"   Update ID: {update.update_id}")
        
        # Пропускаем ботов
        if user.is_bot:
            logger.debug(f"⏭️ Skipping bot user: {user.id}")
            return
        
        # Проверка нашего канала
        if not is_our_channel(chat):
            logger.debug(f"❌ Event not from our channel: {chat.id} vs expected {CHANNEL_ID}")
            return
        
        # ✅ БОЛЕЕ ТОЧНОЕ ОПРЕДЕЛЕНИЕ СОБЫТИЙ
        join_statuses = [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, 
                        ChatMemberStatus.RESTRICTED, ChatMemberStatus.KICKED]
        active_statuses = [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, 
                          ChatMemberStatus.OWNER, ChatMemberStatus.CREATOR]
        
        joined = (old_status in join_statuses and new_status in active_statuses)
        left = (old_status in active_statuses and new_status in join_statuses)
        
        # Дополнительная логика для edge cases
        if not joined and not left:
            if old_status in active_statuses and new_status in active_statuses:
                logger.debug(f"ℹ️ Status change within active statuses ignored: {old_status} → {new_status}")
                return
            
            logger.warning(f"⚠️ Unhandled status change: {old_status} → {new_status}")
            return
        
        logger.info(f"📊 EVENT TYPE: joined={joined}, left={left}")
        
        if joined:
            await handle_user_joined(user, context)
        elif left:
            await handle_user_left(user, context)
            
    except Exception as e:
        logger.error(f"❌ Critical error in chat_member_updates: {e}", exc_info=True)

# ===== ДИАГНОСТИЧЕСКИЕ КОМАНДЫ =====
async def debug_channel_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ Диагностическая команда для проверки настроек канала"""
    user = update.effective_user
    
    if user.id != ADMIN_CHAT_ID:
        return
    
    try:
        chat = await context.bot.get_chat(CHANNEL_ID)
        bot_member = await context.bot.get_chat_member(CHANNEL_ID, context.bot.id)
        updates = await context.bot.get_updates(limit=5)
        
        diagnostics = f"""🔍 <b>ДИАГНОСТИКА КАНАЛА</b>

📋 <b>Конфигурация:</b>
• Channel ID: <code>{CHANNEL_ID}</code>
• Chat ID: <code>{chat.id}</code>  
• Chat Type: {chat.type}
• Chat Title: {chat.title}
• Chat Username: @{chat.username or 'None'}

🤖 <b>Статус бота:</b>
• Status: {bot_member.status}
• Can restrict: {getattr(bot_member, 'can_restrict_members', 'N/A')}
• Can manage chat: {getattr(bot_member, 'can_manage_chat', 'N/A')}

📊 <b>Последние updates:</b>"""

        for upd in updates[-3:]:
            diagnostics += f"\n• Update {upd.update_id}: {type(upd).__name__}"
            if hasattr(upd, 'chat_member') and upd.chat_member:
                diagnostics += f" (chat: {upd.chat_member.chat.id})"

        diagnostics += f"""

⚡ <b>Webhook состояние:</b>
• Queue size: {webhook_queue.qsize()}
• Loop running: {webhook_loop is not None and not webhook_loop.is_closed() if webhook_loop else False}

🔧 <b>База данных:</b>
• Активных пользователей: {len(db.get_users_with_bot_started())}
• Всего пользователей: {len(db.get_all_users())}
"""
        
        await update.message.reply_text(diagnostics, parse_mode='HTML')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка диагностики: {e}")
        logger.error(f"❌ Ошибка в debug_channel_info: {e}")

async def check_webhook_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ Проверка информации о webhook (только для админа)"""
    user = update.effective_user
    
    if user.id != ADMIN_CHAT_ID:
        return
    
    try:
        webhook_info = await context.bot.get_webhook_info()
        
        info_text = f"""🔗 <b>WEBHOOK INFO</b>

📡 <b>Telegram webhook:</b>
• URL: {webhook_info.url or 'Not set'}
• Pending updates: {webhook_info.pending_update_count}
• Max connections: {webhook_info.max_connections}
• Allowed updates: {webhook_info.allowed_updates or 'All'}
• Last error: {webhook_info.last_error_message or 'None'}

🌐 <b>Ожидаемые настройки:</b>
• Expected URL: {WEBHOOK_URL}/bot{BOT_TOKEN}
• Configured port: {RENDER_PORT}

🔧 <b>Наша обработка:</b>
• Queue size: {webhook_queue.qsize()}
• Loop status: {'Running' if webhook_loop and not webhook_loop.is_closed() else 'Stopped'}

📡 <b>Flask endpoints:</b>
• Telegram: /bot{BOT_TOKEN}
• Payment: /webhook/payment  
• Health: /health
"""
        
        await update.message.reply_text(info_text, parse_mode='HTML')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка получения webhook info: {e}")
        logger.error(f"❌ Ошибка в check_webhook_info: {e}")

# ===== TELEGRAM BOT HANDLERS =====
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user = update.effective_user
    
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
        return
    
    logger.info(f"📋 Обработка команды /start для пользователя {user.id}")
    success = await callback_handler.execute_start_logic(user.id, context, user)
    
    if success:
        try:
            conn = db._get_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT value FROM settings WHERE key = "success_message"')
            success_msg = cursor.fetchone()
            
            if not success_msg:
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
        await chat_join_request.approve()
        logger.info(f"✅ Одобрена заявка от пользователя {user.id} (@{user.username})")
        
        db.add_user(user.id, user.username, user.first_name)
        
        await asyncio.sleep(0.5)
        
        await send_welcome_message(user, context)
        
        logger.info(f"✅ Пользователь {user.id} добавлен в базу данных с автоматическим приветственным сообщением")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке заявки от {user.id}: {e}")

async def callback_query_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатий на инлайн-кнопки"""
    query = update.callback_query
    user_id = query.from_user.id
    callback_data = query.data
    
    if user_id == ADMIN_CHAT_ID:
        await query.answer()
        await admin_panel.handle_callback(update, context)
        return
    
    if callback_data in [CALLBACK_USER_CONSENT, CALLBACK_START_NOTIFICATIONS]:
        await query.answer(
            "Пожалуйста, используйте кнопки клавиатуры для взаимодействия с ботом.",
            show_alert=True
        )
        
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
        await query.answer(
            "Эта кнопка больше не активна. Используйте кнопки клавиатуры.",
            show_alert=True
        )

async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений и нажатий на обычные кнопки"""
    user_id = update.effective_user.id
    message_text = update.message.text
    
    if user_id == ADMIN_CHAT_ID:
        await admin_panel.handle_message(update, context)
        return
    
    # Проверяем кнопки, настроенные админом
    welcome_buttons = db.get_welcome_buttons()
    admin_button_texts = [button_text for _, button_text, _ in welcome_buttons]
    
    if message_text in admin_button_texts:
        try:
            success = await callback_handler.handle_welcome_button_press(
                user_id, message_text, context
            )
            
            if success:
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
            if 'Event loop is closed' in str(e):
                logger.warning(f"⚠️ Event loop closed error (ignoring): {e}")
            else:
                logger.error(f"❌ Ошибка при обработке кнопки '{message_text}' от пользователя {user_id}: {e}")
                await update.message.reply_text(
                    "❌ Произошла техническая ошибка. Попробуйте позже.",
                    reply_markup=ReplyKeyboardRemove()
                )
        return
    
    # Обрабатываем стандартные кнопки
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
    
    try:
        logger.info(f"🔘 Пользователь {user_id} нажал кнопку согласия")
        
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
        
        success = await callback_handler.execute_start_logic(user_id, context, update.effective_user)
        
        if success:
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
    """✅ УЛУЧШЕННЫЙ обработчик ошибок"""
    logger.error(f"❌ Exception while handling an update: {context.error}")
    
    # Предотвращаем накопление проблемных event loop'ов
    try:
        if hasattr(context, 'error') and 'Event loop is closed' in str(context.error):
            logger.warning("⚠️ Обнаружена ошибка закрытого event loop, игнорируем")
            return
    except Exception:
        pass

def run_flask_app():
    """Запуск Flask приложения в отдельном потоке"""
    logger.info(f"🌐 Запуск Flask сервера на порту {RENDER_PORT}")
    logger.info(f"📱 Telegram webhook endpoint: /bot{BOT_TOKEN}")
    logger.info(f"💰 Payment webhook endpoint: /webhook/payment")
    logger.info(f"🔍 Health check endpoint: /health")
    
    flask_app.run(
        host='0.0.0.0',
        port=RENDER_PORT,
        debug=False,
        threaded=True,
        use_reloader=False
    )

async def post_init(application: Application) -> None:
    """Инициализация после запуска"""
    global bot_application, bot_instance
    bot_application = application
    bot_instance = application.bot
    
    logger.info("🚀 Бот с оптимизированными webhook'ами успешно запущен!")
    logger.info(f"📱 Telegram webhook: {WEBHOOK_URL}/bot{BOT_TOKEN}")
    logger.info(f"💰 Payment webhook: {WEBHOOK_URL}/webhook/payment")
    logger.info(f"🔍 Health check: {WEBHOOK_URL}/health")
    
    db_info = db.get_database_info()
    logger.info(f"📊 База данных готова: {db_info}")

def main():
    """🚀 ОПТИМИЗИРОВАННАЯ главная функция запуска бота"""
    global bot_application, bot_instance
    
    logger.info("🚀 Запуск Telegram бота для Render с оптимизациями...")
    
    # Создаём Telegram приложение
    application = Application.builder().token(BOT_TOKEN).build()
    bot_instance = application.bot
    
    # Добавляем обработчик инициализации
    application.post_init = post_init
    
    # ===== РЕГИСТРИРУЕМ ВСЕ ОБРАБОТЧИКИ =====
    application.add_handler(CommandHandler("start", start))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    
    # ✅ ОПТИМИЗИРОВАННЫЙ ОБРАБОТЧИК СОБЫТИЙ КАНАЛА
    channel_chat_id = None
    try:
        if CHANNEL_ID.lstrip('-').isdigit():
            channel_chat_id = int(CHANNEL_ID)
        logger.info(f"📋 Настроен фильтр ChatMemberHandler для канала: {channel_chat_id}")
    except:
        logger.warning(f"⚠️ Не удалось преобразовать CHANNEL_ID в число: {CHANNEL_ID}")
    
    application.add_handler(ChatMemberHandler(
        handle_chat_member_updates,
        ChatMemberHandler.CHAT_MEMBER,
        chat_id=[channel_chat_id] if channel_chat_id else None
    ))
    
    # Диагностические команды
    application.add_handler(CommandHandler("debug_channel", debug_channel_info))
    application.add_handler(CommandHandler("webhook_info", check_webhook_info))
    
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, message_handler))
    
    # Добавляем обработчик ошибок
    application.add_error_handler(error_handler)
    
    # ===== ЗАПУСКАЕМ ФОНОВЫЕ ЗАДАЧИ =====
    application.job_queue.run_repeating(
        scheduler.send_scheduled_messages,
        interval=60,
        first=10
    )
    
    application.job_queue.run_repeating(
        scheduler.send_scheduled_broadcasts,
        interval=120,
        first=20
    )
    
    logger.info("🌐 Запуск в режиме WEBHOOK для продакшена на Render...")
    
    if USE_WEBHOOK and WEBHOOK_URL:
        # ✅ ИНИЦИАЛИЗИРУЕМ ОПТИМИЗИРОВАННЫЙ WEBHOOK PROCESSOR
        init_webhook_processor()
        
        # Запускаем обработчик очереди
        webhook_processor_thread = threading.Thread(target=process_webhook_updates, daemon=True)
        webhook_processor_thread.start()
        
        # Запускаем Flask в отдельном потоке
        flask_thread = threading.Thread(target=run_flask_app, daemon=True)
        flask_thread.start()
        
        # Даем Flask время запуститься
        time.sleep(3)
        
        # Настраиваем Telegram webhook
        webhook_path = f"/bot{BOT_TOKEN}"
        webhook_url = f"{WEBHOOK_URL}{webhook_path}"
        
        logger.info(f"📡 Настройка Telegram webhook: {webhook_url}")
        
        try:
            application.run_webhook(
                listen="127.0.0.1",
                port=8443,
                webhook_url=webhook_url,
                url_path=webhook_path,
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске webhook: {e}")
            logger.info("🔄 Переключение на polling mode...")
            application.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES
            )
    else:
        logger.warning("🔄 Запуск в режиме POLLING (не рекомендуется для Render)")
        logger.warning("⚠️ Убедитесь, что установлены USE_WEBHOOK=true и WEBHOOK_URL")
        
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)
    finally:
        # ✅ КОРРЕКТНОЕ ЗАВЕРШЕНИЕ РАБОТЫ
        try:
            if webhook_loop and not webhook_loop.is_closed():
                webhook_loop.call_soon_threadsafe(webhook_loop.stop)
                logger.info("✅ Webhook event loop остановлен")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при завершении webhook loop: {e}")
        
        logger.info("👋 Бот завершен")
