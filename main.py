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
from telegram.constants import ChatMemberStatus
from database import Database
from admin import AdminPanel
from scheduler import MessageScheduler
from flask import Flask, request, jsonify
import threading
import concurrent.futures

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

# ИСПРАВЛЕНО: Настройка для Render Disk - правильный путь
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

# ===== FLASK ПРИЛОЖЕНИЕ ДЛЯ WEBHOOK'ОВ =====
flask_app = Flask(__name__)

# ✅ ИСПРАВЛЕНО: Глобальный executor для изоляции асинхронных задач
executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

@flask_app.route(f'/bot{BOT_TOKEN}', methods=['POST'])
def telegram_webhook():
    """ИСПРАВЛЕННАЯ обработка Telegram webhook через Flask"""
    try:
        # Получаем данные от Telegram
        update_data = request.get_json()
        
        if not update_data:
            logger.warning("⚠️ Получен пустой Telegram webhook")
            return jsonify({'ok': False}), 400
        
        logger.debug(f"📱 Получен Telegram update: {update_data.get('update_id')}")
        
        # ИСПРАВЛЕНИЕ: Используем ThreadPoolExecutor с изолированным event loop
        def process_update_safe():
            """Безопасная обработка update в отдельном потоке с новым event loop"""
            try:
                # КРИТИЧЕСКИ ВАЖНО: Создаем новый event loop для каждого потока
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    # Создаем Update объект
                    update = Update.de_json(update_data, bot_instance)
                    
                    # Обрабатываем update
                    if bot_application:
                        loop.run_until_complete(bot_application.process_update(update))
                    
                    logger.debug(f"✅ Update {update_data.get('update_id')} обработан успешно")
                    
                finally:
                    # ВАЖНО: Правильно закрываем loop
                    try:
                        loop.close()
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка при закрытии event loop: {e}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка в безопасной обработке update: {e}", exc_info=True)
        
        # ИСПРАВЛЕНИЕ: Используем ThreadPoolExecutor для лучшего управления потоками
        executor.submit(process_update_safe)
        
        return jsonify({'ok': True})
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в Telegram webhook: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

@flask_app.route('/webhook/payment', methods=['POST'])
def payment_webhook():
    """ИСПРАВЛЕННАЯ обработка Payment webhook"""
    try:
        # Получаем платежные данные
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
        
        # ИСПРАВЛЕНИЕ: Безопасная обработка в отдельном потоке
        def process_payment_safe():
            """Безопасная обработка платежа в отдельном потоке"""
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    success = loop.run_until_complete(handle_successful_payment(user_id, amount, payment_data))
                    return success
                finally:
                    try:
                        loop.close()
                    except Exception as e:
                        logger.warning(f"⚠️ Ошибка при закрытии event loop в payment: {e}")
                        
            except Exception as e:
                logger.error(f"❌ Ошибка в безопасной обработке платежа: {e}")
                return False
        
        # Обрабатываем только успешные платежи
        if payment_status == 'success':
            # ИСПРАВЛЕНИЕ: Используем ThreadPoolExecutor с таймаутом
            future = executor.submit(process_payment_safe)
            
            try:
                success = future.result(timeout=30)  # Ждём максимум 30 секунд
                
                if success:
                    logger.info(f"✅ Успешно обработан платеж для пользователя {user_id}")
                    return jsonify({
                        'status': 'success',
                        'message': 'Payment processed successfully',
                        'user_id': user_id
                    }), 200
                else:
                    logger.error(f"❌ Ошибка при обработке платежа для пользователя {user_id}")
                    return jsonify({'error': 'Payment processing failed'}), 500
                    
            except concurrent.futures.TimeoutError:
                logger.error(f"❌ Таймаут при обработке платежа для пользователя {user_id}")
                return jsonify({'error': 'Payment processing timeout'}), 500
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
            'bot_running': bot_instance is not None,
            'flask_port': RENDER_PORT,
            'database': db_info,
            'render_disk_configured': RENDER_DISK_PATH is not None,
            'render_disk_path': RENDER_DISK_PATH,
            'webhook_url': WEBHOOK_URL,
            'executor_threads': len(executor._threads) if hasattr(executor, '_threads') else 'N/A'
        }
        
        return jsonify(health_data)
        
    except Exception as e:
        logger.error(f"❌ Ошибка в health check: {e}")
        return jsonify({
            'status': 'error',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

async def handle_successful_payment(user_id: int, amount: str, webhook_data: dict) -> bool:
    """Асинхронная обработка успешного платежа"""
    try:
        logger.info(f"💰 Обрабатываем успешный платеж для пользователя {user_id}")
        
        # Получаем UTM данные
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
            expected_username = CHANNEL_ID[1:]  # убираем @
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

async def send_welcome_message(user, context: ContextTypes.DEFAULT_TYPE):
    """Отправка приветственного сообщения"""
    try:
        welcome_data = db.get_welcome_message()
        welcome_buttons = db.get_welcome_buttons()
        
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
            logger.info("📱 Создана стандартная клавиатуру")
        
        # Отправляем приветственное сообщение
        if welcome_data['photo']:
            await context.bot.send_photo(
                chat_id=user.id,
                photo=welcome_data['photo'],
                caption=welcome_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        else:
            await context.bot.send_message(
                chat_id=user.id,
                text=welcome_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
        
        logger.info(f"✅ Приветственное сообщение отправлено пользователю {user.id}")
        
    except Forbidden:
        logger.warning(f"⚠️ Не удалось отправить приветственное сообщение пользователю {user.id}: пользователь не начал диалог с ботом")
    except Exception as e:
        logger.error(f"❌ Не удалось отправить приветственное сообщение пользователю {user.id}: {e}")

async def send_goodbye_message(user, context: ContextTypes.DEFAULT_TYPE):
    """Отправка прощального сообщения"""
    try:
        goodbye_data = db.get_goodbye_message()
        goodbye_buttons = db.get_goodbye_buttons()
        
        reply_markup = None
        if goodbye_buttons:
            keyboard = []
            for button_id, button_text, button_url, position in goodbye_buttons:
                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
            reply_markup = InlineKeyboardMarkup(keyboard)
        
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
        
    except Forbidden:
        logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: пользователь заблокировал бота")
    except BadRequest as e:
        logger.warning(f"⚠️ Не удалось отправить прощальное сообщение пользователю {user.id}: {e}")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке прощального сообщения пользователю {user.id}: {e}")

# ===== ✅ УЛУЧШЕННЫЙ ОБРАБОТЧИК СОБЫТИЙ КАНАЛА С ДИАГНОСТИКОЙ =====

async def handle_chat_member_updates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ УЛУЧШЕННЫЙ ДИАГНОСТИЧЕСКИЙ обработчик изменений участников канала"""
    
    # ✅ НОВОЕ: Детальная диагностика всех событий
    logger.info(f"🔍 CHAT_MEMBER EVENT RECEIVED:")
    logger.info(f"   Update ID: {update.update_id}")
    logger.info(f"   Has chat_member: {bool(update.chat_member)}")
    
    if not update.chat_member:
        logger.warning("❌ No chat_member in update")
        return
        
    try:
        chat_member_update = update.chat_member
        old_status = chat_member_update.old_chat_member.status  
        new_status = chat_member_update.new_chat_member.status
        user = chat_member_update.new_chat_member.user
        chat = chat_member_update.chat
        
        # ✅ ДЕТАЛЬНАЯ ДИАГНОСТИКА
        logger.info(f"🔍 DETAILED EVENT INFO:")
        logger.info(f"   User: {user.id} (@{user.username})")
        logger.info(f"   Status change: {old_status} → {new_status}")
        logger.info(f"   Chat: {chat.id} ({chat.title})")
        logger.info(f"   Chat type: {chat.type}")
        logger.info(f"   Is bot: {user.is_bot}")
        logger.info(f"   Expected channel: {CHANNEL_ID}")
        
        # Пропускаем ботов
        if user.is_bot:
            logger.debug(f"⏭️ Skipping bot user: {user.id}")
            return
        
        # ✅ УЛУЧШЕННАЯ ПРОВЕРКА КАНАЛА
        is_our_channel_result = is_our_channel(chat)
        logger.info(f"🏠 Is our channel check: {is_our_channel_result}")
        
        if not is_our_channel_result:
            logger.warning(f"❌ Event NOT from our channel:")
            logger.warning(f"   Received from: {chat.id} ({chat.title})")
            logger.warning(f"   Expected: {CHANNEL_ID}")
            logger.warning(f"   Chat username: @{chat.username if chat.username else 'None'}")
            return
        
        # ✅ ОПРЕДЕЛЯЕМ ТИП СОБЫТИЯ
        joined = (old_status in [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED] and 
                 new_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER])
        
        left = (old_status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER] and 
               new_status in [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED, ChatMemberStatus.RESTRICTED])
        
        logger.info(f"📊 EVENT CLASSIFICATION:")
        logger.info(f"   Joined: {joined}")
        logger.info(f"   Left: {left}")
        
        if joined:
            # ===== ЛОГИКА ВСТУПЛЕНИЯ =====
            logger.info(f"👋 PROCESSING JOIN: User {user.id} (@{user.username})")
            logger.info(f"✅ User joined our channel - sending welcome")
            
            # Добавляем пользователя в базу данных
            db.add_user(user.id, user.username, user.first_name)
            
            # Отправляем приветствие
            await send_welcome_message(user, context)
            
        elif left:
            # ===== ЛОГИКА ВЫХОДА =====
            logger.info(f"🚪 PROCESSING LEAVE: User {user.id} (@{user.username})")
            logger.info(f"✅ User left our channel - processing...")
            
            # Деактивируем пользователя
            db.deactivate_user(user.id)
            logger.info(f"❌ User {user.id} deactivated in database")
            
            # Отменяем запланированные сообщения
            db.cancel_user_messages(user.id)
            logger.info(f"🚫 Cancelled scheduled messages for user {user.id}")
            
            # Отправляем прощальное сообщение
            await send_goodbye_message(user, context)
            
        else:
            # Другие изменения статуса (например, админ <-> участник)
            logger.debug(f"ℹ️ Status change ignored: {old_status} → {new_status}")
            
    except Exception as e:
        logger.error(f"❌ Error in chat_member_updates: {e}", exc_info=True)

# ===== ДИАГНОСТИЧЕСКИЕ КОМАНДЫ (ТОЛЬКО ДЛЯ АДМИНА) =====

async def debug_channel_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ НОВАЯ диагностическая команда для проверки настроек канала"""
    user = update.effective_user
    
    if user.id != ADMIN_CHAT_ID:
        return
    
    try:
        # Получаем информацию о канале
        chat = await context.bot.get_chat(CHANNEL_ID)
        bot_member = await context.bot.get_chat_member(CHANNEL_ID, context.bot.id)
        
        # Получаем последние обновления
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

⚡ <b>Проверка событий:</b>
Попросите кого-то зайти/выйти из канала и проверьте логи на наличие:
- "🔍 CHAT_MEMBER EVENT RECEIVED"
- "📊 EVENT CLASSIFICATION"

🔧 <b>База данных:</b>
• Активных пользователей: {len(db.get_users_with_bot_started())}
• Всего пользователей: {len(db.get_all_users())}
"""
        
        await update.message.reply_text(diagnostics, parse_mode='HTML')
        
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка диагностики: {e}")
        logger.error(f"❌ Ошибка в debug_channel_info: {e}")

async def check_webhook_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """✅ НОВАЯ проверка информации о webhook (только для админа)"""
    user = update.effective_user
    
    if user.id != ADMIN_CHAT_ID:
        return
    
    try:
        webhook_info = await context.bot.get_webhook_info()
        
        info_text = f"""🔗 <b>WEBHOOK INFO</b>

📡 <b>Текущие настройки:</b>
• URL: {webhook_info.url or 'Not set'}
• Pending updates: {webhook_info.pending_update_count}
• Max connections: {webhook_info.max_connections}
• Allowed updates: {webhook_info.allowed_updates or 'All'}
• Last error: {webhook_info.last_error_message or 'None'}

🌐 <b>Ожидаемые настройки:</b>
• Expected URL: {WEBHOOK_URL}/bot{BOT_TOKEN}
• Configured port: {RENDER_PORT}

🔧 <b>Flask endpoints:</b>
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
    
    # Проверяем, является ли пользователь админом
    if user.id == ADMIN_CHAT_ID:
        await admin_panel.show_main_menu(update, context)
        return
    
    # Для обычных пользователей выполняем логику подписки
    logger.info(f"📋 Обработка команды /start для пользователя {user.id}")
    success = await callback_handler.execute_start_logic(user.id, context, user)
    
    if success:
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
        
        # Небольшая задержка перед отправкой приветствия
        await asyncio.sleep(0.5)
        
        # Отправляем приветственное сообщение
        await send_welcome_message(user, context)
        
        logger.info(f"✅ Пользователь {user.id} добавлен в базу данных с автоматическим приветственным сообщением")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при обработке заявки от {user.id}: {e}")

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
            # ✅ ИСПРАВЛЕНО: Специальная обработка ошибки "Event loop is closed"
            if 'Event loop is closed' in str(e):
                logger.warning(f"⚠️ Event loop closed error (ignoring): {e}")
                # Функциональность выполнена успешно, просто игнорируем ошибку очистки
            else:
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
        
        # Выполняем логику start()
        success = await callback_handler.execute_start_logic(user_id, context, update.effective_user)
        
        if success:
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
    """✅ УЛУЧШЕННЫЙ обработчик ошибок"""
    logger.error(f"❌ Exception while handling an update: {context.error}")
    
    # НОВОЕ: Предотвращаем накопление проблемных event loop'ов
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
    
    logger.info("🚀 Бот с интегрированными webhook'ами успешно запущен!")
    logger.info(f"📱 Telegram webhook: {WEBHOOK_URL}/bot{BOT_TOKEN}")
    logger.info(f"💰 Payment webhook: {WEBHOOK_URL}/webhook/payment")
    logger.info(f"🔍 Health check: {WEBHOOK_URL}/health")
    
    # Выводим информацию о базе данных
    db_info = db.get_database_info()
    logger.info(f"📊 База данных готова: {db_info}")

def main():
    """✅ ИСПРАВЛЕННАЯ главная функция запуска бота"""
    global bot_application, bot_instance
    
    logger.info("🚀 Запуск Telegram бота для Render с Disk...")
    
    # Создаём Telegram приложение
    application = Application.builder().token(BOT_TOKEN).build()
    bot_instance = application.bot
    
    # Добавляем обработчик инициализации
    application.post_init = post_init
    
    # ===== РЕГИСТРИРУЕМ ВСЕ ОБРАБОТЧИКИ =====
    application.add_handler(CommandHandler("start", start))
    application.add_handler(ChatJoinRequestHandler(handle_join_request))
    
    # ✅ ИСПРАВЛЕННЫЙ ЕДИНЫЙ ОБРАБОТЧИК СОБЫТИЙ КАНАЛА
    # Используем правильный тип CHAT_MEMBER и фильтр по каналу
    channel_chat_id = None
    try:
        if CHANNEL_ID.lstrip('-').isdigit():
            channel_chat_id = int(CHANNEL_ID)
        logger.info(f"📋 Настроен фильтр ChatMemberHandler для канала: {channel_chat_id}")
    except:
        logger.warning(f"⚠️ Не удалось преобразовать CHANNEL_ID в число: {CHANNEL_ID}")
    
    application.add_handler(ChatMemberHandler(
        handle_chat_member_updates,
        ChatMemberHandler.CHAT_MEMBER,  # ✅ ВАЖНО: именно CHAT_MEMBER, а не MY_CHAT_MEMBER!
        chat_id=[channel_chat_id] if channel_chat_id else None  # ✅ Фильтр по нашему каналу
    ))
    
    # ✅ НОВЫЕ ДИАГНОСТИЧЕСКИЕ КОМАНДЫ
    application.add_handler(CommandHandler("debug_channel", debug_channel_info))
    application.add_handler(CommandHandler("webhook_info", check_webhook_info))
    
    application.add_handler(CallbackQueryHandler(callback_query_handler))
    application.add_handler(MessageHandler((filters.TEXT | filters.PHOTO) & ~filters.COMMAND, message_handler))
    
    # Добавляем обработчик ошибок
    application.add_error_handler(error_handler)
    
    # ===== ЗАПУСКАЕМ ФОНОВЫЕ ЗАДАЧИ =====
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
    
    logger.info("🌐 Запуск в режиме WEBHOOK для продакшена на Render...")
    
    if USE_WEBHOOK and WEBHOOK_URL:
        # Запускаем Flask в отдельном потоке
        flask_thread = threading.Thread(target=run_flask_app, daemon=True)
        flask_thread.start()
        
        # Даем Flask время запуститься
        import time
        time.sleep(3)
        
        # Настраиваем Telegram webhook
        webhook_path = f"/bot{BOT_TOKEN}"
        webhook_url = f"{WEBHOOK_URL}{webhook_path}"
        
        logger.info(f"📡 Настройка Telegram webhook: {webhook_url}")
        
        # ✅ ИСПРАВЛЕНО: Убрали неподдерживаемые параметры timeout
        try:
            application.run_webhook(
                listen="127.0.0.1",  # Слушаем только локально
                port=8443,  # Внутренний порт для telegram
                webhook_url=webhook_url,
                url_path=webhook_path,
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES  # ✅ КРИТИЧЕСКИ ВАЖНО!
                # ✅ УБРАЛИ: read_timeout, write_timeout, connect_timeout, pool_timeout
            )
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске webhook: {e}")
            logger.info("🔄 Переключение на polling mode...")
            application.run_polling(
                drop_pending_updates=True,
                allowed_updates=Update.ALL_TYPES  # ✅ КРИТИЧЕСКИ ВАЖНО!
            )
    else:
        logger.warning("🔄 Запуск в режиме POLLING (не рекомендуется для Render)")
        logger.warning("⚠️ Убедитесь, что установлены USE_WEBHOOK=true и WEBHOOK_URL")
        
        application.run_polling(
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES  # ✅ КРИТИЧЕСКИ ВАЖНО!
        )

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)
    finally:
        # ✅ ИСПРАВЛЕНО: Корректное завершение работы
        try:
            if executor:
                executor.shutdown(wait=True)  # ✅ УБРАЛИ timeout=10
                logger.info("✅ ThreadPoolExecutor корректно завершен")
        except Exception as e:
            logger.warning(f"⚠️ Ошибка при завершении executor: {e}")
        
        logger.info("👋 Бот завершен")
