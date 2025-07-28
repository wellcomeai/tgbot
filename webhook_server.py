import asyncio
import logging
import json
import threading
from datetime import datetime
from flask import Flask, request, jsonify
from telegram.ext import ContextTypes
from telegram import Bot

logger = logging.getLogger(__name__)

class WebhookServer:
    def __init__(self, db, host='0.0.0.0', port=8080):
        self.db = db
        self.host = host
        self.port = port
        self.app = Flask(__name__)
        self.bot = None
        self.setup_routes()
        self.server_thread = None
        
        # Отключаем логи Flask для чистоты вывода
        logging.getLogger('werkzeug').setLevel(logging.WARNING)
    
    def setup_routes(self):
        """Настройка маршрутов Flask"""
        
        @self.app.route('/webhook/payment', methods=['POST'])
        def handle_payment():
            return asyncio.run(self.process_payment_webhook())
        
        @self.app.route('/health', methods=['GET'])
        def health_check():
            return jsonify({
                'status': 'ok',
                'timestamp': datetime.now().isoformat(),
                'service': 'payment_webhook'
            })
    
    async def process_payment_webhook(self):
        """Обработка веб-хука об оплате"""
        try:
            # Получаем данные
            data = request.get_json()
            
            if not data:
                logger.warning("Получен пустой webhook")
                return jsonify({'error': 'Empty payload'}), 400
            
            logger.info(f"Получен payment webhook: {data}")
            
            # Валидация обязательных полей
            required_fields = ['user_id', 'payment_status', 'amount']
            for field in required_fields:
                if field not in data:
                    logger.error(f"Отсутствует обязательное поле: {field}")
                    return jsonify({'error': f'Missing required field: {field}'}), 400
            
            user_id = data.get('user_id')
            payment_status = data.get('payment_status')
            amount = data.get('amount')
            
            # Валидация типов данных
            try:
                user_id = int(user_id)
            except (ValueError, TypeError):
                logger.error(f"Неверный формат user_id: {user_id}")
                return jsonify({'error': 'Invalid user_id format'}), 400
            
            if payment_status not in ['success', 'failed', 'pending']:
                logger.error(f"Неверный payment_status: {payment_status}")
                return jsonify({'error': 'Invalid payment_status'}), 400
            
            # Проверяем, существует ли пользователь
            user = self.db.get_user(user_id)
            if not user:
                logger.error(f"Пользователь {user_id} не найден")
                return jsonify({'error': 'User not found'}), 404
            
            # Обрабатываем только успешные платежи
            if payment_status == 'success':
                success = await self.handle_successful_payment(user_id, amount, data)
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
            else:
                # Логируем неуспешные платежи
                self.db.log_payment(user_id, amount, payment_status, data.get('utm_source'), data.get('utm_id'))
                logger.info(f"Зафиксирован неуспешный платеж: {payment_status} для пользователя {user_id}")
                return jsonify({
                    'status': 'logged',
                    'message': f'Payment status {payment_status} logged'
                }), 200
        
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в payment webhook: {e}", exc_info=True)
            return jsonify({'error': 'Internal server error'}), 500
    
    async def handle_successful_payment(self, user_id: int, amount: str, webhook_data: dict) -> bool:
        """Обработка успешного платежа"""
        try:
            # Получаем UTM данные
            utm_source = webhook_data.get('utm_source', '')
            utm_id = webhook_data.get('utm_id', '')
            
            # Отмечаем пользователя как оплатившего
            success = self.db.mark_user_paid(user_id, amount, 'success')
            if not success:
                logger.error(f"❌ Не удалось отметить пользователя {user_id} как оплатившего")
                return False
            
            # Логируем платеж
            self.db.log_payment(user_id, amount, 'success', utm_source, utm_id)
            
            # Отменяем оставшиеся запланированные сообщения
            cancelled_count = self.db.cancel_remaining_messages(user_id)
            logger.info(f"🚫 Отменено {cancelled_count} запланированных сообщений для пользователя {user_id}")
            
            # Отправляем уведомление об успешной оплате
            if self.bot:
                await self.send_payment_success_notification(user_id, amount)
            else:
                logger.warning("❌ Bot не инициализирован, не удалось отправить уведомление")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при обработке успешного платежа для пользователя {user_id}: {e}")
            return False
    
    async def send_payment_success_notification(self, user_id: int, amount: str):
        """Отправка уведомления об успешной оплате"""
        try:
            # Получаем настроенное сообщение
            message_data = self.db.get_payment_success_message()
            
            if not message_data:
                # Сообщение по умолчанию
                message_text = (
                    "🎉 <b>Спасибо за покупку!</b>\n\n"
                    f"💰 Платеж на сумму {amount} успешно обработан.\n\n"
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
                await self.bot.send_photo(
                    chat_id=user_id,
                    photo=photo_url,
                    caption=message_text,
                    parse_mode='HTML'
                )
            else:
                await self.bot.send_message(
                    chat_id=user_id,
                    text=message_text,
                    parse_mode='HTML'
                )
            
            logger.info(f"✅ Отправлено уведомление об оплате пользователю {user_id}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке уведомления об оплате пользователю {user_id}: {e}")
    
    def set_bot(self, bot: Bot):
        """Установка экземпляра бота"""
        self.bot = bot
        logger.info("✅ Bot установлен для webhook сервера")
    
    def start_server(self):
        """Запуск веб-хук сервера в отдельном потоке"""
        try:
            def run_server():
                logger.info(f"🚀 Запуск webhook сервера на {self.host}:{self.port}")
                self.app.run(
                    host=self.host,
                    port=self.port,
                    debug=False,
                    threaded=True,
                    use_reloader=False
                )
            
            self.server_thread = threading.Thread(target=run_server, daemon=True)
            self.server_thread.start()
            
            logger.info(f"✅ Webhook сервер запущен на http://{self.host}:{self.port}")
            logger.info(f"📡 Endpoint для платежей: http://{self.host}:{self.port}/webhook/payment")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске webhook сервера: {e}")
            raise
    
    def stop_server(self):
        """Остановка сервера"""
        # В Flask нет простого способа остановки сервера из кода
        # Эта функция для совместимости
        logger.info("🛑 Запрос на остановку webhook сервера")
    
    def is_running(self):
        """Проверка, работает ли сервер"""
        return self.server_thread and self.server_thread.is_alive()
    
    def get_status(self):
        """Получение статуса сервера"""
        return {
            'running': self.is_running(),
            'host': self.host,
            'port': self.port,
            'bot_initialized': self.bot is not None,
            'endpoint': f"http://{self.host}:{self.port}/webhook/payment"
        }

# Глобальный экземпляр сервера (будет создан в main.py)
webhook_server = None

def create_webhook_server(db, host='0.0.0.0', port=8080):
    """Создание экземпляра webhook сервера"""
    global webhook_server
    webhook_server = WebhookServer(db, host, port)
    return webhook_server

def get_webhook_server():
    """Получение глобального экземпляра webhook сервера"""
    return webhook_server
