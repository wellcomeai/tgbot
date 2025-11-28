import re
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

logger = logging.getLogger(__name__)

def add_utm_to_url(url: str, user_id: int) -> str:
    """Добавление UTM меток к URL"""
    try:
        if not url or not url.startswith(('http://', 'https://')):
            return url
        
        # Парсим URL
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        # Добавляем UTM параметры
        query_params['utm_source'] = ['bot']
        query_params['utm_id'] = [str(user_id)]
        
        # Собираем URL обратно
        new_query = urlencode(query_params, doseq=True)
        new_parsed = parsed._replace(query=new_query)
        
        result_url = urlunparse(new_parsed)
        logger.debug(f"Добавлены UTM метки к URL для пользователя {user_id}: {url} -> {result_url}")
        
        return result_url
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении UTM меток к URL {url}: {e}")
        return url

def process_text_links(text: str, user_id: int) -> str:
    """Обработка ссылок в тексте сообщения"""
    try:
        if not text:
            return text
        
        # Паттерн для поиска HTTP/HTTPS ссылок
        url_pattern = r'https?://[^\s<>"{}|\\^`[\]]+[^\s.,;!?<>"{}|\\^`[\]]'
        
        def replace_url(match):
            original_url = match.group(0)
            return add_utm_to_url(original_url, user_id)
        
        # Заменяем все найденные URL
        processed_text = re.sub(url_pattern, replace_url, text)
        
        if processed_text != text:
            logger.debug(f"Обработаны ссылки в тексте для пользователя {user_id}")
        
        return processed_text
        
    except Exception as e:
        logger.error(f"Ошибка при обработке ссылок в тексте для пользователя {user_id}: {e}")
        return text

def process_message_buttons(buttons: list, user_id: int) -> list:
    """Обработка кнопок с URL и поддержкой messages_count"""
    try:
        if not buttons:
            return buttons
        
        processed_buttons = []
        
        for button in buttons:
            # Кнопка может быть кортежем (id, text, url, position, messages_count) или словарем
            if isinstance(button, (tuple, list)) and len(button) >= 3:
                button_id, button_text, button_url = button[0], button[1], button[2]
                position = button[3] if len(button) > 3 else 1
                messages_count = button[4] if len(button) > 4 else 1  # ✅ НОВОЕ
                
                # Обрабатываем URL
                processed_url = add_utm_to_url(button_url, user_id)
                
                # ✅ ИСПРАВЛЕНО: Сохраняем структуру с messages_count
                if len(button) > 4:
                    # Кнопка с messages_count (5 значений)
                    processed_buttons.append((button_id, button_text, processed_url, position, messages_count))
                elif len(button) > 3:
                    # Кнопка без messages_count (4 значения) - добавляем 1 по умолчанию
                    processed_buttons.append((button_id, button_text, processed_url, position, 1))
                else:
                    # Старый формат (3 значения) - добавляем position и messages_count
                    processed_buttons.append((button_id, button_text, processed_url, 1, 1))
                    
            elif isinstance(button, dict):
                # Если кнопка в виде словаря
                processed_button = button.copy()
                if 'url' in processed_button:
                    processed_button['url'] = add_utm_to_url(processed_button['url'], user_id)
                # Добавляем messages_count если его нет
                if 'messages_count' not in processed_button:
                    processed_button['messages_count'] = 1
                processed_buttons.append(processed_button)
                
            else:
                # Неизвестный формат, оставляем как есть
                processed_buttons.append(button)
        
        logger.debug(f"Обработаны кнопки для пользователя {user_id}: {len(buttons)} кнопок")
        
        return processed_buttons
        
    except Exception as e:
        logger.error(f"Ошибка при обработке кнопок для пользователя {user_id}: {e}")
        return buttons

def extract_user_id_from_utm(url: str) -> int:
    """Извлечение user_id из UTM меток"""
    try:
        if not url:
            return None
        
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        utm_id = query_params.get('utm_id', [])
        if utm_id and utm_id[0].isdigit():
            return int(utm_id[0])
        
        return None
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении user_id из UTM: {e}")
        return None

def validate_utm_source(url: str) -> bool:
    """Проверка, что UTM source = bot"""
    try:
        if not url:
            return False
        
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        utm_source = query_params.get('utm_source', [])
        return utm_source and utm_source[0] == 'bot'
        
    except Exception as e:
        logger.error(f"Ошибка при проверке UTM source: {e}")
        return False
