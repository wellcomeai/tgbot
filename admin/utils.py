"""
Вспомогательные методы для админ-панели
"""

import re
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class UtilsMixin:
    """Миксин с вспомогательными методами"""
    
    def parse_delay_input(self, text):
        """Парсинг ввода задержки в различных форматах"""
        text = text.strip().lower()
        
        try:
            # Проверяем формат с минутами
            if 'м' in text or 'минут' in text:
                # Извлекаем число
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    minutes = float(match.group(1))
                    hours = minutes / 60
                    return hours, f"{int(minutes)} минут"
            
            # Проверяем формат с часами
            elif 'ч' in text or 'час' in text:
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} часов"
            
            # Проверяем просто число (считаем как часы)
            else:
                hours = float(text)
                if hours < 1:
                    minutes = int(hours * 60)
                    return hours, f"{minutes} минут"
                else:
                    return hours, f"{hours} часов"
                    
        except ValueError:
            return None, None
        
        return None, None
    
    def parse_hours_input(self, text):
        """Парсинг ввода часов для планирования рассылок"""
        text = text.strip().lower()
        
        try:
            # Убираем все пробелы и проверяем различные форматы
            text_clean = text.replace(' ', '')
            
            # Форматы: 1ч, 2ч, 3час, 4часа, 5часов
            if 'ч' in text_clean:
                match = re.search(r'(\d+(?:\.\d+)?)', text_clean)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} час(ов)"
            
            # Форматы: час, часа, часов с числом
            elif any(word in text for word in ['час', 'часа', 'часов']):
                match = re.search(r'(\d+(?:\.\d+)?)', text)
                if match:
                    hours = float(match.group(1))
                    return hours, f"{hours} час(ов)"
            
            # Просто число - считаем как часы
            else:
                hours = float(text)
                return hours, f"{hours} час(ов)"
                    
        except ValueError:
            return None, None
        
        return None, None
    
    def validate_waiting_state(self, waiting_data: dict) -> bool:
        """Проверить, что состояние ожидания валидно"""
        if not waiting_data or "type" not in waiting_data:
            return False
        
        # Проверяем, что состояние не слишком старое (30 минут)
        created_at = waiting_data.get("created_at")
        if created_at and (datetime.now() - created_at).total_seconds() > 1800:
            return False
        
        return True
    
    def _get_delay_text(self, message_number):
        """Получить текст для ввода задержки"""
        return (
            f"⏰ Отправьте новую задержку для сообщения {message_number}:\n\n"
            f"📝 <b>Форматы ввода:</b>\n"
            f"• <code>30м</code> или <code>30 минут</code> - для минут\n"
            f"• <code>2ч</code> или <code>2 часа</code> - для часов\n"
            f"• <code>1.5</code> - для 1.5 часов\n"
            f"• <code>0.05</code> - для 3 минут\n\n"
            f"💡 Примеры: <code>3м</code>, <code>30 минут</code>, <code>2ч</code>, <code>1.5</code>"
        )
    
    def format_delay_display(self, delay_hours):
        """Форматировать отображение задержки"""
        if delay_hours < 1:
            delay_str = f"{int(delay_hours * 60)}м"
        else:
            delay_str = f"{delay_hours}ч"
        return delay_str
    
    def format_delay_display_full(self, delay_hours):
        """Полное форматирование отображения задержки"""
        if delay_hours < 1:
            delay_str = f"{int(delay_hours * 60)} минут"
        else:
            delay_str = f"{delay_hours} часов"
        return delay_str
