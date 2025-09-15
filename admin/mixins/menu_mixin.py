"""
Миксин для работы с меню админ-панели
"""

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
import logging

logger = logging.getLogger(__name__)


class MenuMixin:
    """Миксин для построения и отображения меню"""
    
    def build_keyboard(self, buttons_config: list) -> InlineKeyboardMarkup:
        """
        Построить клавиатуру из конфигурации кнопок
        
        Args:
            buttons_config: Список строк кнопок, где каждая строка - список словарей
                          с ключами 'text' и 'callback_data'
        
        Example:
            buttons_config = [
                [{"text": "📊 Статистика", "callback_data": "admin_stats"}],
                [
                    {"text": "✅ Да", "callback_data": "confirm_yes"},
                    {"text": "❌ Нет", "callback_data": "confirm_no"}
                ]
            ]
        """
        keyboard = []
        
        for button_row in buttons_config:
            row = []
            for button in button_row:
                if isinstance(button, dict) and 'text' in button and 'callback_data' in button:
                    row.append(InlineKeyboardButton(button['text'], callback_data=button['callback_data']))
                elif isinstance(button, dict) and 'text' in button and 'url' in button:
                    row.append(InlineKeyboardButton(button['text'], url=button['url']))
            
            if row:  # Добавляем только непустые строки
                keyboard.append(row)
        
        return InlineKeyboardMarkup(keyboard)
    
    def create_back_button(self, callback_data: str = "admin_back") -> list:
        """Создать стандартную кнопку возврата"""
        return [{"text": "« Назад", "callback_data": callback_data}]
    
    def create_cancel_button(self, callback_data: str = "admin_back") -> list:
        """Создать стандартную кнопку отмены"""
        return [{"text": "❌ Отмена", "callback_data": callback_data}]
    
    def create_confirm_buttons(self, confirm_data: str, cancel_data: str) -> list:
        """Создать кнопки подтверждения и отмены"""
        return [
            {"text": "✅ Да, выполнить", "callback_data": confirm_data},
            {"text": "❌ Отмена", "callback_data": cancel_data}
        ]
    
    def create_pagination_buttons(self, current_page: int, total_pages: int, 
                                prefix: str = "page") -> list:
        """
        Создать кнопки пагинации
        
        Args:
            current_page: Текущая страница (начиная с 1)
            total_pages: Общее количество страниц
            prefix: Префикс для callback_data
        """
        buttons = []
        
        if total_pages <= 1:
            return buttons
        
        # Стрелка назад
        if current_page > 1:
            buttons.append({"text": "◀️", "callback_data": f"{prefix}_{current_page - 1}"})
        
        # Информация о странице
        buttons.append({"text": f"{current_page}/{total_pages}", "callback_data": "noop"})
        
        # Стрелка вперед
        if current_page < total_pages:
            buttons.append({"text": "▶️", "callback_data": f"{prefix}_{current_page + 1}"})
        
        return buttons
    
    def format_message_with_header(self, title: str, content: str, 
                                 footer: str = None) -> str:
        """
        Форматировать сообщение со стандартным заголовком
        
        Args:
            title: Заголовок сообщения
            content: Основное содержимое
            footer: Подвал сообщения (опционально)
        """
        message = f"<b>{title}</b>\n\n{content}"
        
        if footer:
            message += f"\n\n{footer}"
        
        return message
    
    def format_status_text(self, status: bool, enabled_text: str = "Включено", 
                          disabled_text: str = "Отключено") -> str:
        """Форматировать текст статуса с иконками"""
        if status:
            return f"🟢 {enabled_text}"
        else:
            return f"🔴 {disabled_text}"
    
    def format_count_text(self, count: int, singular: str, plural: str, 
                         genitive: str = None) -> str:
        """
        Форматировать текст с правильным склонением
        
        Args:
            count: Количество
            singular: Единственное число (1 пользователь)
            plural: Множественное число для 2-4 (2 пользователя)
            genitive: Родительный падеж для 5+ (5 пользователей)
        """
        if genitive is None:
            genitive = plural
        
        if count % 10 == 1 and count % 100 != 11:
            return f"{count} {singular}"
        elif 2 <= count % 10 <= 4 and (count % 100 < 10 or count % 100 >= 20):
            return f"{count} {plural}"
        else:
            return f"{count} {genitive}"
    
    def format_list_items(self, items: list, emoji: str = "•", 
                         max_items: int = None) -> str:
        """
        Форматировать список элементов
        
        Args:
            items: Список элементов для отображения
            emoji: Эмодзи для каждого элемента
            max_items: Максимальное количество элементов (остальные будут скрыты)
        """
        if not items:
            return "<i>Пусто</i>"
        
        display_items = items[:max_items] if max_items else items
        formatted_items = [f"{emoji} {item}" for item in display_items]
        
        result = "\n".join(formatted_items)
        
        if max_items and len(items) > max_items:
            remaining = len(items) - max_items
            result += f"\n<i>... и еще {remaining}</i>"
        
        return result
    
    def create_toggle_button(self, current_state: bool, true_text: str, 
                           false_text: str, callback_prefix: str) -> dict:
        """
        Создать кнопку переключения состояния
        
        Args:
            current_state: Текущее состояние
            true_text: Текст для состояния True
            false_text: Текст для состояния False
            callback_prefix: Префикс для callback_data
        """
        if current_state:
            return {
                "text": f"🔴 {false_text}",
                "callback_data": f"{callback_prefix}_disable"
            }
        else:
            return {
                "text": f"🟢 {true_text}",
                "callback_data": f"{callback_prefix}_enable"
            }
    
    def create_edit_buttons(self, item_id: int, prefix: str) -> list:
        """
        Создать стандартные кнопки редактирования
        
        Args:
            item_id: ID редактируемого элемента
            prefix: Префикс для callback_data
        """
        return [
            [
                {"text": "📝 Редактировать", "callback_data": f"edit_{prefix}_{item_id}"},
                {"text": "🗑 Удалить", "callback_data": f"delete_{prefix}_{item_id}"}
            ]
        ]
    
    def create_management_menu(self, items: list, item_formatter, 
                             add_callback: str, back_callback: str = "admin_back",
                             max_items: int = 10) -> tuple:
        """
        Создать стандартное меню управления элементами
        
        Args:
            items: Список элементов для отображения
            item_formatter: Функция для форматирования элемента в кнопку
            add_callback: Callback для добавления нового элемента
            back_callback: Callback для возврата назад
            max_items: Максимальное количество элементов на странице
        
        Returns:
            tuple: (keyboard, summary_text)
        """
        keyboard = []
        
        # Добавляем кнопки для существующих элементов
        display_items = items[:max_items]
        for item in display_items:
            button = item_formatter(item)
            if button:
                keyboard.append([button])
        
        # Кнопка добавления
        keyboard.append([{"text": "➕ Добавить", "callback_data": add_callback}])
        
        # Кнопка возврата
        keyboard.append(self.create_back_button(back_callback))
        
        # Формируем сводку
        total_count = len(items)
        shown_count = len(display_items)
        
        if total_count > max_items:
            summary = f"Показано {shown_count} из {total_count}"
        else:
            summary = f"Всего: {total_count}"
        
        return InlineKeyboardMarkup(keyboard), summary
    
    def format_datetime_display(self, dt, format_str: str = "%d.%m.%Y %H:%M") -> str:
        """Форматировать дату и время для отображения"""
        if isinstance(dt, str):
            from datetime import datetime
            dt = datetime.fromisoformat(dt)
        
        return dt.strftime(format_str)
    
    def truncate_text(self, text: str, max_length: int = 50, 
                     suffix: str = "...") -> str:
        """Обрезать текст до указанной длины"""
        if len(text) <= max_length:
            return text
        
        return text[:max_length - len(suffix)] + suffix
