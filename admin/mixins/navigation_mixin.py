"""
Миксин для навигации между меню админ-панели
"""

from telegram import Update
from telegram.ext import ContextTypes
import logging

logger = logging.getLogger(__name__)


class NavigationMixin:
    """Миксин для безопасной навигации между меню"""
    
    def __init__(self):
        """Инициализация навигационных данных"""
        self._navigation_stack = {}  # Стек навигации для каждого пользователя
        self._max_stack_size = 10   # Максимальный размер стека
    
    def push_navigation(self, user_id: int, menu_name: str, **kwargs):
        """
        Добавить пункт в стек навигации
        
        Args:
            user_id: ID пользователя
            menu_name: Название меню
            **kwargs: Дополнительные параметры для восстановления меню
        """
        if user_id not in self._navigation_stack:
            self._navigation_stack[user_id] = []
        
        stack = self._navigation_stack[user_id]
        
        # Добавляем новый пункт
        stack.append({
            'menu_name': menu_name,
            'params': kwargs
        })
        
        # Ограничиваем размер стека
        if len(stack) > self._max_stack_size:
            stack.pop(0)
    
    def pop_navigation(self, user_id: int) -> dict:
        """
        Извлечь последний пункт из стека навигации
        
        Args:
            user_id: ID пользователя
            
        Returns:
            dict: Данные меню или None если стек пуст
        """
        if user_id not in self._navigation_stack:
            return None
        
        stack = self._navigation_stack[user_id]
        if stack:
            return stack.pop()
        
        return None
    
    def get_previous_menu(self, user_id: int) -> dict:
        """
        Получить данные предыдущего меню без извлечения из стека
        
        Args:
            user_id: ID пользователя
            
        Returns:
            dict: Данные меню или None если стек пуст
        """
        if user_id not in self._navigation_stack:
            return None
        
        stack = self._navigation_stack[user_id]
        if stack:
            return stack[-1]
        
        return None
    
    def clear_navigation(self, user_id: int):
        """Очистить стек навигации для пользователя"""
        if user_id in self._navigation_stack:
            self._navigation_stack[user_id].clear()
    
    async def navigate_back(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """
        Вернуться к предыдущему меню
        
        Args:
            update: Объект обновления Telegram
            context: Контекст бота
        """
        user_id = update.effective_user.id
        previous_menu = self.pop_navigation(user_id)
        
        if not previous_menu:
            # Если стек пуст, возвращаемся в главное меню
            await self.show_main_menu(update, context)
            return
        
        menu_name = previous_menu['menu_name']
        params = previous_menu.get('params', {})
        
        # Вызываем соответствующий метод
        try:
            if hasattr(self, f'show_{menu_name}'):
                method = getattr(self, f'show_{menu_name}')
                await method(update, context, **params)
            else:
                # Если метод не найден, возвращаемся в главное меню
                logger.warning(f"Метод show_{menu_name} не найден, возвращаемся в главное меню")
                await self.show_main_menu(update, context)
        except Exception as e:
            logger.error(f"Ошибка при навигации к {menu_name}: {e}")
            await self.show_main_menu(update, context)
    
    async def safe_navigate_to_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                  menu_method, *args, push_current: bool = True, 
                                  current_menu: str = None, **kwargs):
        """
        Безопасная навигация к указанному меню с сохранением текущего в стеке
        
        Args:
            update: Объект обновления Telegram
            context: Контекст бота
            menu_method: Метод для показа целевого меню
            *args: Позиционные аргументы для метода
            push_current: Сохранять ли текущее меню в стеке
            current_menu: Название текущего меню для стека
            **kwargs: Именованные аргументы для метода
        """
        user_id = update.effective_user.id
        
        # Сохраняем текущее меню в стеке если нужно
        if push_current and current_menu:
            self.push_navigation(user_id, current_menu, **kwargs)
        
        # Переходим к новому меню
        try:
            await menu_method(update, context, *args, **kwargs)
        except Exception as e:
            logger.error(f"Ошибка при переходе к меню {menu_method.__name__}: {e}")
            await self.show_error_message(update, context, "❌ Ошибка при переходе к меню")
    
    def create_breadcrumb_text(self, user_id: int, current_menu: str) -> str:
        """
        Создать текст навигационной цепочки (хлебные крошки)
        
        Args:
            user_id: ID пользователя  
            current_menu: Название текущего меню
            
        Returns:
            str: Форматированный текст навигации
        """
        if user_id not in self._navigation_stack:
            return ""
        
        stack = self._navigation_stack[user_id]
        if not stack:
            return ""
        
        # Маппинг названий меню к человекочитаемым названиям
        menu_names = {
            'main_menu': 'Главное меню',
            'broadcast_menu': 'Рассылки',
            'message_edit': 'Редактирование сообщения',
            'button_management': 'Управление кнопками',
            'welcome_edit': 'Приветствие',
            'goodbye_edit': 'Прощание',
            'statistics': 'Статистика',
            'users_list': 'Пользователи',
            'payment_stats': 'Платежи',
            'mass_broadcast': 'Массовая рассылка'
        }
        
        # Формируем цепочку
        breadcrumbs = []
        for item in stack[-3:]:  # Показываем последние 3 уровня
            menu_name = item['menu_name']
            display_name = menu_names.get(menu_name, menu_name.replace('_', ' ').title())
            breadcrumbs.append(display_name)
        
        # Добавляем текущее меню
        current_display = menu_names.get(current_menu, current_menu.replace('_', ' ').title())
        breadcrumbs.append(current_display)
        
        return " → ".join(breadcrumbs)
    
    async def handle_navigation_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                       callback_data: str) -> bool:
        """
        Обработать навигационные callback'и
        
        Args:
            update: Объект обновления Telegram
            context: Контекст бота
            callback_data: Данные callback
            
        Returns:
            bool: True если callback обработан, False иначе
        """
        if callback_data == "nav_back":
            await self.navigate_back(update, context)
            return True
        elif callback_data == "nav_main":
            user_id = update.effective_user.id
            self.clear_navigation(user_id)
            await self.show_main_menu(update, context)
            return True
        elif callback_data.startswith("nav_to_"):
            # Навигация к конкретному меню
            menu_name = callback_data[7:]  # Убираем префикс "nav_to_"
            await self._navigate_to_menu_by_name(update, context, menu_name)
            return True
        
        return False
    
    async def _navigate_to_menu_by_name(self, update: Update, context: ContextTypes.DEFAULT_TYPE,
                                      menu_name: str):
        """
        Навигация к меню по его названию
        
        Args:
            update: Объект обновления Telegram
            context: Контекст бота
            menu_name: Название меню
        """
        # Маппинг названий к методам
        menu_methods = {
            'main': self.show_main_menu,
            'broadcast': self.show_broadcast_menu,
            'statistics': self.show_statistics,
            'users': self.show_users_list,
            'welcome': self.show_welcome_edit,
            'goodbye': self.show_goodbye_edit,
            'success_message': self.show_success_message_edit,
            'payment_message': self.show_payment_message_edit,
            'payment_stats': self.show_payment_statistics,
            'mass_broadcast': self.show_send_all_menu,
            'scheduled_broadcasts': self.show_scheduled_broadcasts
        }
        
        method = menu_methods.get(menu_name)
        if method:
            try:
                await method(update, context)
            except Exception as e:
                logger.error(f"Ошибка при навигации к {menu_name}: {e}")
                await self.show_error_message(update, context, "❌ Ошибка навигации")
        else:
            logger.warning(f"Неизвестное меню для навигации: {menu_name}")
            await self.show_main_menu(update, context)
    
    def get_navigation_stats(self, user_id: int = None) -> dict:
        """
        Получить статистику навигации
        
        Args:
            user_id: ID пользователя (если None, возвращает общую статистику)
            
        Returns:
            dict: Статистика навигации
        """
        if user_id:
            stack = self._navigation_stack.get(user_id, [])
            return {
                'user_id': user_id,
                'stack_size': len(stack),
                'current_path': [item['menu_name'] for item in stack]
            }
        else:
            total_users = len(self._navigation_stack)
            total_items = sum(len(stack) for stack in self._navigation_stack.values())
            avg_depth = total_items / total_users if total_users > 0 else 0
            
            return {
                'total_users_with_navigation': total_users,
                'total_navigation_items': total_items,
                'average_navigation_depth': round(avg_depth, 2),
                'max_stack_size': self._max_stack_size
            }
    
    def cleanup_navigation(self, max_age_hours: int = 24):
        """
        Очистить старые данные навигации
        
        Args:
            max_age_hours: Максимальный возраст данных в часах
        """
        # В данной реализации мы не храним временные метки
        # Но можно расширить функциональность при необходимости
        logger.info(f"Навигационная очистка: активных пользователей {len(self._navigation_stack)}")
