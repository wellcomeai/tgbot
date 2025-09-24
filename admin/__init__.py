"""
Модульная админ-панель для Telegram бота
"""

from .base import AdminBaseMixin
from .handlers import HandlersMixin
from .renewals import RenewalMixin

# Проверяем наличие дополнительных миксинов
try:
    from .mixins import MenuMixin, InputMixin, NavigationMixin
    MIXINS_AVAILABLE = True
except ImportError:
    MIXINS_AVAILABLE = False

class AdminPanel(
    AdminBaseMixin,
    HandlersMixin, 
    RenewalMixin,
    *([MenuMixin, InputMixin, NavigationMixin] if MIXINS_AVAILABLE else [])
):
    """
    Главный класс админ-панели, объединяющий все миксины
    
    Включает:
    - AdminBaseMixin: базовая функциональность
    - HandlersMixin: обработка событий
    - RenewalMixin: управление продлением подписок
    - MenuMixin, InputMixin, NavigationMixin: дополнительные миксины (если доступны)
    """
    
    def __init__(self, db, admin_chat_id):
        """
        Инициализация админ-панели
        
        Args:
            db: экземпляр базы данных
            admin_chat_id: ID чата администратора
        """
        # Инициализируем базовый миксин (он содержит __init__)
        AdminBaseMixin.__init__(self, db, admin_chat_id)
        
        # Остальные миксины не требуют отдельной инициализации,
        # так как они не содержат __init__ методов

__all__ = ['AdminPanel']
