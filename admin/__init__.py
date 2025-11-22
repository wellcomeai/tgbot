"""
Модульная админ-панель для Telegram бота
"""

from .base import AdminBaseMixin
from .handlers import HandlersMixin
from .renewals import RenewalMixin
from .broadcasts import BroadcastsMixin
from .messages import MessagesMixin
from .statistics import StatisticsMixin
from .mass_broadcasts import MassBroadcastsMixin
from .paid_broadcasts import PaidBroadcastsMixin
from .paid_buttons import PaidButtonsMixin
from .paid_mass_broadcasts import PaidMassBroadcastsMixin
from .buttons import ButtonsMixin
from .utils import UtilsMixin
from .media_albums import MediaAlbumsMixin

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
    BroadcastsMixin,
    MessagesMixin,
    StatisticsMixin,
    MassBroadcastsMixin,
    PaidBroadcastsMixin,
    PaidButtonsMixin,
    PaidMassBroadcastsMixin,
    ButtonsMixin,
    UtilsMixin,
    MediaAlbumsMixin,
    *([MenuMixin, InputMixin, NavigationMixin] if MIXINS_AVAILABLE else [])
):
    """
    Главный класс админ-панели, объединяющий все миксины
    
    Включает:
    - AdminBaseMixin: базовая функциональность
    - HandlersMixin: обработка событий
    - RenewalMixin: управление продлением подписок
    - BroadcastsMixin: управление основными рассылками
    - MessagesMixin: управление сообщениями бота
    - StatisticsMixin: статистика и аналитика
    - MassBroadcastsMixin: массовые рассылки
    - PaidBroadcastsMixin: рассылки для оплативших
    - PaidButtonsMixin: кнопки платных рассылок
    - PaidMassBroadcastsMixin: массовые рассылки для оплативших
    - ButtonsMixin: управление кнопками всех типов
    - UtilsMixin: вспомогательные методы
    - MediaAlbumsMixin: управление медиа-альбомами
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
        
        # Инициализируем NavigationMixin если доступен
        if MIXINS_AVAILABLE:
            try:
                NavigationMixin.__init__(self)
            except:
                pass
        
        # Инициализируем хранилища для медиа-альбомов
        self.media_album_drafts = {}  # Для основных сообщений рассылки
        self.mass_media_album_drafts = {}  # Для массовых рассылок
        
        # Остальные миксины не требуют отдельной инициализации,
        # так как они не содержат __init__ методов

__all__ = ['AdminPanel']
