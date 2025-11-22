"""
Database package - modular database implementation using mixins.

This module assembles all database functionality from separate mixin classes
into a single Database class, maintaining 100% backward compatibility with
the original monolithic database.py implementation.
"""

from .base import DatabaseBase
from .users import UsersMixin
from .messages import MessagesMixin
from .paid_messages import PaidMessagesMixin
from .buttons import ButtonsMixin
from .broadcasts import BroadcastsMixin
from .payments import PaymentsMixin
from .funnel import FunnelMixin
from .settings import SettingsMixin
from .utils import UtilsMixin
from .media_albums import MediaAlbumsMixin


class Database(
    DatabaseBase,
    UsersMixin,
    MessagesMixin,
    PaidMessagesMixin,
    ButtonsMixin,
    BroadcastsMixin,
    PaymentsMixin,
    FunnelMixin,
    SettingsMixin,
    UtilsMixin,
    MediaAlbumsMixin
):
    """
    Main Database class that combines all functionality from mixin classes.

    Inherits from:
    - DatabaseBase: Core initialization and connection management
    - UsersMixin: User management operations (17 methods)
    - MessagesMixin: Broadcast message operations (13 methods)
    - PaidMessagesMixin: Paid user message operations (9 methods)
    - ButtonsMixin: Button management for all message types (27 methods)
    - BroadcastsMixin: Mass broadcast scheduling (12 methods)
    - PaymentsMixin: Payment tracking and renewals (6 methods)
    - FunnelMixin: Analytics and funnel tracking (6 methods)
    - SettingsMixin: Bot configuration settings (6 methods)
    - UtilsMixin: Utility functions (2 methods)
    - MediaAlbumsMixin: Media albums management (12 methods)

    Total: 110 methods across all mixins
    """

    def __init__(self, db_path=None):
        """
        Initialize the Database instance.

        Args:
            db_path: Optional path to database file. If not provided,
                    uses Render disk path or default 'bot.db'
        """
        # Call base class initializer - this handles all setup
        DatabaseBase.__init__(self, db_path)


__all__ = ['Database']
