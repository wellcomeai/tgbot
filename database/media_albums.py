"""
Media albums module for database operations
"""

import sqlite3
import logging

logger = logging.getLogger(__name__)


class MediaAlbumsMixin:
    """Mixin for media albums database operations"""

    # === ОСНОВНЫЕ СООБЩЕНИЯ РАССЫЛКИ ===

    def get_message_media_album(self, message_number):
        """
        Получение медиа-альбома для сообщения рассылки
        
        Returns:
            List[Tuple]: [(id, media_type, media_url, position), ...]
            Отсортировано по position
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, media_type, media_url, position
                FROM message_media_albums
                WHERE message_number = ?
                ORDER BY position ASC
            ''', (message_number,))
            
            media_list = cursor.fetchall()
            return media_list
        except Exception as e:
            logger.error(f"❌ Ошибка при получении медиа-альбома для сообщения {message_number}: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def add_media_to_album(self, message_number, media_type, media_url, position):
        """
        Добавление медиа в альбом сообщения
        
        Args:
            message_number: номер сообщения
            media_type: 'photo' или 'video'
            media_url: URL или file_id медиа
            position: порядковый номер (1-10)
        
        Returns:
            int: id добавленного медиа или None при ошибке
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO message_media_albums (message_number, media_type, media_url, position)
                VALUES (?, ?, ?, ?)
            ''', (message_number, media_type, media_url, position))

            media_id = cursor.lastrowid
            conn.commit()
            logger.info(f"✅ Добавлено медиа #{media_id} ({media_type}) в альбом сообщения {message_number}")
            return media_id
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении медиа в альбом сообщения {message_number}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            if conn:
                conn.close()

    def delete_message_media_album(self, message_number):
        """
        Удаление всего медиа-альбома сообщения
        
        Args:
            message_number: номер сообщения
        
        Returns:
            int: количество удаленных медиа
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                DELETE FROM message_media_albums
                WHERE message_number = ?
            ''', (message_number,))

            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.info(f"🗑️ Удален медиа-альбом сообщения {message_number} ({deleted_count} файлов)")
            
            return deleted_count
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении медиа-альбома сообщения {message_number}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return 0
        finally:
            if conn:
                conn.close()

    def has_media_album(self, message_number):
        """
        Проверка наличия медиа-альбома у сообщения
        
        Returns:
            bool: True если есть хотя бы одно медиа
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT COUNT(*) FROM message_media_albums
                WHERE message_number = ?
            ''', (message_number,))
            
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке медиа-альбома сообщения {message_number}: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_media_album_stats(self, message_number):
        """
        Получение статистики по медиа-альбому
        
        Returns:
            dict: {'total': int, 'photos': int, 'videos': int}
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN media_type = 'photo' THEN 1 ELSE 0 END) as photos,
                    SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as videos
                FROM message_media_albums
                WHERE message_number = ?
            ''', (message_number,))
            
            result = cursor.fetchone()
            return {
                'total': result[0] or 0,
                'photos': result[1] or 0,
                'videos': result[2] or 0
            }
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики медиа-альбома сообщения {message_number}: {e}")
            return {'total': 0, 'photos': 0, 'videos': 0}
        finally:
            if conn:
                conn.close()

    # === МАССОВЫЕ РАССЫЛКИ ===

    def get_scheduled_broadcast_media_album(self, broadcast_id):
        """
        Получение медиа-альбома для запланированной массовой рассылки
        
        Returns:
            List[Tuple]: [(id, media_type, media_url, position), ...]
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT id, media_type, media_url, position
                FROM scheduled_broadcast_media
                WHERE broadcast_id = ?
                ORDER BY position ASC
            ''', (broadcast_id,))
            
            media_list = cursor.fetchall()
            return media_list
        except Exception as e:
            logger.error(f"❌ Ошибка при получении медиа-альбома для рассылки {broadcast_id}: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def add_scheduled_broadcast_media(self, broadcast_id, media_type, media_url, position):
        """
        Добавление медиа в альбом запланированной рассылки
        
        Args:
            broadcast_id: id рассылки
            media_type: 'photo' или 'video'
            media_url: URL или file_id медиа
            position: порядковый номер (1-10)
        
        Returns:
            int: id добавленного медиа или None при ошибке
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO scheduled_broadcast_media (broadcast_id, media_type, media_url, position)
                VALUES (?, ?, ?, ?)
            ''', (broadcast_id, media_type, media_url, position))

            media_id = cursor.lastrowid
            conn.commit()
            logger.info(f"✅ Добавлено медиа #{media_id} ({media_type}) в альбом рассылки {broadcast_id}")
            return media_id
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении медиа в альбом рассылки {broadcast_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return None
        finally:
            if conn:
                conn.close()

    def delete_scheduled_broadcast_media_album(self, broadcast_id):
        """
        Удаление всего медиа-альбома запланированной рассылки
        
        Args:
            broadcast_id: id рассылки
        
        Returns:
            int: количество удаленных медиа
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                DELETE FROM scheduled_broadcast_media
                WHERE broadcast_id = ?
            ''', (broadcast_id,))

            deleted_count = cursor.rowcount
            conn.commit()
            
            if deleted_count > 0:
                logger.info(f"🗑️ Удален медиа-альбом рассылки {broadcast_id} ({deleted_count} файлов)")
            
            return deleted_count
        except Exception as e:
            logger.error(f"❌ Ошибка при удалении медиа-альбома рассылки {broadcast_id}: {e}")
            try:
                conn.rollback()
            except:
                pass
            return 0
        finally:
            if conn:
                conn.close()

    def has_scheduled_broadcast_media_album(self, broadcast_id):
        """
        Проверка наличия медиа-альбома у запланированной рассылки
        
        Returns:
            bool: True если есть хотя бы одно медиа
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT COUNT(*) FROM scheduled_broadcast_media
                WHERE broadcast_id = ?
            ''', (broadcast_id,))
            
            count = cursor.fetchone()[0]
            return count > 0
        except Exception as e:
            logger.error(f"❌ Ошибка при проверке медиа-альбома рассылки {broadcast_id}: {e}")
            return False
        finally:
            if conn:
                conn.close()

    def get_scheduled_broadcast_media_stats(self, broadcast_id):
        """
        Получение статистики по медиа-альбому запланированной рассылки
        
        Returns:
            dict: {'total': int, 'photos': int, 'videos': int}
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(CASE WHEN media_type = 'photo' THEN 1 ELSE 0 END) as photos,
                    SUM(CASE WHEN media_type = 'video' THEN 1 ELSE 0 END) as videos
                FROM scheduled_broadcast_media
                WHERE broadcast_id = ?
            ''', (broadcast_id,))
            
            result = cursor.fetchone()
            return {
                'total': result[0] or 0,
                'photos': result[1] or 0,
                'videos': result[2] or 0
            }
        except Exception as e:
            logger.error(f"❌ Ошибка при получении статистики медиа-альбома рассылки {broadcast_id}: {e}")
            return {'total': 0, 'photos': 0, 'videos': 0}
        finally:
            if conn:
                conn.close()
