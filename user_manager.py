import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from config import DB_CONFIG, get_shared_db_pool

logger = logging.getLogger(__name__)

class UserManager:
    def __init__(self):
        pass

    async def get_pool(self):
        return await get_shared_db_pool()

    async def close_pool(self):
        pass

    # --- Асинхронные методы для работы с БД ---

    async def _get_user_settings(self, user_id):
        """Асинхронный метод: Возвращает все настройки пользователя."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT subscriptions, language FROM user_preferences WHERE user_id = %s", (user_id,))
                    result = await cur.fetchone()

                    if result:
                        subscriptions = json.loads(result[0]) if result[0] else []
                        logger.debug(f"[DB] [UserManager] Получены настройки для пользователя {user_id}: subscriptions={subscriptions}, language={result[1]}")
                        return {
                            "subscriptions": subscriptions,
                            "language": result[1]
                        }
                    logger.debug(f"[DB] [UserManager] Настройки для пользователя {user_id} не найдены, возвращаем по умолчанию")
                    return {
                        "subscriptions": [],
                        "language": "en"
                    }
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка получения настроек пользователя {user_id}: {e}")
            return {"subscriptions": [], "language": "en"}

    async def _save_user_settings(self, user_id, subscriptions, language):
        """Асинхронный метод: Сохраняет все настройки пользователя."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # First try to update existing record
                    await cur.execute('''
                        UPDATE user_preferences
                        SET subscriptions = %s, language = %s
                        WHERE user_id = %s
                    ''', (json.dumps(subscriptions), language, user_id))

                    # If no rows were updated, insert new record
                    if cur.rowcount == 0:
                        # First ensure user exists in users table
                        await cur.execute('''
                            INSERT INTO users (id, email, password_hash, language, is_active, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO NOTHING
                        ''', (user_id, f'user{user_id}@telegram.bot', 'dummy_hash', language, True, datetime.utcnow(), datetime.utcnow()))

                        # Now insert preferences
                        await cur.execute('''
                            INSERT INTO user_preferences (user_id, subscriptions, language)
                            VALUES (%s, %s, %s)
                        ''', (user_id, json.dumps(subscriptions), language))

                    # В aiopg транзакции управляются автоматически, commit не нужен
                    logger.debug(f"[DB] [UserManager] Сохранены настройки для пользователя {user_id}: subscriptions={subscriptions}, language={language}")
                    return True
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка сохранения настроек пользователя {user_id}: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def _set_user_language(self, user_id, lang_code):
        """Асинхронный метод: Устанавливает язык пользователя."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        INSERT INTO user_preferences (user_id, language)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET language = EXCLUDED.language
                    ''', (user_id, lang_code))
                    
                    return True
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка установки языка пользователя {user_id}: {e}")
            return False

    async def _get_subscribers_for_category(self, category):
        """Асинхронный метод: Получает подписчиков для определенной категории."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        SELECT user_id, subscriptions, language 
                        FROM user_preferences
                    ''')
                    
                    subscribers = []
                    async for row in cur:
                        user_id, subscriptions_json, language = row
                        
                        try:
                            subscriptions_list = json.loads(subscriptions_json) if subscriptions_json else []
                            
                            if 'all' in subscriptions_list or category in subscriptions_list:
                                user = {
                                    'id': user_id,
                                    'language_code': language if language else 'en'
                                }
                                subscribers.append(user)
                                
                        except json.JSONDecodeError:
                            logger.warning(f"[DB] [UserManager] Invalid JSON for user {user_id}: {subscriptions_json}")
                            continue
                    
                    return subscribers
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка получения подписчиков для категории {category}: {e}")
            return []

    async def _get_all_users(self):
        """Асинхронный метод: Получаем список всех пользователей."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT user_id FROM user_preferences")
                    user_ids = []
                    async for row in cur:
                        user_ids.append(row[0])
                    return user_ids
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка получения списка пользователей: {e}")
            return []

    # --- Публичные асинхронные методы ---

    async def get_user_settings(self, user_id):
        """Асинхронно возвращает все настройки пользователя"""
        return await self._get_user_settings(user_id)

    async def save_user_settings(self, user_id, subscriptions, language):
        """Асинхронно сохраняет все настройки пользователя"""
        return await self._save_user_settings(user_id, subscriptions, language)

    async def set_user_language(self, user_id, lang_code):
        """Асинхронно устанавливает язык пользователя"""
        return await self._set_user_language(user_id, lang_code)

    async def get_user_subscriptions(self, user_id):
        """Асинхронно возвращает только подписки пользователя"""
        settings = await self.get_user_settings(user_id)
        subscriptions = settings["subscriptions"]
        # Если subscriptions - список строк, возвращаем как есть
        # Если список объектов, возвращаем их
        return subscriptions

    async def get_user_language(self, user_id):
        """Асинхронно возвращает только язык пользователя"""
        settings = await self.get_user_settings(user_id)
        return settings["language"]

    async def get_subscribers_for_category(self, category):
        """Асинхронно получает подписчиков для определенной категории"""
        return await self._get_subscribers_for_category(category)

    async def get_all_users(self):
        """Асинхронно получаем список всех пользователей"""
        return await self._get_all_users()

    # --- Методы для работы с привязкой Telegram ---

    async def generate_telegram_link_code(self, user_id: int) -> str:
        """Генерирует код для привязки Telegram аккаунта"""
        import secrets
        link_code = secrets.token_urlsafe(16)
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Удаляем старые коды для этого пользователя
                    await cur.execute("DELETE FROM user_telegram_links WHERE user_id = %s AND linked_at IS NULL", (user_id,))
                    # Создаем новый код
                    await cur.execute('''
                        INSERT INTO user_telegram_links (user_id, link_code, created_at)
                        VALUES (%s, %s, %s)
                    ''', (user_id, link_code, datetime.utcnow()))
                    return link_code
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка генерации кода привязки для {user_id}: {e}")
            return None

    async def confirm_telegram_link(self, telegram_id: int, link_code: str) -> bool:
        """Подтверждает привязку Telegram аккаунта по коду"""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Находим запись с кодом
                    await cur.execute('''
                        SELECT user_id FROM user_telegram_links
                        WHERE link_code = %s AND linked_at IS NULL
                        AND created_at > %s
                    ''', (link_code, datetime.utcnow() - timedelta(hours=24)))

                    result = await cur.fetchone()
                    if not result:
                        return False

                    user_id = result[0]

                    # Проверяем, не привязан ли уже этот Telegram ID
                    await cur.execute("SELECT 1 FROM user_telegram_links WHERE telegram_id = %s AND linked_at IS NOT NULL", (telegram_id,))
                    if await cur.fetchone():
                        return False  # Уже привязан

                    # Обновляем запись
                    await cur.execute('''
                        UPDATE user_telegram_links
                        SET telegram_id = %s, linked_at = %s
                        WHERE link_code = %s
                    ''', (telegram_id, datetime.utcnow(), link_code))

                    return True
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка подтверждения привязки: {e}")
            return False

    async def get_user_by_telegram_id(self, telegram_id: int) -> Optional[Dict[str, Any]]:
        """Получает пользователя по Telegram ID"""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        SELECT u.* FROM users u
                        JOIN user_telegram_links utl ON u.id = utl.user_id
                        WHERE utl.telegram_id = %s AND utl.linked_at IS NOT NULL
                    ''', (telegram_id,))

                    result = await cur.fetchone()
                    if result:
                        columns = [desc[0] for desc in cur.description]
                        return dict(zip(columns, result))
                    return None
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка получения пользователя по Telegram ID {telegram_id}: {e}")
            return None

    async def unlink_telegram(self, user_id: int) -> bool:
        """Отвязывает Telegram аккаунт от пользователя"""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("UPDATE user_telegram_links SET linked_at = NULL, telegram_id = NULL WHERE user_id = %s", (user_id,))
                    return cur.rowcount > 0
        except Exception as e:
            logger.error(f"[DB] [UserManager] Ошибка отвязки Telegram для {user_id}: {e}")
            return False