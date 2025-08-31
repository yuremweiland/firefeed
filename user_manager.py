import aiopg
import asyncio
import json
from config import DB_CONFIG

class UserManager:
    def __init__(self):
        self.pool = None
        self._init_lock = asyncio.Lock()  # Для предотвращения race condition при инициализации

    async def init_pool(self):
        """Инициализация пула соединений"""
        if self.pool is None:
            async with self._init_lock:  # Блокировка для предотвращения race condition
                if self.pool is None:  # Проверка еще раз внутри блокировки
                    self.pool = await aiopg.create_pool(**DB_CONFIG)
        return self.pool

    async def close_pool(self):
        """Закрытие пула соединений"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    # --- Асинхронные методы для работы с БД ---

    async def _get_user_settings(self, user_id):
        """Асинхронный метод: Возвращает все настройки пользователя."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT subscriptions, language FROM user_preferences WHERE user_id = %s", (user_id,))
                    result = await cur.fetchone()
                    
                    if result:
                        return {
                            "subscriptions": json.loads(result[0]) if result[0] else [],
                            "language": result[1]
                        }
                    return {
                        "subscriptions": [],
                        "language": "en"
                    }
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка получения настроек пользователя {user_id}: {e}")
            return {"subscriptions": [], "language": "en"}

    async def _save_user_settings(self, user_id, subscriptions, language):
        """Асинхронный метод: Сохраняет все настройки пользователя."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        INSERT INTO user_preferences (user_id, subscriptions, language)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET
                            subscriptions = EXCLUDED.subscriptions,
                            language = EXCLUDED.language
                    ''', (user_id, json.dumps(subscriptions), language))
                    
                    # В aiopg транзакции управляются автоматически, commit не нужен
                    return True
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка сохранения настроек пользователя {user_id}: {e}")
            return False

    async def _set_user_language(self, user_id, lang_code):
        """Асинхронный метод: Устанавливает язык пользователя."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute('''
                        INSERT INTO user_preferences (user_id, language)
                        VALUES (%s, %s)
                        ON CONFLICT (user_id) DO UPDATE SET language = EXCLUDED.language
                    ''', (user_id, lang_code))
                    
                    # В aiopg транзакции управляются автоматически, commit не нужен
                    return True
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка установки языка пользователя {user_id}: {e}")
            return False

    async def _get_subscribers_for_category(self, category):
        """Асинхронный метод: Получает подписчиков для определенной категории."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
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
                            print(f"[DB] [UserManager] Invalid JSON for user {user_id}: {subscriptions_json}")
                            continue
                    
                    return subscribers
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка получения подписчиков для категории {category}: {e}")
            return []

    async def _get_all_users(self):
        """Асинхронный метод: Получаем список всех пользователей."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT user_id FROM user_preferences")
                    user_ids = []
                    async for row in cur:
                        user_ids.append(row[0])
                    return user_ids
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка получения списка пользователей: {e}")
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
        return settings["subscriptions"]

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