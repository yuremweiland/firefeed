import psycopg2
from psycopg2 import Error
import asyncio
import json
import time
from config import DB_CONFIG

class UserManager:
    def __init__(self):
        # Атрибуты self.connection и self.last_used больше не используются
        # для методов, вызываемых через run_in_executor
        pass

    # --- Вспомогательные приватные методы для run_in_executor ---
    # Эти методы будут выполняться в отдельных потоках.
    # Они принимают все необходимые данные как аргументы и не полагаются на self.connection.

    def _get_user_settings(self, user_id):
        """Вспомогательный метод: Возвращает все настройки пользователя. Выполняется в потоке."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute("SELECT subscriptions, language FROM user_preferences WHERE user_id = %s", (user_id,))
            result = cursor.fetchone()
            
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
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _save_user_settings(self, user_id, subscriptions, language):
        """Вспомогательный метод: Сохраняет все настройки пользователя. Выполняется в потоке."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute('''
                INSERT INTO user_preferences (user_id, subscriptions, language)
                VALUES (%s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE SET
                    subscriptions = EXCLUDED.subscriptions,
                    language = EXCLUDED.language
            ''', (user_id, json.dumps(subscriptions), language))
            
            connection.commit()
            return True
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка сохранения настроек пользователя {user_id}: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _set_user_language(self, user_id, lang_code):
        """Вспомогательный метод: Устанавливает язык пользователя. Выполняется в потоке."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute('''
                INSERT INTO user_preferences (user_id, language)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE SET language = EXCLUDED.language
            ''', (user_id, lang_code))
            
            connection.commit()
            return True
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка установки языка пользователя {user_id}: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _get_subscribers_for_category(self, category):
        """Вспомогательный метод: Получает подписчиков для определенной категории. Выполняется в потоке."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute('''
                SELECT user_id, subscriptions, language 
                FROM user_preferences
            ''')
            
            subscribers = []
            for row in cursor.fetchall():
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
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _get_all_users(self):
        """Вспомогательный метод: Получаем список всех пользователей. Выполняется в потоке."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            cursor.execute("SELECT user_id FROM user_preferences")
            user_ids = [row[0] for row in cursor.fetchall()]
            return user_ids
        except Exception as e:
            print(f"[DB] [UserManager] Ошибка получения списка пользователей: {e}")
            return []
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    # --- Публичные асинхронные методы ---
    # Эти методы будут вызываться из вашего асинхронного кода (main.py).
    # Они оборачивают вспомогательные методы в run_in_executor.

    async def get_user_settings(self, user_id):
        """Асинхронно возвращает все настройки пользователя"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_user_settings, user_id)

    async def save_user_settings(self, user_id, subscriptions, language):
        """Асинхронно сохраняет все настройки пользователя"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._save_user_settings, user_id, subscriptions, language)

    async def set_user_language(self, user_id, lang_code):
        """Асинхронно устанавливает язык пользователя"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._set_user_language, user_id, lang_code)

    # get_user_subscriptions теперь тоже должен быть async и использовать run_in_executor
    # Он будет вызывать асинхронный get_user_settings
    async def get_user_subscriptions(self, user_id):
        """Асинхронно возвращает только подписки пользователя"""
        settings = await self.get_user_settings(user_id)
        return settings["subscriptions"]

    # get_user_language теперь тоже должен быть async и использовать run_in_executor
    # Он будет вызывать асинхронный get_user_settings
    async def get_user_language(self, user_id):
        """Асинхронно возвращает только язык пользователя"""
        settings = await self.get_user_settings(user_id)
        return settings["language"]

    async def get_subscribers_for_category(self, category):
        """Асинхронно получает подписчиков для определенной категории"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_subscribers_for_category, category)

    async def get_all_users(self):
        """Асинхронно получаем список всех пользователей"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_all_users)