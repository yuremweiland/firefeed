import mysql.connector
from mysql.connector import Error
import json
import time
from config import DB_CONFIG

class UserManager:
    def __init__(self):
        self.connection = None
        self.last_used = 0

    def get_db_connection(self):
        """Установить или восстановить соединение с базой данных"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**DB_CONFIG)
                print("✅ Подключение к БД установлено")
            return self.connection
        except Error as e:
            print(f"❌ Ошибка подключения к MySQL: {e}")
            return None

    def get_user_settings(self, user_id):
        """Возвращает все настройки пользователя"""
        conn = self.get_db_connection()
        if not conn:
            return {"subscriptions": [], "language": "en"}
            
        cursor = conn.cursor()
        try:
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
            print(f"Ошибка получения настроек пользователя {user_id}: {e}")
            return {"subscriptions": [], "language": "en"}
        finally:
            cursor.close()  # Обязательно закрываем курсор!

    def save_user_settings(self, user_id, subscriptions, language):
        """Сохраняет все настройки пользователя"""
        conn = self.get_db_connection()
        if not conn:
            return False
            
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO user_preferences (user_id, subscriptions, language)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    subscriptions = VALUES(subscriptions),
                    language = VALUES(language)
            ''', (user_id, json.dumps(subscriptions), language))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка сохранения настроек пользователя {user_id}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()  # Обязательно закрываем курсор!

    def get_user_subscriptions(self, user_id):
        """Возвращает только подписки пользователя"""
        return self.get_user_settings(user_id)["subscriptions"]

    def get_user_language(self, user_id):
        """Возвращает только язык пользователя"""
        return self.get_user_settings(user_id)["language"]

    def set_user_language(self, user_id, lang_code):
        """Устанавливает язык пользователя"""
        conn = self.get_db_connection()
        if not conn:
            return False
            
        cursor = conn.cursor()
        try:
            cursor.execute('''
                INSERT INTO user_preferences (user_id, language)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE language = VALUES(language)
            ''', (user_id, lang_code))
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Ошибка установки языка пользователя {user_id}: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()  # Обязательно закрываем курсор!

    def get_subscribers_for_category(self, category):
        """Получает подписчиков для определенной категории"""
        conn = self.get_db_connection()
        if not conn:
            return []
            
        cursor = conn.cursor()
        try:
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
                    print(f"Invalid JSON for user {user_id}: {subscriptions_json}")
                    continue
            
            return subscribers
        except Exception as e:
            print(f"Ошибка получения подписчиков для категории {category}: {e}")
            return []
        finally:
            cursor.close()  # Обязательно закрываем курсор!

    def get_all_users(self):
        """Получаем список всех пользователей"""
        conn = self.get_db_connection()
        if not conn:
            return []
            
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT user_id FROM user_preferences")
            user_ids = [row[0] for row in cursor.fetchall()]
            return user_ids
        except Exception as e:
            print(f"Ошибка получения списка пользователей: {e}")
            return []
        finally:
            cursor.close()  # Обязательно закрываем курсор!

    def close_connection(self):
        """Закрыть соединение с базой данных"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.connection = None
            print("🔌 Соединение с БД закрыто")