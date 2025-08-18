import mysql.connector
import json
from functools import lru_cache
import time

DB_CONFIG = {
    'host': "localhost",
    'user': "firefeed_db_usr",
    'password': "AixLUaCqe68v9oO8",
    'database': "firefeed_db",
    'charset': "utf8mb4"
}

USER_CACHE = {}
CACHE_EXPIRY = 300  # 5 минут

def get_connection():
    """Создает и возвращает соединение с MySQL"""
    return mysql.connector.connect(**DB_CONFIG)

def init_db():
    """Инициализация структуры базы данных"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Таблица для опубликованных новостей (id изменен на VARCHAR)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS published_news (
            id VARCHAR(255) PRIMARY KEY,
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица настроек пользователя
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id BIGINT PRIMARY KEY,
            subscriptions VARCHAR(255),
            language VARCHAR(2) DEFAULT 'en'
        )
    ''')
    
    conn.commit()
    conn.close()

def is_news_new(news_id: str) -> bool:
    """Проверяет новость более эффективно"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Используем EXISTS для оптимальной проверки
    cursor.execute("SELECT NOT EXISTS(SELECT 1 FROM published_news WHERE id = %s)", (news_id,))
    is_new = cursor.fetchone()[0]  # Вернет 1 если новости нет, 0 если есть
    
    conn.close()
    return bool(is_new)

def mark_as_published(news_id: str):
    """Помечает новость как опубликованную с использованием INSERT IGNORE"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Используем INSERT IGNORE для пропуска дубликатов
    cursor.execute("INSERT IGNORE INTO published_news (id) VALUES (%s)", (news_id,))
    
    conn.commit()
    conn.close()

def get_user_settings(user_id):
    """Возвращает все настройки пользователя"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT subscriptions, language FROM user_preferences WHERE user_id = %s", (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            "subscriptions": json.loads(result[0]) if result[0] else [],
            "language": result[1]
        }
    return {
        "subscriptions": [],
        "language": "en"
    }

def save_user_settings(user_id, subscriptions, language):
    """Сохраняет все настройки пользователя"""
    conn = get_connection()
    cursor = conn.cursor()
    
    # Используем ON DUPLICATE KEY UPDATE вместо INSERT OR REPLACE
    cursor.execute('''
        INSERT INTO user_preferences (user_id, subscriptions, language)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            subscriptions = VALUES(subscriptions),
            language = VALUES(language)
    ''', (user_id, json.dumps(subscriptions), language))
    
    conn.commit()
    conn.close()

def get_all_users():
    """Получаем список всех пользователей"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_preferences")
    user_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return user_ids

def get_cached_preferences(user_id):
    """Кеширование настроек пользователя"""
    if user_id in USER_CACHE and time.time() - USER_CACHE[user_id]['timestamp'] < CACHE_EXPIRY:
        return USER_CACHE[user_id]['preferences']
    
    prefs = get_user_preferences(user_id)
    USER_CACHE[user_id] = {
        'preferences': prefs,
        'timestamp': time.time()
    }
    return prefs

@lru_cache(maxsize=100)
def get_user_settings_cached(user_id):
    """Кешированная версия получения настроек"""
    return get_user_settings(user_id)

def get_user_preferences(user_id):
    """Возвращает только подписки пользователя"""
    return get_user_settings_cached(user_id)["subscriptions"]

def get_user_language(user_id):
    """Возвращает только язык пользователя"""
    return get_user_settings_cached(user_id)["language"]

def set_user_language(user_id, lang_code):
    """Устанавливает язык пользователя"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO user_preferences (user_id, language)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE language = VALUES(language)
    ''', (user_id, lang_code))
    conn.commit()
    conn.close()

def get_subscribers_for_category(category):
    """Получает подписчиков для определенной категории"""
    conn = get_connection()
    cursor = conn.cursor()
    
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
    
    conn.close()
    return subscribers