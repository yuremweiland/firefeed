import sqlite3
import json

USER_CACHE = {}
CACHE_EXPIRY = 300  # 5 минут

def init_db():
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    
    # Таблица для опубликованных новостей
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS published_news (
            id TEXT PRIMARY KEY,
            published_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Таблица настроек пользователя (обновленная)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_preferences (
            user_id INTEGER PRIMARY KEY,
            subscriptions TEXT,
            language TEXT DEFAULT 'en'
        )
    ''')
    
    conn.commit()
    conn.close()

def is_news_new(news_id: str) -> bool:
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM published_news WHERE id=?", (news_id,))
    exists = cursor.fetchone() is not None
    conn.close()
    return not exists

def mark_as_published(news_id: str):
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute("INSERT INTO published_news (id) VALUES (?)", (news_id,))
    conn.commit()
    conn.close()

def get_user_preferences(user_id):
    """Получаем настройки пользователя"""
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute("SELECT subscriptions FROM user_preferences WHERE user_id=?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result and result[0]:
        return json.loads(result[0])
    return []

def save_user_preferences(user_id, categories):
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    subscriptions = json.dumps(categories)
    
    cursor.execute('''
        INSERT OR REPLACE INTO user_preferences (user_id, subscriptions)
        VALUES (?, ?)
    ''', (user_id, subscriptions))
    
    conn.commit()
    conn.close()

def get_all_users():
    """Получаем список всех пользователей"""
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM user_preferences")
    user_ids = [row[0] for row in cursor.fetchall()]
    conn.close()
    return user_ids

def get_cached_preferences(user_id):
    if user_id in USER_CACHE and time.time() - USER_CACHE[user_id]['timestamp'] < CACHE_EXPIRY:
        return USER_CACHE[user_id]['preferences']
    
    prefs = get_user_preferences(user_id)
    USER_CACHE[user_id] = {
        'preferences': prefs,
        'timestamp': time.time()
    }
    return prefs

def get_user_language(user_id):
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute("SELECT language FROM user_preferences WHERE user_id=?", (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result[0] if result else 'en'  # Возвращаем 'en' по умолчанию

def set_user_language(user_id, lang_code):
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO user_preferences (user_id, language)
        VALUES (?, ?)
    ''', (user_id, lang_code))
    conn.commit()
    conn.close()


def migrate_db():
    """Добавляем новые колонки в существующую базу"""
    conn = sqlite3.connect('news.db')
    cursor = conn.cursor()
    
    try:
        # Проверяем существование колонки 'language'
        cursor.execute("PRAGMA table_info(user_preferences)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'language' not in columns:
            cursor.execute("ALTER TABLE user_preferences ADD COLUMN language TEXT DEFAULT 'en'")
            print("✅ Добавлена колонка 'language'")
            
        # Можно добавить проверку для других колонок
        # if 'new_column' not in columns: ...
        cursor.execute("UPDATE user_preferences SET language = 'en' WHERE language IS NULL")
            
        conn.commit()
    except sqlite3.Error as e:
        print(f"⚠️ Ошибка миграции: {e}")
    finally:
        conn.close()