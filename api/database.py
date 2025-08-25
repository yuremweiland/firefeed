import psycopg2
from psycopg2 import Error
import os
import sys

# Добавляем корень проекта в путь поиска модулей, чтобы импортировать config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем настройки (адаптируйте под вашу структуру config.py)
from config import DB_CONFIG

def get_db_connection():
    """Создает и возвращает подключение к базе данных."""
    try:
        # Убедитесь, что эти переменные окружения установлены
        connection = psycopg2.connect(**DB_CONFIG)
        print("[DB] Подключение к PostgreSQL установлено.")
        return connection
    except Error as e:
        print(f"[DB] Ошибка при подключении к PostgreSQL: {e}")
        return None

def close_db_connection(connection):
    """Закрывает подключение к базе данных."""
    if connection:
        connection.close()
        print("[DB] Подключение к PostgreSQL закрыто.")

# --- Альтернатива с контекстным менеджером (рекомендуется) ---
from contextlib import contextmanager

@contextmanager
def get_db():
    """Контекстный менеджер для получения подключения к БД."""
    connection = get_db_connection()
    try:
        yield connection
    finally:
        if connection:
            close_db_connection(connection)