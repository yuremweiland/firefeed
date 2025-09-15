import aiopg
import os
import sys
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set
import asyncio

# Добавляем корень проекта в путь поиска модулей, чтобы импортировать config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем настройки и shared pool
import config

# --- Функции для работы с пулом подключений ---

async def get_db_pool():
    """Получает общий пул подключений к базе данных."""
    try:
        pool = await config.get_shared_db_pool()
        return pool
    except Exception as e:
        print(f"[DB] Ошибка при получении пула подключений к PostgreSQL: {e}")
        return None

async def close_db_pool():
    """Закрывает общий пул подключений к базе данных."""
    try:
        await config.close_shared_db_pool()
        print("[DB] Общий пул подключений к PostgreSQL закрыт.")
    except Exception as e:
        print(f"[DB] Ошибка при закрытии пула подключений к PostgreSQL: {e}")

# --- Асинхронный контекстный менеджер для работы с пулом ---
from contextlib import asynccontextmanager

@asynccontextmanager
async def get_db():
    """Асинхронный контекстный менеджер для получения пула подключений к БД."""
    pool = await get_db_pool()
    try:
        yield pool
    except Exception as e:
        print(f"[DB] Ошибка в контекстном менеджере: {e}")
        raise

# --- Функции для работы с пользователями ---

async def create_user(pool, email: str, password_hash: str, language: str = "en") -> Optional[Dict[str, Any]]:
    """Создает нового пользователя в БД"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO users (email, password_hash, language)
                VALUES (%s, %s, %s)
                RETURNING id, email, language, is_active, created_at
                """
                await cur.execute(query, (email, password_hash, language))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при создании пользователя: {e}")
                return None

async def get_user_by_email(pool, email: str) -> Optional[Dict[str, Any]]:
    """Получает пользователя по email"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, password_hash, language, is_active, created_at, updated_at
                FROM users
                WHERE email = %s
                """
                await cur.execute(query, (email,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при получении пользователя по email: {e}")
                return None

async def get_user_by_id(pool, user_id: int) -> Optional[Dict[str, Any]]:
    """Получает пользователя по ID"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, language, is_active, created_at, updated_at
                FROM users
                WHERE id = %s
                """
                await cur.execute(query, (user_id,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при получении пользователя по ID: {e}")
                return None

async def update_user(pool, user_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Обновляет данные пользователя"""
    if not update_data:
        return None
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Формируем динамический UPDATE запрос
                set_parts = []
                params = []
                for key, value in update_data.items():
                    if key in ['email', 'language']:
                        set_parts.append(f"{key} = %s")
                        params.append(value)
                
                if not set_parts:
                    return None
                
                set_parts.append("updated_at = CURRENT_TIMESTAMP")
                params.append(user_id)
                
                query = f"""
                UPDATE users
                SET {', '.join(set_parts)}
                WHERE id = %s
                RETURNING id, email, language, is_active, created_at, updated_at
                """
                
                await cur.execute(query, params)
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при обновлении пользователя: {e}")
                return None

async def delete_user(pool, user_id: int) -> bool:
    """Удаляет пользователя (деактивирует)"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                UPDATE users
                SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                await cur.execute(query, (user_id,))
                return cur.rowcount > 0
            except Exception as e:
                print(f"[DB] Ошибка при удалении пользователя: {e}")
                return False

# --- Функции для работы с токенами сброса пароля ---

async def create_password_reset_token(pool, user_id: int, token: str, expires_at: datetime) -> bool:
    """Создает токен для сброса пароля"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO password_reset_tokens (user_id, token, expires_at)
                VALUES (%s, %s, %s)
                """
                await cur.execute(query, (user_id, token, expires_at))
                return True
            except Exception as e:
                print(f"[DB] Ошибка при создании токена сброса пароля: {e}")
                return False

async def get_password_reset_token(pool, token: str) -> Optional[Dict[str, Any]]:
    """Получает токен сброса пароля"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, user_id, token, expires_at, used_at
                FROM password_reset_tokens
                WHERE token = %s AND used_at IS NULL
                """
                await cur.execute(query, (token,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при получении токена сброса пароля: {e}")
                return None

async def use_password_reset_token(pool, token: str) -> bool:
    """Помечает токен как использованный"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                UPDATE password_reset_tokens
                SET used_at = CURRENT_TIMESTAMP
                WHERE token = %s
                """
                await cur.execute(query, (token,))
                return cur.rowcount > 0
            except Exception as e:
                print(f"[DB] Ошибка при использовании токена сброса пароля: {e}")
                return False

async def update_user_password(pool, user_id: int, new_password_hash: str) -> bool:
    """Обновляет пароль пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                UPDATE users
                SET password_hash = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """
                await cur.execute(query, (new_password_hash, user_id))
                return cur.rowcount > 0
            except Exception as e:
                print(f"[DB] Ошибка при обновлении пароля пользователя: {e}")
                return False

# --- Функции для работы с пользовательскими категориями ---

async def update_user_categories(pool, user_id: int, category_ids: Set[int]) -> bool:
    """Обновляет список категорий пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Начинаем транзакцию
                await cur.execute("BEGIN")
                
                # Удаляем все текущие категории пользователя
                await cur.execute(
                    "DELETE FROM user_categories WHERE user_id = %s",
                    (user_id,)
                )
                
                # Добавляем новые категории
                if category_ids:
                    values = [(user_id, cat_id) for cat_id in category_ids]
                    # Для aiopg используем executemany с корректным синтаксисом
                    for user_id_val, cat_id_val in values:
                        await cur.execute(
                            "INSERT INTO user_categories (user_id, category_id) VALUES (%s, %s)",
                            (user_id_val, cat_id_val)
                        )
                
                # Завершаем транзакцию
                await cur.execute("COMMIT")
                return True
                
            except Exception as e:
                # Откатываем транзакцию в случае ошибки
                await cur.execute("ROLLBACK")
                print(f"Error updating user categories: {e}")
                return False

async def get_user_categories(pool, user_id: int) -> List[dict]:
    """Получает список категорий пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("""
                    SELECT c.id, c.name 
                    FROM user_categories uc
                    JOIN categories c ON uc.category_id = c.id
                    WHERE uc.user_id = %s
                    ORDER BY c.name
                """, (user_id,))
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1]})
                return results
            except Exception as e:
                print(f"Error getting user categories: {e}")
                return []

# --- Функции для работы с пользовательскими RSS-лентами ---

async def create_user_rss_feed(pool, user_id: int, url: str, name: Optional[str], 
                              category_id: Optional[int], language: str) -> Optional[Dict[str, Any]]:
    """Создает пользовательскую RSS-ленту"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO user_rss_feeds (user_id, url, name, category_id, language)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id, user_id, url, name, category_id, language, is_active, created_at
                """
                await cur.execute(query, (user_id, url, name, category_id, language))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при создании пользовательской RSS-ленты: {e}")
                return None

async def get_user_rss_feeds(pool, user_id: int, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """Получает список RSS-лент пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, user_id, url, name, category_id, language, is_active, created_at, updated_at
                FROM user_rss_feeds
                WHERE user_id = %s
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """
                await cur.execute(query, (user_id, limit, offset))
                results = []
                async for row in cur:
                    columns = [desc[0] for desc in cur.description]
                    results.append(dict(zip(columns, row)))
                return results
            except Exception as e:
                print(f"[DB] Ошибка при получении RSS-лент пользователя: {e}")
                return []

async def get_user_rss_feed_by_id(pool, user_id: int, feed_id: int) -> Optional[Dict[str, Any]]:
    """Получает конкретную RSS-ленту пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, user_id, url, name, category_id, language, is_active, created_at, updated_at
                FROM user_rss_feeds
                WHERE user_id = %s AND id = %s
                """
                await cur.execute(query, (user_id, feed_id))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при получении RSS-ленты пользователя: {e}")
                return None

async def update_user_rss_feed(pool, user_id: int, feed_id: int, 
                              update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Обновляет пользовательскую RSS-ленту"""
    if not update_data:
        return None
    
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                set_parts = []
                params = []
                for key, value in update_data.items():
                    if key in ['name', 'category_id', 'is_active']:
                        set_parts.append(f"{key} = %s")
                        params.append(value)
                
                if not set_parts:
                    return None
                
                set_parts.append("updated_at = CURRENT_TIMESTAMP")
                params.extend([user_id, feed_id])
                
                query = f"""
                UPDATE user_rss_feeds
                SET {', '.join(set_parts)}
                WHERE user_id = %s AND id = %s
                RETURNING id, user_id, url, name, category_id, language, is_active, created_at, updated_at
                """
                
                await cur.execute(query, params)
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Ошибка при обновлении RSS-ленты пользователя: {e}")
                return None

async def delete_user_rss_feed(pool, user_id: int, feed_id: int) -> bool:
    """Удаляет пользовательскую RSS-ленту"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                DELETE FROM user_rss_feeds
                WHERE user_id = %s AND id = %s
                """
                await cur.execute(query, (user_id, feed_id))
                return cur.rowcount > 0
            except Exception as e:
                print(f"[DB] Ошибка при удалении RSS-ленты пользователя: {e}")
                return False

# --- Функции для работы с существующими данными (новости, категории и т.д.) ---

async def get_news_list(pool, display_language: str, original_language: Optional[str] = None,
                       category_id: Optional[int] = None, source_id: Optional[int] = None,
                       telegram_published: Optional[bool] = None, limit: int = 50, offset: int = 0):
    """Получает список новостей с фильтрацией"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Сначала получаем общее количество новостей для пагинации
                count_query = """
                SELECT COUNT(*)
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                WHERE 1=1
                """
                count_params = []
                
                # Добавляем фильтры для подсчета
                if original_language:
                    count_query += " AND nd.original_language = %s"
                    count_params.append(original_language)
                if category_id:
                    count_query += " AND nd.category_id = %s"
                    count_params.append(category_id)
                if source_id:
                    count_query += " AND rf.source_id = %s"
                    count_params.append(source_id)
                if telegram_published is not None:
                    if isinstance(telegram_published, str):
                        telegram_published_value = telegram_published.lower() == 'true'
                    else:
                        telegram_published_value = bool(telegram_published)
                    if telegram_published_value:
                        count_query += " AND nd.telegram_published_at IS NOT NULL"
                    else:
                        count_query += " AND nd.telegram_published_at IS NULL"
                
                await cur.execute(count_query, count_params)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                # Затем получаем сами новости с JOIN'ами
                query_params = []
                
                query = """
                SELECT 
                    nd.news_id,
                    nd.original_title,
                    nd.original_content,
                    nd.original_language,
                    nd.image_filename,
                    COALESCE(c.name, 'Unknown Category') AS category_name,
                    COALESCE(s.name, 'Unknown Source') AS source_name,
                    pn.source_url,
                    pn.published_at,
                    COALESCE(nt_display.translated_title, nd.original_title) as display_title,
                    COALESCE(nt_display.translated_content, nd.original_content) as display_content,
                    nt_ru.translated_title as title_ru,
                    nt_ru.translated_content as content_ru,
                    nt_en.translated_title as title_en,
                    nt_en.translated_content as content_en,
                    nt_de.translated_title as title_de,
                    nt_de.translated_content as content_de,
                    nt_fr.translated_title as title_fr,
                    nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
                WHERE 1=1
                """
                
                query_params.extend(['ru', 'en', 'de', 'fr', display_language])
                
                # Добавляем фильтры в WHERE
                if original_language:
                    query += " AND nd.original_language = %s"
                    query_params.append(original_language)
                if category_id:
                    query += " AND nd.category_id = %s"
                    query_params.append(category_id)
                if source_id:
                    query += " AND rf.source_id = %s"
                    query_params.append(source_id)
                if telegram_published is not None:
                    if isinstance(telegram_published, str):
                        telegram_published_value = telegram_published.lower() == 'true'
                    else:
                        telegram_published_value = bool(telegram_published)
                    if telegram_published_value:
                        query += " AND nd.telegram_published_at IS NOT NULL"
                    else:
                        query += " AND nd.telegram_published_at IS NULL"
                
                query += " ORDER BY pn.published_at DESC LIMIT %s OFFSET %s"
                query_params.append(limit)
                query_params.append(offset)
                
                await cur.execute(query, query_params)
                results = []
                async for row in cur:
                    results.append(row)

                columns = [desc[0] for desc in cur.description]
                return total_count, results, columns

            except Exception as e:
                print(f"[DB] Ошибка при получении списка новостей: {e}")
                raise

async def get_news_by_id(pool, news_id: str):
    """Получает новость по ID"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT 
                    nd.*,
                    COALESCE(c.name, 'Unknown Category') AS category_name,
                    COALESCE(s.name, 'Unknown Source') AS source_name,
                    pn.source_url,
                    pn.published_at,
                    nt_ru.translated_title as title_ru,
                    nt_ru.translated_content as content_ru,
                    nt_en.translated_title as title_en,
                    nt_en.translated_content as content_en,
                    nt_de.translated_title as title_de,
                    nt_de.translated_content as content_de,
                    nt_fr.translated_title as title_fr,
                    nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON pn.source_url LIKE CONCAT(rf.url, %s) OR rf.url LIKE CONCAT(pn.source_url, %s)
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                WHERE nd.news_id = %s
                """
                query_params = ['%%', '%%', 'ru', 'en', 'de', 'fr', news_id]
                
                await cur.execute(query, query_params)
                result = await cur.fetchone()
                return result

            except Exception as e:
                print(f"[DB] Ошибка при получении новости по ID: {e}")
                raise

async def get_categories_list(pool, limit: int = 100, offset: int = 0):
    """Получает список категорий"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                count_query = "SELECT COUNT(*) FROM categories"
                await cur.execute(count_query)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                query = "SELECT id, name FROM categories ORDER BY name LIMIT %s OFFSET %s"
                await cur.execute(query, (limit, offset))
                results = []
                async for row in cur:
                    results.append(row)
                
                return total_count, results

            except Exception as e:
                print(f"[DB] Ошибка при получении списка категорий: {e}")
                raise

async def get_sources_list(pool, limit: int = 100, offset: int = 0):
    """Получает список источников"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                count_query = "SELECT COUNT(*) FROM sources"
                await cur.execute(count_query)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                query = "SELECT id, name, description FROM sources ORDER BY name LIMIT %s OFFSET %s"
                await cur.execute(query, (limit, offset))
                results = []
                async for row in cur:
                    results.append(row)
                
                return total_count, results

            except Exception as e:
                print(f"[DB] Ошибка при получении списка источников: {e}")
                raise

async def get_languages_list(pool, limit: int = 100, offset: int = 0):
    """Получает список языков"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                count_query = "SELECT COUNT(DISTINCT language) FROM rss_feeds WHERE is_active = TRUE"
                await cur.execute(count_query)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                query = "SELECT DISTINCT language FROM rss_feeds WHERE is_active = TRUE ORDER BY language LIMIT %s OFFSET %s"
                await cur.execute(query, (limit, offset))
                results = []
                async for row in cur:
                    results.append(row)
                
                return total_count, results

            except Exception as e:
                print(f"[DB] Ошибка при получении списка языков: {e}")
                raise

# --- Функции для фоновых задач ---
async def get_recent_news_for_broadcast(pool, since_time: datetime, limit: int = 10):
    """Получает недавние новости для рассылки по WebSocket"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT 
                    nd.news_id,
                    nd.original_title,
                    nd.original_content,
                    nd.original_language,
                    nd.image_filename,
                    COALESCE(c.name, 'Unknown Category') AS category_name,
                    COALESCE(s.name, 'Unknown Source') AS source_name,
                    pn.source_url,
                    pn.published_at
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                WHERE pn.published_at > %s
                ORDER BY pn.published_at DESC
                LIMIT %s
                """
                
                check_time_str = since_time.strftime('%Y-%m-%d %H:%M:%S')
                await cur.execute(query, (check_time_str, limit))
                results = []
                async for row in cur:
                    results.append(row)
                
                return results

            except Exception as e:
                print(f"[DB] Ошибка при получении новостей для broadcast: {e}")
                return []