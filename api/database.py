# database.py
import aiopg
import os
import sys
import hashlib
import secrets
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple
import asyncio

# Добавляем корень проекта в путь поиска модулей, чтобы импортировать config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Импортируем настройки и shared pool
import config

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

# --- Функции для работы с пользователями ---

async def create_user(pool, email: str, password_hash: str, language: str) -> Optional[Dict[str, Any]]:
    """Создает нового пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO users (email, password_hash, language, is_active, is_verified, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id, email, language, is_active, is_verified, created_at, updated_at
                """
                now = datetime.utcnow()
                await cur.execute(query, (email, password_hash, language, False, False, now, now))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error creating user: {e}")
                return None

async def get_user_by_email(pool, email: str) -> Optional[Dict[str, Any]]:
    """Получает пользователя по email"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, password_hash, language, is_active, is_verified, created_at, updated_at
                FROM users WHERE email = %s
                """
                await cur.execute(query, (email,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error getting user by email: {e}")
                return None

async def get_user_by_id(pool, user_id: int) -> Optional[Dict[str, Any]]:
    """Получает пользователя по ID"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, password_hash, language, is_active, is_verified, created_at, updated_at
                FROM users WHERE id = %s
                """
                await cur.execute(query, (user_id,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error getting user by id: {e}")
                return None

async def update_user(pool, user_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Обновляет данные пользователя"""
    if not update_data:
        return await get_user_by_id(pool, user_id)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                set_parts = []
                params = []
                for key, value in update_data.items():
                    set_parts.append(f"{key} = %s")
                    params.append(value)
                params.append(user_id)

                query = f"""
                UPDATE users
                SET {', '.join(set_parts)}, updated_at = %s
                WHERE id = %s
                RETURNING id, email, password_hash, language, is_active, is_verified, created_at, updated_at
                """
                params.append(datetime.utcnow()) # updated_at
                await cur.execute(query, params)
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error updating user: {e}")
                return None

async def delete_user(pool, user_id: int) -> bool:
    """Деактивирует (удаляет) пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Вместо физического удаления деактивируем
                query = "UPDATE users SET is_active = FALSE, updated_at = %s WHERE id = %s"
                await cur.execute(query, (datetime.utcnow(), user_id))
                # Проверяем, была ли затронута строка
                if cur.rowcount > 0:
                    return True
                return False
            except Exception as e:
                print(f"[DB] Error deleting user: {e}")
                return False

async def activate_user(pool, user_id: int) -> bool:
    """Активирует пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = "UPDATE users SET is_active = TRUE, is_verified = TRUE, updated_at = %s WHERE id = %s"
                await cur.execute(query, (datetime.utcnow(), user_id))
                if cur.rowcount > 0:
                    return True
                return False
            except Exception as e:
                print(f"[DB] Error activating user: {e}")
                return False

async def update_user_password(pool, user_id: int, new_hashed_password: str) -> bool:
    """Обновляет пароль пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = "UPDATE users SET password_hash = %s, updated_at = %s WHERE id = %s"
                await cur.execute(query, (new_hashed_password, datetime.utcnow(), user_id))
                if cur.rowcount > 0:
                    return True
                return False
            except Exception as e:
                print(f"[DB] Error updating user password: {e}")
                return False

# --- Функции для работы с кодами верификации ---

async def save_verification_code(pool, user_id: int, code: str) -> bool:
    """Сохраняет код верификации для пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Удаляем старые коды для этого пользователя
                await cur.execute("DELETE FROM user_verification_codes WHERE user_id = %s", (user_id,))
                # Вставляем новый код
                expires_at = datetime.utcnow() + timedelta(hours=config.VERIFICATION_CODE_EXPIRE_HOURS)
                query = """
                INSERT INTO user_verification_codes (user_id, code, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                """
                await cur.execute(query, (user_id, code, expires_at, datetime.utcnow()))
                return True
            except Exception as e:
                print(f"[DB] Error saving verification code: {e}")
                return False

async def verify_user_email(pool, email: str, code: str) -> Optional[int]:
    """Проверяет код верификации и возвращает user_id, если код действителен"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT uvc.user_id
                FROM user_verification_codes uvc
                JOIN users u ON uvc.user_id = u.id
                WHERE u.email = %s AND uvc.code = %s AND uvc.expires_at > %s
                """
                await cur.execute(query, (email, code, datetime.utcnow()))
                result = await cur.fetchone()
                if result:
                    return result[0] # Возвращаем user_id
                return None
            except Exception as e:
                print(f"[DB] Error verifying user email: {e}")
                return None

# --- Функции для работы с токенами сброса пароля ---

async def save_password_reset_token(pool, user_id: int, token: str, expires_at: datetime) -> bool:
    """Сохраняет токен сброса пароля"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Удаляем старые токены для этого пользователя
                await cur.execute("DELETE FROM password_reset_tokens WHERE user_id = %s", (user_id,))
                # Вставляем новый токен
                query = """
                INSERT INTO password_reset_tokens (user_id, token, expires_at, created_at)
                VALUES (%s, %s, %s, %s)
                """
                await cur.execute(query, (user_id, token, expires_at, datetime.utcnow()))
                return True
            except Exception as e:
                print(f"[DB] Error saving password reset token: {e}")
                return False

async def get_user_id_by_reset_token(pool, token: str) -> Optional[int]:
    """Получает user_id по токену сброса, если токен действителен"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT user_id FROM password_reset_tokens
                WHERE token = %s AND expires_at > %s
                """
                await cur.execute(query, (token, datetime.utcnow()))
                result = await cur.fetchone()
                if result:
                    return result[0]
                return None
            except Exception as e:
                print(f"[DB] Error getting user ID by reset token: {e}")
                return None

async def delete_password_reset_token(pool, token: str) -> bool:
    """Удаляет использованный токен сброса пароля"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("DELETE FROM password_reset_tokens WHERE token = %s", (token,))
                return True
            except Exception as e:
                print(f"[DB] Error deleting password reset token: {e}")
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
                await cur.execute("DELETE FROM user_categories WHERE user_id = %s", (user_id,))

                # Добавляем новые категории
                if category_ids:
                    values = [(user_id, cat_id) for cat_id in category_ids]
                    # Для aiopg используем executemany с корректным синтаксисом
                    await cur.executemany(
                        "INSERT INTO user_categories (user_id, category_id) VALUES (%s, %s)",
                        values
                    )

                # Коммитим транзакцию
                await cur.execute("COMMIT")
                return True
            except Exception as e:
                await cur.execute("ROLLBACK")
                print(f"[DB] Error updating user categories: {e}")
                return False

async def get_user_categories(pool, user_id: int) -> List[Dict[str, Any]]:
    """Получает список категорий пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT c.id, c.name
                FROM user_categories uc
                JOIN categories c ON uc.category_id = c.id
                WHERE uc.user_id = %s
                """
                await cur.execute(query, (user_id,))
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1]})
                return results
            except Exception as e:
                print(f"[DB] Error getting user categories: {e}")
                return []

# --- Функции для работы с пользовательскими RSS-лентами ---

async def create_user_rss_feed(pool, user_id: int, url: str, name: str, category_id: int, language: str) -> Optional[Dict[str, Any]]:
    """Создает пользовательскую RSS-ленту"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO user_rss_feeds (user_id, url, name, category_id, language, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, url, name, category_id, language, is_active, created_at, updated_at
                """
                now = datetime.utcnow()
                await cur.execute(query, (user_id, url, name, category_id, language, True, now, now))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error creating user RSS feed: {e}")
                return None

async def get_user_rss_feeds(pool, user_id: int, limit: int, offset: int) -> List[Dict[str, Any]]:
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
                print(f"[DB] Error getting user RSS feeds: {e}")
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
                print(f"[DB] Error getting user RSS feed by ID: {e}")
                return None

async def update_user_rss_feed(pool, user_id: int, feed_id: int, update_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Обновляет пользовательскую RSS-ленту"""
    if not update_data:
        return await get_user_rss_feed_by_id(pool, user_id, feed_id)

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                set_parts = []
                params = []
                for key, value in update_data.items():
                    set_parts.append(f"{key} = %s")
                    params.append(value)
                params.append(user_id)
                params.append(feed_id)

                query = f"""
                UPDATE user_rss_feeds
                SET {', '.join(set_parts)}, updated_at = %s
                WHERE user_id = %s AND id = %s
                RETURNING id, user_id, url, name, category_id, language, is_active, created_at, updated_at
                """
                params.append(datetime.utcnow()) # updated_at
                await cur.execute(query, params)
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                print(f"[DB] Error updating user RSS feed: {e}")
                return None

async def delete_user_rss_feed(pool, user_id: int, feed_id: int) -> bool:
    """Удаляет пользовательскую RSS-ленту"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("DELETE FROM user_rss_feeds WHERE user_id = %s AND id = %s", (user_id, feed_id))
                # Проверяем, была ли затронута строка
                if cur.rowcount > 0:
                    return True
                return False
            except Exception as e:
                print(f"[DB] Error deleting user RSS feed: {e}")
                return False

# --- Функции для получения RSS-элементов ---

# --- Изменено: название функции ---
async def get_user_rss_items_list(
    pool,
    user_id: int,
    display_language: str,
    original_language: Optional[str],
    limit: int,
    offset: int
) -> Tuple[int, List[Tuple], List[str]]:
    """
    Получает список RSS-элементов для текущего пользователя на основе его подписок.
    Возвращает кортеж (total_count, results_rows, column_names).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # 1. Получить ID категорий, на которые подписан пользователь
                user_categories = await get_user_categories(pool, user_id)
                user_category_ids = [cat['id'] for cat in user_categories]

                # Если у пользователя нет подписок, возвращаем пустой результат
                if not user_category_ids:
                    return 0, [], []

                # 2. Получить ID пользовательских RSS-лент пользователя из этих категорий
                await cur.execute("""
                    SELECT id FROM user_rss_feeds
                    WHERE user_id = %s AND category_id = ANY(%s) AND is_active = TRUE
                """, (user_id, user_category_ids))
                user_rss_feed_ids = [row[0] for row in await cur.fetchall()]

                # Если нет активных лент в подписанных категориях, возвращаем пустой результат
                if not user_rss_feed_ids:
                    return 0, [], []

                # 3. Подсчет общего количества RSS-элементов для пагинации
                count_query = """
                SELECT COUNT(*)
                FROM published_news_data nd
                WHERE nd.rss_feed_id = ANY(%s) -- Фильтр по пользовательским RSS-лентам
                """
                count_params = [user_rss_feed_ids]

                # Добавляем фильтры для подсчета
                if original_language:
                    count_query += " AND nd.original_language = %s"
                    count_params.append(original_language)

                await cur.execute(count_query, count_params)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                # 4. Получение самих RSS-элементов с JOIN'ами
                query_params = []

                query = """
                SELECT
                nd.*,
                COALESCE(c.name, 'Unknown Category') AS category_name,
                COALESCE(s.name, 'Unknown Source') AS source_name,
                rf.url as source_url, -- Получаем URL источника из rss_feeds
                nd.created_at as published_at, -- Используем created_at из published_news_data как published_at
                nt_ru.translated_title as title_ru,
                nt_ru.translated_content as content_ru,
                nt_en.translated_title as title_en,
                nt_en.translated_content as content_en,
                nt_de.translated_title as title_de,
                nt_de.translated_content as content_de,
                nt_fr.translated_title as title_fr,
                nt_fr.translated_content as content_fr,
                nt_display.translated_title as display_title,
                nt_display.translated_content as display_content
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
                WHERE nd.rss_feed_id = ANY(%s) -- Фильтр по пользовательским RSS-лентам
                """

                # Добавляем параметры для языковых JOIN'ов
                query_params.extend(['ru', 'en', 'de', 'fr', display_language, user_rss_feed_ids])

                # Добавляем фильтры в WHERE
                if original_language:
                    query += " AND nd.original_language = %s"
                    query_params.append(original_language)

                # Добавляем ORDER BY, LIMIT и OFFSET
                # Используем created_at из published_news_data для сортировки
                query += " ORDER BY nd.created_at DESC LIMIT %s OFFSET %s"
                query_params.append(limit)
                query_params.append(offset)

                await cur.execute(query, query_params)
                results = []
                async for row in cur:
                    results.append(row)

                # Получаем названия колонок
                columns = [desc[0] for desc in cur.description]

                return total_count, results, columns

            except Exception as e:
                print(f"[DB] Error in get_user_rss_items_list: {e}")
                raise # Перебрасываем исключение, чтобы обработать его в API

# --- Изменено: название функции ---
async def get_user_rss_items_list_by_feed(
    pool,
    user_id: int,
    feed_id: int,
    display_language: str,
    original_language: Optional[str],
    limit: int,
    offset: int
) -> Tuple[int, List[Tuple], List[str]]:
    """
    Получает список RSS-элементов из конкретной пользовательской RSS-ленты текущего пользователя.
    Возвращает кортеж (total_count, results_rows, column_names).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # 1. Проверить, принадлежит ли лента пользователю и активна ли она
                await cur.execute("""
                    SELECT 1 FROM user_rss_feeds
                    WHERE id = %s AND user_id = %s AND is_active = TRUE
                """, (feed_id, user_id))
                feed_exists = await cur.fetchone()

                if not feed_exists:
                    # Здесь мы просто возвращаем 0, [] для единообразия с другими функциями
                    return 0, [], []

                # 2. Подсчет общего количества RSS-элементов для пагинации
                count_query = """
                SELECT COUNT(*)
                FROM published_news_data nd
                WHERE nd.rss_feed_id = %s -- Фильтр по конкретной пользовательской RSS-ленте
                """
                count_params = [feed_id]

                # Добавляем фильтры для подсчета
                if original_language:
                    count_query += " AND nd.original_language = %s"
                    count_params.append(original_language)

                await cur.execute(count_query, count_params)
                total_count = await cur.fetchone()
                total_count = total_count[0] if total_count else 0

                # 3. Получение самих RSS-элементов с JOIN'ами
                query_params = []

                query = """
                SELECT
                nd.*,
                COALESCE(c.name, 'Unknown Category') AS category_name,
                COALESCE(s.name, 'Unknown Source') AS source_name,
                rf.url as source_url, -- Получаем URL источника из rss_feeds
                nd.created_at as published_at, -- Используем created_at из published_news_data как published_at
                nt_ru.translated_title as title_ru,
                nt_ru.translated_content as content_ru,
                nt_en.translated_title as title_en,
                nt_en.translated_content as content_en,
                nt_de.translated_title as title_de,
                nt_de.translated_content as content_de,
                nt_fr.translated_title as title_fr,
                nt_fr.translated_content as content_fr,
                nt_display.translated_title as display_title,
                nt_display.translated_content as display_content
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
                WHERE nd.rss_feed_id = %s -- Фильтр по конкретной пользовательской RSS-ленте
                """

                # Добавляем параметры для языковых JOIN'ов
                query_params.extend(['ru', 'en', 'de', 'fr', display_language, feed_id])

                # Добавляем фильтры в WHERE
                if original_language:
                    query += " AND nd.original_language = %s"
                    query_params.append(original_language)

                # Добавляем ORDER BY, LIMIT и OFFSET
                # Используем created_at из published_news_data для сортировки
                query += " ORDER BY nd.created_at DESC LIMIT %s OFFSET %s"
                query_params.append(limit)
                query_params.append(offset)

                await cur.execute(query, query_params)
                results = []
                async for row in cur:
                    results.append(row)

                # Получаем названия колонок
                columns = [desc[0] for desc in cur.description]

                return total_count, results, columns

            except Exception as e:
                print(f"[DB] Error in get_user_rss_items_list_by_feed: {e}")
                raise # Перебрасываем исключение, чтобы обработать его в API

# --- Перенесено: функция get_rss_item_by_id ---
async def get_rss_item_by_id(pool, news_id: str) -> Optional[Tuple]:
    """Получает RSS-элемент по её ID."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Аналогично, добавляем JOIN с rss_feeds, categories, sources и переводами
                # Исправляем параметры и убираем ссылки на несуществующую таблицу pn
                query = """
                SELECT
                nd.*,
                COALESCE(c.name, 'Unknown Category') AS category_name,
                COALESCE(s.name, 'Unknown Source') AS source_name,
                rf.url as source_url, -- Получаем URL источника из rss_feeds
                nd.created_at as published_at, -- Используем created_at из published_news_data как published_at
                nt_ru.translated_title as title_ru,
                nt_ru.translated_content as content_ru,
                nt_en.translated_title as title_en,
                nt_en.translated_content as content_en,
                nt_de.translated_title as title_de,
                nt_de.translated_content as content_de,
                nt_fr.translated_title as title_fr,
                nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                WHERE nd.news_id = %s
                """
                # Исправляем параметры запроса
                query_params = ['ru', 'en', 'de', 'fr', news_id]
                await cur.execute(query, query_params)
                result = await cur.fetchone()
                return result
            except Exception as e:
                print(f"[DB] Ошибка при получении RSS-элемента по ID: {e}")
                raise

# --- Добавлено: обертка для get_rss_item_by_id, возвращающая row и columns ---
async def get_rss_item_by_id_full(pool, news_id: str) -> Tuple[Optional[Tuple], List[str]]:
    """Получает RSS-элемент по её ID, возвращая кортеж (row, columns)."""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT
                nd.*,
                COALESCE(c.name, 'Unknown Category') AS category_name,
                COALESCE(s.name, 'Unknown Source') AS source_name,
                rf.url as source_url,
                nd.created_at as published_at,
                nt_ru.translated_title as title_ru,
                nt_ru.translated_content as content_ru,
                nt_en.translated_title as title_en,
                nt_en.translated_content as content_en,
                nt_de.translated_title as title_de,
                nt_de.translated_content as content_de,
                nt_fr.translated_title as title_fr,
                nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                WHERE nd.news_id = %s
                """
                query_params = ['ru', 'en', 'de', 'fr', news_id]
                await cur.execute(query, query_params)
                result = await cur.fetchone()
                columns = [desc[0] for desc in cur.description]
                return result, columns
            except Exception as e:
                print(f"[DB] Ошибка при получении RSS-элемента по ID (full): {e}")
                return None, []

# --- Добавлено: функция для получения всех новостей (перенос из main.py) ---
async def get_all_rss_items_list(
    pool,
    display_language: str,
    original_language: Optional[str],
    category_id: Optional[int],
    source_id: Optional[int],
    telegram_published: Optional[bool],
    limit: int,
    offset: int
) -> Tuple[int, List[Tuple], List[str]]:
    """
    Получает список всех RSS-элементов с фильтрацией.
    Возвращает кортеж (total_count, results_rows, column_names).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # --- Основной запрос для получения данных ---
                query = """
                SELECT
                nd.*,
                COALESCE(c.name, 'Unknown Category') AS category_name,
                COALESCE(s.name, 'Unknown Source') AS source_name,
                rf.url as source_url,
                nd.created_at as published_at,
                nt_ru.translated_title as title_ru,
                nt_ru.translated_content as content_ru,
                nt_en.translated_title as title_en,
                nt_en.translated_content as content_en,
                nt_de.translated_title as title_de,
                nt_de.translated_content as content_de,
                nt_fr.translated_title as title_fr,
                nt_fr.translated_content as content_fr
                FROM published_news_data nd
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
                # Добавляем параметры для языковых JOIN'ов
                query_params = ['ru', 'en', 'de', 'fr', display_language]

                # Добавляем фильтры в WHERE и соответствующие параметры
                if original_language:
                    query += " AND nd.original_language = %s"
                    query_params.append(original_language)
                if category_id:
                    query += " AND nd.category_id = %s"
                    query_params.append(category_id)
                if source_id:
                    query += " AND rf.source_id = %s"
                    query_params.append(source_id)
                # Добавляем фильтр по telegram_published
                if telegram_published is not None:
                    # Преобразуем в boolean если строка (это делалось в main.py, переносим логику)
                    if isinstance(telegram_published, str):
                        telegram_published_value = telegram_published.lower() == 'true'
                    else:
                        telegram_published_value = bool(telegram_published)
                    if telegram_published_value:
                        query += " AND nd.telegram_published_at IS NOT NULL"
                    else:
                        query += " AND nd.telegram_published_at IS NULL"
                    # query_params не нужен для этого фильтра, так как он использует IS NULL/IS NOT NULL

                # Добавляем ORDER BY, LIMIT и OFFSET
                query += " ORDER BY nd.created_at DESC LIMIT %s OFFSET %s"
                query_params.append(limit)
                query_params.append(offset)

                await cur.execute(query, query_params)
                results = []
                async for row in cur:
                    results.append(row)

                # Получаем названия колонок
                columns = [desc[0] for desc in cur.description]

                # --- Подсчет общего количества ---
                count_query = """
                SELECT COUNT(*)
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                WHERE 1=1
                """
                count_params = []

                # Добавляем те же фильтры для подсчета
                if original_language:
                    count_query += " AND nd.original_language = %s"
                    count_params.append(original_language)
                if category_id:
                    count_query += " AND nd.category_id = %s"
                    count_params.append(category_id)
                if source_id:
                    count_query += " AND rf.source_id = %s"
                    count_params.append(source_id)
                # Добавляем фильтр по telegram_published для подсчета
                if telegram_published is not None:
                     if telegram_published_value: # Используем уже вычисленное значение
                        count_query += " AND nd.telegram_published_at IS NOT NULL"
                     else:
                        count_query += " AND nd.telegram_published_at IS NULL"
                    # count_params не нужен

                await cur.execute(count_query, count_params)
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0
                # --- Конец подсчета общего количества ---

                return total_count, results, columns

            except Exception as e:
                print(f"[DB] Ошибка при выполнении запроса в get_all_rss_items_list: {e}")
                raise # Перебрасываем исключение

# --- Добавлено: функция для получения всех категорий (перенос из main.py) ---
async def get_all_categories_list(pool, limit: int, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Получает список всех категорий с пагинацией.
    Возвращает кортеж (total_count, results).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Получаем общее количество
                await cur.execute("SELECT COUNT(*) FROM categories")
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0

                # Получаем список с пагинацией
                query = "SELECT id, name FROM categories ORDER BY name LIMIT %s OFFSET %s"
                await cur.execute(query, (limit, offset))
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1]})

                return total_count, results
            except Exception as e:
                print(f"[DB] Ошибка при выполнении запроса в get_all_categories_list: {e}")
                raise

# --- Добавлено: функция для получения всех источников (перенос из main.py) ---
async def get_all_sources_list(pool, limit: int, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Получает список всех источников с пагинацией.
    Возвращает кортеж (total_count, results).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Получаем общее количество
                await cur.execute("SELECT COUNT(*) FROM sources")
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0

                # Получаем список с пагинацией
                query = "SELECT id, name, url FROM sources ORDER BY name LIMIT %s OFFSET %s"
                await cur.execute(query, (limit, offset))
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1], "url": row[2]})

                return total_count, results
            except Exception as e:
                print(f"[DB] Ошибка при выполнении запроса в get_all_sources_list: {e}")
                raise

# --- Добавлено: функция для получения последних новостей для броадкаста (перенос из main.py) ---
async def get_recent_news_for_broadcast(pool, last_check_time: datetime) -> List[Dict[str, Any]]:
    """
    Получает список последних новостей для отправки по WebSocket.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT
                    nd.news_id,
                    nd.original_title,
                    c.name as category_name,
                    nd.created_at as published_at
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                WHERE nd.created_at > %s
                ORDER BY nd.created_at DESC
                LIMIT 10
                """
                check_time_str = last_check_time.strftime('%Y-%m-%d %H:%M:%S')
                await cur.execute(query, (check_time_str,))
                results = []
                async for row in cur:
                    results.append(row)

                # Преобразуем в формат для отправки
                columns = [desc[0] for desc in cur.description]
                news_items = []
                for row in results:
                    row_dict = dict(zip(columns, row))
                    news_items.append({
                        "news_id": row_dict['news_id'],
                        "original_title": row_dict['original_title'],
                        "category": row_dict['category_name'],
                        "published_at": row_dict['published_at'].isoformat() if row_dict['published_at'] else None
                    })
                return news_items
            except Exception as e:
                print(f"[DB] Error in get_recent_news_for_broadcast: {e}")
                return [] # Возвращаем пустой список в случае ошибки, чтобы не прерывать фоновую задачу
