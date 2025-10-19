# database.py
import os
import sys
import logging
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Set, Tuple

logger = logging.getLogger(__name__)

async def get_db_pool():
    """Получает общий пул подключений к базе данных."""
    try:
        pool = await config.get_shared_db_pool()
        return pool
    except Exception as e:
        logger.info(f"[DB] Ошибка при получении пула подключений к PostgreSQL: {e}")
        return None

async def close_db_pool():
    """Закрывает общий пул подключений к базе данных."""
    try:
        await config.close_shared_db_pool()
        logger.info("[DB] Общий пул подключений к PostgreSQL закрыт.")
    except Exception as e:
        logger.info(f"[DB] Ошибка при закрытии пула подключений к PostgreSQL: {e}")

# --- Функции для работы с пользователями ---

async def create_user(pool, email: str, password_hash: str, language: str) -> Optional[Dict[str, Any]]:
    """Создает нового пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO users (email, password_hash, language, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id, email, language, is_active, created_at, updated_at
                """
                now = datetime.utcnow()
                await cur.execute(query, (email, password_hash, language, False, now, now))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.info(f"[DB] Error creating user: {e}")
                return None

async def get_user_by_email(pool, email: str) -> Optional[Dict[str, Any]]:
    """Получает пользователя по email"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, password_hash, language, is_active, created_at, updated_at
                FROM users WHERE email = %s
                """
                await cur.execute(query, (email,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.info(f"[DB] Error getting user by email: {e}")
                return None

async def get_user_by_id(pool, user_id: int) -> Optional[Dict[str, Any]]:
    """Получает пользователя по ID"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT id, email, password_hash, language, is_active, created_at, updated_at
                FROM users WHERE id = %s
                """
                await cur.execute(query, (user_id,))
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.info(f"[DB] Error getting user by id: {e}")
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
                RETURNING id, email, password_hash, language, is_active, created_at, updated_at
                """
                params.append(datetime.utcnow()) # updated_at
                await cur.execute(query, params)
                result = await cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
            except Exception as e:
                logger.info(f"[DB] Error updating user: {e}")
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
                logger.info(f"[DB] Error deleting user: {e}")
                return False

async def activate_user(pool, user_id: int) -> bool:
    """Активирует пользователя"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = "UPDATE users SET is_active = TRUE, updated_at = %s WHERE id = %s"
                await cur.execute(query, (datetime.utcnow(), user_id))
                if cur.rowcount > 0:
                    return True
                return False
            except Exception as e:
                logger.info(f"[DB] Error activating user: {e}")
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
                logger.info(f"[DB] Error updating user password: {e}")
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
                logger.info(f"[DB] Error saving verification code: {e}")
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
                logger.info(f"[DB] Error verifying user email: {e}")
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
                logger.info(f"[DB] Error saving password reset token: {e}")
                return False

async def get_password_reset_token(pool, token: str) -> Optional[Dict[str, Any]]:
    """Получает данные токена сброса пароля, если токен действителен"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT user_id, expires_at FROM password_reset_tokens
                WHERE token = %s AND expires_at > %s
                """
                await cur.execute(query, (token, datetime.utcnow()))
                result = await cur.fetchone()
                if result:
                    return {"user_id": result[0], "expires_at": result[1]}
                return None
            except Exception as e:
                logger.info(f"[DB] Error getting password reset token: {e}")
                return None

async def delete_password_reset_token(pool, token: str) -> bool:
    """Удаляет использованный токен сброса пароля"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute("DELETE FROM password_reset_tokens WHERE token = %s", (token,))
                return True
            except Exception as e:
                logger.info(f"[DB] Error deleting password reset token: {e}")
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
                    for cat_id in category_ids:
                        await cur.execute(
                            "INSERT INTO user_categories (user_id, category_id) VALUES (%s, %s)",
                            (user_id, cat_id)
                        )

                # Коммитим транзакцию
                await cur.execute("COMMIT")
                return True
            except Exception as e:
                await cur.execute("ROLLBACK")
                logger.info(f"[DB] Error updating user categories: {e}")
                return False

async def get_user_categories(
    pool, 
    user_id: int, 
    source_ids: Optional[List[int]] = None
) -> List[Dict[str, Any]]:
    """Получает список категорий пользователя с фильтрацией по source_id"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                SELECT c.id, c.name
                FROM user_categories uc
                JOIN categories c ON uc.category_id = c.id
                WHERE uc.user_id = %s AND c.id != %s
                """
                params = [user_id, config.USER_DEFINED_RSS_CATEGORY_ID]

                if source_ids:
                    placeholders = ','.join(['%s'] * len(source_ids))
                    query += f" AND c.id IN (SELECT category_id FROM source_categories WHERE source_id IN ({placeholders})  AND category_id != %s)"
                    params.extend(source_ids)

                await cur.execute(query, params)
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1]})
                return results
            except Exception as e:
                logger.info(f"[DB] Error getting user categories: {e}")
                return []

# --- Функции для работы с пользовательскими RSS-лентами ---

async def create_user_rss_feed(pool, user_id: int, url: str, name: str, category_id: int, language: str) -> Optional[Dict[str, Any]]:
    """Создает пользовательскую RSS-ленту"""
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = """
                INSERT INTO user_rss_feeds (user_id, url, name, category_id, language, is_active, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
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
                logger.info(f"[DB] Error creating user RSS feed: {e}")
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
                logger.info(f"[DB] Error getting user RSS feeds: {e}")
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
                logger.info(f"[DB] Error getting user RSS feed by ID: {e}")
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
                logger.info(f"[DB] Error updating user RSS feed: {e}")
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
                logger.info(f"[DB] Error deleting user RSS feed: {e}")
                return False

# --- Функции для получения RSS-элементов ---

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
                nd.source_url as source_url, -- Получаем URL оригинальной новости из published_news_data
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
                logger.info(f"[DB] Error in get_user_rss_items_list: {e}")
                raise # Перебрасываем исключение, чтобы обработать его в API

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
                nd.source_url as source_url, -- Получаем URL оригинальной новости из published_news_data
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
                logger.info(f"[DB] Error in get_user_rss_items_list_by_feed: {e}")
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
                nd.source_url as source_url, -- Получаем URL оригинальной новости из published_news_data
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
                logger.info(f"[DB] Ошибка при получении RSS-элемента по ID: {e}")
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
                nd.source_url as source_url,
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
                logger.info(f"[DB] Ошибка при получении RSS-элемента по ID (full): {e}")
                return None, []

async def get_all_rss_items_list(
    pool,
    display_language: str,
    original_language: Optional[str],
    category_id: Optional[List[int]],
    source_id: Optional[List[int]],
    telegram_published: Optional[bool],
    from_date: Optional[datetime],
    search_phrase: Optional[str],
    include_all_translations: bool,
    before_published_at: Optional[datetime],
    cursor_news_id: Optional[str],
    limit: int,
    offset: int
) -> Tuple[int, List[Tuple], List[str]]:
    """
    Получает список всех RSS-элементов с фильтрацией.
    По умолчанию джоинит только переводы nt_display (display_language). При include_all_translations=True
    добавляет JOIN для ru/en/de/fr.
    Поддерживает keyset-пагинацию через before_published_at и cursor_news_id.
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                params = []
                # Базовый SELECT
                select_parts = [
                    "nd.*",
                    "COALESCE(c.name, 'Unknown Category') AS category_name",
                    "COALESCE(s.name, 'Unknown Source') AS source_name",
                    "nd.source_url as source_url",
                    "nd.created_at as published_at",
                    "nt_display.translated_title as display_title",
                    "nt_display.translated_content as display_content",
                ]
                join_parts = [
                    "LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id",
                    "LEFT JOIN categories c ON nd.category_id = c.id",
                    "LEFT JOIN sources s ON rf.source_id = s.id",
                    "LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s",
                ]
                params.append(display_language)

                if include_all_translations:
                    # Дополнительные JOIN'ы и колонки только при необходимости
                    select_parts.extend([
                        "nt_ru.translated_title as title_ru",
                        "nt_ru.translated_content as content_ru",
                        "nt_en.translated_title as title_en",
                        "nt_en.translated_content as content_en",
                        "nt_de.translated_title as title_de",
                        "nt_de.translated_content as content_de",
                        "nt_fr.translated_title as title_fr",
                        "nt_fr.translated_content as content_fr",
                    ])
                    join_parts.extend([
                        "LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = 'ru'",
                        "LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = 'en'",
                        "LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = 'de'",
                        "LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = 'fr'",
                    ])

                query = f"""
                SELECT {', '.join(select_parts)}
                FROM published_news_data nd
                {chr(10).join(join_parts)}
                WHERE 1=1
                """

                # Фильтры
                if original_language:
                    query += " AND nd.original_language = %s"
                    params.append(original_language)
                if category_id:
                    if len(category_id) == 1:
                        query += " AND nd.category_id = %s"
                        params.append(category_id[0])
                    else:
                        placeholders = ','.join(['%s'] * len(category_id))
                        query += f" AND nd.category_id IN ({placeholders})"
                        params.extend(category_id)
                if source_id:
                    if len(source_id) == 1:
                        query += " AND rf.source_id = %s"
                        params.append(source_id[0])
                    else:
                        placeholders = ','.join(['%s'] * len(source_id))
                        query += f" AND rf.source_id IN ({placeholders})"
                        params.extend(source_id)

                telegram_published_value = None
                if telegram_published is not None:
                    telegram_published_value = bool(str(telegram_published).lower() == 'true') if isinstance(telegram_published, str) else bool(telegram_published)
                    if telegram_published_value:
                        # Для опубликованных: проверяем либо переводы, либо оригиналы
                        query += " AND (EXISTS (SELECT 1 FROM rss_items_telegram_published rtp WHERE rtp.translation_id = nt_display.id) OR EXISTS (SELECT 1 FROM rss_items_telegram_published_originals rtpo WHERE rtpo.news_id = nd.news_id))"
                    else:
                        # Для неопубликованных: проверяем отсутствие как переводов, так и оригиналов
                        query += " AND (NOT EXISTS (SELECT 1 FROM rss_items_telegram_published rtp WHERE rtp.translation_id = nt_display.id) AND NOT EXISTS (SELECT 1 FROM rss_items_telegram_published_originals rtpo WHERE rtpo.news_id = nd.news_id))"

                if from_date is not None:
                    query += " AND nd.created_at > %s"
                    params.append(from_date)

                # Поиск: OR-условия по каждому полю, без конкатенаций
                phrase = None
                if search_phrase:
                    sp = search_phrase.strip()
                    if sp:
                        phrase = f"%{sp}%"
                        query += " AND ((nt_display.translated_title ILIKE %s OR nt_display.translated_content ILIKE %s) OR (nd.original_title ILIKE %s OR nd.original_content ILIKE %s))"
                        params.extend([phrase, phrase, phrase, phrase])

                # Keyset pagination (по убыванию created_at, затем news_id)
                if before_published_at is not None:
                    query += " AND (nd.created_at < %s OR (nd.created_at = %s AND nd.news_id < %s))"
                    params.extend([before_published_at, before_published_at, cursor_news_id or "\uffff"])

                query += " ORDER BY nd.created_at DESC, nd.news_id DESC LIMIT %s OFFSET %s"
                params.extend([limit, offset])

                await cur.execute(query, params)
                results = [row async for row in cur]
                columns = [desc[0] for desc in cur.description]

                # Подсчет общего количества (без учета курсора keyset, но с остальными фильтрами)
                count_query = """
                SELECT COUNT(*)
                FROM published_news_data nd
                LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
                WHERE 1=1
                """
                count_params = [display_language]

                if original_language:
                    count_query += " AND nd.original_language = %s"
                    count_params.append(original_language)
                if category_id:
                    if len(category_id) == 1:
                        count_query += " AND nd.category_id = %s"
                        count_params.append(category_id[0])
                    else:
                        placeholders = ','.join(['%s'] * len(category_id))
                        count_query += f" AND nd.category_id IN ({placeholders})"
                        count_params.extend(category_id)
                if source_id:
                    if len(source_id) == 1:
                        count_query += " AND rf.source_id = %s"
                        count_params.append(source_id[0])
                    else:
                        placeholders = ','.join(['%s'] * len(source_id))
                        count_query += f" AND rf.source_id IN ({placeholders})"
                        count_params.extend(source_id)
                if telegram_published is not None:
                    if telegram_published_value:
                        # Для опубликованных: проверяем либо переводы, либо оригиналы
                        count_query += " AND (EXISTS (SELECT 1 FROM rss_items_telegram_published rtp WHERE rtp.translation_id = nt_display.id) OR EXISTS (SELECT 1 FROM rss_items_telegram_published_originals rtpo WHERE rtpo.news_id = nd.news_id))"
                    else:
                        # Для неопубликованных: проверяем отсутствие как переводов, так и оригиналов
                        count_query += " AND (NOT EXISTS (SELECT 1 FROM rss_items_telegram_published rtp WHERE rtp.translation_id = nt_display.id) AND NOT EXISTS (SELECT 1 FROM rss_items_telegram_published_originals rtpo WHERE rtpo.news_id = nd.news_id))"
                if from_date is not None:
                    count_query += " AND nd.created_at > %s"
                    count_params.append(from_date)

                if phrase:
                    count_query += " AND ((nt_display.translated_title ILIKE %s OR nt_display.translated_content ILIKE %s) OR (nd.original_title ILIKE %s OR nd.original_content ILIKE %s))"
                    count_params.extend([phrase, phrase, phrase, phrase])

                await cur.execute(count_query, count_params)
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0

                return total_count, results, columns
            except Exception as e:
                logger.info(f"[DB] Ошибка при выполнении запроса в get_all_rss_items_list: {e}")
                raise

async def get_all_categories_list(
    pool, 
    limit: int, 
    offset: int, 
    source_ids: Optional[List[int]] = None
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Получает список всех категорий с пагинацией и фильтрацией по source_id.
    Возвращает кортеж (total_count, results).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Базовый запрос
                count_query = "SELECT COUNT(*) FROM categories WHERE id != %s"
                data_query = "SELECT id, name FROM categories WHERE id != %s"
                conditions = []
                params = [config.USER_DEFINED_RSS_CATEGORY_ID]
                count_params = [config.USER_DEFINED_RSS_CATEGORY_ID]

                # Добавляем фильтр по source_id, если передан
                if source_ids:
                    placeholders = ','.join(['%s'] * len(source_ids))
                    conditions.append(f"id IN (SELECT category_id FROM source_categories WHERE source_id IN ({placeholders}) AND category_id != %s)")
                    params.extend(source_ids)
                    params.append(config.USER_DEFINED_RSS_CATEGORY_ID)
                    count_params.extend(source_ids)
                    count_params.append(config.USER_DEFINED_RSS_CATEGORY_ID)

                where_clause = ""
                if conditions:
                    where_clause = " AND " + " AND ".join(conditions)
                else:
                    where_clause = ""

                # Получаем общее количество
                await cur.execute(count_query + where_clause, count_params)
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0

                # Получаем список с пагинацией
                final_query = data_query + where_clause + " ORDER BY name LIMIT %s OFFSET %s"
                await cur.execute(final_query, params + [limit, offset])
                results = []
                async for row in cur:
                    results.append({"id": row[0], "name": row[1]})

                return total_count, results
            except Exception as e:
                logger.info(f"[DB] Ошибка при выполнении запроса в get_all_categories_list: {e}")
                raise

async def get_all_sources_list(
    pool, 
    limit: int, 
    offset: int, 
    category_id: Optional[List[int]] = None
) -> Tuple[int, List[Dict[str, Any]]]:
    """
    Получает список всех источников с пагинацией и опциональной фильтрацией по категориям.
    Возвращает кортеж (total_count, results).
    """
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Формируем базовые части запроса
                base_query_select = """
                    SELECT DISTINCT s.id, s.name, s.description, s.alias, s.logo, s.site_url
                    FROM sources s
                """
                base_query_count = """
                    SELECT COUNT(DISTINCT s.id)
                    FROM sources s
                """

                # Если переданы категории, добавляем JOIN
                if category_id:
                    join_clause = """
                        JOIN source_categories sc ON s.id = sc.source_id
                        WHERE sc.category_id = ANY(%s)
                    """
                    full_query_select = base_query_select + join_clause + " ORDER BY s.name LIMIT %s OFFSET %s"
                    full_query_count = base_query_count + join_clause
                else:
                    full_query_select = base_query_select + " ORDER BY s.name LIMIT %s OFFSET %s"
                    full_query_count = base_query_count

                # Выполняем подсчёт общего количества записей
                await cur.execute(full_query_count, (category_id,) if category_id else ())
                total_count_row = await cur.fetchone()
                total_count = total_count_row[0] if total_count_row else 0

                # Выполняем выборку данных с пагинацией
                params = (category_id, limit, offset) if category_id else (limit, offset)
                await cur.execute(full_query_select, params)

                results = []
                async for row in cur:
                    results.append({
                        "id": row[0],
                        "name": row[1],
                        "description": row[2],
                        "alias": row[3],
                        "logo": row[4],
                        "site_url": row[5]
                    })

                return total_count, results
            except Exception as e:
                logger.info(f"[DB] Ошибка при выполнении запроса в get_all_sources_list: {e}")
                raise

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
                    nd.original_language,
                    c.name as category_name,
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
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = 'ru'
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = 'en'
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = 'de'
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = 'fr'
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
                        "original_language": row_dict['original_language'],
                        "category": row_dict['category_name'],
                        "published_at": row_dict['published_at'].isoformat() if row_dict['published_at'] else None,
                        "translations": {
                            "ru": {"title": row_dict.get('title_ru'), "content": row_dict.get('content_ru')},
                            "en": {"title": row_dict.get('title_en'), "content": row_dict.get('content_en')},
                            "de": {"title": row_dict.get('title_de'), "content": row_dict.get('content_de')},
                            "fr": {"title": row_dict.get('title_fr'), "content": row_dict.get('content_fr')}
                        }
                    })
                return news_items
            except Exception as e:
                logger.info(f"[DB] Error in get_recent_news_for_broadcast: {e}")
                return [] # Возвращаем пустой список в случае ошибки, чтобы не прерывать фоновую задачу