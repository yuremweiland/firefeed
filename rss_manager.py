import aiopg
import hashlib
import feedparser
import asyncio
import re
import pytz
from datetime import datetime, timezone, timedelta
from dateutil import parser
from config import DB_CONFIG, MAX_ENTRIES_PER_FEED, MAX_TOTAL_NEWS
from translator import prepare_translations

class RSSManager:
    def __init__(self, duplicate_detector=None):
        self.dublicate_detector = duplicate_detector
        self.pool = None
        self._init_lock = asyncio.Lock() # Для предотвращения race condition при инициализации

    async def init_pool(self):
        """Инициализация пула соединений"""
        if self.pool is None:
            async with self._init_lock: # Блокировка для предотвращения race condition
                if self.pool is None:  # Проверка еще раз внутри блокировки
                    self.pool = await aiopg.create_pool(**DB_CONFIG)
        return self.pool

    async def close_pool(self):
        """Закрытие пула соединений"""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()
            self.pool = None

    async def _get_all_feeds(self):
        """Вспомогательный метод: Получает список ВСЕХ RSS-лент."""
        await self.init_pool()
        feeds = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT 
                        f.id AS feed_id,
                        f.url AS feed_url,
                        f.name AS feed_name,
                        f.language AS feed_lang,
                        s.name AS source_name,
                        s.id AS source_id,
                        c.name AS category_name,
                        c.display_name AS category_display_name
                    FROM rss_feeds f
                    JOIN sources s ON f.source_id = s.id
                    LEFT JOIN categories c ON f.category_id = c.id
                """
                await cur.execute(query)
                async for row in cur:
                    feeds.append({
                        'id': row[0],
                        'url': row[1].strip(),
                        'name': row[2],
                        'lang': row[3],
                        'source': row[4],
                        'source_id': row[5],
                        'category': row[6] if row[6] else 'uncategorized',
                        'category_display': row[7]
                    })
        return feeds

    async def _get_all_active_feeds(self):
        """Вспомогательный метод: Получает список АКТИВНЫХ RSS-лент."""
        await self.init_pool()
        feeds = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT 
                        f.id AS feed_id,
                        f.url AS feed_url,
                        f.name AS feed_name,
                        f.language AS feed_lang,
                        s.name AS source_name,
                        s.id AS source_id,
                        c.name AS category_name,
                        c.display_name AS category_display_name
                    FROM rss_feeds f
                    JOIN sources s ON f.source_id = s.id
                    LEFT JOIN categories c ON f.category_id = c.id
                    WHERE f.is_active = TRUE
                """
                await cur.execute(query)
                async for row in cur:
                    feeds.append({
                        'id': row[0],
                        'url': row[1].strip(),
                        'name': row[2],
                        'lang': row[3],
                        'source': row[4],
                        'source_id': row[5],
                        'category': row[6] if row[6] else 'uncategorized',
                        'category_display': row[7]
                    })
        return feeds

    async def _get_feeds_by_category(self, category_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени категории."""
        await self.init_pool()
        feeds = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE c.name = %s AND rf.is_active = TRUE
                """
                await cur.execute(query, (category_name,))
                columns = [desc[0] for desc in cur.description]
                async for row in cur:
                    feeds.append(dict(zip(columns, row)))
        return feeds

    async def _get_feeds_by_lang(self, lang):
        """Вспомогательный метод: Получить активные RSS-ленты по языку."""
        await self.init_pool()
        feeds = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name 
                    FROM rss_feeds rf 
                    JOIN categories c ON rf.category_id = c.id 
                    JOIN sources s ON rf.source_id = s.id 
                    WHERE rf.language = %s AND rf.is_active = TRUE
                """
                await cur.execute(query, (lang,))
                columns = [desc[0] for desc in cur.description]
                async for row in cur:
                    feeds.append(dict(zip(columns, row)))
        return feeds

    async def _get_feeds_by_source(self, source_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени источника."""
        await self.init_pool()
        feeds = []
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE s.name = %s AND rf.is_active = TRUE
                """
                await cur.execute(query, (source_name,))
                columns = [desc[0] for desc in cur.description]
                async for row in cur:
                    feeds.append(dict(zip(columns, row)))
        return feeds

    async def _add_feed(self, category_name, url, language, source_name):
        """Вспомогательный метод: Добавить новую RSS-ленту."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 1. Получить ID категории по имени
                    await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                    cat_result = await cur.fetchone()
                    if not cat_result:
                        print(f"[DB] [RSSManager] Ошибка: Категория '{category_name}' не найдена в таблице 'categories'.")
                        return False
                    category_id = cat_result[0]
                    # 2. Получить ID источника по имени
                    await cur.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
                    src_result = await cur.fetchone()
                    if not src_result:
                        print(f"[DB] [RSSManager] Ошибка: Источник '{source_name}' не найден в таблице 'sources'.")
                        return False
                    source_id = src_result[0]
                    # 3. Вставить новую ленту
                    feed_name = url.split('/')[-1] or "Новая лента"
                    query = """
                        INSERT INTO rss_feeds (source_id, url, name, category_id, language, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    await cur.execute(query, (source_id, url, feed_name, category_id, language, True))
                    # В aiopg транзакции управляются автоматически, commit не нужен
                    print(f"[DB] [RSSManager] Лента '{url}' успешно добавлена.")
                    return True
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при добавлении фида '{url}': {e}")
            return False

    async def _update_feed(self, feed_id, category_name, url, language, source_name, is_active, feed_name):
        """Вспомогательный метод: Обновить RSS-ленту. None означает "не обновлять это поле"."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    updates = []
                    values = []
                    # Обработка изменения категории по имени (если не None)
                    if category_name is not None:
                        await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                        cat_result = await cur.fetchone()
                        if cat_result:
                            updates.append("category_id = %s")
                            values.append(cat_result[0])
                        else:
                            print(f"[DB] [RSSManager] Предупреждение: Категория '{category_name}' не найдена. Поле category_id не обновлено.")
                    # Обработка изменения источника по имени (если не None)
                    if source_name is not None:
                        await cur.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
                        src_result = await cur.fetchone()
                        if src_result:
                            updates.append("source_id = %s")
                            values.append(src_result[0])
                        else:
                            print(f"[DB] [RSSManager] Предупреждение: Источник '{source_name}' не найден. Поле source_id не обновлено.")
                    # Обработка других полей (если не None)
                    if url is not None:
                        updates.append("url = %s")
                        values.append(url)
                    if language is not None:
                        updates.append("language = %s")
                        values.append(language)
                    if is_active is not None:
                        updates.append("is_active = %s")
                        values.append(is_active)
                    if feed_name is not None:
                        updates.append("name = %s")
                        values.append(feed_name)
                    if not updates:
                        print("[DB] [RSSManager] Нет полей для обновления.")
                        return False
                    values.append(feed_id)
                    query = f"UPDATE rss_feeds SET {', '.join(updates)} WHERE id = %s"
                    await cur.execute(query, values)
                    # В aiopg транзакции управляются автоматически, commit не нужен
                    await cur.execute("SELECT COUNT(*) FROM rss_feeds WHERE id = %s", (feed_id,))
                    result = await cur.fetchone()
                    affected_rows = result[0] if result else 0
                    if affected_rows > 0:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно обновлена.")
                    else:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена или не была изменена.")
                    return affected_rows > 0
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при обновлении фида с ID {feed_id}: {e}")
            return False

    async def _delete_feed(self, feed_id):
        """Вспомогательный метод: Удалить RSS-ленту по ID."""
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = "DELETE FROM rss_feeds WHERE id = %s"
                    await cur.execute(query, (feed_id,))
                    # В aiopg транзакции управляются автоматически, commit не нужен
                    affected_rows = cur.rowcount
                    if affected_rows > 0:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно удалена.")
                    else:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена.")
                    return affected_rows > 0
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при удалении фида с ID {feed_id}: {e}")
            return False

    async def _get_categories(self):
        """Вспомогательный метод: Получить список всех категорий."""
        await self.init_pool()
        categories = []
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    get_categories_query = """
                        SELECT DISTINCT c.name AS category
                        FROM categories c
                        JOIN rss_feeds rf ON c.id = rf.category_id
                        WHERE rf.is_active = TRUE
                        ORDER BY c.name;
                    """
                    await cur.execute(get_categories_query)
                    async for row in cur:
                        categories.append(row[0])
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении категорий: {e}")
        return categories

    async def _get_feed_cooldown_minutes(self, rss_feed_id):
        """Вспомогательный метод: Получить время кулдауна в минутах для конкретной RSS-ленты"""
        await self.init_pool()
        minutes = 20
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                        SELECT cooldown_minutes 
                        FROM rss_feeds 
                        WHERE id = %s AND is_active = true
                    """
                    await cur.execute(query, (rss_feed_id,))
                    row = await cur.fetchone()
                    minutes = row[0] if row else 20
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении времени кулдауна: {e}")
        return minutes

    async def _get_last_published_time_for_feed(self, rss_feed_id):
        """Вспомогательный метод: Получить время последней публикации из конкретной RSS-ленты"""
        await self.init_pool()
        published_time = None
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                        SELECT created_at 
                        FROM published_news_data 
                        WHERE rss_feed_id = %s 
                        ORDER BY created_at DESC 
                        LIMIT 1
                    """
                    await cur.execute(query, (rss_feed_id,))
                    row = await cur.fetchone()
                    published_time = row[0] if row else None
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении времени последней публикации из конкретной RSS-ленты: {e}")
        return published_time

    async def _get_recent_news_count_for_feed(self, rss_feed_id, minutes=60):
        """Вспомогательный метод: Получает количество новостей из ленты за последние N минут"""
        await self.init_pool()
        news_count = 0
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT COUNT(*) 
                        FROM published_news_data 
                        WHERE rss_feed_id = %s 
                        AND created_at >= NOW() - INTERVAL '%s minutes'
                    """, (rss_feed_id, minutes))
                    row = await cur.fetchone()
                    news_count = row[0] if row else 0
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка в _get_recent_news_count_for_feed: {e}")
        return news_count

    async def _get_max_news_per_hour_for_feed(self, rss_feed_id):
        """Вспомогательный метод: Получает максимальное количество новостей в час для ленты"""
        await self.init_pool()
        max_news = 1
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT cooldown_minutes 
                        FROM rss_feeds 
                        WHERE id = %s
                    """, (rss_feed_id,))
                    row = await cur.fetchone()
                    if row and row[0]:
                        cooldown_minutes = row[0]
                        # Пропорциональная формула: 1 новость за cooldown_minutes
                        # Например: 360 минут = 1 новость за 6 часов = 1/6 новостей в час
                        max_news = max(1, round(60 / cooldown_minutes))
                        # Для корректного округления при больших значениях
                        if cooldown_minutes > 60:
                            # Округляем вниз для больших интервалов
                            max_news = max(1, 60 // cooldown_minutes)
                            # Если деление дает 0, то ставим 1 (минимум одна новость за период)
                            if max_news == 0:
                                max_news = 1
                        return max_news
                    else:
                        return 1  # По умолчанию 1 новость в час
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка в _get_max_news_per_hour_for_feed: {e}")
            return 1
        return max_news

    async def is_news_new(self, title_hash, content_hash, url):
        """
        Асинхронно проверяет, является ли новость новой (не опубликованной ранее).
        """
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Проверяем существование по title_hash ИЛИ content_hash
                    query = """
                        SELECT 1 FROM published_news 
                        WHERE title_hash = %s OR content_hash = %s 
                        LIMIT 1
                    """
                    await cur.execute(query, (title_hash, content_hash))
                    result = await cur.fetchone()
                    # Если результат есть (result не None), новость считается НЕ новой
                    is_duplicate = result is not None
                    return not is_duplicate # Возвращаем True, если НЕ дубликат
        except Exception as err:
            print(f"[DB] [is_news_new] Ошибка БД: {err}")
            # В случае ошибки БД лучше считать новость НЕ новой, чтобы избежать дубликатов
            return False

    async def mark_as_published(self, title, content, url, original_language, translations_dict, category_name=None, image_filename=None, rss_feed_id=None):
        """
        Асинхронно сохраняет информацию о опубликованной новости с проверкой уникальности (хэши).
        Сохраняет оригинальные данные и переводы новости для API.
        """
        # 1. Генерируем ID ОДИН РАЗ
        title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        news_id = f"{title_hash}_{content_hash}"
        short_id = news_id[:20] + "..." if len(news_id) > 20 else news_id
        print(f"[DB] [mark_as_published] Начало обработки для ID: {short_id}")
        await self.init_pool()
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # --- Получаем category_id по названию категории ---
                    category_id = None
                    if category_name:
                        category_query = "SELECT id FROM categories WHERE name = %s LIMIT 1"
                        await cur.execute(category_query, (category_name,))
                        category_result = await cur.fetchone()
                        if category_result:
                            category_id = category_result[0]
                        else:
                            print(f"[DB] [WARN] Категория '{category_name}' не найдена в таблице categories")
                    # --- ГАРАНТИРУЕМ существование записи в published_news ---
                    query_published_news = """
                    INSERT INTO published_news (id, title_hash, content_hash, source_url, published_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET 
                        source_url = EXCLUDED.source_url,
                        published_at = NOW()
                    """
                    print(f"[DB] [mark_as_published] Подготовка запроса к 'published_news' (ID: {short_id})")
                    await cur.execute(query_published_news, (news_id, title_hash, content_hash, url))
                    print(f"[DB] [mark_as_published] Запрос к 'published_news' выполнен. (ID: {short_id})")
                    # --- УБИРАЕМ commit - пусть работает в autocommit режиме ---
                    print(f"[DB] [mark_as_published] Операция в 'published_news' выполнена. (ID: {short_id})")
                    # -------------------------------------------------------------
                    # 2b. Проверяем существование
                    check_query = "SELECT 1 FROM published_news WHERE id = %s LIMIT 1"
                    print(f"[DB] [mark_as_published] Выполнение проверочного SELECT (ID: {short_id})")
                    await cur.execute(check_query, (news_id,))
                    exists_in_parent = await cur.fetchone()
                    if not exists_in_parent:
                        # Критическая ошибка
                        error_msg = f"[DB] [CRITICAL] Запись в 'published_news' НЕ существует! FK constraint будет нарушено. (ID: {short_id})"
                        print(error_msg)
                        # Отладочный запрос
                        debug_query = "SELECT id, title_hash, content_hash FROM published_news WHERE id = %s OR title_hash = %s OR content_hash = %s LIMIT 5"
                        debug_params = (news_id, title_hash, content_hash)
                        print(f"[DB] [DEBUG] Выполнение отладочного запроса по ID, title_hash, content_hash...")
                        await cur.execute(debug_query, debug_params)
                        debug_results = []
                        async for row in cur:
                            debug_results.append(row)
                        if debug_results:
                            print(f"[DB] [DEBUG] Найдены потенциально конфликтующие записи в 'published_news':")
                            for row in debug_results:
                                print(f"  - ID: {row[0]}, Title_Hash: {row[1][:20]}..., Content_Hash: {row[2][:20]}...")
                        else:
                            print(f"[DB] [DEBUG] Записи с таким ID, title_hash или content_hash в 'published_news' НЕ НАЙДЕНЫ.")
                        # Возвращаем False вместо исключения, чтобы не прерывать всю задачу
                        return False 
                    else:
                        print(f"[DB] [mark_as_published] Подтверждено: запись в 'published_news' существует. (ID: {short_id})")
                    # -------------------------------------------------------------
                    # 3. ВСТАВЛЯЕМ или ОБНОВЛЯЕМ в дочерней таблице published_news_data
                    query_published_news_data = """
                    INSERT INTO published_news_data 
                    (news_id, original_title, original_content, original_language, category_id, image_filename, rss_feed_id, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (news_id) DO UPDATE SET
                        original_title = EXCLUDED.original_title,
                        original_content = EXCLUDED.original_content,
                        original_language = EXCLUDED.original_language,
                        category_id = EXCLUDED.category_id,
                        image_filename = EXCLUDED.image_filename,
                        rss_feed_id = EXCLUDED.rss_feed_id,
                        updated_at = NOW()
                    """
                    print(f"[DB] [mark_as_published] Подготовка запроса к 'published_news_data' (ID: {short_id})")
                    await cur.execute(query_published_news_data, (
                        news_id,
                        title, 
                        content, 
                        original_language, 
                        category_id,
                        image_filename,
                        rss_feed_id
                    ))
                    print(f"[DB] [mark_as_published] Выполнен запрос к 'published_news_data'. (ID: {short_id})")
                    # 4. ВСТАВЛЯЕМ или ОБНОВЛЯЕМ переводы в news_translations
                    for lang_code, trans_data in translations_dict.items():
                        if lang_code in ['ru', 'en', 'de', 'fr'] and isinstance(trans_data, dict):
                            trans_title = trans_data.get('title', title)
                            trans_content = trans_data.get('description', content)
                            query_translation = """
                            INSERT INTO news_translations (news_id, language, translated_title, translated_content, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (news_id, language) DO UPDATE SET
                                translated_title = EXCLUDED.translated_title,
                                translated_content = EXCLUDED.translated_content,
                                updated_at = NOW()
                            """
                            await cur.execute(query_translation, (news_id, lang_code, trans_title, trans_content))
                    # УБИРАЕМ commit - все операции выполняются по отдельности
                    print(f"[DB] [SUCCESS] Новость и переводы сохранены: {short_id}")
                    print(f"[DB] [mark_as_published] Обработка переводов завершена. (ID: {short_id})")
                    return True # <-- Возвращаем True при успехе
        except Exception as err:
            print(f"[DB] [ERROR] Ошибка БД при сохранении (ID: {short_id}): {err}")
            import traceback
            traceback.print_exc()
            return False

    # --- Публичные асинхронные методы ---
    async def get_all_feeds(self):
        """Асинхронно получает список ВСЕХ RSS-лент."""
        return await self._get_all_feeds()

    async def get_all_active_feeds(self):
        """Асинхронно получает список АКТИВНЫХ RSS-лент."""
        return await self._get_all_active_feeds()

    async def get_feeds_by_category(self, category_name):
        """Асинхронно получить активные RSS-ленты по имени категории."""
        return await self._get_feeds_by_category(category_name)

    async def get_feeds_by_lang(self, lang):
        """Асинхронно получить активные RSS-ленты по языку."""
        return await self._get_feeds_by_lang(lang)

    async def get_feeds_by_source(self, source_name):
        """Асинхронно получить активные RSS-ленты по имени источника."""
        return await self._get_feeds_by_source(source_name)

    async def add_feed(self, category_name, url, language, source_name):
        """Асинхронно добавить новую RSS-ленту."""
        return await self._add_feed(category_name, url, language, source_name)

    async def update_feed(self, feed_id, category_name=None, url=None, language=None, source_name=None, is_active=None, feed_name=None):
        """Асинхронно обновить RSS-ленту. None означает "не обновлять это поле"."""
        return await self._update_feed(feed_id, category_name, url, language, source_name, is_active, feed_name)

    async def delete_feed(self, feed_id):
        """Асинхронно удалить RSS-ленту по ID."""
        return await self._delete_feed(feed_id)

    async def get_categories(self):
        """Асинхронно получить список всех категорий."""
        return await self._get_categories()

    async def get_feed_cooldown_minutes(self, rss_feed_id):
        """Асинхронно получить время кулдауна в минутах для конкретной RSS-ленты"""
        return await self._get_feed_cooldown_minutes(rss_feed_id)

    async def get_last_published_time_for_feed(self, rss_feed_id):
        """Асинхронно получить время последней публикации из конкретной RSS-ленты"""
        return await self._get_last_published_time_for_feed(rss_feed_id)

    async def get_recent_news_count_for_feed(self, rss_feed_id, minutes=60):
        """Асинхронно получает количество новостей из ленты за последние N минут"""
        return await self._get_recent_news_count_for_feed(rss_feed_id, minutes)

    async def get_max_news_per_hour_for_feed(self, rss_feed_id):
        """Асинхронно получает максимальное количество новостей в час для ленты"""
        return await self._get_max_news_per_hour_for_feed(rss_feed_id)

    async def fetch_single_feed(self, feed_info, seen_keys, headers):
        """
        Асинхронно парсит одну RSS-ленту и возвращает список новостей из неё.
        Перед парсингом проверяет cooldown и лимиты.
        """
        local_news = []
        rss_feed_id = feed_info['id']
        # Получаем параметры ленты
        cooldown_minutes = await self.get_feed_cooldown_minutes(rss_feed_id)
        max_news_per_hour = await self.get_max_news_per_hour_for_feed(rss_feed_id)
        recent_count = await self.get_recent_news_count_for_feed(rss_feed_id, cooldown_minutes)
        # Проверка лимита новостей за период кулдауна
        if recent_count >= max_news_per_hour:
            print(f"[SKIP] Лента ID {rss_feed_id} достигла лимита {max_news_per_hour} новостей за {cooldown_minutes} минут. Опубликовано: {recent_count}")
            return local_news
        # Проверка кулдауна (время последней публикации)
        last_published = await self.get_last_published_time_for_feed(rss_feed_id)
        if last_published:
            elapsed = datetime.now(timezone.utc) - last_published
            if elapsed < timedelta(minutes=cooldown_minutes):
                print(f"[SKIP] Лента ID {rss_feed_id} находится на кулдауне ({cooldown_minutes} мин). Прошло: {elapsed}")
                return local_news
        # Если лента прошла все проверки — парсим её
        try:
            print(f"[RSS] Парсинг ленты: {feed_info['name']} ({feed_info['url']})")
            feed = feedparser.parse(feed_info['url'], request_headers=headers)
            # Логируем ошибки парсинга
            if getattr(feed, 'bozo', 0):
                exc = getattr(feed, 'bozo_exception', None)
                if exc:
                    error_type = type(exc).__name__
                    print(f"[RSS] Ошибка парсинга ({error_type}) в {feed_info['url']}: {str(exc)[:200]}")
            if not feed.entries:
                print(f"[RSS] Нет записей в {feed_info['url']}")
                return local_news
            for entry in feed.entries[:MAX_ENTRIES_PER_FEED]:
                title = getattr(entry, 'title', 'Untitled').strip()
                description = entry.get('description', '')
                if title == description:
                    continue
                normalized_title = re.sub(r'\s+', ' ', title).lower()
                unique_key = (feed_info['source'], feed_info['category'], normalized_title)
                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)
                entry_link = entry.get('link', '#')

                # Извлечение изображения
                image_url = self.extract_image_from_rss_item(entry)

                # Проверяем уникальность через БД (хэши)
                title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
                content_hash = hashlib.sha256(description.encode('utf-8')).hexdigest()
                is_new = await self.is_news_new(title_hash, content_hash, entry_link)
                is_unique = await self.dublicate_detector.process_news(
                    news_id=f"{title_hash}_{content_hash}",
                    title=title,
                    content=description
                )
                if not is_new or not is_unique:
                    continue

                # Обработка даты с fallback
                pub_date = getattr(entry, 'published', None)
                if pub_date:
                    try:
                        published = parser.parse(pub_date)
                        published = published.replace(tzinfo=pytz.utc)
                    except Exception as e:
                        print(f"[RSS] Ошибка парсинга даты '{pub_date}': {e}. Используется текущее время.")
                        published = datetime.now(pytz.utc)
                else:
                    published = datetime.now(pytz.utc)

                news_item = {
                    'id': f"{title_hash}_{content_hash}",
                    'title': title,
                    'description': description,
                    'link': entry_link,
                    'published': published,
                    'category': feed_info['category'],
                    'lang': feed_info['lang'],
                    'source': feed_info['source'],
                    'image_url': image_url
                }
                local_news.append(news_item)
        except Exception as e:
            print(f"[RSS] Ошибка при обработке ленты {feed_info['url']}: {e}")
            import traceback
            traceback.print_exc()
        return local_news

    async def fetch_news(self):
        """Асинхронная функция для получения новостей из RSS-лент"""
        seen_keys = set()
        all_news = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }
        try:
            active_feeds = await self.get_all_active_feeds()
            print(f"[RSS] Найдено {len(active_feeds)} активных RSS-лент.")
            # Создаём задачи по обработке всех активных лент
            tasks = [
                self.fetch_single_feed(feed_info, seen_keys, headers)
                for feed_info in active_feeds
            ]
            # Выполняем все задачи одновременно
            results = await asyncio.gather(*tasks, return_exceptions=True)
            # Обрабатываем результаты
            for i, result in enumerate(results):
                feed_info = active_feeds[i] if i < len(active_feeds) else None
                feed_url = feed_info['url'] if feed_info else "Unknown Feed"
                feed_id = feed_info['id'] if feed_info else None
                if isinstance(result, Exception):
                    print(f"[RSS] [ERROR] Исключение при парсинге {feed_url}: {result}")
                elif isinstance(result, list):
                    # Добавляем rss_feed_id к каждой новости
                    for news_item in result:
                        news_item['rss_feed_id'] = feed_id
                    all_news.extend(result)
                else:
                    print(f"[RSS] [WARN] Неожиданный тип результата для {feed_url}: {type(result)}")
        except Exception as e:
            import traceback
            print(f"❌ Критическая ошибка в fetch_news: {e}")
            traceback.print_exc()
        # Сортировка и ограничение количества новостей
        sorted_news = sorted(all_news, key=lambda x: x['published'], reverse=True)
        final_news = sorted_news[:MAX_TOTAL_NEWS]
        print(f"[RSS] Всего собрано уникальных новостей: {len(final_news)}")
        return final_news

    def extract_image_from_rss_item(self, item):
        """
        Извлекает URL изображения из элемента RSS item.
        Приоритет: enclosure -> media:content -> media:thumbnail -> rbc_news:image
        """
        try:
            # 1. enclosure с типом image/*
            enclosures = item.get('enclosures', [])
            for enc in enclosures:
                if enc.get('type', '').startswith('image/'):
                    url = enc.get('href') or enc.get('url')
                    if url:
                        print(f"[INFO] Найдено изображение в enclosure: {url}")
                        return url
            # 2. enclosure с расширением файла
            for enc in enclosures:
                url = enc.get('href') or enc.get('url')
                if url and re.search(r'\.(jpe?g|png|gif|webp)(\?.*)?$', url, re.IGNORECASE):
                    print(f"[INFO] Найдено изображение в enclosure (по расширению): {url}")
                    return url
            # 3. media:content
            media_content = item.get('media_content') or item.get('media:content')
            if media_content:
                if isinstance(media_content, list):
                    for media in media_content:
                        if isinstance(media, dict) and media.get('medium') == 'image':
                            media_url = media.get('url')
                            if media_url:
                                print(f"[INFO] Найдено изображение в media:content (list): {media_url}")
                                return media_url
                elif isinstance(media_content, dict) and media_content.get('medium') == 'image':
                    media_url = media_content.get('url')
                    if media_url:
                        print(f"[INFO] Найдено изображение в media:content (dict): {media_url}")
                        return media_url
            # 4. media:thumbnail
            media_thumbnail = item.get('media_thumbnail') or item.get('media:thumbnail')
            if media_thumbnail:
                if isinstance(media_thumbnail, list) and len(media_thumbnail) > 0:
                    thumb = media_thumbnail[0]
                    if isinstance(thumb, dict):
                        thumb_url = thumb.get('url')
                        if thumb_url:
                            print(f"[INFO] Найдено изображение в media:thumbnail (list): {thumb_url}")
                            return thumb_url
                elif isinstance(media_thumbnail, dict):
                    thumb_url = media_thumbnail.get('url')
                    if thumb_url:
                        print(f"[INFO] Найдено изображение в media:thumbnail (dict): {thumb_url}")
                        return thumb_url
            # 5. rbc_news:image (проверяем оба возможных формата ключа)
            rbc_image_data = item.get('rbc_news_image') or item.get('rbc_news:image')
            if isinstance(rbc_image_data, dict):
                image_url = rbc_image_data.get('rbc_news_url') or rbc_image_data.get('url')
                if image_url:
                    print(f"[INFO] Найдено изображение в rbc_news:image (dict): {image_url}")
                    return image_url.strip()
            elif isinstance(rbc_image_data, list) and len(rbc_image_data) > 0:
                first_image = rbc_image_data[0]
                if isinstance(first_image, dict):
                    image_url = first_image.get('rbc_news_url') or first_image.get('url')
                    if image_url:
                        print(f"[INFO] Найдено изображение в rbc_news:image (list): {image_url}")
                        return image_url.strip()
            print("[INFO] Изображение не найдено в RSS item.")
            return None
        except Exception as e:
            print(f"[WARN] Ошибка при извлечении изображения из RSS item: {e}")
            return None

    def extract_video_from_rss_item(self, item):
        """
        Извлекает URL видео из элемента RSS item с учетом ограничений размера Telegram.
        Приоритет: enclosure -> media:content
        """
        # Дефолтные ограничения Telegram для ботов
        TELEGRAM_VIDEO_LIMIT = 50 * 1024 * 1024  # 50 MB
        try:
            # 1. enclosure с типом video/* и проверкой размера
            enclosures = item.get('enclosures', [])
            for enc in enclosures:
                if enc.get('type', '').startswith('video/'):
                    url = enc.get('href') or enc.get('url')
                    if url:
                        # Проверяем размер, если доступен
                        length = enc.get('length')
                        size_ok = True
                        if length is not None:
                            try:
                                length = int(length)
                                if length > TELEGRAM_VIDEO_LIMIT:
                                    print(f"[INFO] Видео превышает лимит размера Telegram ({length} > {TELEGRAM_VIDEO_LIMIT}): {url}")
                                    size_ok = False
                            except (ValueError, TypeError):
                                pass  # Не удалось преобразовать размер
                        if size_ok:
                            print(f"[INFO] Найдено видео в enclosure: {url}")
                            return url
            # 2. enclosure с расширением видео файла и проверкой размера
            for enc in enclosures:
                url = enc.get('href') or enc.get('url')
                if url and re.search(r'\.(mp4|avi|mov|wmv|flv|webm|mkv)(\?.*)?$', url, re.IGNORECASE):
                    # Проверяем размер, если доступен
                    length = enc.get('length')
                    size_ok = True
                    if length is not None:
                        try:
                            length = int(length)
                            if length > TELEGRAM_VIDEO_LIMIT:
                                print(f"[INFO] Видео превышает лимит размера Telegram ({length} > {TELEGRAM_VIDEO_LIMIT}): {url}")
                                size_ok = False
                        except (ValueError, TypeError):
                            pass  # Не удалось преобразовать размер
                    if size_ok:
                        print(f"[INFO] Найдено видео в enclosure (по расширению): {url}")
                        return url
            # 3. media:content с типом video и проверкой размера
            media_content = item.get('media_content') or item.get('media:content')
            if media_content:
                if isinstance(media_content, list):
                    for media in media_content:
                        if isinstance(media, dict) and media.get('medium') == 'video':
                            media_url = media.get('url')
                            if media_url:
                                # Проверяем размер, если доступен
                                file_size = media.get('fileSize') or media.get('filesize')
                                size_ok = True
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > TELEGRAM_VIDEO_LIMIT:
                                            print(f"[INFO] Видео превышает лимит размера Telegram ({file_size} > {TELEGRAM_VIDEO_LIMIT}): {media_url}")
                                            size_ok = False
                                    except (ValueError, TypeError):
                                        pass  # Не удалось преобразовать размер
                                if size_ok:
                                    print(f"[INFO] Найдено видео в media:content (list): {media_url}")
                                    return media_url
                elif isinstance(media_content, dict) and media_content.get('medium') == 'video':
                    media_url = media_content.get('url')
                    if media_url:
                        # Проверяем размер, если доступен
                        file_size = media_content.get('fileSize') or media_content.get('filesize')
                        size_ok = True
                        if file_size is not None:
                            try:
                                file_size = int(file_size)
                                if file_size > TELEGRAM_VIDEO_LIMIT:
                                    print(f"[INFO] Видео превышает лимит размера Telegram ({file_size} > {TELEGRAM_VIDEO_LIMIT}): {media_url}")
                                    size_ok = False
                            except (ValueError, TypeError):
                                pass  # Не удалось преобразовать размер
                        if size_ok:
                            print(f"[INFO] Найдено видео в media:content (dict): {media_url}")
                            return media_url
            # 4. media:content с типом video/* в атрибуте type и проверкой размера
            if media_content:
                if isinstance(media_content, list):
                    for media in media_content:
                        if isinstance(media, dict):
                            content_type = media.get('type', '')
                            if content_type.startswith('video/'):
                                media_url = media.get('url')
                                if media_url:
                                    # Проверяем размер, если доступен
                                    file_size = media.get('fileSize') or media.get('filesize')
                                    size_ok = True
                                    if file_size is not None:
                                        try:
                                            file_size = int(file_size)
                                            if file_size > TELEGRAM_VIDEO_LIMIT:
                                                print(f"[INFO] Видео превышает лимит размера Telegram ({file_size} > {TELEGRAM_VIDEO_LIMIT}): {media_url}")
                                                size_ok = False
                                        except (ValueError, TypeError):
                                            pass  # Не удалось преобразовать размер
                                    if size_ok:
                                        print(f"[INFO] Найдено видео в media:content по типу: {media_url}")
                                        return media_url
                elif isinstance(media_content, dict):
                    content_type = media_content.get('type', '')
                    if content_type.startswith('video/'):
                        media_url = media_content.get('url')
                        if media_url:
                            # Проверяем размер, если доступен
                            file_size = media_content.get('fileSize') or media_content.get('filesize')
                            size_ok = True
                            if file_size is not None:
                                try:
                                    file_size = int(file_size)
                                    if file_size > TELEGRAM_VIDEO_LIMIT:
                                        print(f"[INFO] Видео превышает лимит размера Telegram ({file_size} > {TELEGRAM_VIDEO_LIMIT}): {media_url}")
                                        size_ok = False
                                except (ValueError, TypeError):
                                    pass  # Не удалось преобразовать размер
                            if size_ok:
                                print(f"[INFO] Найдено видео в media:content по типу: {media_url}")
                                return media_url
            print("[INFO] Видео не найдено в RSS item или все видео превышают лимит размера Telegram.")
            return None
        except Exception as e:
            print(f"[WARN] Ошибка при извлечении видео из RSS item: {e}")
            return None