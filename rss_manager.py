# rss_manager.py
import asyncio
import hashlib
import os
from datetime import datetime, timezone, timedelta
import feedparser
import pytz
import aiohttp
from urllib.parse import urljoin, urlparse
from firefeed_utils import download_and_save_image, extract_image_from_preview
from config import IMAGES_ROOT_DIR, get_shared_db_pool, MAX_TOTAL_NEWS, MAX_ENTRIES_PER_FEED, MAX_CONCURRENT_FEEDS
import mimetypes
import traceback
import json

class RSSManager:
    # Сохраняем оригинальную сигнатуру конструктора, но без duplicate_detector
    def __init__(self, translator_queue=None):
        # translator_queue остается для совместимости, если используется
        self.translator_queue = translator_queue
        # Семафор для ограничения количества одновременно обрабатываемых фидов
        self._feed_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FEEDS)

    async def get_pool(self):
        """Вспомогательный метод: Получает общий пул подключений из config.py."""
        # Используем общий пул подключений из config.py
        return await get_shared_db_pool()

    async def close_pool(self):
        """Заглушка - пул закрывается глобально через config.py"""
        pass

    # - МЕТОДЫ РАБОТЫ С БД -
    async def get_all_feeds(self):
        """Вспомогательный метод: Получает список ВСЕХ RSS-лент."""
        try:
            pool = await self.get_pool()
            feeds = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN sources s ON rf.source_id = s.id
                    LEFT JOIN categories c ON rf.category_id = c.id
                    """
                    await cur.execute(query)
                    async for row in cur:
                        feeds.append({
                            'id': row[0],
                            'url': row[1].strip(),
                            'name': row[2],
                            'lang': row[3],
                            'source_id': row[4],
                            'category_id': row[5],
                            'source': row[6], # s.name
                            'category': row[7] if row[7] else 'uncategorized', # c.name
                            'category_display': row[7] # Для совместимости, если использовалось
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения всех лент: {e}")
            return []

    async def get_all_active_feeds(self):
        """Вспомогательный метод: Получает список активных RSS-лент."""
        try:
            pool = await self.get_pool()
            feeds = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Явно перечисляем поля из rss_feeds
                    query = """
                    SELECT 
                        rf.id, 
                        rf.url, 
                        rf.name, 
                        rf.language, 
                        rf.source_id, 
                        rf.category_id,
                        s.name as source_name, -- Получаем имя источника
                        c.name as category_name, -- Получаем имя категории
                        rf.cooldown_minutes, -- Можно добавить, если нужно
                        rf.max_news_per_hour  -- Можно добавить, если нужно
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE rf.is_active = TRUE
                    """
                    await cur.execute(query)
                    async for row in cur:
                        feeds.append({
                            'id': row[0],
                            'url': row[1].strip(),
                            'name': row[2],
                            'lang': row[3],
                            'source_id': row[4],
                            'category_id': row[5],
                            'source': row[6], # s.name
                            'category': row[7] if row[7] else 'uncategorized', # c.name
                            # Добавьте cooldown_minutes, max_news_per_hour, если они нужны в feed_info
                            # 'cooldown_minutes': row[8],
                            # 'max_news_per_hour': row[9]
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения активных лент: {e}")
            import traceback
            traceback.print_exc() # Добавим traceback для лучшей отладки
            return []

    async def get_feeds_by_category(self, category_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени категории."""
        try:
            pool = await self.get_pool()
            feeds = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE c.name = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (category_name,))
                    async for row in cur:
                        feeds.append({
                            'id': row[0],
                            'url': row[1].strip(),
                            'name': row[2],
                            'lang': row[3],
                            'source_id': row[4],
                            'category_id': row[5],
                            'source': row[6], # s.name
                            'category': row[7] if row[7] else 'uncategorized', # c.name
                            'category_display': row[7] # Для совместимости, если использовалось
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения лент по категории {category_name}: {e}")
            return []

    async def get_feeds_by_language(self, lang):
        """Вспомогательный метод: Получить активные RSS-ленты по языку."""
        try:
            pool = await self.get_pool()
            feeds = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE rf.language = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (lang,))
                    async for row in cur:
                        feeds.append({
                            'id': row[0],
                            'url': row[1].strip(),
                            'name': row[2],
                            'lang': row[3],
                            'source_id': row[4],
                            'category_id': row[5],
                            'source': row[6], # s.name
                            'category': row[7] if row[7] else 'uncategorized', # c.name
                            'category_display': row[7] # Для совместимости, если использовалось
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения лент по языку {lang}: {e}")
            return []

    async def get_feeds_by_source(self, source_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени источника."""
        try:
            pool = await self.get_pool()
            feeds = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT rf.*, c.name as category_name, s.name as source_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE s.name = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (source_name,))
                    async for row in cur:
                        feeds.append({
                            'id': row[0],
                            'url': row[1].strip(),
                            'name': row[2],
                            'lang': row[3],
                            'source_id': row[4],
                            'category_id': row[5],
                            'source': row[6], # s.name
                            'category': row[7] if row[7] else 'uncategorized', # c.name
                            'category_display': row[7] # Для совместимости, если использовалось
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения лент по источнику {source_name}: {e}")
            return []

    async def add_feed(self, url, category_name, source_name, language, is_active=True):
        """Вспомогательный метод: Добавить новую RSS-ленту."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
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
                    INSERT INTO rss_feeds (url, name, category_id, source_id, language, is_active, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (url) DO NOTHING
                    """
                    await cur.execute(query, (url, feed_name, category_id, source_id, language, is_active))
                    # await conn.commit() # Явный коммит для этой операции - не нужен в aiopg?
                    print(f"[DB] [RSSManager] Лента '{url}' добавлена или уже существует.")
                    return True
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при добавлении фида {url}: {e}")
            return False

    async def update_feed(self, feed_id, url=None, name=None, category_name=None, source_name=None, language=None, is_active=None):
        """Вспомогательный метод: Обновить существующую RSS-ленту по ID."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
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
                    if name is not None:
                        updates.append("name = %s")
                        values.append(name)
                    if language is not None:
                        updates.append("language = %s")
                        values.append(language)
                    if is_active is not None:
                        updates.append("is_active = %s")
                        values.append(is_active)

                    # Добавляем updated_at
                    updates.append("updated_at = NOW()")
                    values.append(feed_id) # Для WHERE clause

                    if updates:
                        query = f"UPDATE rss_feeds SET {', '.join(updates)} WHERE id = %s"
                        await cur.execute(query, values)
                        affected_rows = cur.rowcount
                        # await conn.commit() # Явный коммит - не нужен в aiopg?
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно обновлена.")
                    else:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена или не была изменена.")
                        affected_rows = 0

                    return affected_rows > 0
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при обновлении фида с ID {feed_id}: {e}")
            return False

    async def delete_feed(self, feed_id):
        """Вспомогательный метод: Удалить RSS-ленту по ID."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = "DELETE FROM rss_feeds WHERE id = %s"
                    await cur.execute(query, (feed_id,))
                    affected_rows = cur.rowcount
                    # await conn.commit() # Явный коммит - не нужен в aiopg?
                    print(f"[DB] [RSSManager] Лента с ID {feed_id} удалена. Затронуто строк: {affected_rows}")
                    return affected_rows > 0
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при удалении фида с ID {feed_id}: {e}")
            return False

    async def get_feed_cooldown_minutes(self, rss_feed_id):
        """Вспомогательный метод: Получает кулдаун для RSS-ленты (по умолчанию 60 минут)."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT COALESCE(cooldown_minutes, 60) FROM rss_feeds WHERE id = %s", (rss_feed_id,))
                    row = await cur.fetchone()
                    return row[0] if row else 60
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении кулдауна для ленты {rss_feed_id}: {e}")
            return 60 # Возвращаем значение по умолчанию

    async def get_max_news_per_hour_for_feed(self, rss_feed_id):
        """Вспомогательный метод: Получает максимальное количество RSS-элементов в час для RSS-ленты (по умолчанию 10)."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT COALESCE(max_news_per_hour, 10) FROM rss_feeds WHERE id = %s", (rss_feed_id,))
                    row = await cur.fetchone()
                    return row[0] if row else 10
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении max_news_per_hour для ленты {rss_feed_id}: {e}")
            return 10 # Возвращаем значение по умолчанию

    async def get_last_published_time_for_feed(self, rss_feed_id):
        """Вспомогательный метод: Получает время последней публикации из конкретной RSS-ленты."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Теперь используем только таблицу published_news_data
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
                    return published_time
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при получении времени последней публикации из конкретной RSS-ленты: {e}")
            return None

    async def get_recent_news_count_for_feed(self, rss_feed_id, minutes=60):
        """Вспомогательный метод: Получает количество RSS-элементов из ленты за последние N минут"""
        try:
            pool = await self.get_pool()
            news_count = 0
            time_threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Теперь используем только таблицу published_news_data
                    query = """
                    SELECT COUNT(*) FROM published_news_data
                    WHERE rss_feed_id = %s AND created_at >= %s
                    """
                    await cur.execute(query, (rss_feed_id, time_threshold))
                    row = await cur.fetchone()
                    news_count = row[0] if row else 0
            return news_count
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при подсчете RSS-элементов за последние {minutes} минут для ленты {rss_feed_id}: {e}")
            return 0

    # - ОСНОВНАЯ ЛОГИКА ПАРСИНГА -
    def _create_translation_callbacks(self, news_id):
        async def on_success(translations, task_id=None):
            try:
                await self.save_translations_to_db(news_id, translations)
                print(f"[DB] [CALLBACK] Переводы для {str(news_id)[:20]} успешно сохранены из callback")
            except Exception as e:
                print(f"[DB] [CALLBACK] Ошибка сохранения переводов в callback для {str(news_id)[:20]}: {e}")
                traceback.print_exc()
        async def on_error(error_data):
            print(f"[TRANSLATOR] [CALLBACK] Ошибка при подготовке переводов для {str(news_id)[:20]}: {error_data}")
        return on_success, on_error

    async def fetch_single_feed(self, feed_info, headers):
        """Асинхронно парсит одну RSS-ленту и возвращает список RSS-элементов из неё.
        Перед парсингом проверяет cooldown и лимиты.
        """
        # Задача будет ждать здесь, если уже обрабатывается MAX_CONCURRENT_FEEDS фидов
        async with self._feed_semaphore:
            local_news = []
            rss_feed_id = feed_info['id']

            # Получаем параметры ленты
            try:
                cooldown_minutes = await self.get_feed_cooldown_minutes(rss_feed_id)
                max_news_per_hour = await self.get_max_news_per_hour_for_feed(rss_feed_id)
                recent_count = await self.get_recent_news_count_for_feed(rss_feed_id, cooldown_minutes)
                print(f"[RSS] fetch_single_feed: cooldown_minutes = {cooldown_minutes}, max_news_per_hour = {max_news_per_hour}, recent_count = {recent_count}")
            except Exception as e:
                print(f"[RSS] [ERROR] Ошибка получения параметров ленты {rss_feed_id}: {e}")
                return local_news # Возвращаем пустой список в случае ошибки БД

            # Проверка лимита RSS-элементов за период кулдауна
            if recent_count >= max_news_per_hour:
                print(f"[SKIP] Лента ID {rss_feed_id} достигла лимита {max_news_per_hour} RSS-элементов за {cooldown_minutes} минут. Опубликовано: {recent_count}")
                return local_news

            # Проверка кулдауна (время последней публикации)
            try:
                last_published = await self.get_last_published_time_for_feed(rss_feed_id)
            except Exception as e:
                print(f"[RSS] [ERROR] Ошибка получения времени последней публикации для ленты {rss_feed_id}: {e}")
                return local_news # Возвращаем пустой список в случае ошибки БД

            if last_published:
                elapsed = datetime.now(timezone.utc) - last_published
                if elapsed < timedelta(minutes=cooldown_minutes):
                    print(f"[SKIP] Лента ID {rss_feed_id} находится на кулдауне ({cooldown_minutes} мин). Прошло: {elapsed}")
                    return local_news

            # Если лента прошла все проверки — парсим её
            try:
                print(f"[RSS] Парсинг ленты: {feed_info['name']} ({feed_info['url']})")
                feed = feedparser.parse(feed_info['url'], request_headers=headers)
                # Альтернативная попытка с использованием aiohttp для получения сырого содержимого
                if not feed.entries and feed.bozo:
                    print(f"[RSS] [DEBUG] feedparser не смог распарсить {feed_info['url']}. Пробуем aiohttp...")
                    try:
                        timeout = aiohttp.ClientTimeout(total=15)
                        async with aiohttp.ClientSession(timeout=timeout) as session:
                            async with session.get(feed_info['url'], headers=headers) as response:
                                raw_content = await response.text()
                                feed = feedparser.parse(raw_content)
                                if feed.entries:
                                    print(f"[RSS] [DEBUG] aiohttp помог распарсить {feed_info['url']}")
                    except asyncio.TimeoutError:
                        print(f"[RSS] [DEBUG] Таймаут при получении сырого содержимого для {feed_info['url']}")
                    except Exception as fetch_err: # Еще более общий exception
                        print(f"[RSS] [DEBUG] Неожиданная ошибка при получении сырого содержимого для {feed_info['url']}: {type(fetch_err).__name__}: {fetch_err}")
                        import traceback
                        traceback.print_exc()

                if not feed.entries:
                    print(f"[RSS] Нет записей в {feed_info['url']}")
                    return local_news

                # - Обработка записей из RSS -
                for i, entry in enumerate(feed.entries[:MAX_ENTRIES_PER_FEED]):
                    # Даем возможность другим задачам выполниться
                    if i % 5 == 0: # yield каждые 5 итераций
                        await asyncio.sleep(0)

                    title = (entry.get('title', '') or '').strip()
                    if not title:
                        print(f"[RSS] [SKIP] Пропуск записи без заголовка в {feed_info['url']}")
                        continue

                    description = (entry.get('summary', '') or '').strip()
                    content = (entry.get('content', [{}])[0].get('value', '') or description or '').strip()

                    # --- ИЗМЕНЕНИЕ: Генерация news_id ДО создания news_item ---
                    # Комбинируем URL новости и ID RSS-ленты для создания уникального ключа
                    link = entry.get('link', '')
                    unique_string = f"{link}_{rss_feed_id}"
                    news_id = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
                    short_id = news_id[:20] # Для логов
                    print(f"[RSS] [NEWS_ID] Сгенерирован news_id: {short_id} для '{title[:30]}...' (link: {link[:50]}...)")
                    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

                    # - Инициализация базовых данных RSS-элемента -
                    # --- ИЗМЕНЕНИЕ: news_id теперь часть news_item с самого начала ---
                    news_item = {
                        'id': news_id, # Добавляем ID сразу
                        'title': title,
                        'description': description,
                        'link': link,
                        'lang': feed_info['lang'],
                        'category': feed_info['category'], # Теперь берется из feed_info
                        'source': feed_info['source'],     # Теперь берется из feed_info
                        'published': entry.get('published_parsed') or entry.get('updated_parsed'),
                        'image_filename': None, # Инициализируем как None
                        'video_url': None # Инициализируем как None
                    }
                    # --- КОНЕЦ ИЗМЕНЕНИЯ ---

                    # - Извлечение и обработка изображения -
                    image_url_from_rss = self.extract_image_from_rss_item(entry)
                    image_url_for_processing = image_url_from_rss # URL для дальнейшей обработки
                    local_image_path = None # Для хранения пути к скачанному изображению
                    print(f"[RSS] [IMG] image_url_from_rss = {image_url_from_rss}")

                    # Если из RSS не удалось извлечь изображение, пробуем извлечь из web preview
                    if not image_url_from_rss and news_item['link']:
                        print(f"[RSS] [IMG] Попытка извлечения изображения из web preview для: {news_item['link']}")
                        try:
                            image_url_from_preview = await extract_image_from_preview(news_item['link'])
                            if image_url_from_preview:
                                print(f"[RSS] [IMG] Найдено изображение в web preview: {image_url_from_preview[:100]}...")
                                image_url_for_processing = image_url_from_preview
                            else:
                                print(f"[RSS] [IMG] Изображение в web preview не найдено.")
                        except Exception as e:
                            print(f"[RSS] [IMG] Ошибка извлечения из web preview: {e}")

                    # Если удалось получить URL изображения, скачиваем его
                    if image_url_for_processing:
                        try:
                            # --- ИЗМЕНЕНИЕ: Используем news_id из news_item вместо unique_key ---
                            print(f"[RSS] [IMG] Обработка изображения с URL: {image_url_for_processing[:100]}... (news_id: {short_id})")
                            # --- КОНЕЦ ИЗМЕНЕНИЯ ---

                            # Проверяем тип контента перед скачиванием
                            timeout = aiohttp.ClientTimeout(total=10)
                            async with aiohttp.ClientSession(timeout=timeout) as session:
                                async with session.head(image_url_for_processing, timeout=timeout) as response:
                                    content_type = response.headers.get('Content-Type', '').lower()
                                    print(f"[RSS] [IMG] HEAD-запрос вернул Content-Type: {content_type}")
                                    if content_type.startswith('image/'):
                                        print(f"[RSS] [IMG] Подтвержден тип изображения через HEAD. Скачиваем...")
                                        # --- ИЗМЕНЕНИЕ: Передаем news_id из news_item в download_and_save_image ---
                                        # Скачиваем и сохраняем изображение, используя news_id как идентификатор
                                        local_image_path = await download_and_save_image(image_url_for_processing, news_item['id'], save_directory=IMAGES_ROOT_DIR)
                                        # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                                    else:
                                        print(f"[RSS] [IMG] URL не является изображением (Content-Type: {content_type}). Пропуск.")

                        except asyncio.TimeoutError:
                            print(f"[RSS] [IMG] Таймаут при проверке/скачивании изображения: {image_url_for_processing[:100]}...")
                        except Exception as e:
                            print(f"[RSS] [IMG] Ошибка при обработке изображения {image_url_for_processing[:100]}... : {e}")

                    # Если изображение было успешно скачано, обновляем путь
                    if local_image_path:
                        news_item['image_filename'] = os.path.relpath(local_image_path, IMAGES_ROOT_DIR)
                        print(f"[RSS] [IMG] Изображение сохранено как: {news_item['image_filename']}")
                    else:
                        print(f"[RSS] [IMG] Изображение не будет связано с элементом.")

                    # - Извлечение видео -
                    video_url = self.extract_video_from_rss_item(entry)
                    if video_url:
                        news_item['video_url'] = video_url
                    else:
                        news_item['video_url'] = None

                    # - Сохранение RSS-элемента в БД (ОДИН запрос с изображением) -
                    try:
                        # --- ИЗМЕНЕНИЕ: news_item уже содержит news_id, передаем его как есть ---
                        # Передаем news_item с уже обработанным image_filename и встроенным news_id
                        saved_news_id = await self.save_rss_item_to_db(news_item, rss_feed_id)
                        # --- КОНЕЦ ИЗМЕНЕНИЯ ---
                        if not saved_news_id:
                            print(f"[RSS] [WARN] Не удалось получить news_id для элемента '{title[:30]}...'. Пропуск.")
                            # Если изображение было скачано, но элемент не сохранился, возможно, его нужно удалить
                            # (опционально, зависит от политики хранения)
                            continue # Пропускаем, если не удалось сохранить

                        # news_item['id'] уже установлено выше
                        print(f"[DB] [SUCCESS] RSS-элемент и данные об изображении сохранены в БД: {saved_news_id[:20]}...")

                    except Exception as e:
                        print(f"[RSS] [ERROR] Ошибка сохранения RSS-элемента в БД: {e}")
                        continue # Пропускаем элемент, если не удалось сохранить

                    # - Обработка перевода -
                    translations = {}
                    if self.translator_queue:
                        try:
                            print(f"[DEBUG] fetch_single_feed: Перед добавлением задачи перевода для {news_item['id'][:20]}...")
                            success_cb, error_cb = self._create_translation_callbacks(news_item['id'])
                            # Добавляем задачу перевода в очередь корректным способом
                            await self.translator_queue.add_task(
                                title=news_item['title'],
                                description=news_item['description'],
                                original_lang=news_item['lang'],
                                callback=success_cb,
                                error_callback=error_cb,
                                task_id=news_item['id']
                            )
                            print(f"[DEBUG] fetch_single_feed: Задача перевода добавлена в очередь для {news_item['id'][:20]}")
                        except Exception as e:
                            print(f"[RSS] [ERROR] Ошибка добавления задачи перевода в очередь: {e}")
                            traceback.print_exc()
                    else:
                        print("[DEBUG] fetch_single_feed: translator_queue не предоставлена, переводы не будут обработаны.")

                    news_item['translations'] = translations

                    # 5. Добавляем элемент в список для возврата
                    local_news.append(news_item)
                    print(f"[DB] [SUCCESS] RSS-элемент полностью обработан (сохранен с изображением, видео, переведен): {title[:50]}...")

            except Exception as e:
                print(f"[RSS] Ошибка при обработке ленты {feed_info['url']}: {e}")
                import traceback
                traceback.print_exc()

            return local_news

    async def fetch_rss_items(self):
        """Асинхронная функция для получения RSS-элементов из RSS-лент"""
        all_news = []
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            }

            active_feeds = await self.get_all_active_feeds()
            print(f"[RSS] Найдено {len(active_feeds)} активных RSS-лент.")
            if not active_feeds:
                print("[RSS] Нет активных лент для парсинга.")
                return []

            # Создаем список задач (но они не запускаются сразу)
            tasks = [
                asyncio.create_task(self.fetch_single_feed(feed_info, headers))
                for feed_info in active_feeds
            ]

            # Обрабатываем задачи по мере завершения
            completed_count = 0
            try:
                for coro in asyncio.as_completed(tasks):
                    try:
                        local_news = await coro
                        if local_news:
                            all_news.extend(local_news)
                            print(f"[RSS] [FETCH] Получено {len(local_news)} элементов. Всего: {len(all_news)}")
                        else:
                            print(f"[RSS] [FETCH] Получено 0 элементов от одной из лент.")

                        completed_count += 1
                        print(f"[RSS] [PROGRESS] Завершено {completed_count}/{len(tasks)} лент.")

                    except asyncio.CancelledError:
                        print(f"[RSS] [TASK] Одна из задач была отменена.")
                        continue # Продолжаем обработку других завершенных задач
                    except Exception as task_e:
                        print(f"[RSS] [TASK_ERROR] Ошибка в задаче парсинга одной ленты: {task_e}")
                        import traceback
                        traceback.print_exc()
                        continue # Продолжаем обработку других задач

            finally:
                # Отменяем любые оставшиеся (хотя их быть не должно при использовании as_completed)
                for task in tasks:
                    if not task.done():
                        print(f"[RSS] [WARN] Отмена незавершенной задачи: {task}")
                        task.cancel()

        except Exception as e:
            import traceback
            print(f"❌ Критическая ошибка в fetch_rss_items: {e}")
            traceback.print_exc()

        # Сортировка и ограничение количества RSS-элементов
        try:
            sorted_news = sorted(all_news, key=lambda x: x.get('published', datetime.min), reverse=True)
        except Exception as e:
            print(f"[RSS] [WARN] Ошибка сортировки RSS-элементов по дате: {e}")
            sorted_news = all_news # Если сортировка не удалась, возвращаем как есть

        final_news = sorted_news[:MAX_TOTAL_NEWS]
        print(f"[RSS] [FINAL] Возвращаем {len(final_news)} последних RSS-элементов.")
        return final_news

    def extract_image_from_rss_item(self, entry):
        """Извлекает URL изображения из записи RSS."""
        # 1. Проверяем media:content (Yahoo Media RSS)
        if 'media_content' in entry:
            for media in entry['media_content']:
                if media.get('medium') == 'image':
                    return media.get('url')

        # 2. Проверяем media:thumbnail
        if 'media_thumbnail' in entry and entry['media_thumbnail']:
            return entry['media_thumbnail'][0].get('url')

        # 3. Проверяем enclosure
        if 'enclosures' in entry:
            for enclosure in entry['enclosures']:
                if enclosure.get('type', '').startswith('image/'):
                    return enclosure.get('href') or enclosure.get('url')

        # 4. Проверяем links с типом изображения
        if 'links' in entry:
            for link in entry['links']:
                if link.get('type', '').startswith('image/'):
                    return link.get('href')

        return None

    def extract_video_from_rss_item(self, entry):
        """Извлекает URL видео из записи RSS (упрощенная реализация)."""
        # 1. Проверяем media:content на видео
        if 'media_content' in entry:
            for media in entry['media_content']:
                if media.get('medium') == 'video':
                    return media.get('url')

        # 2. Проверяем enclosure на видео
        if 'enclosures' in entry:
            for enclosure in entry['enclosures']:
                content_type = enclosure.get('type', '')
                if content_type.startswith('video/') or content_type in ['application/mp4', 'application/x-mpegURL']:
                    return enclosure.get('href') or enclosure.get('url')

        return None

    async def save_rss_item_to_db(self, news_item, rss_feed_id):
        """Сохраняет RSS-элемент в таблицу published_news_data и возвращает его news_id."""
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 1. Генерируем уникальный news_id
                    # Комбинируем URL новости и ID RSS-ленты для создания уникального ключа
                    unique_string = f"{news_item['link']}_{rss_feed_id}"
                    news_id = hashlib.sha256(unique_string.encode('utf-8')).hexdigest()
                    short_id = news_id[:20] # Для логов

                    # 2. Подготавливаем данные
                    title = news_item['title'][:255] # Ограничиваем длину заголовка
                    content = news_item['description'] # Полное описание
                    original_language = news_item['lang']
                    image_filename = news_item['image_filename']
                    category_name = news_item['category']
                    source_name = news_item['source']
                    source_url = news_item['link'] # Ссылка на оригинальную новость

                    # 3. Получаем category_id
                    await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                    cat_result = await cur.fetchone()
                    if not cat_result:
                        print(f"[DB] [save_rss_item_to_db] Предупреждение: Категория '{category_name}' не найдена. Пропуск сохранения элемента.")
                        return None
                    category_id = cat_result[0]

                    # 4. Выполняем запрос к published_news_data с INSERT ... ON CONFLICT
                    query_published_news_data = """
                    INSERT INTO published_news_data
                    (news_id, original_title, original_content, original_language, category_id, image_filename, rss_feed_id, source_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (news_id) DO UPDATE SET
                    original_title = EXCLUDED.original_title,
                    original_content = EXCLUDED.original_content,
                    original_language = EXCLUDED.original_language,
                    category_id = EXCLUDED.category_id,
                    image_filename = EXCLUDED.image_filename,
                    rss_feed_id = EXCLUDED.rss_feed_id,
                    source_url = EXCLUDED.source_url,
                    updated_at = NOW()
                    """
                    # 4a. Выполняем запрос
                    print(f"[DB] [save_rss_item_to_db] Подготовка запроса к 'published_news_data' (ID: {short_id})")
                    await cur.execute(query_published_news_data, (
                        news_id, title, content, original_language, category_id, image_filename, rss_feed_id, source_url
                    ))
                    print(f"[DB] [save_rss_item_to_db] Запрос к 'published_news_data' выполнен. (ID: {short_id})")

                    # - ИСПРАВЛЕНИЕ -
                    # УДАЛЕН вызов await self.save_translations_to_db(news_id, translations={})
                    # Переводы будут сохранены позже, в callback'е on_translation_success
                    # ИЛИ в fetch_single_feed, если очередь не передана
                    # - КОНЕЦ ИСПРАВЛЕНИЯ -

                    print(f"[DB] [save_rss_item_to_db] RSS-элемент успешно сохранен в БД (ID: {short_id}). Переводы будут обработаны отдельно.")
                    return news_id

        except Exception as e:
            print(f"[DB] [save_rss_item_to_db] Ошибка сохранения RSS-элемента: {e}")
            traceback.print_exc()
            return None

    # - функция для сохранения переводов в БД -
    async def save_translations_to_db(self, news_id, translations):
        """Асинхронно сохраняет переводы RSS-элемента в таблицу news_translations."""
        short_news_id = news_id[:20] if news_id else 'Unknown'
        print(f"[DB] [save_translations_to_db] Начало сохранения переводов для элемента ID: {short_news_id}")
        if not translations:
            print(f"[DB] [save_translations_to_db] Нет переводов для сохранения для элемента {short_news_id}...")
            return True
        if not isinstance(translations, dict):
            print(f"[DB] [save_translations_to_db] ❌ ОШИБКА: translations должен быть словарем, но является {type(translations)}. Пропуск.")
            return False
        pool = None
        try:
            print(f"[DB] [save_translations_to_db] Получение пула соединений для элемента {short_news_id}...")
            pool = await self.get_pool()
            if not pool:
                print(f"[DB] [save_translations_to_db] ❌ ОШИБКА: Не удалось получить пул соединений для элемента {short_news_id}.")
                return False

            print(f"[DB] [save_translations_to_db] Пул соединений получен для элемента {short_news_id}.")
            async with pool.acquire() as conn:
                print(f"[DB] [save_translations_to_db] Получено соединение из пула для элемента {short_news_id}.")
                async with conn.cursor() as cur:
                    print(f"[DB] [save_translations_to_db] Получен курсор для элемента {short_news_id}.")
                    # Получаем оригинальный язык и тексты новости
                    await cur.execute("SELECT original_language, original_title, original_content FROM published_news_data WHERE news_id = %s", (news_id,))
                    row = await cur.fetchone()
                    original_language = row[0] if row else 'en'
                    original_title = row[1] if row else ''
                    original_content = row[2] if row else ''
                    print(f"[DB] [save_translations_to_db] Оригинальный язык новости: {original_language}")

                    translation_count = 0
                    for lang, data in translations.items():
                        translation_count += 1
                        if not isinstance(data, dict):
                            print(f"[DB] [save_translations_to_db] [{translation_count}] ❌ ОШИБКА: Данные перевода для '{lang}' не являются словарем. Пропуск.")
                            continue

                        title = data.get('title', '')
                        description = data.get('description', '')  # описание хранится под ключом 'description'

                        # Пропускаем оригинальный язык и пустые переводы
                        if lang == original_language or (not title and not description):
                            print(f"[DB] [save_translations_to_db] [{translation_count}] Пропуск сохранения для '{lang}' ({short_news_id})")
                            continue

                        # Пропускаем, если перевод идентичен оригиналу
                        if title == original_title and description == original_content:
                            print(f"[DB] [save_translations_to_db] [{translation_count}] Пропуск сохранения идентичного оригиналу перевода для '{lang}' ({short_news_id})")
                            continue

                        # Подготавливаем SQL-запрос для вставки или обновления перевода
                        insert_query = """
                        INSERT INTO news_translations (news_id, language, translated_title, translated_content, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (news_id, language)
                        DO UPDATE SET
                            translated_title = EXCLUDED.translated_title,
                            translated_content = EXCLUDED.translated_content,
                            updated_at = NOW()
                        """
                        print(f"[DB] [save_translations_to_db] [{translation_count}] Подготовка SQL-запроса для '{lang}' ({short_news_id})...")
                        try:
                            await cur.execute(insert_query, (news_id, lang, title, description))
                            print(f"[DB] [save_translations_to_db] [{translation_count}] Перевод на '{lang}' для {short_news_id} сохранен/обновлен.")
                        except Exception as execute_error:
                            error_msg = f"[DB] [save_translations_to_db] [{translation_count}] ❌ ОШИБКА SQL-запроса для '{lang}' ({short_news_id}): {execute_error}"
                            print(error_msg)
                            # Логируем ошибку, но продолжаем обработку других переводов
                            traceback.print_exc()
                            continue
                        finally:
                            print(f"[DB] [save_translations_to_db] [{translation_count}] Курсор закрыт для '{lang}' ({short_news_id}).")
                    print(f"[DB] [save_translations_to_db] [{translation_count}] Соединение возвращено в пул для '{lang}' ({short_news_id}).")
                    print(f"[DB] [save_translations_to_db] Обработано {translation_count} переводов для {short_news_id}.")
                    print(f"[DB] [save_translations_to_db] Все переводы для {short_news_id} успешно обработаны.")
                    return True
        except Exception as e:
            error_msg = f"[DB] [save_translations_to_db] ❌ КРИТИЧЕСКАЯ ОШИБКА при сохранении переводов для {short_news_id}: {e}"
            print(error_msg)
            print(f"[DB] [save_translations_to_db] Тип исключения: {type(e)}")
            traceback.print_exc()
            return False

    # - МЕТОДЫ ИЗВЛЕЧЕНИЯ МЕДИА -
    def extract_image_from_rss_item(self, item):
        """Извлекает URL изображения из RSS item."""
        try:
            # 1. media:thumbnail (Atom)
            media_thumbnail = item.get('media_thumbnail', [])
            if media_thumbnail and isinstance(media_thumbnail, list) and len(media_thumbnail) > 0:
                thumbnail = media_thumbnail[0]
                if isinstance(thumbnail, dict):
                    url = thumbnail.get('url')
                    if url:
                        print(f"[INFO] Найдено изображение в media:thumbnail: {url}")
                        return url

            # 2. enclosure с типом image/*
            enclosures = item.get('enclosures', [])
            if enclosures:
                for enclosure in enclosures:
                    if isinstance(enclosure, dict):
                        content_type = enclosure.get('type', '')
                        if content_type.startswith('image/'):
                            url = enclosure.get('href') or enclosure.get('url')
                            if url:
                                print(f"[INFO] Найдено изображение в enclosure: {url}")
                                return url

            # 3. media:content с типом image/* (Atom)
            media_content = item.get('media_content', [])
            if media_content:
                if isinstance(media_content, list):
                    for content in media_content:
                        if isinstance(content, dict) and content.get('medium') == 'image':
                            url = content.get('url')
                            if url:
                                print(f"[INFO] Найдено изображение в media:content (list): {url}")
                                return url
                elif isinstance(media_content, dict) and media_content.get('medium') == 'image':
                    url = media_content.get('url')
                    if url:
                        print(f"[INFO] Найдено изображение в media:content (dict): {url}")
                        return url

            # 4. og:image из links (если доступно)
            # (Это менее надежно, так как требует парсинга HTML, который feedparser может не предоставить полностью)
        except Exception as e:
            print(f"[WARN] Ошибка при извлечении изображения из RSS item: {e}")
        print("[INFO] Изображение не найдено в RSS item.")
        return None

    def extract_video_from_rss_item(self, item):
        """Извлекает URL видео из RSS item."""
        TELEGRAM_VIDEO_LIMIT = 50 * 1024 * 1024  # 50 МБ
        try:
            # 1. enclosure с типом video/* и проверкой размера
            enclosures = item.get('enclosures', [])
            if enclosures:
                for enclosure in enclosures:
                    if isinstance(enclosure, dict):
                        content_type = enclosure.get('type', '')
                        if content_type.startswith('video/'):
                            url = enclosure.get('href') or enclosure.get('url')
                            if url:
                                # Проверяем размер, если доступен
                                file_size = enclosure.get('length') or enclosure.get('filesize')
                                size_ok = True
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > TELEGRAM_VIDEO_LIMIT:
                                            print(f"[INFO] Видео превышает лимит размера Telegram ({file_size} > {TELEGRAM_VIDEO_LIMIT}): {url}")
                                            size_ok = False
                                    except (ValueError, TypeError):
                                        pass  # Не удалось преобразовать размер
                                if size_ok:
                                    print(f"[INFO] Найдено видео в enclosure: {url}")
                                    return url

            # 2. media:content с типом video/* и проверкой размера (Atom)
            media_content = item.get('media_content', [])
            if media_content:
                if isinstance(media_content, list):
                    for content in media_content:
                        if isinstance(content, dict) and content.get('medium') == 'video':
                            media_url = content.get('url')
                            if media_url:
                                # Проверяем размер, если доступен
                                file_size = content.get('fileSize') or content.get('filesize')
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
        except Exception as e:
            print(f"[WARN] Ошибка при извлечении видео из RSS item: {e}")
        print("[INFO] Видео не найдено в RSS item.")
        return None

    # - МЕТОДЫ ДЛЯ ТЕЛЕГРАМ-БОТА -
    async def fetch_unprocessed_news(self):
        """Получает необработанные (непереведенные) RSS-элементы из БД."""
        try:
            pool = await self.get_pool()
            unprocessed_news = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Получаем необработанные RSS-элементы без переводов
                    # Теперь используем только таблицу published_news_data
                    query = """
                    SELECT 
                        nd.news_id,
                        nd.original_title,
                        nd.original_content,
                        nd.original_language,
                        nd.image_filename,
                        nd.category_id,
                        nd.rss_feed_id,
                        nd.telegram_published_at,
                        nd.created_at,
                        nd.updated_at,
                        c.name as category_name,
                        s.name as source_name,
                        nd.source_url as source_url
                    FROM published_news_data nd
                    LEFT JOIN categories c ON nd.category_id = c.id
                    LEFT JOIN rss_feeds rf ON nd.rss_feed_id = rf.id
                    LEFT JOIN sources s ON rf.source_id = s.id
                    LEFT JOIN news_translations nt ON nd.news_id = nt.news_id
                    WHERE nt.news_id IS NULL OR nt.translated_title IS NULL OR nt.translated_content IS NULL
                    ORDER BY nd.created_at DESC
                    LIMIT 100
                    """
                    await cur.execute(query)
                    results = []
                    async for row in cur:
                        results.append(row)

                    # Получаем названия колонок
                    columns = [desc[0] for desc in cur.description]

                    for row in results:
                        row_dict = dict(zip(columns, row))
                        # Создаем структуру RSS-элемента для бота
                        news_item = {
                            'news_id': row_dict['news_id'],
                            'title': row_dict['original_title'],
                            'description': row_dict['original_content'],
                            'lang': row_dict['original_language'],
                            'category': row_dict['category_name'],
                            'source': row_dict['source_name'],
                            'link': row_dict['source_url'],
                            'published': row_dict['created_at'], # Используем created_at как published
                            'image_filename': row_dict['image_filename'],
                            # Переводы будут добавлены ниже
                            'translations': {}
                        }
                        # Добавляем заглушку для published если она отсутствует
                        if 'published' not in news_item:
                            news_item['published'] = datetime.now(pytz.utc)
                        unprocessed_news.append(news_item)
            return unprocessed_news
        except Exception as e:
            print(f"[DB] [ERROR] Ошибка при получении необработанных RSS-элементов: {e}")
            traceback.print_exc()
            return []
