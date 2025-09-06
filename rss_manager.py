import asyncio
import hashlib
import os
from datetime import datetime, timezone, timedelta
import feedparser
import pytz
import aiohttp
from urllib.parse import urljoin, urlparse
from firefeed_utils import download_and_save_image, extract_image_from_preview
from config import IMAGES_ROOT_DIR
import mimetypes
import traceback
import json

# --- КОНФИГУРАЦИЯ ---
MAX_CONCURRENT_FEEDS = 2  # Максимальное количество RSS-лент, обрабатываемых одновременно
MAX_ENTRIES_PER_FEED = 5  # Максимальное количество записей из одной RSS-ленты
MAX_TOTAL_NEWS = 50  # Максимальное общее количество новостей за один цикл парсинга
TELEGRAM_VIDEO_LIMIT = 50 * 1024 * 1024  # 50 МБ лимит для Telegram
# --- КОНЕЦ КОНФИГУРАЦИИ ---

class RSSManager:
    def __init__(self, duplicate_detector, translator_queue=None):
        self.dublicate_detector = duplicate_detector
        self.translator_queue = translator_queue
        # Создаем семафор для ограничения паралелизма обработки фидов
        self._feed_semaphore = asyncio.Semaphore(MAX_CONCURRENT_FEEDS)
    async def get_pool(self):
        """Получает общий пул подключений из config.py"""
        # Предполагается, что get_shared_db_pool определен глобально или импортирован
        from config import get_shared_db_pool
        return await get_shared_db_pool()
    async def close_pool(self):
        """Заглушка - пул закрывается глобально"""
        pass

    # --- МЕТОДЫ РАБОТЫ С БД ---
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
                            'source': row[4],
                            'source_id': row[5],
                            'category': row[6] if row[6] else 'uncategorized',
                            'category_display': row[7]
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
                        rf.cooldown_minutes, -- Можно добавить, если нужно
                        rf.max_news_per_hour, -- Можно добавить, если нужно
                        s.name as source_name,
                        COALESCE(c.name, 'uncategorized') as category_name
                    FROM rss_feeds rf
                    JOIN sources s ON rf.source_id = s.id
                    LEFT JOIN categories c ON rf.category_id = c.id
                    WHERE rf.is_active = TRUE
                    """
                    await cur.execute(query)
                    async for row in cur:
                        feeds.append({
                            'id': row[0],              # rf.id
                            'url': str(row[1]).strip(),  # rf.url (теперь точно URL)
                            'name': row[2],            # rf.name (теперь точно name)
                            'lang': row[3],            # rf.language
                            'source_id': row[4],       # rf.source_id
                            'source': row[8],          # s.name (source_name)
                            'category': row[9],        # c.name или 'uncategorized' (category_name)
                            # Добавьте cooldown_minutes, max_news_per_hour, если они нужны в feed_info
                            # 'cooldown_minutes': row[6],
                            # 'max_news_per_hour': row[7]
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
                            'source': row[4],
                            'source_id': row[5],
                            'category': row[6],
                            'category_display': row[7]
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
                            'source': row[4],
                            'source_id': row[5],
                            'category': row[6],
                            'category_display': row[7]
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
                            'source': row[4],
                            'source_id': row[5],
                            'category': row[6],
                            'category_display': row[7]
                        })
            return feeds
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка получения лент по источнику {source_name}: {e}")
            return []

    async def add_feed(self, url, category_name, source_name, language='en', is_active=True):
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
                    await conn.commit() # Явный коммит для этой операции
                    print(f"[DB] [RSSManager] Лента '{url}' добавлена или уже существует.")
                    return True
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка БД при добавлении фида {url}: {e}")
            return False

    async def update_feed(self, feed_id, url=None, category_name=None, source_name=None, language=None, is_active=None):
        """Вспомогательный метод: Обновить RSS-ленту. None означает "не обновлять это поле"."""
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
                        await conn.commit() # Явный коммит
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно обновлена.")
                    else:
                        print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена или не была изменена.")
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
                    await conn.commit() # Явный коммит
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
        """Вспомогательный метод: Получает максимальное количество новостей в час для RSS-ленты (по умолчанию 10)."""
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
        """Вспомогательный метод: Получает количество новостей из ленты за последние N минут"""
        try:
            pool = await self.get_pool()
            news_count = 0
            time_threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT COUNT(*) FROM published_news_data
                    WHERE rss_feed_id = %s AND created_at >= %s
                    """
                    await cur.execute(query, (rss_feed_id, time_threshold))
                    row = await cur.fetchone()
                    news_count = row[0] if row else 0
            return news_count
        except Exception as e:
            print(f"[DB] [RSSManager] Ошибка при подсчете новостей за последние {minutes} минут для ленты {rss_feed_id}: {e}")
            return 0

    # --- ОСНОВНАЯ ЛОГИКА ПАРСИНГА ---
    async def fetch_single_feed(self, feed_info, seen_keys, headers):
        """Асинхронно парсит одну RSS-ленту и возвращает список новостей из неё.
        Перед парсингом проверяет cooldown и лимиты."""
        # Задача будет ждать здесь, если уже обрабатывается MAX_CONCURRENT_FEEDS фидов
        async with self._feed_semaphore:
            local_news = []
            rss_feed_id = feed_info['id']
            # Получаем параметры ленты
            try:
                cooldown_minutes = await self.get_feed_cooldown_minutes(rss_feed_id)
                max_news_per_hour = await self.get_max_news_per_hour_for_feed(rss_feed_id)
                recent_count = await self.get_recent_news_count_for_feed(rss_feed_id, cooldown_minutes)
            except Exception as e:
                print(f"[RSS] [ERROR] Ошибка получения параметров ленты {rss_feed_id}: {e}")
                return local_news # Возвращаем пустой список в случае ошибки БД
            # Проверка лимита новостей за период кулдауна
            if recent_count >= max_news_per_hour:
                print(f"[SKIP] Лента ID {rss_feed_id} достигла лимита {max_news_per_hour} новостей за {cooldown_minutes} минут. Опубликовано: {recent_count}")
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
                # Логируем ошибки парсинга
                if getattr(feed, 'bozo', 0):
                    exc = getattr(feed, 'bozo_exception', None)
                    if exc:
                        error_type = type(exc).__name__
                        error_message = str(exc)
                        print(f"[RSS] Ошибка парсинга ({error_type}) в {feed_info['url']}: {error_message[:200]}")
                        
                        # --- Добавлено: Получение и логирование сырого содержимого ---
                        # Только если ошибка связана с синтаксисом XML, это может помочь
                        # Убрана некорректная ссылка на feedparser.zlib.error
                        if 'syntax error' in error_message.lower(): # Упростим условие для теста
                            print(f"[RSS] [DEBUG] Попытка получить сырое содержимое для {feed_info['url']}...")
                            try:
                                # Простое логирование начала
                                print(f"[RSS] [DEBUG] Создание aiohttp.ClientSession...")
                                async with aiohttp.ClientSession() as debug_session:
                                    print(f"[RSS] [DEBUG] aiohttp.ClientSession создана.")
                                    # Явно задаем таймаут
                                    timeout = aiohttp.ClientTimeout(total=15, connect=5, sock_read=10)
                                    print(f"[RSS] [DEBUG] Вызов debug_session.get с таймаутом {timeout.total}с...")
                                    # Используем тот же URL и headers
                                    async with debug_session.get(feed_info['url'], headers=headers, timeout=timeout) as resp:
                                        print(f"[RSS] [DEBUG] Получен ответ. Статус: {resp.status}")
                                        content_type = resp.headers.get('Content-Type', 'Unknown')
                                        print(f"[RSS] [DEBUG] Content-Type: {content_type}")
                                        
                                        # Попробуем прочитать как байты сначала, чтобы избежать проблем с декодированием
                                        print(f"[RSS] [DEBUG] Чтение тела ответа как байты...")
                                        raw_bytes = await resp.read()
                                        print(f"[RSS] [DEBUG] Прочитано {len(raw_bytes)} байт.")
                                        print(f"[RSS] [DEBUG] Первые 200 байт (hex): {raw_bytes[:200].hex()}")
                                        
                                        # Попробуем декодировать
                                        try:
                                            # Попробуем сначала charset из headers
                                            charset = resp.get_encoding()
                                            print(f"[RSS] [DEBUG] Определенная кодировка: {charset}")
                                            raw_text = raw_bytes.decode(charset)
                                        except Exception as dec_err1:
                                            print(f"[RSS] [DEBUG] Ошибка декодирования с определенной кодировкой ({charset}): {dec_err1}")
                                            try:
                                                # Пробуем UTF-8
                                                raw_text = raw_bytes.decode('utf-8')
                                                print(f"[RSS] [DEBUG] Успешно декодировано как UTF-8.")
                                            except Exception as dec_err2:
                                                print(f"[RSS] [DEBUG] Ошибка декодирования как UTF-8: {dec_err2}")
                                                # Заменяем ошибки
                                                raw_text = raw_bytes.decode('utf-8', errors='replace')
                                                print(f"[RSS] [DEBUG] Декодировано с заменой ошибок.")
                                        
                                        print(f"[RSS] [DEBUG] Первые 500 символов декодированного текста:\n{raw_text[:500]}")
                                        
                            except asyncio.TimeoutError:
                                print(f"[RSS] [DEBUG] Таймаут при попытке получить сырое содержимое для {feed_info['url']}")
                            except aiohttp.ClientResponseError as cre:
                                print(f"[RSS] [DEBUG] ClientResponseError aiohttp для {feed_info['url']}: Status={cre.status}, Message={cre.message}")
                            except aiohttp.ClientConnectorError as cce:
                                print(f"[RSS] [DEBUG] ClientConnectorError aiohttp для {feed_info['url']}: {cce}")
                            except aiohttp.ClientError as client_err: # Более общий aiohttp exception
                                print(f"[RSS] [DEBUG] ClientError aiohttp для {feed_info['url']}: {type(client_err).__name__}: {client_err}")
                            except Exception as fetch_err: # Еще более общий exception
                                print(f"[RSS] [DEBUG] Неожиданная ошибка при получении сырого содержимого для {feed_info['url']}: {type(fetch_err).__name__}: {fetch_err}")
                                import traceback
                                traceback.print_exc()

                if not feed.entries:
                    print(f"[RSS] Нет записей в {feed_info['url']}")
                    return local_news
                # --- Обработка записей из RSS ---
                for i, entry in enumerate(feed.entries[:MAX_ENTRIES_PER_FEED]):
                    # Даем возможность другим задачам выполниться
                    if i % 5 == 0: # yield каждые 5 итераций
                        await asyncio.sleep(0)
                    title = (entry.get('title', '') or '').strip()
                    description = (entry.get('summary', '') or '').strip()
                    # Проверка на пустой заголовок
                    if not title:
                        continue
                    # --- Проверка уникальности и генерация ключа ---
                    # Генерируем хэши для проверки дубликатов и использования в качестве идентификатора
                    title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
                    content_hash = hashlib.sha256(description.encode('utf-8')).hexdigest()
                    unique_key = f"{title_hash}_{content_hash}"
                    
                    if unique_key in seen_keys:
                        continue # Пропускаем дубликат
                    seen_keys.add(unique_key)
                    
                    # --- Инициализация базовых данных новости ---
                    news_item = {
                        'title': title,
                        'description': description,
                        'link': entry.get('link', ''),
                        'lang': feed_info['lang'],
                        'category': feed_info['category'],
                        'source': feed_info['source'],
                        'published': entry.get('published_parsed') or entry.get('updated_parsed'),
                        'image_filename': None, # Инициализируем как None
                        'video_url': None       # Инициализируем как None
                    }
                    
                    # --- Извлечение и обработка изображения ---
                    image_url_from_rss = self.extract_image_from_rss_item(entry)
                    image_url_for_processing = image_url_from_rss  # URL для дальнейшей обработки
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
                                print(f"[RSS] [IMG] Изображение не найдено в web preview для: {news_item['link']}")
                        except Exception as preview_error:
                            print(f"[ERROR] [IMG] Ошибка при извлечении изображения из web preview для {news_item['link']}: {preview_error}")

                    # Обрабатываем изображение, если оно было найдено (из RSS или из preview)
                    if image_url_for_processing:
                        try:
                            # Проверка типа контента по URL
                            parsed_url = urlparse(image_url_for_processing)
                            mime_type, _ = mimetypes.guess_type(parsed_url.path)
                            is_image_by_url = mime_type and mime_type.startswith('image/')

                            if is_image_by_url:
                                print(f"[RSS] [IMG] Найден URL изображения по MIME в URL: {image_url_for_processing[:100]}...")
                                # Скачиваем и сохраняем изображение, используя unique_key как идентификатор
                                local_image_path = await download_and_save_image(image_url_for_processing, unique_key, save_directory=IMAGES_ROOT_DIR)
                                print(f"[RSS] [IMG] local_image_path = {local_image_path}")
                            else:
                                # Пытаемся получить Content-Type через HEAD-запрос
                                print(f"[RSS] [IMG] Проверка типа изображения через HEAD-запрос для: {image_url_for_processing[:100]}...")
                                try:
                                    async with aiohttp.ClientSession() as session:
                                        timeout = aiohttp.ClientTimeout(total=10)
                                        async with session.head(image_url_for_processing, timeout=timeout) as response:
                                            content_type = response.headers.get('Content-Type', '').lower()
                                            print(f"[RSS] [IMG] HEAD-запрос вернул Content-Type: {content_type}")
                                            if content_type.startswith('image/'):
                                                print(f"[RSS] [IMG] Подтвержден тип изображения через HEAD. Скачиваем...")
                                                # Скачиваем и сохраняем изображение, используя unique_key как идентификатор
                                                local_image_path = await download_and_save_image(image_url_for_processing, unique_key, save_directory=IMAGES_ROOT_DIR)
                                                print(f"[RSS] [IMG] local_image_path = {local_image_path}")
                                            else:
                                                print(f"[WARN] URL изображения {image_url_for_processing[:100]}... не является изображением (Content-Type: {content_type})")
                                except asyncio.TimeoutError:
                                    print(f"[WARN] Таймаут при HEAD-запросе для изображения {image_url_for_processing[:100]}...")
                                except aiohttp.ClientError as client_err:
                                    print(f"[WARN] Ошибка клиента aiohttp при HEAD-запросе для изображения {image_url_for_processing[:100]}...: {client_err}")
                                except Exception as head_e:
                                    print(f"[WARN] Неожиданная ошибка при HEAD-запросе для изображения {image_url_for_processing[:100]}...: {head_e}")

                        except Exception as img_process_error:
                            print(f"[ERROR] [IMG] Ошибка при обработке изображения из URL {image_url_for_processing[:100] if image_url_for_processing else 'None'}: {img_process_error}")
                            # Не прерываем обработку новости из-за ошибки изображения
                    
                    # Если изображение было успешно скачано, обновляем путь
                    if local_image_path:
                        # Сохраняем ТОЛЬКО относительный путь к файлу
                        relative_image_path = os.path.relpath(local_image_path, IMAGES_ROOT_DIR)
                        news_item['image_filename'] = relative_image_path
                        print(f"[RSS] [IMG] Изображение сохранено. Относительный путь для сохранения в БД: {relative_image_path}")
                    else:
                        print(f"[RSS] [IMG] Изображение не будет связано с новостью.")

                    # --- Извлечение видео ---
                    video_url = self.extract_video_from_rss_item(entry)
                    if video_url:
                        news_item['video_url'] = video_url
                    else:
                        news_item['video_url'] = None

                    if news_item.get('image_filename'):
                        print(f"[DEBUG] Перед save_news_to_db image_filename = '{news_item['image_filename']}' для новости '{news_item['title'][:30]}...'")
                    else:
                        print(f"[DEBUG] Перед save_news_to_db image_filename отсутствует или None для новости '{news_item['title'][:30]}...'")

                    # --- Сохранение новости в БД (ОДИН запрос с изображением) ---
                    try:
                        # Передаем news_item с уже обработанным image_filename
                        news_id = await self.save_news_to_db(news_item, rss_feed_id)
                        if not news_id:
                            print(f"[RSS] [WARN] Не удалось получить news_id для новости '{title[:30]}...'. Пропуск.")
                            # Если изображение было скачано, но новость не сохранилась, возможно, его нужно удалить
                            # (опционально, зависит от политики хранения)
                            continue # Пропускаем, если не удалось сохранить
                        news_item['id'] = news_id # Добавляем ID к объекту новости
                        print(f"[DB] [SUCCESS] Новость и данные об изображении сохранены в БД: {news_id[:20]}...")
                    except Exception as e:
                        print(f"[RSS] [ERROR] Ошибка сохранения новости в БД: {e}")
                        continue # Пропускаем новость, если не удалось сохранить

                    # --- Обработка перевода ---
                    translations = {}
                    if self.translator_queue:
                        try:
                            print(f"[DEBUG] fetch_single_feed: Перед добавлением задачи перевода для {news_item['id'][:20]}...")
                            
                            # --- ИСПРАВЛЕНИЕ: Используем asyncio.Queue вместо asyncio.Future ---
                            translation_queue = asyncio.Queue(maxsize=1)
                            parser_event_loop = asyncio.get_running_loop()

                            async def on_translation_success(result, task_id=None):
                                print(f"[DEBUG] fetch_single_feed: Перевод УСПЕШЕН для {task_id[:20] if task_id else 'Unknown ID'}")
                                try:
                                    if not result or not isinstance(result, dict):
                                        print(f"[DB] [WARN] on_translation_success: Получен пустой или некорректный результат перевода для {task_id[:20] if task_id else 'Unknown ID'}.")
                                        # Планируем завершение в основном loop'е
                                        parser_event_loop.call_soon_threadsafe(
                                            lambda: asyncio.create_task(translation_queue.put({'status': 'done', 'data': result, 'error': None}))
                                        )
                                        return
                                    translation_count = sum(1 for lang_data in result.values() if isinstance(lang_data, dict) and ('title' in lang_data or 'description' in lang_data))
                                    if translation_count == 0:
                                        print(f"[DB] [WARN] on_translation_success: Результат перевода для {task_id[:20] if task_id else 'Unknown ID'} не содержит данных.")
                                        parser_event_loop.call_soon_threadsafe(
                                            lambda: asyncio.create_task(translation_queue.put({'status': 'done', 'data': result, 'error': None}))
                                        )
                                        return
                                    print(f"[DB] [on_translation_success] Подготовка к сохранению {translation_count} переводов для новости {task_id[:20] if task_id else 'Unknown ID'}...")
                                    
                                    # --- ИСПРАВЛЕНИЕ 2 ---
                                    # Вместо прямого вызова await self.save_translations_to_db(task_id, result)
                                    # Мы создаем задачу, которая вызывает save_translations_to_db в правильном loop
                                    async def put_result_to_queue(save_success_flag):
                                        """Помещает результат в очередь после сохранения."""
                                        try:
                                            if save_success_flag:
                                                print(f"[DB] [on_translation_success] Переводы для {task_id[:20] if task_id else 'Unknown ID'} успешно сохранены.")
                                            else:
                                                print(f"[DB] [WARN] [on_translation_success] Ошибка при сохранении переводов для {task_id[:20] if task_id else 'Unknown ID'} в БД.")
                                            await translation_queue.put({
                                                'status': 'done',
                                                'data': result,
                                                'db_saved': save_success_flag,
                                                'error': None
                                            })
                                        except Exception as put_error:
                                            error_msg = f"[DB] [on_translation_success] Ошибка при помещении результата в очередь: {put_error}"
                                            print(error_msg)
                                            await translation_queue.put({
                                                'status': 'error',
                                                'data': None,
                                                'error': error_msg
                                            })

                                    def schedule_save_and_put():
                                        """Планирует асинхронное сохранение и последующее помещение результата."""
                                        async def do_save_and_put():
                                            try:
                                                save_success = await self.save_translations_to_db(task_id, result)
                                                # После сохранения, планируем помещение результата в очередь
                                                # снова в правильном loop
                                                parser_event_loop.call_soon_threadsafe(
                                                    lambda: asyncio.create_task(put_result_to_queue(save_success))
                                                )
                                            except Exception as db_error:
                                                error_msg = f"[DB] [on_translation_success] [schedule_save_and_put] Ошибка БД при сохранении: {db_error}"
                                                print(error_msg)
                                                import traceback
                                                traceback.print_exc()
                                                # Планируем ошибку в очередь
                                                parser_event_loop.call_soon_threadsafe(
                                                    lambda: asyncio.create_task(put_result_to_queue(False)) # False означает ошибку сохранения
                                                )
                                        
                                        # Планируем выполнение do_save_and_put в правильном event loop
                                        asyncio.create_task(do_save_and_put())

                                    # Планируем выполнение schedule_save_and_put в правильном event loop
                                    parser_event_loop.call_soon_threadsafe(schedule_save_and_put)
                                    # --- КОНЕЦ ИСПРАВЛЕНИЯ 2 ---
                                    
                                except Exception as e:
                                    error_msg = f"[DB] [on_translation_success] Неожиданная ошибка в обработчике: {e}"
                                    print(error_msg)
                                    import traceback
                                    traceback.print_exc()
                                    parser_event_loop.call_soon_threadsafe(
                                        lambda: asyncio.create_task(translation_queue.put({
                                            'status': 'error',
                                            'data': None,
                                            'error': error_msg
                                        }))
                                    )

                            def on_translation_error(error_data):
                                error_msg = error_data.get('error', 'Unknown translation error')
                                task_id = error_data.get('task_id', 'Unknown')
                                print(f"[ERROR] fetch_single_feed: Ошибка перевода для {task_id[:20]}: {error_msg}")
                                # Планируем завершение в основном loop'е
                                parser_event_loop.call_soon_threadsafe(
                                    lambda: asyncio.create_task(translation_queue.put({
                                        'status': 'error',
                                        'data': None,
                                        'error': error_msg
                                    }))
                                )
                            # --- КОНЕЦ ИСПРАВЛЕНИЯ ---

                            # Добавляем задачу в очередь
                            await self.translator_queue.add_task(
                                news_item['title'],
                                news_item['description'],
                                news_item['category'],
                                news_item['lang'],
                                on_translation_success,
                                on_translation_error,
                                news_item['id']
                            )
                            print(f"[DEBUG] fetch_single_feed: Задача перевода добавлена в очередь для {news_item['id'][:20]}...")
                            
                            # Ждем результат перевода с таймаутом
                            try:
                                # --- ИСПРАВЛЕНИЕ: Ждем из очереди ---
                                result_item = await asyncio.wait_for(translation_queue.get(), timeout=130.0)
                                print(f"[DEBUG] fetch_single_feed: Перевод завершен для {news_item['id'][:20]}...")
                                
                                if result_item.get('status') == 'error':
                                    print(f"[ERROR] fetch_single_feed: Ошибка при переводе для {news_item['id'][:20]}: {result_item.get('error')}")
                                    translations = {}
                                else:
                                    translations = result_item.get('data', {})
                                # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
                            except asyncio.TimeoutError:
                                print(f"[ERROR] fetch_single_feed: Таймаут ожидания перевода для {news_item['id'][:20]}...")
                                translations = {}
                            except Exception as e:
                                print(f"[ERROR] fetch_single_feed: Критическая ошибка при обработке перевода для {news_item['id'][:20]}: {e}")
                                import traceback
                                traceback.print_exc()
                                translations = {}
                        except Exception as e:
                            print(f"[ERROR] fetch_single_feed: Ошибка при постановке в очередь перевода для {news_item['id'][:20]}: {e}")
                            translations = {}
                    else:
                        print("[WARN] fetch_single_feed: translator_queue не передан в RSSManager. Переводы не будут выполнены.")
                        original_lang = news_item['lang']
                        translations[original_lang] = {
                            'title': news_item['title'],
                            'description': news_item['description']
                        }
                        try:
                            await self.save_translations_to_db(news_item['id'], translations)
                            print(f"[DB] [SUCCESS] Оригинальный текст сохранен как перевод на {original_lang} для {news_item['id'][:20]}")
                        except Exception as e:
                            print(f"[DB] [ERROR] Ошибка при сохранении оригинального текста как перевода: {e}")
                        translations = {}

                    # 3. Добавляем rss_feed_id к новости для дальнейшей обработки
                    news_item['rss_feed_id'] = rss_feed_id
                    # 4. Добавляем полученные переводы в объект новости
                    news_item['translations'] = translations
                    # 5. Добавляем новость в список для возврата
                    local_news.append(news_item)
                    print(f"[DB] [SUCCESS] Новость полностью обработана (сохранена с изображением, видео, переведена): {title[:50]}...")
            except Exception as e:
                print(f"[RSS] Ошибка при обработке ленты {feed_info['url']}: {e}")
                import traceback
                traceback.print_exc()
            return local_news

    async def fetch_news(self):
        """Асинхронная функция для получения новостей из RSS-лент"""
        seen_keys = set()
        all_news = []
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
        try:
            active_feeds = await self.get_all_active_feeds()
            print(f"[RSS] Найдено {len(active_feeds)} активных RSS-лент.")
            if not active_feeds:
                print("[RSS] Нет активных лент для парсинга.")
                return []
            # Создаем список задач (но они не запускаются сразу)
            tasks = [asyncio.create_task(self.fetch_single_feed(feed_info, seen_keys, headers))
                     for feed_info in active_feeds]
            # Обрабатываем задачи по мере завершения
            completed_count = 0
            try:
                for coro in asyncio.as_completed(tasks):
                    try:
                        result = await coro
                        completed_count += 1
                        print(f"[RSS] [INFO] Завершена задача {completed_count}/{len(tasks)}. Получено новостей: {len(result) if isinstance(result, list) else 'N/A'}")
                        if isinstance(result, list):
                            all_news.extend(result)
                        else:
                            print(f"[RSS] [WARN] Неожиданный тип результата из завершенной задачи: {type(result)}")
                    except Exception as e:
                        completed_count += 1
                        print(f"[RSS] [ERROR] Ошибка при ожидании результата задачи {completed_count}: {e}")
            except Exception as e:
                completed_count += 1
                print(f"[RSS] [ERROR] Ошибка при ожидании результата задачи {completed_count}: {e}")
            # Отменяем любые оставшиеся (хотя их быть не должно при использовании as_completed)
            for task in tasks:
                if not task.done():
                    print(f"[RSS] [WARN] Отмена незавершенной задачи: {task}")
                    task.cancel()
        except Exception as e:
            import traceback
            print(f"❌ Критическая ошибка в fetch_news: {e}")
            traceback.print_exc()
        # Сортировка и ограничение количества новостей
        try:
            sorted_news = sorted(all_news, key=lambda x: x.get('published', datetime.min), reverse=True)
        except Exception as e:
            print(f"[RSS] [WARN] Ошибка сортировки новостей по дате: {e}")
            sorted_news = all_news # Если сортировка не удалась, возвращаем как есть
        final_news = sorted_news[:MAX_TOTAL_NEWS]
        print(f"[RSS] Всего собрано уникальных новостей: {len(final_news)}")
        return final_news

    # --- МЕТОДЫ СОХРАНЕНИЯ В БД ---
    async def save_news_to_db(self, news_item, rss_feed_id):
        print(f"[DEBUG] save_news_to_db news_item = {news_item}")

        """Асинхронно сохраняет информацию о новости в базу данных."""
        title = news_item['title']
        content = news_item['description']
        url = news_item['link']
        original_language = news_item['lang']
        category_name = news_item['category']
        image_filename = news_item.get('image_filename')
        # 1. Генерируем ID ОДИН РАЗ
        title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        news_id = f"{title_hash}_{content_hash}"
        short_id = news_id[:20] + "..." if len(news_id) > 20 else news_id
        print(f"[DB] [save_news_to_db] Начало обработки для ID: {short_id}")
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 2a. ВСТАВЛЯЕМ или ОБНОВЛЯЕМ в родительской таблице published_news
                    query_published_news = """
                    INSERT INTO published_news (id, title_hash, content_hash, source_url, published_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE SET
                    source_url = EXCLUDED.source_url,
                    published_at = NOW()
                    """
                    print(f"[DB] [save_news_to_db] Подготовка запроса к 'published_news' (ID: {short_id})")
                    await cur.execute(query_published_news, (news_id, title_hash, content_hash, url))
                    print(f"[DB] [save_news_to_db] Запрос к 'published_news' выполнен. (ID: {short_id})")
                    # 2b. Проверяем существование
                    check_query = "SELECT 1 FROM published_news WHERE id = %s LIMIT 1"
                    print(f"[DB] [save_news_to_db] Выполнение проверочного SELECT (ID: {short_id})")
                    await cur.execute(check_query, (news_id,))
                    exists_in_parent = await cur.fetchone()
                    if not exists_in_parent:
                        # Критическая ошибка
                        error_msg = f"[DB] [CRITICAL] Запись в 'published_news' НЕ существует! FK constraint будет нарушено. (ID: {short_id})"
                        print(error_msg)
                        # Отладочный запрос
                        debug_query = "SELECT id, title_hash, content_hash FROM published_news WHERE id = %s OR title_hash = %s OR content_hash = %s LIMIT 5"
                        await cur.execute(debug_query, (news_id, title_hash, content_hash))
                        debug_rows = await cur.fetchall()
                        print(f"[DB] [DEBUG] Результаты отладочного запроса: {debug_rows}")
                        return None # Возвращаем None, чтобы сигнализировать об ошибке
                    print(f"[DB] [save_news_to_db] Подтверждено: запись в 'published_news' существует. (ID: {short_id})")
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
                    # 3a. Получаем category_id
                    await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                    cat_result = await cur.fetchone()
                    if not cat_result:
                        print(f"[DB] [save_news_to_db] Предупреждение: Категория '{category_name}' не найдена. Пропуск сохранения новости.")
                        return None
                    category_id = cat_result[0]
                    # 3b. Выполняем запрос
                    print(f"[DB] [save_news_to_db] Подготовка запроса к 'published_news_data' (ID: {short_id})")
                    await cur.execute(query_published_news_data, (
                        news_id, title, content, original_language, category_id, image_filename, rss_feed_id
                    ))
                    print(f"[DB] [save_news_to_db] Запрос к 'published_news_data' выполнен. (ID: {short_id})")
                    # --- ИСПРАВЛЕНИЕ ---
                    # УДАЛЕН вызов await self.save_translations_to_db(news_id, translations={})
                    # Переводы будут сохранены позже, в callback'е on_translation_success
                    # ИЛИ в fetch_single_feed, если очередь не передана
                    # --- КОНЕЦ ИСПРАВЛЕНИЯ ---
                    print(f"[DB] [save_news_to_db] Новость успешно сохранена в БД (ID: {short_id}). Переводы будут обработаны отдельно.")
                    return news_id
        except Exception as e:
            print(f"[DB] [save_news_to_db] ❌ Критическая ошибка БД при сохранении новости (ID: {short_id}): {e}")
            import traceback
            traceback.print_exc()
            return None

    # - функция для сохранения переводов в БД -
    async def save_translations_to_db(self, news_id, translations):
        """Асинхронно сохраняет переводы новости в таблицу news_translations."""
        short_news_id = news_id[:20] if news_id else 'Unknown'
        print(f"[DB] [save_translations_to_db] Начало сохранения переводов для новости ID: {short_news_id}")

        if not translations:
            print(f"[DB] [save_translations_to_db] Нет переводов для сохранения для новости {short_news_id}...")
            return True

        if not isinstance(translations, dict):
            print(f"[DB] [save_translations_to_db] ❌ ОШИБКА: translations должен быть словарем, но является {type(translations)}. Пропуск.")
            return False

        pool = None
        try:
            print(f"[DB] [save_translations_to_db] Получение пула соединений для новости {short_news_id}...")
            pool = await self.get_pool()
            if not pool:
                print(f"[DB] [save_translations_to_db] ❌ ОШИБКА: Не удалось получить пул соединений для новости {short_news_id}.")
                return False
            print(f"[DB] [save_translations_to_db] Пул соединений получен для новости {short_news_id}.")

            # Для каждой операции создаем отдельное соединение и курсор
            # Это предотвращает конфликты при асинхронном выполнении запросов
            translation_count = 0
            for lang, translation_data in translations.items():
                translation_count += 1
                print(f"[DB] [save_translations_to_db] [{translation_count}] Обработка перевода для языка '{lang}' новости {short_news_id}...")
                
                if not isinstance(translation_data, dict):
                    print(f"[DB] [save_translations_to_db] [{translation_count}] Некорректный формат данных перевода для языка {lang} у новости {short_news_id}: {translation_data} (тип: {type(translation_data)}). Пропуск.")
                    continue

                title = translation_data.get('title', '')
                description = translation_data.get('description', '')
                print(f"[DB] [save_translations_to_db] [{translation_count}] Данные для языка '{lang}': title_len={len(title)}, description_len={len(description)}")

                # Проверки на None/пустоту
                if news_id is None:
                    print(f"[DB] [save_translations_to_db] [{translation_count}] ❌ ОШИБКА: news_id равен None. Пропуск перевода для языка {lang}.")
                    continue
                if lang is None:
                    print(f"[DB] [save_translations_to_db] [{translation_count}] ❌ ОШИБКА: lang равен None. Пропуск перевода для news_id {short_news_id}.")
                    continue

                # Создаем отдельное соединение для каждой операции
                print(f"[DB] [save_translations_to_db] [{translation_count}] Получение нового соединения из пула для '{lang}' ({short_news_id})...")
                async with pool.acquire() as conn:
                    print(f"[DB] [save_translations_to_db] [{translation_count}] Соединение получено для '{lang}' ({short_news_id}).")
                    async with conn.cursor() as cur:
                        print(f"[DB] [save_translations_to_db] [{translation_count}] Курсор создан для '{lang}' ({short_news_id}).")

                        # Вставляем или обновляем перевод
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
                            import traceback
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
            import traceback
            traceback.print_exc()
            
            return False


    # --- МЕТОДЫ ИЗВЛЕЧЕНИЯ МЕДИА ---
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
            for enc in enclosures:
                if enc.get('type', '').startswith('image/'):
                    url = enc.get('href') or enc.get('url')
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
                                pass # Не удалось преобразовать размер
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
                                        pass # Не удалось преобразовать размер
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
                                pass # Не удалось преобразовать размер
                        if size_ok:
                            print(f"[INFO] Найдено видео в media:content (dict): {media_url}")
                            return media_url
        except Exception as e:
            print(f"[WARN] Ошибка при извлечении видео из RSS item: {e}")
        print("[INFO] Видео не найдено в RSS item.")
        return None
    # --- МЕТОДЫ ДЛЯ ТЕЛЕГРАМ-БОТА ---
    async def fetch_unprocessed_news(self):
        """Получает новости, которые еще не были опубликованы в Telegram."""
        try:
            pool = await self.get_pool()
            unprocessed_news = []
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Запрос на получение новостей, у которых telegram_published_at IS NULL
                    # и JOIN с rss_feeds для получения category_display
                    query = """
                    SELECT
                        pnd.news_id,
                        pnd.original_title,
                        pnd.original_content,
                        pnd.original_language,
                        pnd.category_id,
                        pnd.image_url,
                        pnd.rss_feed_id,
                        pnd.created_at,
                        pnd.updated_at,
                        -- JSON агрегация переводов
                        (SELECT json_agg(row_to_json(t)) FROM (
                            SELECT nt.language, nt.translated_title, nt.translated_content
                            FROM news_translations nt
                            WHERE nt.news_id = pnd.news_id
                        ) t) as translations,
                        -- Получение отображаемого имени категории из rss_feeds
                        COALESCE(rf.category_display, c.name) as category_display,
                        -- Получение имени источника
                        s.name as source_name,
                        pn.source_url as link
                    FROM published_news_data pnd
                    JOIN published_news pn ON pnd.news_id = pn.id
                    JOIN rss_feeds rf ON pnd.rss_feed_id = rf.id
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE pnd.telegram_published_at IS NULL
                    ORDER BY pnd.created_at ASC
                    LIMIT 100
                    """
                    await cur.execute(query)
                    columns = [desc[0] for desc in cur.description]
                    async for row in cur:
                        news_item = dict(zip(columns, row))
                        # Преобразуем JSON с переводами в словарь
                        if news_item.get('translations'):
                            import json
                            try:
                                translations_data = json.loads(news_item['translations'])
                                # print(f"[RSSManager] [fetch_unprocessed_news] raw translations_data = {translations_data}")
                                # Преобразуем список переводов в словарь {язык: {данные}}
                                # Исправлено: используем правильные ключи из данных
                                news_item['translations'] = {
                                    item.get('language', 'unknown'): {
                                        'title': item.get('translated_title', ''),
                                        'description': item.get('translated_content', '')
                                    } for item in translations_data if isinstance(item, dict)
                                }
                                # print(f"[RSSManager] [fetch_unprocessed_news] processed translations = {news_item['translations']}")
                            except (json.JSONDecodeError, TypeError) as e:
                                print(f"[RSSManager] [fetch_unprocessed_news] Ошибка парсинга переводов: {e}")
                                news_item['translations'] = {}
                        else:
                            news_item['translations'] = {}
                        # Добавляем заглушку для published если она отсутствует
                        if 'published' not in news_item:
                            news_item['published'] = datetime.now(pytz.utc)
                        unprocessed_news.append(news_item)
            return unprocessed_news
        except Exception as e:
            print(f"[DB] [ERROR] Ошибка при получении необработанных новостей: {e}")
            import traceback
            traceback.print_exc()
            return []
