import psycopg2
from psycopg2 import Error
import hashlib
import feedparser
import asyncio
import re
import pytz
from datetime import datetime
from dateutil import parser
from config import DB_CONFIG, MAX_ENTRIES_PER_FEED, MAX_TOTAL_NEWS
from translator import prepare_translations

class RSSManager:
    def __init__(self):
        self.connection = None

    def _get_all_feeds(self):
        """Вспомогательный метод: Получает список ВСЕХ RSS-лент."""
        connection = None
        cursor = None
        feeds = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
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
            cursor.execute(query)
            results = cursor.fetchall()
            
            for row in results:
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
            
        except psycopg2.Error as err:
            print(f"[DB] [RSSManager] Ошибка в _get_all_feeds: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return feeds

    def _get_all_active_feeds(self):
        """Вспомогательный метод: Получает список АКТИВНЫХ RSS-лент."""
        connection = None
        cursor = None
        feeds = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
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
            cursor.execute(query)
            results = cursor.fetchall()
            
            for row in results:
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
            
        except psycopg2.Error as err:
            print(f"[DB] [RSSManager] Ошибка в _get_all_active_feeds: {err}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return feeds

    def _get_feeds_by_category(self, category_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени категории."""
        connection = None
        cursor = None
        feeds = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            query = """
                SELECT rf.*, c.name as category_name, s.name as source_name
                FROM rss_feeds rf
                JOIN categories c ON rf.category_id = c.id
                JOIN sources s ON rf.source_id = s.id
                WHERE c.name = %s AND rf.is_active = TRUE
            """
            cursor.execute(query, (category_name,))
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            feeds = [dict(zip(columns, row)) for row in results]
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка при получении фидов по категории '{category_name}': {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return feeds

    def _get_feeds_by_lang(self, lang):
        """Вспомогательный метод: Получить активные RSS-ленты по языку."""
        connection = None
        cursor = None
        feeds = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            query = """
                SELECT rf.*, c.name as category_name, s.name as source_name 
                FROM rss_feeds rf 
                JOIN categories c ON rf.category_id = c.id 
                JOIN sources s ON rf.source_id = s.id 
                WHERE rf.language = %s AND rf.is_active = TRUE
            """
            cursor.execute(query, (lang,))
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            feeds = [dict(zip(columns, row)) for row in results]
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка при получении фидов по языку '{lang}': {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return feeds

    def _get_feeds_by_source(self, source_name):
        """Вспомогательный метод: Получить активные RSS-ленты по имени источника."""
        connection = None
        cursor = None
        feeds = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            query = """
                SELECT rf.*, c.name as category_name, s.name as source_name
                FROM rss_feeds rf
                JOIN categories c ON rf.category_id = c.id
                JOIN sources s ON rf.source_id = s.id
                WHERE s.name = %s AND rf.is_active = TRUE
            """
            cursor.execute(query, (source_name,))
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            feeds = [dict(zip(columns, row)) for row in results]
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка при получении фидов по источнику '{source_name}': {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return feeds

    def _add_feed(self, category_name, url, language, source_name):
        """Вспомогательный метод: Добавить новую RSS-ленту."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            # 1. Получить ID категории по имени
            cursor.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
            cat_result = cursor.fetchone()
            if not cat_result:
                print(f"[DB] [RSSManager] Ошибка: Категория '{category_name}' не найдена в таблице 'categories'.")
                return False
            category_id = cat_result[0]

            # 2. Получить ID источника по имени
            cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
            src_result = cursor.fetchone()
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
            cursor.execute(query, (source_id, url, feed_name, category_id, language, True))
            connection.commit()
            print(f"[DB] [RSSManager] Лента '{url}' успешно добавлена.")
            return True
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка БД при добавлении фида '{url}': {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _update_feed(self, feed_id, category_name, url, language, source_name, is_active, feed_name):
        """Вспомогательный метод: Обновить RSS-ленту. None означает "не обновлять это поле"."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            updates = []
            values = []
            
            # Обработка изменения категории по имени (если не None)
            if category_name is not None:
                cursor.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                cat_result = cursor.fetchone()
                if cat_result:
                    updates.append("category_id = %s")
                    values.append(cat_result[0])
                else:
                    print(f"[DB] [RSSManager] Предупреждение: Категория '{category_name}' не найдена. Поле category_id не обновлено.")
            
            # Обработка изменения источника по имени (если не None)
            if source_name is not None:
                cursor.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
                src_result = cursor.fetchone()
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
            cursor.execute(query, values)
            connection.commit()
            cursor.execute("SELECT COUNT(*) FROM rss_feeds WHERE id = %s", (feed_id,))
            affected_rows = cursor.fetchone()[0]
            if affected_rows > 0:
                print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно обновлена.")
            else:
                print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена или не была изменена.")
            return affected_rows > 0
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка БД при обновлении фида с ID {feed_id}: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _delete_feed(self, feed_id):
        """Вспомогательный метод: Удалить RSS-ленту по ID."""
        connection = None
        cursor = None
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            query = "DELETE FROM rss_feeds WHERE id = %s"
            cursor.execute(query, (feed_id,))
            connection.commit()
            affected_rows = cursor.rowcount
            if affected_rows > 0:
                print(f"[DB] [RSSManager] Лента с ID {feed_id} успешно удалена.")
            else:
                print(f"[DB] [RSSManager] Лента с ID {feed_id} не найдена.")
            return affected_rows > 0
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка БД при удалении фида с ID {feed_id}: {e}")
            if connection:
                connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _get_categories(self):
        """Вспомогательный метод: Получить список всех категорий."""
        connection = None
        cursor = None
        categories = []
        try:
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()
            
            get_categories_query = """
                SELECT DISTINCT c.name AS category
                FROM categories c
                JOIN rss_feeds rf ON c.id = rf.category_id
                WHERE rf.is_active = TRUE
                ORDER BY c.name;
            """
            cursor.execute(get_categories_query)
            categories = [row[0] for row in cursor.fetchall()]
            
        except psycopg2.Error as e:
            print(f"[DB] [RSSManager] Ошибка при получении категорий: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return categories

    # --- Публичные асинхронные методы ---
    # Эти методы будут вызываться из вашего асинхронного кода.
    # Они оборачивают вспомогательные методы в run_in_executor.

    async def get_all_feeds(self):
        """Асинхронно получает список ВСЕХ RSS-лент."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_all_feeds)

    async def get_all_active_feeds(self):
        """Асинхронно получает список АКТИВНЫХ RSS-лент."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_all_active_feeds)

    async def get_feeds_by_category(self, category_name):
        """Асинхронно получить активные RSS-ленты по имени категории."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_feeds_by_category, category_name)

    async def get_feeds_by_lang(self, lang):
        """Асинхронно получить активные RSS-ленты по языку."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_feeds_by_lang, lang)

    async def get_feeds_by_source(self, source_name):
        """Асинхронно получить активные RSS-ленты по имени источника."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_feeds_by_source, source_name)

    async def add_feed(self, category_name, url, language, source_name):
        """Асинхронно добавить новую RSS-ленту."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._add_feed, category_name, url, language, source_name)

    async def update_feed(self, feed_id, category_name=None, url=None, language=None, source_name=None, is_active=None, feed_name=None):
        """Асинхронно обновить RSS-ленту. None означает "не обновлять это поле"."""
        # Примечание: run_in_executor требует передачи всех аргументов, даже если они None.
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._update_feed, feed_id, category_name, url, language, source_name, is_active, feed_name)

    async def delete_feed(self, feed_id):
        """Асинхронно удалить RSS-ленту по ID."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._delete_feed, feed_id)

    async def get_categories(self):
        """Асинхронно получить список всех категорий."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._get_categories)
    
    def is_news_new(self, title_hash, content_hash, url):
        """
        Проверяет, является ли новость новой (не опубликованной ранее).
        Создает собственное соединение с БД.
        Эта версия предназначена для вызова через run_in_executor.
        """
        connection = None
        cursor = None
        try:
            # 1. Создаем новое соединение внутри метода
            connection = psycopg2.connect(**DB_CONFIG)
            cursor = connection.cursor()

            # 2. Проверяем существование по title_hash ИЛИ content_hash
            query = """
                SELECT 1 FROM published_news 
                WHERE title_hash = %s OR content_hash = %s 
                LIMIT 1
            """
            cursor.execute(query, (title_hash, content_hash))
            result = cursor.fetchone()
            
            # Если результат есть (result не None), новость считается НЕ новой
            is_duplicate = result is not None

            return not is_duplicate # Возвращаем True, если НЕ дубликат
            
        except psycopg2.Error as err:
            print(f"[DB] [is_news_new] Ошибка БД: {err}")
            # В случае ошибки БД лучше считать новость НЕ новой, чтобы избежать дубликатов
            return False 
        except Exception as e: # Ловим все остальные исключения
            print(f"[DB] [is_news_new] Неожиданная ошибка: {e}")
            return False
        finally:
            # 3. ВАЖНО: Закрываем курсор и соединение
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                # print("[DB] [is_news_new] Соединение закрыто") # Опционально

    def mark_as_published(self, title, content, url, original_language, translations_dict, category_name=None, image_filename=None):
        """
        Сохраняет информацию о опубликованной новости с проверкой уникальности (хэши).
        Сохраняет оригинальные данные и переводы новости для API.
        Создает собственное соединение с БД. Предназначена для вызова через run_in_executor.

        :param category_name: название категории (опционально)
        :param image_filename: имя файла изображения (опционально)
        """
        # 1. Генерируем ID ОДИН РАЗ
        title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(content.encode('utf-8')).hexdigest()
        news_id = f"{title_hash}_{content_hash}"
        short_id = news_id[:20] + "..." if len(news_id) > 20 else news_id
        print(f"[DB] [mark_as_published] Начало обработки для ID: {short_id}")

        # --- ВАЖНО: Создаем собственное соединение ---
        connection = None
        cursor = None
        try:
            # Создаем новое соединение внутри метода
            connection = psycopg2.connect(**DB_CONFIG) # <-- Используем DB_CONFIG напрямую
            cursor = connection.cursor()
            # --- Конец создания соединения ---

            # --- Получаем category_id по названию категории ---
            category_id = None
            if category_name:
                category_query = "SELECT id FROM categories WHERE name = %s LIMIT 1"
                cursor.execute(category_query, (category_name,))
                category_result = cursor.fetchone()
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
            params_news = (news_id, title_hash, content_hash, url)
            print(f"[DB] [mark_as_published] Подготовка запроса к 'published_news' (ID: {short_id})")

            cursor.execute(query_published_news, params_news)
            rows_affected_news = cursor.rowcount
            print(f"[DB] [mark_as_published] Запрос к 'published_news' выполнен. ROWS AFFECTED: {rows_affected_news} (ID: {short_id})")

            # --- ВАЖНО: Коммитим сразу после вставки в родительскую таблицу ---
            connection.commit()
            print(f"[DB] [mark_as_published] Коммит после вставки в 'published_news' выполнен. (ID: {short_id})")
            # ---------------------------------------------------------------

            # 2b. Проверяем существование ПОСЛЕ коммита
            check_query = "SELECT 1 FROM published_news WHERE id = %s LIMIT 1"
            print(f"[DB] [mark_as_published] Выполнение проверочного SELECT (ID: {short_id})")
            cursor.execute(check_query, (news_id,))
            exists_in_parent = cursor.fetchone()
            
            if not exists_in_parent:
                # Критическая ошибка
                error_msg = f"[DB] [CRITICAL] Запись в 'published_news' НЕ существует после КОММИТА! FK constraint будет нарушено. (ID: {short_id})"
                print(error_msg)
                # Отладочный запрос
                debug_query = "SELECT id, title_hash, content_hash FROM published_news WHERE id = %s OR title_hash = %s OR content_hash = %s LIMIT 5"
                debug_params = (news_id, title_hash, content_hash)
                print(f"[DB] [DEBUG] Выполнение отладочного запроса по ID, title_hash, content_hash...")
                cursor.execute(debug_query, debug_params)
                debug_results = cursor.fetchall()
                if debug_results:
                    print(f"[DB] [DEBUG] Найдены потенциально конфликтующие записи в 'published_news':")
                    for row in debug_results:
                        print(f"  - ID: {row[0]}, Title_Hash: {row[1][:20]}..., Content_Hash: {row[2][:20]}...")
                else:
                    print(f"[DB] [DEBUG] Записи с таким ID, title_hash или content_hash в 'published_news' НЕ НАЙДЕНЫ.")
                # Возвращаем False вместо исключения, чтобы не прерывать всю задачу
                # Исключение будет перехвачено run_in_executor и превращено в failed future
                return False 
            else:
                print(f"[DB] [mark_as_published] Подтверждено: запись в 'published_news' существует ПОСЛЕ КОММИТА. (ID: {short_id})")
            # -------------------------------------------------------------

            # 3. ВСТАВЛЯЕМ или ОБНОВЛЯЕМ в дочерней таблице published_news_data
            query_published_news_data = """
            INSERT INTO published_news_data 
            (news_id, original_title, original_content, original_language, category_id, image_filename, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (news_id) DO UPDATE SET
                original_title = EXCLUDED.original_title,
                original_content = EXCLUDED.original_content,
                original_language = EXCLUDED.original_language,
                category_id = EXCLUDED.category_id,
                image_filename = EXCLUDED.image_filename,
                updated_at = NOW()
            """
            print(f"[DB] [mark_as_published] Подготовка запроса к 'published_news_data' (ID: {short_id})")
            cursor.execute(query_published_news_data, (
                news_id,
                title, 
                content, 
                original_language, 
                category_id,
                image_filename
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
                    cursor.execute(query_translation, (news_id, lang_code, trans_title, trans_content))
            
            connection.commit()
            print(f"[DB] [SUCCESS] Новость и переводы сохранены: {short_id}")
            print(f"[DB] [mark_as_published] Обработка переводов завершена. (ID: {short_id})")
            
            return True # <-- Возвращаем True при успехе
            
        except psycopg2.Error as err:
            print(f"[DB] [ERROR] Ошибка БД при сохранении (ID: {short_id}): {err}")
            if connection:
                connection.rollback()
            return False # <-- Возвращаем False при ошибке БД
        except Exception as e:
            print(f"[DB] [ERROR] Неожиданная ошибка в mark_as_published (ID: {short_id}): {e}")
            import traceback
            traceback.print_exc()
            if connection:
                connection.rollback()
            return False # <-- Возвращаем False при любой другой ошибке
        finally:
            # --- ВАЖНО: Закрываем курсор и соединение ---
            if cursor:
                cursor.close()
            if connection:
                connection.close()
                print(f"[DB] [mark_as_published] Соединение закрыто. (ID: {short_id})")
            # --- Конец закрытия соединения ---
    
    
    async def fetch_single_feed(self, feed_info, seen_keys, headers):
        """
        Асинхронно парсит одну RSS-ленту и возвращает список новостей из неё.
        """
        local_news = []
        try:
            print(f"[RSS] Парсинг ленты: {feed_info['name']} ({feed_info['url']})")
            # feedparser.parse синхронный, но его можно запустить в executor'е
            # для неблокирующего выполнения в асинхронном коде.
            # loop = asyncio.get_event_loop()
            # feed = await loop.run_in_executor(None, feedparser.parse, feed_info['url'], headers)
            # Однако, профилирование показывает, что feedparser.parse внутри
            # run_in_executor всё равно может блокировать. Лучше оставить как есть
            # или перенести всю fetch_news в executor.
            # Пока оставим, как есть, но помним о потенциальной блокировке.
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
                # Уникальный ключ для предотвращения дубликатов в текущей итерации
                # Включаем источник и категорию для большей уникальности
                unique_key = (feed_info['source'], feed_info['category'], normalized_title)

                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)

                entry_link = entry.get('link', '#')

                # Проверяем уникальность через БД (хэши)
                title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
                content_hash = hashlib.sha256(description.encode('utf-8')).hexdigest()

                loop = asyncio.get_event_loop()
                is_new = await loop.run_in_executor(None, self.is_news_new, title_hash, content_hash, entry_link)
                if not is_new:
                    continue

                # Обработка даты с fallback
                pub_date = getattr(entry, 'published', None)
                if pub_date:
                    try:
                        # parser.parse также синхронный
                        loop = asyncio.get_event_loop()
                        published = await loop.run_in_executor(None, parser.parse, pub_date)
                        published = published.replace(tzinfo=pytz.utc)
                        
                        # published = parser.parse(pub_date).replace(tzinfo=pytz.utc)
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
                }

                local_news.append(news_item)

        except Exception as e:
            print(f"[RSS] Ошибка при обработке ленты {feed_info['url']}: {e}")
            import traceback
            traceback.print_exc()

        return local_news

    async def fetch_news(self):
        """Асинхронная функция для получения новостей из RSS-лент"""
        seen_keys = set() # Этот set будет разделяться между задачами, но только для чтения/записи в текущей итерации fetch_news
                          # Это может привести к гонке данных, но для простоты и скорости оставим.
                          # Для строгой асинхронной безопасности лучше передавать его в каждую задачу
                          # или использовать asyncio.Lock, но это усложнит код.
        all_news = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        }

        try:
            active_feeds = await self.get_all_active_feeds()
            print(f"[RSS] Найдено {len(active_feeds)} активных RSS-лент.")

            if not active_feeds:
                 print("[RSS] Нет активных лент для парсинга.")
                 return []

            # Создаем список задач для парсинга каждой ленты
            tasks = [
                self.fetch_single_feed(feed_info, seen_keys, headers)
                for feed_info in active_feeds
            ]

            # Выполняем все задачи одновременно
            # return_exceptions=True позволяет продолжить выполнение других задач,
            # если одна из них выбросит исключение
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Обрабатываем результаты
            for i, result in enumerate(results):
                feed_url = active_feeds[i]['url'] if i < len(active_feeds) else "Unknown Feed"
                if isinstance(result, Exception):
                    print(f"[RSS] [ERROR] Исключение при парсинге {feed_url}: {result}")
                elif isinstance(result, list): # Ожидаемый тип результата
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