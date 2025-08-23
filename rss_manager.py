import mysql.connector
from mysql.connector import Error
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

    def get_db_connection(self):
        """Установить или восстановить соединение с базой данных"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**DB_CONFIG)
                print("✅ Подключение к БД установлено")
            return self.connection
        except Error as e:
            print(f"❌ Ошибка подключения к MySQL: {e}")
            return None

    def get_all_feeds(self):
        """Получить все RSS-ленты"""
        connection = self.get_db_connection()
        if connection is None:
            return []
        
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute("SELECT * FROM rss_feeds")
            return cursor.fetchall()
        except Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            cursor.close()
    
    def get_all_active_feeds(self):
        """Получить все RSS-ленты сгруппированные по категориям"""
        connection = self.get_db_connection()
        if connection is None:
            return {}
        
        cursor = connection.cursor(dictionary=True)
        try:
            cursor.execute("SELECT category, url, lang, source FROM rss_feeds WHERE is_active = TRUE ORDER BY category")
            feeds = cursor.fetchall()
            
            # Группируем по категориям как в исходном CATEGORIES
            categories = {}
            for feed in feeds:
                category = feed['category']
                if category not in categories:
                    categories[category] = []
                
                categories[category].append({
                    'url': feed['url'],
                    'lang': feed['lang'],
                    'source': feed['source']
                })
            
            return categories
            
        except Error as e:
            print(f"Ошибка при получении данных: {e}")
            return {}
        finally:
            cursor.close()
    
    def get_feeds_by_category(self, category):
        """Получить RSS-ленты по категории"""
        connection = self.get_db_connection()
        if connection is None:
            return []
        
        cursor = connection.cursor(dictionary=True)
        try:
            query = "SELECT * FROM rss_feeds WHERE category = %s AND is_active = TRUE"
            cursor.execute(query, (category,))
            return cursor.fetchall()
        except Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            cursor.close()
    
    def get_feeds_by_lang(self, lang):
        """Получить RSS-ленты по языку"""
        connection = self.get_db_connection()
        if connection is None:
            return []
        
        cursor = connection.cursor(dictionary=True)
        try:
            query = "SELECT * FROM rss_feeds WHERE lang = %s AND is_active = TRUE"
            cursor.execute(query, (lang,))
            return cursor.fetchall()
        except Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            cursor.close()
    
    def get_feeds_by_source(self, source):
        """Получить RSS-ленты по источнику"""
        connection = self.get_db_connection()
        if connection is None:
            return []
        
        cursor = connection.cursor(dictionary=True)
        try:
            query = "SELECT * FROM rss_feeds WHERE source = %s AND is_active = TRUE"
            cursor.execute(query, (source,))
            return cursor.fetchall()
        except Error as e:
            print(f"Ошибка при получении данных: {e}")
            return []
        finally:
            cursor.close()
    
    def add_feed(self, category, url, lang, source):
        """Добавить новую RSS-ленту"""
        connection = self.get_db_connection()
        if connection is None:
            return False
        
        cursor = connection.cursor()
        try:
            query = """
            INSERT INTO rss_feeds (category, url, lang, source)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(query, (category, url, lang, source))
            connection.commit()
            return True
        except Error as e:
            print(f"Ошибка при добавлении данных: {e}")
            connection.rollback()
            return False
        finally:
            cursor.close()
    
    def update_feed(self, feed_id, category=None, url=None, lang=None, source=None, is_active=None):
        """Обновить RSS-ленту"""
        connection = self.get_db_connection()
        if connection is None:
            return False
        
        cursor = connection.cursor()
        try:
            updates = []
            values = []
            
            if category is not None:
                updates.append("category = %s")
                values.append(category)
            if url is not None:
                updates.append("url = %s")
                values.append(url)
            if lang is not None:
                updates.append("lang = %s")
                values.append(lang)
            if source is not None:
                updates.append("source = %s")
                values.append(source)
            if is_active is not None:
                updates.append("is_active = %s")
                values.append(is_active)
            
            if not updates:
                return False
                
            values.append(feed_id)
            query = f"UPDATE rss_feeds SET {', '.join(updates)} WHERE id = %s"
            cursor.execute(query, values)
            connection.commit()
            return True
        except Error as e:
            print(f"Ошибка при обновлении данных: {e}")
            connection.rollback()
            return False
        finally:
            cursor.close()
    
    def delete_feed(self, feed_id):
        """Удалить RSS-ленту"""
        connection = self.get_db_connection()
        if connection is None:
            return False
        
        cursor = connection.cursor()
        try:
            query = "DELETE FROM rss_feeds WHERE id = %s"
            cursor.execute(query, (feed_id,))
            connection.commit()
            return True
        except Error as e:
            print(f"Ошибка при удалении данных: {e}")
            connection.rollback()
            return False
        finally:
            cursor.close()
    
    def get_categories(self):
        """Получить список всех категорий"""
        connection = self.get_db_connection()
        if connection is None:
            print("Нет подключения к БД для get_categories")
            return []
        
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT DISTINCT category FROM rss_feeds WHERE is_active = TRUE")
            return [row[0] for row in cursor.fetchall()]
        except Error as e:
            print(f"Ошибка при получении категорий: {e}")
            return []
        finally:
            cursor.close()

    def is_news_new(self, title, content, url, publish_date, check_period_hours=24):
        """
        Проверяет уникальность новости по хешам и временному периоду
        """
        # Генерируем хеши
        title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(content[:500].encode('utf-8')).hexdigest()
        
        connection = self.get_db_connection()
        if connection is None:
            return True
        
        try:
            cursor = connection.cursor()
            
            # Проверяем по хешам за указанный период
            query = """
            SELECT COUNT(*) FROM published_news 
            WHERE title_hash = %s AND content_hash = %s
            AND published_at >= DATE_SUB(NOW(), INTERVAL %s HOUR)
            """
            cursor.execute(query, (title_hash, content_hash, check_period_hours))
            count = cursor.fetchone()[0]
            
            return count == 0
            
        except Exception as e:
            print(f"Ошибка проверки уникальности: {e}")
            return True
        finally:
            cursor.close()

    def mark_as_published(self, title: str, content: str, url: str, original_language: str, translations_dict: dict, category: str = None):
        """
        Сохраняет информацию о опубликованной новости с проверкой уникальности (хэши).
        Сохраняет оригинальные данные и переводы новости для API.

        :param title: Оригинальный заголовок новости.
        :param content: Оригинальное содержимое новости.
        :param url: URL источника новости.
        :param original_language: Язык оригинальной новости (например, 'en').
        :param translations_dict: Словарь переводов, полученный из prepare_translations.
                                Формат: {'ru': {'title': '...', 'description': '...', 'category': '...'}, ...}
        :param category: Категория новости (оригинальная, например, на английском).
        :return: True, если успешно, иначе False.
        """
        import hashlib # Убедитесь, что импортировано

        title_hash = hashlib.sha256(title.encode('utf-8')).hexdigest()
        content_hash = hashlib.sha256(content[:500].encode('utf-8')).hexdigest() # Используем оригинальный content
        
        # Генерируем ID на основе хешей
        news_id = f"{title_hash}_{content_hash}"
        
        connection = self.get_db_connection()
        if connection is None:
            return False
        
        try:
            cursor = connection.cursor()
            
            # 1. Вставка или обновление в таблице хэшей (существующая логика)
            query_published_news = """
            INSERT INTO published_news (id, title_hash, content_hash, source_url, published_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON DUPLICATE KEY UPDATE 
                source_url = VALUES(source_url),
                published_at = NOW()
            """
            cursor.execute(query_published_news, (news_id, title_hash, content_hash, url))
            
            # 2. Вставка или обновление оригинальных данных новости
            query_published_news_data = """
            INSERT INTO published_news_data 
            (news_id, original_title, original_content, original_language, category, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
            ON DUPLICATE KEY UPDATE
                original_title = VALUES(original_title),
                original_content = VALUES(original_content),
                original_language = VALUES(original_language),
                category = VALUES(category),
                updated_at = NOW()
            """
            cursor.execute(query_published_news_data, (
                news_id, 
                title, 
                content, 
                original_language, 
                category
            ))

            # 3. Вставка или обновление переводов
            # Убедимся, что перевод на оригинальный язык тоже сохранен (если не передан)
            # (Логика уже внутри prepare_translations, но на всякий случай проверим)
            # if original_language not in translations_dict:
            #      translations_dict[original_language] = {'title': title, 'description': content, 'category': category}

            for lang_code, trans_data in translations_dict.items():
                # Проверка на поддерживаемые языки и наличие данных
                if lang_code in ['ru', 'en', 'de', 'fr'] and isinstance(trans_data, dict):
                    trans_title = trans_data.get('title', title) # fallback на оригинал
                    trans_content = trans_data.get('description', content) # fallback на оригинал
                    # trans_category = trans_data.get('category', category) # Если нужно сохранять и её
                    
                    query_translation = """
                    INSERT INTO news_translations (news_id, language, translated_title, translated_content, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, NOW(), NOW())
                    ON DUPLICATE KEY UPDATE
                        translated_title = VALUES(translated_title),
                        translated_content = VALUES(translated_content),
                        updated_at = NOW()
                    """
                    cursor.execute(query_translation, (news_id, lang_code, trans_title, trans_content))
            
            connection.commit()
            print(f"[DB] Новость и переводы сохранены: {news_id}")
            return True
            
        except Exception as e:
            print(f"[ERROR] Ошибка сохранения публикации и данных для API: {e}")
            connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
    
    async def fetch_news(self):
        """Асинхронная функция для получения новостей из RSS-лент"""
        seen_keys = set()
        all_news = []
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}

        try:
            for category, sources in self.get_all_active_feeds().items():
                for source in sources:
                    try:
                        feed = feedparser.parse(source['url'], request_headers=headers)
                        
                        # Логируем ошибки парсинга
                        if getattr(feed, 'bozo', 0):
                            exc = getattr(feed, 'bozo_exception', None)
                            if exc:
                                error_type = type(exc).__name__
                                print(f"RSS error ({error_type}) in {source['url']}: {str(exc)[:200]}")
                    except Exception as e:
                        print(f"Network error for {source['url']}: {str(e)}")
                        continue
                    
                    # Пропускаем источник, если нет записей
                    if not feed.entries:
                        continue
                        
                    for entry in feed.entries[:MAX_ENTRIES_PER_FEED]:
                        # Защита от отсутствия title
                        title = getattr(entry, 'title', 'Untitled').strip()
                        description = entry.get('description', '')
                        
                        # Пропускаем новости с идентичными заголовком и описанием
                        if title == description:
                            continue
                            
                        normalized_title = re.sub(r'\s+', ' ', title).lower()
                        unique_key = (source['source'], normalized_title)
                        
                        # Пропускаем уже обработанные в текущей сессии
                        if unique_key in seen_keys:
                            continue
                        seen_keys.add(unique_key)
                        
                        # Проверяем уникальность через БД
                        if not self.is_news_new(title, description, entry.get('link', '#'), None, 24):
                            continue
                        
                        # Обработка даты с fallback
                        pub_date = getattr(entry, 'published', None)
                        if pub_date:
                            try:
                                published = parser.parse(pub_date).replace(tzinfo=pytz.utc)
                            except:
                                published = datetime.now(pytz.utc)
                        else:
                            published = datetime.now(pytz.utc)
                        
                        news_item = {
                            'id': f"{entry.get('link', '')}_{pub_date}",
                            'title': title,
                            'description': description,
                            'link': entry.get('link', '#'),
                            'published': published,
                            'category': category,
                            'lang': source['lang'],
                            'source': source['source']
                        }
                        
                        all_news.append(news_item)
        
        except Exception as e:
            print(f"❌ Ошибка в fetch_news: {e}")
        
        sorted_news = sorted(all_news, key=lambda x: x['published'], reverse=True)
        return sorted_news[:MAX_TOTAL_NEWS]
    
    def close_connection(self):
        """Закрыть соединение с базой данных"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.connection = None
            print("🔌 Соединение с БД закрыто")
