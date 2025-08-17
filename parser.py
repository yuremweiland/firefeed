import feedparser
import asyncio
import re
import pytz
from datetime import datetime
from config import CATEGORIES

MAX_ENTRIES_PER_FEED = 10
MAX_TOTAL_NEWS = 100

async def fetch_news():
    seen_keys = set()
    all_news = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}

    for category, sources in CATEGORIES.items():
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
                normalized_title = re.sub(r'\s+', ' ', title).lower()
                unique_key = (source['source'], normalized_title)
                
                if unique_key in seen_keys:
                    continue
                seen_keys.add(unique_key)
                
                # Обработка даты с fallback
                pub_date = getattr(entry, 'published_parsed', None)
                if pub_date:
                    published = datetime(*pub_date[:6], tzinfo=pytz.utc)
                else:
                    published = datetime.now(pytz.utc)  # Используем текущее время
                
                news_item = {
                    'id': f"{entry.get('link', '')}_{pub_date}",
                    'title': title,
                    'description': entry.get('description', ''),
                    'link': entry.get('link', '#'),
                    'published': published,
                    'category': category,
                    'lang': source['lang'],
                    'source': source['source']
                }
                all_news.append(news_item)
    
    sorted_news = sorted(all_news, key=lambda x: x['published'], reverse=True)
    return sorted_news[:MAX_TOTAL_NEWS]