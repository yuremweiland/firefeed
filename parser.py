import feedparser
import asyncio
import re
import pytz
from datetime import datetime
from config import CATEGORIES

async def fetch_news():
    all_news = []
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"}
    
    for category, sources in CATEGORIES.items():
        for source in sources:
            feed = feedparser.parse(source['url'], request_headers=headers)
            for entry in feed.entries:
                # Генерируем уникальный ID на основе ссылки и даты
                news_id = f"{entry.link}_{entry.published_parsed}"
                
                news_item = {
                    'id': news_id,
                    'title': entry.title,
                    'description': entry.description,
                    'link': entry.link,
                    'published': datetime(*entry.published_parsed[:6], tzinfo=pytz.utc),
                    'category': category,
                    'lang': source['lang'],
                    'source': source['source']
                }
                all_news.append(news_item)
    
    return sorted(all_news, key=lambda x: x['published'], reverse=True)