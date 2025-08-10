import asyncio
import re
import html
from telegram import Bot
from telegram.error import TelegramError
from config import BOT_TOKEN, CHANNEL_ID
from parser import fetch_news
from database import init_db, is_news_new, mark_as_published

def clean_html(raw_html):
    """Удаляет все HTML-теги и преобразует HTML-сущности"""
    if not raw_html:
        return ""
    
    # Удаляем все теги
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    
    # Заменяем HTML-сущности (например, &amp; → &)
    clean_text = html.unescape(clean_text)
    
    # Удаляем лишние пробелы
    return re.sub(r'\s+', ' ', clean_text).strip()

async def post_to_channel(bot, news_item):
    try:
        # Очищаем описание от HTML
        clean_description = clean_html(news_item['description'])

        if len(clean_description) > 3000:
            clean_description = clean_description[:3000] + "..."
        
        # Форматируем сообщение
        message = (
            f"🔥 <b>{html.escape(news_item['title'])}</b>\n\n"
            f"{clean_description}\n\n"
            f"⚡ <a href='{news_item['link']}'>Читать полностью</a>"
        )
        
        # Отправляем с HTML-разметкой
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=False
        )
        mark_as_published(news_item['id'])
        print(f"✅ Опубликовано: {news_item['title'][:50]}...")
    except TelegramError as e:
        print(f"❌ Ошибка отправки: {e}")

async def monitor_news():
    bot = Bot(token=BOT_TOKEN)
    init_db()

    # Принудительная отправка последней новости
    # test_news = {
    #     'id': 'test_id',
    #     'title': 'ТЕСТ: FireFeed работает!',
    #     'description': 'Поздравляем! Бот успешно отправляет новости.',
    #     'link': 'https://t.me/firefeed_news'
    # }
    # await post_to_channel(bot, test_news)
    
    print("🟢 Бот запущен и мониторит новости Reuters...")
    while True:
        try:
            news_list = await fetch_news()
            for news in news_list:
                if is_news_new(news['id']):
                    await post_to_channel(bot, news)
                    await asyncio.sleep(1)  # Пауза между отправками
        except Exception as e:
            print(f"⚠️ Критическая ошибка: {e}")
        
        await asyncio.sleep(60)  # Проверка каждую минуту

if __name__ == "__main__":
    asyncio.run(monitor_news())