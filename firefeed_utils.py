import re
import html
import requests
import asyncio
import os
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from functools import partial


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

async def download_and_save_image(url, news_id, save_directory="/var/www/firefeed/data/www/firefeed.net/data/images"):
    """
    Скачивает изображение и сохраняет его локально с именем файла на основе news_id.
    
    :param url: URL изображения
    :param news_id: уникальный ID новости для БД
    :param save_directory: директория для сохранения изображений
    :return: путь к сохраненному файлу или None
    """
    if not url or not news_id:
        print(f"[DEBUG] Пропущено сохранение изображения: нет URL ({url}) или news_id ({news_id})")
        return None
        
    try:
        print(f"[DEBUG] Начинаем сохранять изображение из {url} в {save_directory}")
        os.makedirs(save_directory, exist_ok=True)
        
        loop = asyncio.get_event_loop()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

        # Используем partial для передачи kwargs
        response = await loop.run_in_executor(
            None,
            partial(requests.get, url, headers=headers, timeout=30)
        )

        response.raise_for_status()

        content_type = response.headers.get('content-type', '').lower()
        extension = '.jpg'

        if 'jpeg' in content_type:
            extension = '.jpg'
        elif 'png' in content_type:
            extension = '.png'
        elif 'gif' in content_type:
            extension = '.gif'
        elif 'webp' in content_type:
            extension = '.webp'
        else:
            parsed_url = urlparse(url)
            path = parsed_url.path
            if path.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.webp')):
                extension = os.path.splitext(path)[1].lower()

        safe_news_id = "".join(c for c in str(news_id) if c.isalnum() or c in ('-', '_')).rstrip()
        if not safe_news_id:
            safe_news_id = hashlib.md5(url.encode()).hexdigest()

        filename = f"{safe_news_id}{extension}"
        file_path = os.path.join(save_directory, filename)

        with open(file_path, 'wb') as f:
            f.write(response.content)

        print(f"[LOG] Изображение успешно сохранено: {file_path}")
        return file_path

    except requests.exceptions.RequestException as e:
        print(f"[WARN] Ошибка сети при скачивании изображения {url}: {e}")
        return None
    except OSError as e:
        print(f"[WARN] Ошибка файловой системы при сохранении изображения {url} в {save_directory}: {e}")
        return None
    except Exception as e:
        print(f"[WARN] Неожиданная ошибка при скачивании/сохранении изображения {url}: {e}")
        return None

async def extract_image_from_preview(url):
    """
    Извлекает URL изображения из web preview страницы.
    
    :param url: URL страницы для парсинга
    :return: URL изображения или None
    """
    if not url:
        return None
        
    try:
        # Выполняем синхронный запрос в отдельном потоке с правильными заголовками
        loop = asyncio.get_event_loop()
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        
        response = await loop.run_in_executor(
            None, 
            lambda: requests.get(url, headers=headers, timeout=10)
        )
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Ищем og:image
        og_image = soup.find('meta', property='og:image')
        if og_image and og_image.get('content'):
            return og_image['content']
        
        # Ищем twitter:image
        twitter_image = soup.find('meta', property='twitter:image')
        if twitter_image and twitter_image.get('content'):
            return twitter_image['content']
        
        # Ищем первый img с src, содержащим "image" или "photo"
        image_tags = soup.find_all('img')
        for img in image_tags:
            src = img.get('src') or img.get('data-src')
            if src and ('image' in src.lower() or 'photo' in src.lower()):
                # Конвертируем относительные URL в абсолютные
                if src.startswith('//'):
                    return 'https:' + src
                elif src.startswith('/'):
                    return urljoin(url, src)
                return src
        
        return None
    except Exception as e:
        print(f"[WARN] Ошибка при извлечении изображения из {url}: {e}")
        return None