import re
import html
import aiohttp
import os
import hashlib
import logging
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
from config import IMAGES_ROOT_DIR, IMAGE_FILE_EXTENSIONS
from datetime import datetime

logger = logging.getLogger(__name__)

def clean_html(raw_html):
    """Удаляет все HTML-теги и преобразует HTML-сущности"""
    if not raw_html:
        return ""
    
    # Работаем с копией
    clean_text = str(raw_html)
    
    # Заменяем специальные кавычки от NLP моделей
    # Обрабатываем различные варианты: <<, < <, <  < и т.д.
    clean_text = re.sub(r'<\s*<', '«', clean_text)
    clean_text = re.sub(r'>\s*>', '»', clean_text)
    
    # Удаляем HTML-теги
    clean_text = re.sub(r'<[^>]*>', '', clean_text)
    
    # Декодируем HTML-сущности
    try:
        clean_text = html.unescape(clean_text)
    except Exception:
        # Если html.unescape падает, оставляем как есть
        pass
    
    # Нормализуем пробелы
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    return clean_text.strip()

async def download_and_save_image(url, news_id, save_directory=IMAGES_ROOT_DIR):
    """
    Скачивает изображение и сохраняет его локально с именем файла на основе news_id.
    Сохраняет по пути: save_directory/YYYY/MM/DD/{news_id}{ext}

    :param url: URL изображения
    :param news_id: уникальный ID новости для БД
    :param save_directory: директория для сохранения изображений
    :return: путь к сохраненному файлу или None
    """
    if not url or not news_id:
        logger.debug(f"[DEBUG] Пропущено сохранение изображения: нет URL ({url}) или news_id ({news_id})")
        return None

    try:
        # Используем текущее время для формирования пути
        created_at = datetime.now()
        date_path = created_at.strftime("%Y/%m/%d")
        full_save_directory = os.path.join(save_directory, date_path)

        logger.debug(f"[DEBUG] Начинаем сохранять изображение из {url} в {full_save_directory}")
        os.makedirs(full_save_directory, exist_ok=True)

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }

        # Используем aiohttp для асинхронного скачивания
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()

                content_type = response.headers.get('Content-Type', '').lower()
                content_lower = content_type.lower()
                extension = '.jpg'

                # Проверяем content_type
                for ext in IMAGE_FILE_EXTENSIONS:
                    if ext[1:] in content_lower:
                        extension = ext
                        break
                else:
                    # Проверяем URL
                    parsed_url = urlparse(url)
                    path = parsed_url.path
                    if path.lower().endswith(tuple(IMAGE_FILE_EXTENSIONS)):
                        extension = os.path.splitext(path)[1].lower()

                safe_news_id = "".join(c for c in str(news_id) if c.isalnum() or c in ('-', '_')).rstrip()
                if not safe_news_id:
                    safe_news_id = hashlib.md5(url.encode()).hexdigest()

                filename = f"{safe_news_id}{extension}"
                file_path = os.path.join(full_save_directory, filename)

                # Читаем контент асинхронно
                content = await response.read()

                # Сохраняем файл асинхронно
                with open(file_path, 'wb') as f:
                    f.write(content)

        logger.info(f"[LOG] Изображение успешно сохранено: {file_path}")
        return file_path

    except aiohttp.ClientError as e:
        logger.warning(f"[WARN] Ошибка сети при скачивании изображения {url}: {e}")
        return None
    except OSError as e:
        logger.warning(f"[WARN] Ошибка файловой системы при сохранении изображения {url} в {full_save_directory}: {e}")
        return None
    except Exception as e:
        logger.warning(f"[WARN] Неожиданная ошибка при скачивании/сохранении изображения {url}: {e}")
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
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                html_content = await response.text()

        soup = BeautifulSoup(html_content, 'html.parser')

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
        logger.warning(f"[WARN] Ошибка при извлечении изображения из {url}: {e}")
        return None