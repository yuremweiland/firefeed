# utils/media_extractors.py
import logging

logger = logging.getLogger(__name__)


def _extract_media_from_rss_item(item, media_type, size_limit=None):
    """Извлекает URL медиа из RSS item."""
    try:
        # 1. media:thumbnail (Atom) - только для изображений
        if media_type == "image":
            media_thumbnail = item.get("media_thumbnail", [])
            if media_thumbnail and isinstance(media_thumbnail, list) and len(media_thumbnail) > 0:
                thumbnail = media_thumbnail[0]
                if isinstance(thumbnail, dict):
                    url = thumbnail.get("url")
                    if url:
                        logger.debug(f"[INFO] Найдено изображение в media:thumbnail: {url}")
                        return url

        # 2. enclosure с соответствующим типом
        enclosures = item.get("enclosures", [])
        if enclosures:
            for enclosure in enclosures:
                if isinstance(enclosure, dict):
                    content_type = enclosure.get("type", "")
                    if content_type.startswith(f"{media_type}/"):
                        url = enclosure.get("href") or enclosure.get("url")
                        if url:
                            # Для видео проверяем размер файла
                            if media_type == "video" and size_limit is not None:
                                file_size = enclosure.get("length") or enclosure.get("filesize")
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > size_limit:
                                            logger.debug(
                                                f"[INFO] {media_type.capitalize()} превышает лимит размера ({file_size} > {size_limit}): {url}"
                                            )
                                            continue  # Пропускаем это enclosure
                                    except (ValueError, TypeError):
                                        pass  # Не удалось преобразовать размер
                            logger.debug(f"[INFO] Найдено {media_type} в enclosure: {url}")
                            return url

        # 3. media:content с соответствующим типом (Atom)
        media_content = item.get("media_content", [])
        if media_content:
            if isinstance(media_content, list):
                for content in media_content:
                    if isinstance(content, dict) and content.get("medium") == media_type:
                        url = content.get("url")
                        if url:
                            # Для видео проверяем размер файла
                            if media_type == "video" and size_limit is not None:
                                file_size = content.get("fileSize") or content.get("filesize")
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > size_limit:
                                            logger.debug(
                                                f"[INFO] {media_type.capitalize()} превышает лимит размера ({file_size} > {size_limit}): {url}"
                                            )
                                            continue
                                    except (ValueError, TypeError):
                                        pass  # Не удалось преобразовать размер
                            logger.debug(f"[INFO] Найдено {media_type} в media:content (list): {url}")
                            return url
            elif isinstance(media_content, dict) and media_content.get("medium") == media_type:
                url = media_content.get("url")
                if url:
                    # Для видео проверяем размер файла
                    if media_type == "video" and size_limit is not None:
                        file_size = media_content.get("fileSize") or media_content.get("filesize")
                        if file_size is not None:
                            try:
                                file_size = int(file_size)
                                if file_size > size_limit:
                                    logger.debug(
                                        f"[INFO] {media_type.capitalize()} превышает лимит размера ({file_size} > {size_limit}): {url}"
                                    )
                                    return None  # Пропускаем это видео
                            except (ValueError, TypeError):
                                pass  # Не удалось преобразовать размер
                    logger.debug(f"[INFO] Найдено {media_type} в media:content (dict): {url}")
                    return url

        # 4. og:image из links (если доступно) - только для изображений
        # (Это менее надежно, так как требует парсинга HTML, который feedparser может не предоставить полностью)
    except Exception as e:
        logger.warning(f"[WARN] Ошибка при извлечении {media_type} из RSS item: {e}")
    logger.debug(f"[INFO] {media_type.capitalize()} не найдено в RSS item.")
    return None


def extract_image_from_rss_item(item):
    """Извлекает URL изображения из RSS item."""
    return _extract_media_from_rss_item(item, "image")


def extract_video_from_rss_item(item):
    """Извлекает URL видео из RSS item."""
    return _extract_media_from_rss_item(item, "video", 50 * 1024 * 1024)  # 50 МБ