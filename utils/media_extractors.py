# utils/media_extractors.py
import logging
import asyncio
from utils.image import ImageProcessor

logger = logging.getLogger(__name__)


async def _extract_media_from_rss_item(item, media_type, size_limit=None):
    """Extracts media URL from RSS item."""
    try:
        # 1. media:thumbnail (Atom) - images only
        if media_type == "image":
            media_thumbnail = item.get("media_thumbnail", [])
            if media_thumbnail and isinstance(media_thumbnail, list) and len(media_thumbnail) > 0:
                thumbnail = media_thumbnail[0]
                if isinstance(thumbnail, dict):
                    url = thumbnail.get("url")
                    if url:
                        logger.debug(f"[INFO] Found image in media:thumbnail: {url}")
                        return url

        # 2. enclosure with corresponding type
        enclosures = item.get("enclosures", [])
        if enclosures:
            for enclosure in enclosures:
                if isinstance(enclosure, dict):
                    content_type = enclosure.get("type", "")
                    if content_type.startswith(f"{media_type}/"):
                        url = enclosure.get("href") or enclosure.get("url")
                        if url:
                            # For video, check file size
                            if media_type == "video" and size_limit is not None:
                                file_size = enclosure.get("length") or enclosure.get("filesize")
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > size_limit:
                                            logger.debug(
                                                f"[INFO] {media_type.capitalize()} exceeds size limit ({file_size} > {size_limit}): {url}"
                                            )
                                            continue  # Skip this enclosure
                                    except (ValueError, TypeError):
                                        pass  # Failed to convert size
                            logger.debug(f"[INFO] Found {media_type} in enclosure: {url}")
                            return url

        # 3. media:content with corresponding type (Atom)
        media_content = item.get("media_content", [])
        if media_content:
            if isinstance(media_content, list):
                for content in media_content:
                    if isinstance(content, dict) and content.get("medium") == media_type:
                        url = content.get("url")
                        if url:
                            # For video, check file size
                            if media_type == "video" and size_limit is not None:
                                file_size = content.get("fileSize") or content.get("filesize")
                                if file_size is not None:
                                    try:
                                        file_size = int(file_size)
                                        if file_size > size_limit:
                                            logger.debug(
                                                f"[INFO] {media_type.capitalize()} exceeds size limit ({file_size} > {size_limit}): {url}"
                                            )
                                            continue
                                    except (ValueError, TypeError):
                                        pass  # Failed to convert size
                            logger.debug(f"[INFO] Found {media_type} in media:content (list): {url}")
                            return url
            elif isinstance(media_content, dict) and media_content.get("medium") == media_type:
                url = media_content.get("url")
                if url:
                    # For video, check file size
                    if media_type == "video" and size_limit is not None:
                        file_size = media_content.get("fileSize") or media_content.get("filesize")
                        if file_size is not None:
                            try:
                                file_size = int(file_size)
                                if file_size > size_limit:
                                    logger.debug(
                                        f"[INFO] {media_type.capitalize()} exceeds size limit ({file_size} > {size_limit}): {url}"
                                    )
                                    return None  # Skip this video
                            except (ValueError, TypeError):
                                pass  # Failed to convert size
                    logger.debug(f"[INFO] Found {media_type} in media:content (dict): {url}")
                    return url

        # 4. og:image from links (if available) - images only
        # (This is less reliable as it requires HTML parsing, which feedparser may not provide completely)
    except Exception as e:
        logger.warning(f"[WARN] Error extracting {media_type} from RSS item: {e}")
    logger.debug(f"[INFO] {media_type.capitalize()} not found in RSS item.")
    return None


async def extract_image_from_rss_item(item):
    """Extracts image URL from RSS item with extended format support."""
    image_url = None
    try:
        # 1. enclosure with image/* type
        enclosures = item.get("enclosures", [])
        for enclosure in enclosures:
            if isinstance(enclosure, dict):
                content_type = enclosure.get("type", "")
                if content_type.startswith("image/"):
                    url = enclosure.get("href") or enclosure.get("url")
                    if url:
                        logger.debug(f"[INFO] Found image in enclosure by type: {url}")
                        return url

        # 2. enclosure with file extension (even without MIME type)
        for enclosure in enclosures:
            if isinstance(enclosure, dict):
                url = enclosure.get("href") or enclosure.get("url")
                if url and _has_image_extension(url):
                    logger.debug(f"[INFO] Found image in enclosure by extension: {url}")
                    return url

        # 3. media:content with image type
        media_content = item.get("media_content", [])
        if media_content:
            if isinstance(media_content, list):
                for content in media_content:
                    if isinstance(content, dict) and content.get("medium") == "image":
                        url = content.get("url")
                        if url:
                            logger.debug(f"[INFO] Found image in media:content (list): {url}")
                            return url
            elif isinstance(media_content, dict) and media_content.get("medium") == "image":
                url = media_content.get("url")
                if url:
                    logger.debug(f"[INFO] Found image in media:content (dict): {url}")
                    return url

        # 4. media:thumbnail
        media_thumbnail = item.get("media_thumbnail", [])
        if media_thumbnail and isinstance(media_thumbnail, list) and len(media_thumbnail) > 0:
            thumbnail = media_thumbnail[0]
            if isinstance(thumbnail, dict):
                url = thumbnail.get("url")
                if url:
                    logger.debug(f"[INFO] Found image in media:thumbnail: {url}")
                    return url

        # 5. rbc_news:image (RBC specific format)
        rbc_image_data = item.get('rbc_news_image') or item.get('rbc_news:image')
        if rbc_image_data:
            if isinstance(rbc_image_data, dict):
                image_url = rbc_image_data.get('rbc_news_url') or rbc_image_data.get('url')
                if image_url:
                    logger.debug(f"[INFO] Found image in rbc_news:image (dict): {image_url}")
                    return image_url.strip()
            elif isinstance(rbc_image_data, list) and len(rbc_image_data) > 0:
                first_image = rbc_image_data[0]
                if isinstance(first_image, dict):
                    image_url = first_image.get('rbc_news_url') or first_image.get('url')
                    if image_url:
                        logger.debug(f"[INFO] Found image in rbc_news:image (list): {image_url}")
                        return image_url.strip()

        # 6. media:content with image/* type in type attribute
        if media_content:
            if isinstance(media_content, list):
                for content in media_content:
                    if isinstance(content, dict):
                        content_type = content.get("type", "")
                        if content_type.startswith("image/"):
                            url = content.get("url")
                            if url:
                                logger.debug(f"[INFO] Found image in media:content by type: {url}")
                                return url
            elif isinstance(media_content, dict):
                content_type = media_content.get("type", "")
                if content_type.startswith("image/"):
                    url = media_content.get("url")
                    if url:
                        logger.debug(f"[INFO] Found image in media:content by type: {url}")
                        return url

    except Exception as e:
        logger.warning(f"[WARN] Error extracting image from RSS item: {e}")

    # 7. Fallback: extract from web preview if no image found in RSS
    if not image_url:
        news_link = item.get('link')
        if news_link:
            try:
                image_url = await ImageProcessor.extract_image_from_preview(news_link)
                if image_url:
                    logger.debug(f"[INFO] Found image in web preview: {image_url}")
            except Exception as e:
                logger.warning(f"[WARN] Error extracting image from web preview for {news_link}: {e}")

    logger.debug("[INFO] Image not found in RSS item.")
    return image_url


def _has_image_extension(url):
    """Checks if URL has an image extension."""
    import re
    return bool(re.search(r'\.(jpe?g|png|gif|webp|bmp|tiff?|svg)(\?.*)?$', url, re.IGNORECASE))


async def extract_video_from_rss_item(item):
    """Extracts video URL from RSS item with extended format support."""
    try:
        # 1. enclosure with video/* type and size check
        enclosures = item.get("enclosures", [])
        for enclosure in enclosures:
            if isinstance(enclosure, dict):
                content_type = enclosure.get("type", "")
                if content_type.startswith("video/"):
                    url = enclosure.get("href") or enclosure.get("url")
                    if url:
                        # Check file size
                        file_size = enclosure.get("length") or enclosure.get("filesize")
                        if file_size is not None:
                            try:
                                file_size = int(file_size)
                                if file_size > 50 * 1024 * 1024:  # 50 MB limit
                                    logger.debug(f"[INFO] Video exceeds size limit (file_size > 50MB): {url}")
                                    continue
                            except (ValueError, TypeError):
                                pass  # Failed to convert size
                        logger.debug(f"[INFO] Found video in enclosure: {url}")
                        return url

        # 2. enclosure with video file extension
        for enclosure in enclosures:
            if isinstance(enclosure, dict):
                url = enclosure.get("href") or enclosure.get("url")
                if url and _has_video_extension(url):
                    # Check file size
                    file_size = enclosure.get("length") or enclosure.get("filesize")
                    if file_size is not None:
                        try:
                            file_size = int(file_size)
                            if file_size > 50 * 1024 * 1024:  # 50 MB limit
                                logger.debug(f"[INFO] Video exceeds size limit (file_size > 50MB): {url}")
                                continue
                        except (ValueError, TypeError):
                            pass  # Failed to convert size
                    logger.debug(f"[INFO] Found video in enclosure by extension: {url}")
                    return url

        # Use common function for remaining checks
        return await _extract_media_from_rss_item(item, "video", 50 * 1024 * 1024)

    except Exception as e:
        logger.warning(f"[WARN] Error extracting video from RSS item: {e}")

    logger.debug("[INFO] Video not found in RSS item.")
    return None


def _has_video_extension(url):
    """Checks if URL has a video file extension."""
    import re
    return bool(re.search(r'\.(mp4|avi|mkv|mov|wmv|flv|webm|m4v)(\?.*)?$', url, re.IGNORECASE))