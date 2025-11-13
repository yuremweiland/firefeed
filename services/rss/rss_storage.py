# services/rss/rss_storage.py
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta
from interfaces import IRSSStorage, IDatabasePool

logger = logging.getLogger(__name__)


class RSSStorage(IRSSStorage):
    """Service for RSS data storage operations"""

    def __init__(self, db_pool: IDatabasePool):
        self.db_pool = db_pool

    async def save_rss_item(self, rss_item: Dict[str, Any], feed_id: int) -> Optional[str]:
        """Save RSS item to database"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    news_id = rss_item["id"]
                    short_id = news_id[:20]

                    title = rss_item["title"][:255]
                    content = rss_item["content"]
                    original_language = rss_item["lang"]
                    image_filename = rss_item.get("image_filename")
                    category_name = rss_item["category"]
                    source_name = rss_item["source"]
                    source_url = rss_item["link"]

                    # Get category_id
                    await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                    cat_result = await cur.fetchone()
                    if not cat_result:
                        logger.warning(f"[STORAGE] Category '{category_name}' not found")
                        return None
                    category_id = cat_result[0]

                    # Insert RSS item
                    query = """
                    INSERT INTO published_news_data
                    (news_id, original_title, original_content, original_language, category_id,
                     image_filename, rss_feed_id, source_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (news_id) DO UPDATE SET
                    original_title = EXCLUDED.original_title,
                    original_content = EXCLUDED.original_content,
                    original_language = EXCLUDED.original_language,
                    category_id = EXCLUDED.category_id,
                    image_filename = EXCLUDED.image_filename,
                    rss_feed_id = EXCLUDED.rss_feed_id,
                    source_url = EXCLUDED.source_url,
                    updated_at = NOW()
                    """
                    await cur.execute(query, (
                        news_id, title, content, original_language, category_id,
                        image_filename, feed_id, source_url
                    ))

                    logger.info(f"[STORAGE] RSS item saved: {short_id}")
                    return news_id

        except Exception as e:
            logger.error(f"[STORAGE] Error saving RSS item: {e}")
            return None

    async def save_translations(self, news_id: str, translations: Dict[str, Dict[str, str]]) -> bool:
        """Save translations for RSS item"""
        short_news_id = news_id[:20]
        logger.info(f"[STORAGE] Saving translations for {short_news_id}")

        if not translations:
            logger.debug(f"[STORAGE] No translations to save for {short_news_id}")
            return True

        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Get original language and content
                    await cur.execute(
                        "SELECT original_language, original_title, original_content FROM published_news_data WHERE news_id = %s",
                        (news_id,)
                    )
                    row = await cur.fetchone()
                    original_language = row[0] if row else "en"
                    original_title = row[1] if row else ""
                    original_content = row[2] if row else ""

                    translation_count = 0
                    for lang, data in translations.items():
                        translation_count += 1
                        if not isinstance(data, dict):
                            logger.error(f"[STORAGE] Invalid translation data for '{lang}'")
                            continue

                        title = data.get("title", "")
                        content = data.get("content", "")

                        # Skip original language and empty translations
                        if lang == original_language or (not title and not content):
                            continue

                        # Skip if translation identical to original
                        if title == original_title and content == original_content:
                            continue

                        insert_query = """
                        INSERT INTO news_translations (news_id, language, translated_title, translated_content, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, NOW(), NOW())
                        ON CONFLICT (news_id, language)
                        DO UPDATE SET
                            translated_title = EXCLUDED.translated_title,
                            translated_content = EXCLUDED.translated_content,
                            updated_at = NOW()
                        """
                        await cur.execute(insert_query, (news_id, lang, title, content))
                        logger.info(f"[STORAGE] Translation saved for {short_news_id} -> {lang}")

                    logger.info(f"[STORAGE] Saved {translation_count} translations for {short_news_id}")
                    return True

        except Exception as e:
            logger.error(f"[STORAGE] Error saving translations for {short_news_id}: {e}")
            return False

    async def get_feed_cooldown(self, feed_id: int) -> int:
        """Get cooldown minutes for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT COALESCE(cooldown_minutes, 60) FROM rss_feeds WHERE id = %s", (feed_id,)
                    )
                    row = await cur.fetchone()
                    return row[0] if row else 60
        except Exception as e:
            logger.error(f"[STORAGE] Error getting cooldown for feed {feed_id}: {e}")
            return 60

    async def get_feed_max_news_per_hour(self, feed_id: int) -> int:
        """Get max news per hour for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT COALESCE(max_news_per_hour, 10) FROM rss_feeds WHERE id = %s", (feed_id,)
                    )
                    row = await cur.fetchone()
                    return row[0] if row else 10
        except Exception as e:
            logger.error(f"[STORAGE] Error getting max_news_per_hour for feed {feed_id}: {e}")
            return 10

    async def get_last_published_time(self, feed_id: int) -> Optional[datetime]:
        """Get last published time for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT created_at
                    FROM published_news_data
                    WHERE rss_feed_id = %s
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                    await cur.execute(query, (feed_id,))
                    row = await cur.fetchone()
                    return row[0] if row else None
        except Exception as e:
            logger.error(f"[STORAGE] Error getting last published time for feed {feed_id}: {e}")
            return None

    async def get_recent_items_count(self, feed_id: int, minutes: int) -> int:
        """Get count of recent items for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    time_threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
                    query = """
                    SELECT COUNT(*) FROM published_news_data
                    WHERE rss_feed_id = %s AND created_at >= %s
                    """
                    await cur.execute(query, (feed_id, time_threshold))
                    row = await cur.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"[STORAGE] Error getting recent items count for feed {feed_id}: {e}")
            return 0