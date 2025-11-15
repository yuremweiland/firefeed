# services/rss/rss_storage.py
import logging
from typing import Dict, Any, Optional, List
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
                    video_filename = rss_item.get("video_filename")
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
                     image_filename, video_filename, rss_feed_id, source_url, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                    ON CONFLICT (news_id) DO UPDATE SET
                    original_title = EXCLUDED.original_title,
                    original_content = EXCLUDED.original_content,
                    original_language = EXCLUDED.original_language,
                    category_id = EXCLUDED.category_id,
                    image_filename = EXCLUDED.image_filename,
                    video_filename = EXCLUDED.video_filename,
                    rss_feed_id = EXCLUDED.rss_feed_id,
                    source_url = EXCLUDED.source_url,
                    updated_at = NOW()
                    """
                    await cur.execute(query, (
                        news_id, title, content, original_language, category_id,
                        image_filename, video_filename, feed_id, source_url
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

    async def get_feeds_by_category(self, category_name: str) -> List[Dict[str, Any]]:
        """Get feeds by category name"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT
                        rf.id,
                        rf.url,
                        rf.name,
                        rf.language,
                        rf.source_id,
                        rf.category_id,
                        s.name as source_name,
                        c.name as category_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE c.name = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (category_name,))
                    feeds = []
                    async for row in cur:
                        feeds.append({
                            "id": row[0],
                            "url": row[1].strip(),
                            "name": row[2],
                            "lang": row[3],
                            "source_id": row[4],
                            "category_id": row[5],
                            "source": row[6],
                            "category": row[7] if row[7] else "uncategorized"
                        })
                    logger.info(f"Found {len(feeds)} feeds for category '{category_name}'")
                    return feeds
        except Exception as e:
            logger.error(f"[STORAGE] Error getting feeds by category '{category_name}': {e}")
            return []

    async def get_feeds_by_language(self, lang: str) -> List[Dict[str, Any]]:
        """Get feeds by language"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT
                        rf.id,
                        rf.url,
                        rf.name,
                        rf.language,
                        rf.source_id,
                        rf.category_id,
                        s.name as source_name,
                        c.name as category_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE rf.language = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (lang,))
                    feeds = []
                    async for row in cur:
                        feeds.append({
                            "id": row[0],
                            "url": row[1].strip(),
                            "name": row[2],
                            "lang": row[3],
                            "source_id": row[4],
                            "category_id": row[5],
                            "source": row[6],
                            "category": row[7] if row[7] else "uncategorized"
                        })
                    logger.info(f"Found {len(feeds)} feeds for language '{lang}'")
                    return feeds
        except Exception as e:
            logger.error(f"[STORAGE] Error getting feeds by language '{lang}': {e}")
            return []

    async def get_feeds_by_source(self, source_name: str) -> List[Dict[str, Any]]:
        """Get feeds by source name"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT
                        rf.id,
                        rf.url,
                        rf.name,
                        rf.language,
                        rf.source_id,
                        rf.category_id,
                        s.name as source_name,
                        c.name as category_name
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE s.name = %s AND rf.is_active = TRUE
                    """
                    await cur.execute(query, (source_name,))
                    feeds = []
                    async for row in cur:
                        feeds.append({
                            "id": row[0],
                            "url": row[1].strip(),
                            "name": row[2],
                            "lang": row[3],
                            "source_id": row[4],
                            "category_id": row[5],
                            "source": row[6],
                            "category": row[7] if row[7] else "uncategorized"
                        })
                    logger.info(f"Found {len(feeds)} feeds for source '{source_name}'")
                    return feeds
        except Exception as e:
            logger.error(f"[STORAGE] Error getting feeds by source '{source_name}': {e}")
            return []

    async def add_feed(self, url: str, category_name: str, source_name: str, language: str, is_active: bool = True) -> bool:
        """Add new RSS feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Get category_id
                    await cur.execute("SELECT id FROM categories WHERE name = %s", (category_name,))
                    cat_result = await cur.fetchone()
                    if not cat_result:
                        logger.error(f"[STORAGE] Category '{category_name}' not found")
                        return False
                    category_id = cat_result[0]

                    # Get or create source
                    await cur.execute("SELECT id FROM sources WHERE name = %s", (source_name,))
                    source_result = await cur.fetchone()
                    if source_result:
                        source_id = source_result[0]
                    else:
                        # Create new source
                        await cur.execute(
                            "INSERT INTO sources (name, created_at) VALUES (%s, NOW()) RETURNING id",
                            (source_name,)
                        )
                        source_id = (await cur.fetchone())[0]

                    # Insert RSS feed
                    query = """
                    INSERT INTO rss_feeds (url, name, language, source_id, category_id, is_active, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                    """
                    await cur.execute(query, (url, f"{source_name} Feed", language, source_id, category_id, is_active))

                    logger.info(f"[STORAGE] RSS feed added: {url}")
                    return True
        except Exception as e:
            logger.error(f"[STORAGE] Error adding RSS feed '{url}': {e}")
            return False

    async def update_feed(self, feed_id: int, **kwargs) -> bool:
        """Update RSS feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Build update query dynamically
                    if not kwargs:
                        logger.warning(f"[STORAGE] No fields to update for feed {feed_id}")
                        return True

                    set_parts = []
                    values = []
                    for key, value in kwargs.items():
                        set_parts.append(f"{key} = %s")
                        values.append(value)

                    set_clause = ", ".join(set_parts)
                    values.append(feed_id)  # Add feed_id at the end

                    query = f"""
                    UPDATE rss_feeds
                    SET {set_clause}, updated_at = NOW()
                    WHERE id = %s
                    """
                    await cur.execute(query, values)

                    if cur.rowcount > 0:
                        logger.info(f"[STORAGE] RSS feed {feed_id} updated")
                        return True
                    else:
                        logger.warning(f"[STORAGE] RSS feed {feed_id} not found")
                        return False
        except Exception as e:
            logger.error(f"[STORAGE] Error updating RSS feed {feed_id}: {e}")
            return False

    async def delete_feed(self, feed_id: int) -> bool:
        """Delete RSS feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Check if feed exists and has published items
                    await cur.execute("SELECT COUNT(*) FROM published_news_data WHERE rss_feed_id = %s", (feed_id,))
                    count = (await cur.fetchone())[0]

                    if count > 0:
                        logger.warning(f"[STORAGE] Cannot delete feed {feed_id} - has {count} published items")
                        return False

                    # Delete the feed
                    await cur.execute("DELETE FROM rss_feeds WHERE id = %s", (feed_id,))

                    if cur.rowcount > 0:
                        logger.info(f"[STORAGE] RSS feed {feed_id} deleted")
                        return True
                    else:
                        logger.warning(f"[STORAGE] RSS feed {feed_id} not found")
                        return False
        except Exception as e:
            logger.error(f"[STORAGE] Error deleting RSS feed {feed_id}: {e}")
            return False

    async def fetch_unprocessed_rss_items(self) -> List[Dict[str, Any]]:
        """Fetch unprocessed RSS items (not published to Telegram)"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    query = """
                    SELECT
                        p.news_id,
                        p.original_title,
                        p.original_content,
                        p.original_language,
                        p.category_id,
                        p.image_filename,
                        p.video_filename,
                        p.source_url,
                        c.name as category_name,
                        rf.name as feed_name,
                        rf.id as feed_id
                    FROM published_news_data p
                    LEFT JOIN rss_feeds rf ON p.rss_feed_id = rf.id
                    LEFT JOIN categories c ON p.category_id = c.id
                    WHERE p.telegram_published = FALSE
                    ORDER BY p.created_at DESC
                    LIMIT 100
                    """
                    await cur.execute(query)
                    items = []
                    async for row in cur:
                        items.append({
                            "news_id": row[0],
                            "title": row[1],
                            "content": row[2],
                            "language": row[3],
                            "category_id": row[4],
                            "image_filename": row[5],
                            "video_filename": row[6],
                            "source_url": row[7],
                            "category": row[8],
                            "feed_name": row[9],
                            "feed_id": row[10]
                        })
                    logger.info(f"Found {len(items)} unprocessed RSS items")
                    return items
        except Exception as e:
            logger.error(f"[STORAGE] Error fetching unprocessed RSS items: {e}")
            return []

    async def get_last_telegram_publication_time(self, feed_id: int) -> Optional[datetime]:
        """Get last Telegram publication time for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Get latest publication time from both tables
                    query = """
                    SELECT GREATEST(
                        COALESCE((
                            SELECT MAX(rtp.published_at)
                            FROM rss_items_telegram_published rtp
                            JOIN news_translations nt ON rtp.translation_id = nt.id
                            JOIN published_news_data pnd ON nt.news_id = pnd.news_id
                            WHERE pnd.rss_feed_id = %s
                        ), '1970-01-01'::timestamp),
                        COALESCE((
                            SELECT MAX(rtpo.created_at)
                            FROM rss_items_telegram_published_originals rtpo
                            JOIN published_news_data pnd ON rtpo.news_id = pnd.news_id
                            WHERE pnd.rss_feed_id = %s
                        ), '1970-01-01'::timestamp)
                    ) as last_time
                    """
                    await cur.execute(query, (feed_id, feed_id))
                    row = await cur.fetchone()
                    if row and row[0] and row[0] > datetime(1970, 1, 1, tzinfo=timezone.utc):
                        return row[0]
                    return None
        except Exception as e:
            logger.error(f"[STORAGE] Error getting last Telegram publication time for feed {feed_id}: {e}")
            return None

    async def get_recent_telegram_publications_count(self, feed_id: int, minutes: int) -> int:
        """Get count of recent Telegram publications for feed"""
        try:
            async with self.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    time_threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
                    # Count publications from both tables
                    query = """
                    SELECT COUNT(*) FROM (
                        SELECT rtp.published_at
                        FROM rss_items_telegram_published rtp
                        JOIN news_translations nt ON rtp.translation_id = nt.id
                        JOIN published_news_data pnd ON nt.news_id = pnd.news_id
                        WHERE pnd.rss_feed_id = %s AND rtp.published_at >= %s
                        UNION ALL
                        SELECT rtpo.created_at as published_at
                        FROM rss_items_telegram_published_originals rtpo
                        JOIN published_news_data pnd ON rtpo.news_id = pnd.news_id
                        WHERE pnd.rss_feed_id = %s AND rtpo.created_at >= %s
                    ) as combined_publications
                    """
                    await cur.execute(query, (feed_id, time_threshold, feed_id, time_threshold))
                    row = await cur.fetchone()
                    return row[0] if row else 0
        except Exception as e:
            logger.error(f"[STORAGE] Error getting recent Telegram publications count for feed {feed_id}: {e}")
            return 0