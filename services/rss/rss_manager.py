# services/rss/rss_manager.py
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from config import DEFAULT_USER_AGENT
from interfaces import (
    IRSSFetcher, IRSSValidator, IRSSStorage, IMediaExtractor,
    ITranslationService, IDuplicateDetector, ITranslatorQueue, IMaintenanceService
)

logger = logging.getLogger(__name__)


class RSSManager:
    """Orchestrator for RSS processing operations using dependency injection"""

    def __init__(self,
                 rss_fetcher: IRSSFetcher,
                 rss_validator: IRSSValidator,
                 rss_storage: IRSSStorage,
                 media_extractor: IMediaExtractor,
                 translation_service: ITranslationService,
                 duplicate_detector: IDuplicateDetector,
                 translator_queue: ITranslatorQueue,
                 maintenance_service: IMaintenanceService,
                 translator_task_queue=None):  # For backward compatibility

        self.rss_fetcher = rss_fetcher
        self.rss_validator = rss_validator
        self.rss_storage = rss_storage
        self.media_extractor = media_extractor
        self.translation_service = translation_service
        self.duplicate_detector = duplicate_detector
        self.translator_queue = translator_queue
        self.maintenance_service = maintenance_service
        self.translator_task_queue = translator_task_queue or translator_queue

    # Legacy methods for backward compatibility - delegate to services
    async def get_pool(self):
        """Get database pool - now handled by DI"""
        # This is now handled by the DI container
        # Return None to indicate this should be injected
        return None

    async def close_pool(self):
        """Close pool - now handled by DI"""
        pass

    async def get_all_active_feeds(self) -> List[Dict[str, Any]]:
        """Get all active feeds"""
        try:
            # Use rss_storage to get database connection
            async with self.rss_storage.db_pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # Explicitly list fields from rss_feeds
                    query = """
                    SELECT
                        rf.id,
                        rf.url,
                        rf.name,
                        rf.language,
                        rf.source_id,
                        rf.category_id,
                        s.name as source_name, -- Get source name
                        c.name as category_name, -- Get category name
                        rf.cooldown_minutes, -- Can add if needed
                        rf.max_news_per_hour  -- Can add if needed
                    FROM rss_feeds rf
                    JOIN categories c ON rf.category_id = c.id
                    JOIN sources s ON rf.source_id = s.id
                    WHERE rf.is_active = TRUE
                    """
                    await cur.execute(query)
                    feeds = []
                    async for row in cur:
                        feeds.append(
                            {
                                "id": row[0],
                                "url": row[1].strip(),
                                "name": row[2],
                                "lang": row[3],
                                "source_id": row[4],
                                "category_id": row[5],
                                "source": row[6],  # s.name
                                "category": row[7] if row[7] else "uncategorized"
                            }
                        )
                    logger.info(f"Found {len(feeds)} active feeds")
                    return feeds
        except Exception as e:
            logger.error(f"[RSS_MANAGER] Error getting active feeds: {e}")
            return []

    async def get_feeds_by_category(self, category_name: str) -> List[Dict[str, Any]]:
        """Get feeds by category"""
        return await self.rss_storage.get_feeds_by_category(category_name)

    async def get_feeds_by_language(self, lang: str) -> List[Dict[str, Any]]:
        """Get feeds by language"""
        return await self.rss_storage.get_feeds_by_language(lang)

    async def get_feeds_by_source(self, source_name: str) -> List[Dict[str, Any]]:
        """Get feeds by source"""
        return await self.rss_storage.get_feeds_by_source(source_name)

    async def add_feed(self, url: str, category_name: str, source_name: str, language: str, is_active: bool = True) -> bool:
        """Add feed"""
        return await self.rss_storage.add_feed(url, category_name, source_name, language, is_active)

    async def update_feed(self, feed_id: int, **kwargs) -> bool:
        """Update feed"""
        return await self.rss_storage.update_feed(feed_id, **kwargs)

    async def delete_feed(self, feed_id: int) -> bool:
        """Delete feed"""
        return await self.rss_storage.delete_feed(feed_id)

    async def validate_rss_feed(self, url: str, headers: Dict[str, str]) -> bool:
        """Validate RSS feed"""
        return await self.rss_validator.validate_feed(url, headers)

    async def fetch_rss_items(self) -> List[List[Dict[str, Any]]]:
        """Fetch RSS items from all active feeds"""
        # Get active feeds (this should come from repository)
        feeds_info = await self.get_all_active_feeds()

        if not feeds_info:
            logger.info("No active feeds found")
            return []

        # Prepare headers (this should be configurable)
        headers = {
            "User-Agent": DEFAULT_USER_AGENT
        }

        # Fetch from all feeds concurrently
        logger.info(f"Starting concurrent fetch from {len(feeds_info)} feeds")
        rss_items_batches = await self.rss_fetcher.fetch_feeds(feeds_info, headers)

        # Filter out exceptions and flatten results
        successful_batches = []
        for i, batch in enumerate(rss_items_batches):
            if isinstance(batch, Exception):
                logger.error(f"Error fetching from feed {feeds_info[i]['name']}: {batch}")
            else:
                successful_batches.append(batch)

        total_items = sum(len(batch) for batch in successful_batches)
        logger.info(f"Successfully fetched {total_items} RSS items from {len(successful_batches)} feeds")

        return successful_batches

    async def save_rss_item_to_db(self, rss_item: Dict[str, Any], rss_feed_id: int) -> Optional[str]:
        """Save RSS item to database"""
        return await self.rss_storage.save_rss_item(rss_item, rss_feed_id)

    async def save_translations_to_db(self, news_id: str, translations: Dict[str, Dict[str, str]]) -> bool:
        """Save translations to database"""
        return await self.rss_storage.save_translations(news_id, translations)

    async def get_feed_cooldown_minutes(self, rss_feed_id: int) -> int:
        """Get feed cooldown"""
        return await self.rss_storage.get_feed_cooldown(rss_feed_id)

    async def get_max_news_per_hour_for_feed(self, rss_feed_id: int) -> int:
        """Get max news per hour for feed"""
        return await self.rss_storage.get_feed_max_news_per_hour(rss_feed_id)

    async def get_last_published_time_for_feed(self, rss_feed_id: int) -> Optional[datetime]:
        """Get last published time for feed"""
        return await self.rss_storage.get_last_published_time(rss_feed_id)

    async def get_recent_rss_items_count_for_feed(self, rss_feed_id: int, minutes: int) -> int:
        """Get recent items count for feed"""
        return await self.rss_storage.get_recent_items_count(rss_feed_id, minutes)

    # Interface compatibility methods
    async def get_feed_cooldown(self, feed_id: int) -> int:
        """Get cooldown minutes for feed (interface compatibility)"""
        return await self.get_feed_cooldown_minutes(feed_id)

    async def get_feed_max_news_per_hour(self, feed_id: int) -> int:
        """Get max news per hour for feed (interface compatibility)"""
        return await self.get_max_news_per_hour_for_feed(feed_id)

    async def get_last_published_time(self, feed_id: int) -> Optional[datetime]:
        """Get last published time for feed (interface compatibility)"""
        return await self.get_last_published_time_for_feed(feed_id)

    async def get_recent_items_count(self, feed_id: int, minutes: int) -> int:
        """Get count of recent items for feed (interface compatibility)"""
        return await self.get_recent_rss_items_count_for_feed(feed_id, minutes)

    def generate_news_id(self, title: str, content: str, link: str, feed_id: int) -> str:
        """Generate news ID - delegate to fetcher"""
        return self.rss_fetcher.generate_news_id(title, content, link, feed_id)

    async def check_for_duplicates(self, title: str, content: str, link: str, lang: str) -> bool:
        """Check for duplicates - delegate to fetcher"""
        return await self.rss_fetcher.check_for_duplicates(title, content, link, lang)

    async def extract_image_from_rss_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract image - delegate to media extractor"""
        return await self.media_extractor.extract_image(item)

    def extract_video_from_rss_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract video - delegate to media extractor"""
        return self.media_extractor.extract_video(item)

    async def fetch_unprocessed_rss_items(self) -> List[Dict[str, Any]]:
        """Fetch unprocessed RSS items"""
        return await self.rss_storage.fetch_unprocessed_rss_items()

    async def cleanup_duplicates(self) -> None:
        """Cleanup duplicates"""
        await self.maintenance_service.cleanup_duplicates()

    # Main processing methods
    async def process_rss_feed(self, feed_info: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Process a single RSS feed end-to-end"""
        feed_name = feed_info.get("name", feed_info["url"])
        feed_id = feed_info["id"]

        try:
            # Check cooldown and rate limits before fetching
            cooldown_minutes = await self.get_feed_cooldown(feed_id)
            max_news_per_hour = await self.get_feed_max_news_per_hour(feed_id)
            recent_count = await self.get_recent_items_count(feed_id, cooldown_minutes)

            # Check rate limit (news per cooldown period)
            if recent_count >= max_news_per_hour:
                logger.info(f"[SKIP] Feed {feed_name} (ID: {feed_id}) reached limit {max_news_per_hour} news per {cooldown_minutes} minutes. Published: {recent_count}")
                return []

            # Check cooldown (time since last publication)
            last_published = await self.get_last_published_time(feed_id)
            if last_published:
                elapsed = datetime.now(timezone.utc) - last_published
                if elapsed < timedelta(minutes=cooldown_minutes):
                    remaining_time = timedelta(minutes=cooldown_minutes) - elapsed
                    logger.info(f"[SKIP] Feed {feed_name} (ID: {feed_id}) on cooldown. Remaining: {remaining_time}")
                    return []

            # Fetch RSS items
            rss_items = await self.rss_fetcher.fetch_feed(feed_info, headers)

            if not rss_items:
                logger.info(f"No items to process from {feed_name}")
                return []

            # Process each item
            processed_items = []
            for rss_item in rss_items:
                try:
                    # Save to database
                    news_id = await self.save_rss_item_to_db(rss_item, feed_info["id"])
                    if news_id:
                        rss_item["id"] = news_id
                        processed_items.append(rss_item)

                        # Queue for translation if needed
                        if self.translator_task_queue:
                            await self.translator_task_queue.add_task(
                                title=rss_item["title"],
                                content=rss_item["content"],
                                original_lang=feed_info["lang"],
                                callback=self._on_translation_complete,
                                error_callback=self._on_translation_error,
                                task_id=news_id
                            )

                except Exception as e:
                    logger.error(f"Error processing RSS item from {feed_name}: {e}")
                    continue

            logger.info(f"Processed {len(processed_items)} items from {feed_name}")
            return processed_items

        except Exception as e:
            logger.error(f"Error processing feed {feed_name}: {e}")
            return []

    async def _on_translation_complete(self, translations: Dict[str, Any], task_id: str = None) -> None:
        """Handle completed translation"""
        if not task_id or not translations:
            return

        try:
            # Save translations to database
            success = await self.save_translations_to_db(task_id, translations)
            if success:
                logger.info(f"Translations saved for news item {task_id}")
            else:
                logger.error(f"Failed to save translations for news item {task_id}")
        except Exception as e:
            logger.error(f"Error handling translation completion for {task_id}: {e}")

    async def _on_translation_error(self, error_data: Dict[str, Any], task_id: str = None) -> None:
        """Handle translation error"""
        logger.error(f"Translation error for task {task_id}: {error_data}")

    async def process_all_feeds(self) -> Dict[str, Any]:
        """Process all active feeds"""
        start_time = datetime.now(timezone.utc)

        # Get active feeds
        feeds_info = await self.get_all_active_feeds()

        if not feeds_info:
            return {"status": "no_feeds", "processed_feeds": 0, "total_items": 0}

        headers = {"User-Agent": DEFAULT_USER_AGENT}

        # Process feeds concurrently with some limit
        semaphore = asyncio.Semaphore(5)  # Limit concurrent feed processing

        async def process_feed_with_limit(feed_info):
            async with semaphore:
                return await self.process_rss_feed(feed_info, headers)

        # Process all feeds
        tasks = [process_feed_with_limit(feed_info) for feed_info in feeds_info]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect results
        total_items = 0
        successful_feeds = 0

        for i, result in enumerate(results):
            feed_name = feeds_info[i].get("name", feeds_info[i]["url"])
            if isinstance(result, Exception):
                logger.error(f"Failed to process feed {feed_name}: {result}")
            else:
                successful_feeds += 1
                total_items += len(result)
                logger.info(f"Processed {len(result)} items from {feed_name}")

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return {
            "status": "completed",
            "processed_feeds": successful_feeds,
            "total_feeds": len(feeds_info),
            "total_items": total_items,
            "duration_seconds": duration
        }