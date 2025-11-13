# services/rss/rss_manager.py
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone, timedelta

from interfaces import (
    IRSSFetcher, IRSSValidator, IRSSStorage, IMediaExtractor,
    ITranslationService, IDuplicateDetector, ITranslatorQueue
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
                 translator_task_queue=None):  # For backward compatibility

        self.rss_fetcher = rss_fetcher
        self.rss_validator = rss_validator
        self.rss_storage = rss_storage
        self.media_extractor = media_extractor
        self.translation_service = translation_service
        self.duplicate_detector = duplicate_detector
        self.translator_queue = translator_queue
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
        """Get all active feeds - now this would be a separate repository/service"""
        # This method should be moved to a repository service
        # For now, return empty list
        logger.warning("get_all_active_feeds should be implemented in repository service")
        return []

    async def get_feeds_by_category(self, category_name: str) -> List[Dict[str, Any]]:
        """Get feeds by category - repository method"""
        logger.warning("get_feeds_by_category should be implemented in repository service")
        return []

    async def get_feeds_by_language(self, lang: str) -> List[Dict[str, Any]]:
        """Get feeds by language - repository method"""
        logger.warning("get_feeds_by_language should be implemented in repository service")
        return []

    async def get_feeds_by_source(self, source_name: str) -> List[Dict[str, Any]]:
        """Get feeds by source - repository method"""
        logger.warning("get_feeds_by_source should be implemented in repository service")
        return []

    async def add_feed(self, url: str, category_name: str, source_name: str, language: str, is_active: bool = True) -> bool:
        """Add feed - repository method"""
        logger.warning("add_feed should be implemented in repository service")
        return False

    async def update_feed(self, feed_id: int, **kwargs) -> bool:
        """Update feed - repository method"""
        logger.warning("update_feed should be implemented in repository service")
        return False

    async def delete_feed(self, feed_id: int) -> bool:
        """Delete feed - repository method"""
        logger.warning("delete_feed should be implemented in repository service")
        return False

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
            "User-Agent": "FireFeed/1.0 (RSS Aggregator)"
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

    def generate_news_id(self, title: str, content: str, link: str, feed_id: int) -> str:
        """Generate news ID - delegate to fetcher"""
        return self.rss_fetcher.generate_news_id(title, content, link, feed_id)

    async def check_for_duplicates(self, title: str, content: str, link: str, lang: str) -> bool:
        """Check for duplicates - delegate to fetcher"""
        return await self.rss_fetcher.check_for_duplicates(title, content, link, lang)

    def extract_image_from_rss_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract image - delegate to media extractor"""
        return self.media_extractor.extract_image(item)

    def extract_video_from_rss_item(self, item: Dict[str, Any]) -> Optional[str]:
        """Extract video - delegate to media extractor"""
        return self.media_extractor.extract_video(item)

    async def fetch_unprocessed_rss_items(self) -> List[Dict[str, Any]]:
        """Fetch unprocessed RSS items - this should be in storage service"""
        logger.warning("fetch_unprocessed_rss_items should be implemented in storage service")
        return []

    async def cleanup_duplicates(self) -> None:
        """Cleanup duplicates - this should be in maintenance service"""
        logger.warning("cleanup_duplicates should be implemented in maintenance service")

    # Main processing methods
    async def process_rss_feed(self, feed_info: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Process a single RSS feed end-to-end"""
        feed_name = feed_info.get("name", feed_info["url"])

        try:
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

        headers = {"User-Agent": "FireFeed/1.0 (RSS Aggregator)"}

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