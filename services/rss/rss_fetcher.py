# services/rss/rss_fetcher.py
import asyncio
import hashlib
import logging
from typing import Dict, Any, List, Optional, Union
from datetime import datetime, timezone
import aiohttp
import feedparser
from urllib.parse import urljoin, urlparse
from interfaces import IRSSFetcher, IMediaExtractor, IDuplicateDetector
from utils.image import ImageProcessor
from exceptions import RSSFetchError, RSSParseError, RSSValidationError

logger = logging.getLogger(__name__)


class RSSFetcher(IRSSFetcher):
    """Service for fetching and parsing RSS feeds"""

    def __init__(self, media_extractor: IMediaExtractor, duplicate_detector: IDuplicateDetector,
                 max_concurrent_feeds: int = 10, max_entries_per_feed: int = 50) -> None:
        self.media_extractor: IMediaExtractor = media_extractor
        self.duplicate_detector: IDuplicateDetector = duplicate_detector
        self._feed_semaphore: asyncio.Semaphore = asyncio.Semaphore(max_concurrent_feeds)
        self.max_entries_per_feed: int = max_entries_per_feed

    def generate_news_id(self, title: str, content: str, link: str, feed_id: int) -> str:
        """Generate unique ID for news item"""
        content_hash = hashlib.sha256(
            f"{title.strip()}_{content.strip()[:500]}_{link.strip()}".encode("utf-8")
        ).hexdigest()
        return content_hash

    async def check_for_duplicates(self, title: str, content: str, link: str, lang: str) -> bool:
        """Check if content is duplicate"""
        try:
            is_duplicate, duplicate_info = await self.duplicate_detector.is_duplicate(title, content, link, lang)
            if is_duplicate:
                logger.warning(f"[DUPLICATE] Found duplicate: {title[:50]}... ID: {duplicate_info.get('news_id', 'unknown')}")
                return True
            return False
        except Exception as e:
            logger.error(f"[DUPLICATE] Error checking duplicates: {e}")
            return False

    async def fetch_feed(self, feed_info: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Fetch and parse a single RSS feed"""
        async with self._feed_semaphore:
            return await self._fetch_single_feed(feed_info, headers)

    async def fetch_feeds(self, feeds_info: List[Dict[str, Any]], headers: Dict[str, str]) -> List[Union[List[Dict[str, Any]], Exception]]:
        """Fetch and parse multiple RSS feeds concurrently"""
        tasks = [self.fetch_feed(feed_info, headers) for feed_info in feeds_info]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_single_feed(self, feed_info: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Internal method to fetch and parse a single feed"""
        url = feed_info["url"]
        feed_id = feed_info["id"]
        feed_name = feed_info.get("name", url)

        logger.info(f"[RSS] Starting fetch: {feed_name} ({url})")

        try:
            # Parse RSS feed
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, url)

            if feed.bozo:
                logger.error(f"[RSS] Parse error for {feed_name}: {feed.bozo_exception}")
                return []

            if not hasattr(feed, "entries") or len(feed.entries) == 0:
                logger.warning(f"[RSS] No entries found in {feed_name}")
                return []

            logger.info(f"[RSS] Found {len(feed.entries)} entries in {feed_name}")

            rss_items = []
            processed_count = 0

            for entry in feed.entries[:self.max_entries_per_feed]:
                try:
                    rss_item = await self._process_feed_entry(entry, feed_info)
                    if rss_item:
                        rss_items.append(rss_item)
                        processed_count += 1

                    # Yield control every 5 iterations
                    if processed_count % 5 == 0:
                        await asyncio.sleep(0)

                except Exception as e:
                    logger.error(f"[RSS] Error processing entry in {feed_name}: {e}")
                    continue

            logger.info(f"[RSS] Successfully processed {len(rss_items)} items from {feed_name}")
            return rss_items

        except Exception as e:
            logger.error(f"[RSS] Error fetching {feed_name}: {e}")
            return []

    async def _process_feed_entry(self, entry, feed_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process individual RSS feed entry"""
        try:
            # Extract basic information
            title = self._extract_entry_title(entry)
            content = self._extract_entry_content(entry)
            link = self._extract_entry_link(entry, feed_info["url"])

            if not title or not link:
                logger.debug("[RSS] Entry missing title or link, skipping")
                return None

            # Check for duplicates
            if await self.check_for_duplicates(title, content, link, feed_info["lang"]):
                logger.debug(f"[RSS] Duplicate found, skipping: {title[:50]}...")
                return None

            # Generate news ID
            news_id = self.generate_news_id(title, content, link, feed_info["id"])

            # Extract media
            image_url = self.media_extractor.extract_image(entry)
            video_url = self.media_extractor.extract_video(entry)

            # Process image if found
            image_filename = None
            if image_url:
                try:
                    image_processor = ImageProcessor()
                    image_filename = await image_processor.process_image_from_url(image_url)
                except Exception as e:
                    logger.warning(f"[RSS] Error processing image {image_url}: {e}")

            # Create RSS item
            rss_item = {
                "id": news_id,
                "title": title,
                "content": content,
                "link": link,
                "lang": feed_info["lang"],
                "category": feed_info["category"],
                "source": feed_info["source"],
                "image_filename": image_filename,
                "published": self._extract_entry_published(entry),
                "feed_id": feed_info["id"]
            }

            return rss_item

        except Exception as e:
            logger.error(f"[RSS] Error processing feed entry: {e}")
            return None

    def _extract_entry_title(self, entry) -> str:
        """Extract title from RSS entry"""
        title = getattr(entry, 'title', '')
        return str(title).strip() if title else ''

    def _extract_entry_content(self, entry) -> str:
        """Extract content from RSS entry"""
        # Try different content fields
        content = ''
        if hasattr(entry, 'content') and entry.content:
            content = entry.content[0].value if isinstance(entry.content, list) else entry.content
        elif hasattr(entry, 'summary'):
            content = entry.summary
        elif hasattr(entry, 'description'):
            content = entry.description

        return str(content).strip() if content else ''

    def _extract_entry_link(self, entry, feed_url: str) -> str:
        """Extract link from RSS entry"""
        link = ''
        if hasattr(entry, 'link'):
            link = entry.link
        elif hasattr(entry, 'links') and entry.links:
            # Find the first alternate link
            for link_obj in entry.links:
                if isinstance(link_obj, dict) and link_obj.get('rel') == 'alternate':
                    link = link_obj.get('href', '')
                    break
            if not link and entry.links:
                link = entry.links[0].get('href', '') if isinstance(entry.links[0], dict) else str(entry.links[0])

        # Convert relative URLs to absolute
        if link and not link.startswith(('http://', 'https://')):
            link = urljoin(feed_url, link)

        return str(link).strip() if link else ''

    def _extract_entry_published(self, entry) -> datetime:
        """Extract published date from RSS entry"""
        published = datetime.now(timezone.utc)

        # Try different date fields
        date_fields = ['published_parsed', 'updated_parsed', 'created_parsed']
        for field in date_fields:
            if hasattr(entry, field) and getattr(entry, field):
                dt_tuple = getattr(entry, field)
                if dt_tuple and len(dt_tuple) >= 6:
                    try:
                        published = datetime(*dt_tuple[:6], tzinfo=timezone.utc)
                        break
                    except (ValueError, TypeError):
                        continue

        return published