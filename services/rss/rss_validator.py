# services/rss/rss_validator.py
import asyncio
import logging
import time
from typing import Dict
import aiohttp
import feedparser
from interfaces import IRSSValidator

logger = logging.getLogger(__name__)


class RSSValidator(IRSSValidator):
    """Service for validating RSS feeds"""

    def __init__(self):
        self._validation_cache = {}  # url -> (is_valid, timestamp)
        self._cache_ttl = 300  # 5 minutes

    async def validate_feed(self, url: str, headers: Dict[str, str]) -> bool:
        """Validate if URL contains valid RSS feed with caching"""
        current_time = time.time()

        # Check cache first
        if url in self._validation_cache:
            is_valid, timestamp = self._validation_cache[url]
            if current_time - timestamp < self._cache_ttl:
                logger.debug(f"[RSS] [VALIDATE] Using cache for {url}: valid={is_valid}")
                return is_valid
            else:
                del self._validation_cache[url]

        try:
            # Check headers first
            content_type_valid = False
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.head(url, headers=headers) as response:
                    content_type = response.headers.get("Content-Type", "").lower()
                    if "xml" in content_type or "rss" in content_type or "atom" in content_type:
                        content_type_valid = True
                    else:
                        logger.warning(f"[RSS] [VALIDATE] URL {url} has Content-Type: {content_type}, checking content...")

            # Try to parse content
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, url)

            if feed.bozo:
                # Ignore encoding errors
                if "document declared as us-ascii, but parsed as utf-8" in str(feed.bozo_exception):
                    logger.warning(f"[RSS] [VALIDATE] Ignoring encoding error for {url}: {feed.bozo_exception}")
                else:
                    logger.error(f"[RSS] [VALIDATE] Parse error for {url}: {feed.bozo_exception}")
                    self._validation_cache[url] = (False, current_time)
                    return False

            if not hasattr(feed, "entries") or len(feed.entries) == 0:
                logger.warning(f"[RSS] [VALIDATE] {url} contains no entries")
                self._validation_cache[url] = (False, current_time)
                return False

            # If Content-Type was incorrect but parsing succeeded - it's valid RSS
            if not content_type_valid:
                logger.info(f"[RSS] [VALIDATE] {url} valid despite incorrect Content-Type, {len(feed.entries)} entries")
            else:
                logger.info(f"[RSS] [VALIDATE] {url} valid, {len(feed.entries)} entries")

            self._validation_cache[url] = (True, current_time)
            return True

        except Exception as e:
            logger.error(f"[RSS] [VALIDATE] Validation error for {url}: {e}")
            # Try with raw content if initial parsing failed
            if "expected string or bytes-like object, got 'dict'" in str(e):
                try:
                    logger.debug(f"[RSS] [VALIDATE] Trying raw content parsing for {url}")
                    timeout = aiohttp.ClientTimeout(total=15)
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        async with session.get(url, headers=headers) as response:
                            raw_content = await response.text()
                            loop = asyncio.get_event_loop()
                            feed = await loop.run_in_executor(None, feedparser.parse, raw_content)

                            if feed.bozo:
                                if "document declared as us-ascii, but parsed as utf-8" in str(feed.bozo_exception):
                                    logger.warning(f"[RSS] [VALIDATE] Ignoring encoding error for raw content {url}: {feed.bozo_exception}")
                                else:
                                    logger.error(f"[RSS] [VALIDATE] Raw content parse error for {url}: {feed.bozo_exception}")
                                    self._validation_cache[url] = (False, current_time)
                                    return False

                            if not hasattr(feed, "entries") or len(feed.entries) == 0:
                                logger.warning(f"[RSS] [VALIDATE] Raw content {url} contains no entries")
                                self._validation_cache[url] = (False, current_time)
                                return False

                            logger.info(f"[RSS] [VALIDATE] {url} valid after raw content parsing, {len(feed.entries)} entries")
                            self._validation_cache[url] = (True, current_time)
                            return True
                except Exception as raw_e:
                    logger.error(f"[RSS] [VALIDATE] Raw content validation error for {url}: {raw_e}")
                    self._validation_cache[url] = (False, current_time)
                    return False

            self._validation_cache[url] = (False, current_time)
            return False