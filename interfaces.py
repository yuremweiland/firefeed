# interfaces.py - Base interfaces and abstractions for FireFeed
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple, Protocol, Union, Callable, Awaitable
from datetime import datetime


class IRSSFetcher(ABC):
    """Interface for RSS feed fetching and parsing"""

    @abstractmethod
    async def fetch_feed(self, feed_info: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
        """Fetch and parse a single RSS feed"""
        pass

    @abstractmethod
    async def fetch_feeds(self, feeds_info: List[Dict[str, Any]], headers: Dict[str, str]) -> List[List[Dict[str, Any]]]:
        """Fetch and parse multiple RSS feeds concurrently"""
        pass


class IRSSValidator(ABC):
    """Interface for RSS feed validation"""

    @abstractmethod
    async def validate_feed(self, url: str, headers: Dict[str, str]) -> bool:
        """Validate if URL contains valid RSS feed"""
        pass


class IRSSStorage(ABC):
    """Interface for RSS data storage operations"""

    @abstractmethod
    async def save_rss_item(self, rss_item: Dict[str, Any], feed_id: int) -> Optional[str]:
        """Save RSS item to database"""
        pass

    @abstractmethod
    async def save_translations(self, news_id: str, translations: Dict[str, Dict[str, str]]) -> bool:
        """Save translations for RSS item"""
        pass

    @abstractmethod
    async def get_feed_cooldown(self, feed_id: int) -> int:
        """Get cooldown minutes for feed"""
        pass

    @abstractmethod
    async def get_feed_max_news_per_hour(self, feed_id: int) -> int:
        """Get max news per hour for feed"""
        pass

    @abstractmethod
    async def get_last_published_time(self, feed_id: int) -> Optional[datetime]:
        """Get last published time for feed"""
        pass

    @abstractmethod
    async def get_recent_items_count(self, feed_id: int, minutes: int) -> int:
        """Get count of recent items for feed"""
        pass


class IMediaExtractor(ABC):
    """Interface for media extraction from RSS items"""

    @abstractmethod
    def extract_image(self, rss_item: Dict[str, Any]) -> Optional[str]:
        """Extract image URL from RSS item"""
        pass

    @abstractmethod
    def extract_video(self, rss_item: Dict[str, Any]) -> Optional[str]:
        """Extract video URL from RSS item"""
        pass


class IModelManager(ABC):
    """Interface for ML model management"""

    @abstractmethod
    async def get_model(self, source_lang: str, target_lang: str) -> Tuple[Any, Any]:
        """Get model and tokenizer for translation direction"""
        pass

    @abstractmethod
    async def preload_popular_models(self) -> None:
        """Preload commonly used models"""
        pass

    @abstractmethod
    def clear_cache(self) -> None:
        """Clear model cache"""
        pass

    @abstractmethod
    def get_stats(self) -> Dict[str, Any]:
        """Get model cache statistics"""
        pass


class ITranslationService(ABC):
    """Interface for text translation operations"""

    @abstractmethod
    async def translate_async(self, texts: List[str], source_lang: str, target_lang: str,
                            context_window: int = 2, beam_size: Optional[int] = None) -> List[str]:
        """Translate texts asynchronously"""
        pass

    @abstractmethod
    async def prepare_translations(self, title: str, content: str, original_lang: str,
                                 target_langs: List[str]) -> Dict[str, Dict[str, str]]:
        """Prepare translations for title and content to multiple languages"""
        pass


class ITranslationCache(ABC):
    """Interface for translation caching"""

    @abstractmethod
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached translation"""
        pass

    @abstractmethod
    async def set(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """Set cached translation with TTL"""
        pass

    @abstractmethod
    async def clear(self) -> None:
        """Clear all cached translations"""
        pass


class IDuplicateDetector(ABC):
    """Interface for duplicate content detection"""

    @abstractmethod
    async def is_duplicate(self, title: str, content: str, link: str, lang: str) -> Tuple[bool, Dict[str, Any]]:
        """Check if content is duplicate"""
        pass

    @abstractmethod
    async def process_rss_item(self, rss_item_id: str, title: str, content: str, lang_code: str) -> bool:
        """Process RSS item for duplicate detection and embedding generation"""
        pass


class ILogger(ABC):
    """Interface for logging operations"""

    @abstractmethod
    def debug(self, message: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def info(self, message: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def warning(self, message: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def error(self, message: str, *args, **kwargs) -> None:
        pass

    @abstractmethod
    def critical(self, message: str, *args, **kwargs) -> None:
        pass


class IDatabasePool(ABC):
    """Interface for database connection pool"""

    @abstractmethod
    async def acquire(self):
        """Acquire database connection"""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Close pool"""
        pass


class ITranslatorQueue(ABC):
    """Interface for translation task queue"""

    @abstractmethod
    async def add_task(self, title: str, content: str, original_lang: str,
                      callback=None, error_callback=None, task_id=None) -> None:
        """Add translation task to queue"""
        pass

    @abstractmethod
    async def wait_completion(self) -> None:
        """Wait for all tasks to complete"""
        pass

    @abstractmethod
    def print_stats(self) -> None:
        """Print queue statistics"""
        pass