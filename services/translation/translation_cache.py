# services/translation/translation_cache.py
import asyncio
import hashlib
import json
import logging
from typing import Dict, Any, Optional
from interfaces import ITranslationCache

logger = logging.getLogger(__name__)


class TranslationCache(ITranslationCache):
    """Service for caching translation results"""

    def __init__(self, cache_ttl: int = 3600, max_cache_size: int = 10000):
        self.cache_ttl = cache_ttl  # Default 1 hour
        self.max_cache_size = max_cache_size
        self.cache: Dict[str, Dict[str, Any]] = {}
        self._cleanup_task = None

        # Start cleanup task
        self._start_cleanup_task()

    def _start_cleanup_task(self) -> None:
        """Start background cleanup task"""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_expired_entries())

    async def _cleanup_expired_entries(self) -> None:
        """Background task to remove expired cache entries"""
        while True:
            try:
                await asyncio.sleep(300)  # Clean up every 5 minutes
                await self._remove_expired()
            except Exception as e:
                logger.error(f"[CACHE] Error in cleanup task: {e}")

    async def _remove_expired(self) -> None:
        """Remove expired cache entries"""
        import time
        current_time = time.time()

        expired_keys = []
        for key, data in self.cache.items():
            if current_time - data.get('timestamp', 0) > self.cache_ttl:
                expired_keys.append(key)

        for key in expired_keys:
            del self.cache[key]

        if expired_keys:
            logger.debug(f"[CACHE] Removed {len(expired_keys)} expired entries")

    def _generate_cache_key(self, text: str, source_lang: str, target_lang: str) -> str:
        """Generate cache key for translation"""
        # Create hash of text + languages to avoid key length issues
        content = f"{text}_{source_lang}_{target_lang}".encode('utf-8')
        return hashlib.sha256(content).hexdigest()

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Get cached translation"""
        if key in self.cache:
            data = self.cache[key]
            import time
            if time.time() - data.get('timestamp', 0) <= self.cache_ttl:
                logger.debug(f"[CACHE] Cache hit for key: {key[:16]}...")
                return data.get('translation')
            else:
                # Remove expired entry
                del self.cache[key]
                logger.debug(f"[CACHE] Removed expired entry: {key[:16]}...")

        logger.debug(f"[CACHE] Cache miss for key: {key[:16]}...")
        return None

    async def set(self, key: str, value: Dict[str, Any], ttl: int = None) -> None:
        """Set cached translation with TTL"""
        # Enforce cache size limit
        if len(self.cache) >= self.max_cache_size:
            await self._evict_old_entries()

        import time
        ttl_value = ttl or self.cache_ttl
        self.cache[key] = {
            'translation': value,
            'timestamp': time.time(),
            'ttl': ttl_value
        }
        logger.debug(f"[CACHE] Cached translation for key: {key[:16]}...")

    async def _evict_old_entries(self) -> None:
        """Remove oldest entries when cache is full"""
        # Remove 10% of entries (oldest first)
        entries_to_remove = max(1, int(self.max_cache_size * 0.1))

        # Sort by timestamp (oldest first)
        sorted_entries = sorted(self.cache.items(), key=lambda x: x[1]['timestamp'])

        removed_count = 0
        for key, _ in sorted_entries[:entries_to_remove]:
            del self.cache[key]
            removed_count += 1

        logger.info(f"[CACHE] Evicted {removed_count} old entries")

    async def clear(self) -> None:
        """Clear all cached translations"""
        cache_size = len(self.cache)
        self.cache.clear()
        logger.info(f"[CACHE] Cleared all {cache_size} cached translations")

    # Synchronous versions for backward compatibility
    def get_sync(self, key: str) -> Optional[Dict[str, Any]]:
        """Synchronous version of get"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If event loop is running, we need to handle this differently
                # For now, just return None to avoid blocking
                logger.warning("[CACHE] Synchronous get called in async context")
                return None
            else:
                return loop.run_until_complete(self.get(key))
        except Exception as e:
            logger.error(f"[CACHE] Error in sync get: {e}")
            return None

    def set_sync(self, key: str, value: Dict[str, Any], ttl: int = None) -> None:
        """Synchronous version of set"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule as task
                asyncio.create_task(self.set(key, value, ttl))
            else:
                loop.run_until_complete(self.set(key, value, ttl))
        except Exception as e:
            logger.error(f"[CACHE] Error in sync set: {e}")

    def clear_sync(self) -> None:
        """Synchronous version of clear"""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(self.clear())
            else:
                loop.run_until_complete(self.clear())
        except Exception as e:
            logger.error(f"[CACHE] Error in sync clear: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        import time
        current_time = time.time()

        total_entries = len(self.cache)
        expired_entries = sum(1 for data in self.cache.values()
                            if current_time - data.get('timestamp', 0) > data.get('ttl', self.cache_ttl))

        # Estimate memory usage (rough approximation)
        memory_usage_kb = total_entries * 2  # ~2KB per entry

        return {
            'total_entries': total_entries,
            'expired_entries': expired_entries,
            'max_cache_size': self.max_cache_size,
            'estimated_memory_usage_kb': memory_usage_kb,
            'cache_hit_ratio': 0.0  # Would need to track hits/misses for this
        }