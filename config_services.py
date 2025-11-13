# config_services.py - Service configuration via environment variables
import os
from typing import Optional, Dict, Any
from dataclasses import dataclass


@dataclass
class RSSConfig:
    """Configuration for RSS services"""
    max_concurrent_feeds: int = 10
    max_entries_per_feed: int = 50
    validation_cache_ttl: int = 300  # 5 minutes
    request_timeout: int = 15
    max_total_rss_items: int = 1000

    @classmethod
    def from_env(cls) -> 'RSSConfig':
        return cls(
            max_concurrent_feeds=int(os.getenv('RSS_MAX_CONCURRENT_FEEDS', '10')),
            max_entries_per_feed=int(os.getenv('RSS_MAX_ENTRIES_PER_FEED', '50')),
            validation_cache_ttl=int(os.getenv('RSS_VALIDATION_CACHE_TTL', '300')),
            request_timeout=int(os.getenv('RSS_REQUEST_TIMEOUT', '15')),
            max_total_rss_items=int(os.getenv('RSS_MAX_TOTAL_ITEMS', '1000'))
        )


@dataclass
class TranslationConfig:
    """Configuration for translation services"""
    max_concurrent_translations: int = 3
    max_cached_models: int = 15
    model_cleanup_interval: int = 1800  # 30 minutes
    default_device: str = "cpu"
    max_workers: int = 4

    @classmethod
    def from_env(cls) -> 'TranslationConfig':
        return cls(
            max_concurrent_translations=int(os.getenv('TRANSLATION_MAX_CONCURRENT', '3')),
            max_cached_models=int(os.getenv('TRANSLATION_MAX_CACHED_MODELS', '15')),
            model_cleanup_interval=int(os.getenv('TRANSLATION_CLEANUP_INTERVAL', '1800')),
            default_device=os.getenv('TRANSLATION_DEVICE', 'cpu'),
            max_workers=int(os.getenv('TRANSLATION_MAX_WORKERS', '4'))
        )


@dataclass
class CacheConfig:
    """Configuration for caching services"""
    default_ttl: int = 3600  # 1 hour
    max_cache_size: int = 10000
    cleanup_interval: int = 300  # 5 minutes

    @classmethod
    def from_env(cls) -> 'CacheConfig':
        return cls(
            default_ttl=int(os.getenv('CACHE_DEFAULT_TTL', '3600')),
            max_cache_size=int(os.getenv('CACHE_MAX_SIZE', '10000')),
            cleanup_interval=int(os.getenv('CACHE_CLEANUP_INTERVAL', '300'))
        )


@dataclass
class QueueConfig:
    """Configuration for queue services"""
    max_queue_size: int = 30
    default_workers: int = 1
    task_timeout: int = 300  # 5 minutes

    @classmethod
    def from_env(cls) -> 'QueueConfig':
        return cls(
            max_queue_size=int(os.getenv('QUEUE_MAX_SIZE', '30')),
            default_workers=int(os.getenv('QUEUE_DEFAULT_WORKERS', '1')),
            task_timeout=int(os.getenv('QUEUE_TASK_TIMEOUT', '300'))
        )


@dataclass
class ServiceConfig:
    """Main service configuration"""
    rss: RSSConfig
    translation: TranslationConfig
    cache: CacheConfig
    queue: QueueConfig

    @classmethod
    def from_env(cls) -> 'ServiceConfig':
        return cls(
            rss=RSSConfig.from_env(),
            translation=TranslationConfig.from_env(),
            cache=CacheConfig.from_env(),
            queue=QueueConfig.from_env()
        )


# Global configuration instance
_config: Optional[ServiceConfig] = None


def get_service_config() -> ServiceConfig:
    """Get global service configuration"""
    global _config
    if _config is None:
        _config = ServiceConfig.from_env()
    return _config


def reset_config() -> None:
    """Reset configuration (for testing)"""
    global _config
    _config = None