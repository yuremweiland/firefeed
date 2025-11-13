# di_container.py - Dependency Injection Container
import logging
from typing import Dict, Any, Type, TypeVar, Optional
from interfaces import *

logger = logging.getLogger(__name__)

T = TypeVar('T')


class DIContainer:
    """Simple Dependency Injection Container"""

    def __init__(self):
        self._services: Dict[Type, Any] = {}
        self._singletons: Dict[Type, Any] = {}
        self._factories: Dict[Type, callable] = {}

    def register(self, interface: Type[T], implementation: Type[T], singleton: bool = True) -> None:
        """Register a service implementation"""
        if singleton:
            self._services[interface] = implementation
        else:
            self._factories[interface] = implementation

    def register_instance(self, interface: Type[T], instance: T) -> None:
        """Register a singleton instance"""
        self._singletons[interface] = instance

    def register_factory(self, interface: Type[T], factory: callable) -> None:
        """Register a factory function"""
        self._factories[interface] = factory

    def resolve(self, interface: Type[T]) -> T:
        """Resolve a service instance"""
        # Check singletons first
        if interface in self._singletons:
            return self._singletons[interface]

        # Check services
        if interface in self._services:
            impl_class = self._services[interface]
            instance = self._instantiate(impl_class)
            self._singletons[interface] = instance  # Cache as singleton
            return instance

        # Check factories
        if interface in self._factories:
            factory = self._factories[interface]
            return factory()

        raise ValueError(f"No registration found for {interface}")

    def _instantiate(self, cls: Type[T]) -> T:
        """Instantiate a class with dependency injection"""
        import inspect

        # Get constructor parameters
        init_signature = inspect.signature(cls.__init__)
        params = {}

        for param_name, param in init_signature.parameters.items():
            if param_name == 'self':
                continue

            # Try to resolve parameter type
            if param.annotation != inspect.Parameter.empty:
                try:
                    params[param_name] = self.resolve(param.annotation)
                except ValueError:
                    # If can't resolve, try to get default value
                    if param.default != inspect.Parameter.empty:
                        params[param_name] = param.default
                    else:
                        raise ValueError(f"Cannot resolve parameter {param_name} for {cls}")
            elif param.default != inspect.Parameter.empty:
                params[param_name] = param.default
            else:
                raise ValueError(f"Cannot resolve parameter {param_name} for {cls}")

        # If no parameters needed, just instantiate
        if not params:
            return cls()

        return cls(**params)

    def clear(self) -> None:
        """Clear all registrations and instances"""
        self._services.clear()
        self._singletons.clear()
        self._factories.clear()


# Global DI container instance
di_container = DIContainer()


def setup_di_container() -> DIContainer:
    """Setup the global DI container with all services"""
    global di_container

    # Import configuration
    from config_services import get_service_config

    config = get_service_config()

    # Import services
    from services.rss import MediaExtractor, RSSValidator, RSSStorage, RSSFetcher
    from services.translation import ModelManager, TranslationService, TranslationCache
    from firefeed_translator_task_queue import FireFeedTranslatorTaskQueue
    from firefeed_dublicate_detector import FireFeedDuplicateDetector

    # Register simple services first
    di_container.register(IRSSStorage, RSSStorage)
    di_container.register(IMediaExtractor, MediaExtractor)

    # Register RSS services with configuration (after dependencies)
    di_container.register_factory(IRSSFetcher, lambda: RSSFetcher(
        media_extractor=di_container.resolve(IMediaExtractor),
        duplicate_detector=di_container.resolve(IDuplicateDetector),
        max_concurrent_feeds=config.rss.max_concurrent_feeds,
        max_entries_per_feed=config.rss.max_entries_per_feed
    ))

    di_container.register_factory(IRSSValidator, lambda: RSSValidator(
        cache_ttl=config.rss.validation_cache_ttl,
        request_timeout=config.rss.request_timeout
    ))

    # Register translation services with configuration
    di_container.register_factory(IModelManager, lambda: ModelManager(
        device=config.translation.default_device,
        max_cached_models=config.translation.max_cached_models,
        model_cleanup_interval=config.translation.model_cleanup_interval
    ))

    # Create translator queue first
    translator_queue = FireFeedTranslatorTaskQueue(
        translator=None,  # Will be set later
        max_workers=config.queue.default_workers,
        queue_size=config.queue.max_queue_size
    )
    di_container.register_instance(ITranslatorQueue, translator_queue)

    # Create translation service
    translation_service = TranslationService(
        model_manager=di_container.resolve(IModelManager),
        translator_queue=translator_queue,
        max_concurrent_translations=config.translation.max_concurrent_translations
    )
    di_container.register_instance(ITranslationService, translation_service)

    # Set translation service as translator for the queue
    translator_queue.set_translator(translation_service)

    di_container.register_factory(ITranslationCache, lambda: TranslationCache(
        default_ttl=config.cache.default_ttl,
        max_size=config.cache.max_cache_size
    ))

    di_container.register(IDuplicateDetector, FireFeedDuplicateDetector)

    logger.info("DI container setup completed with configuration")
    return di_container


def get_service(interface: Type[T]) -> T:
    """Get a service instance from the global DI container"""
    return di_container.resolve(interface)