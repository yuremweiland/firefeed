# rss_manager.py - RSS Manager with service-based architecture
import logging
from services.rss import RSSManager as NewRSSManager
from di_container import get_service
from interfaces import IRSSFetcher, IRSSValidator, IRSSStorage, IMediaExtractor, ITranslationService, IDuplicateDetector, ITranslatorQueue

logger = logging.getLogger(__name__)


class RSSManager(NewRSSManager):
    """RSS Manager with service-based architecture using dependency injection"""

    def __init__(self, translator_queue=None):
        # Initialize with services from DI container
        try:
            rss_fetcher = get_service(IRSSFetcher)
            rss_validator = get_service(IRSSValidator)
            rss_storage = get_service(IRSSStorage)
            media_extractor = get_service(IMediaExtractor)
            translation_service = get_service(ITranslationService)
            duplicate_detector = get_service(IDuplicateDetector)
            translator_queue_service = get_service(ITranslatorQueue)
        except Exception as e:
            logger.error(f"Failed to initialize services from DI container: {e}")
            # Fallback to None - methods will log warnings
            rss_fetcher = None
            rss_validator = None
            rss_storage = None
            media_extractor = None
            translation_service = None
            duplicate_detector = None
            translator_queue_service = translator_queue

        super().__init__(
            rss_fetcher=rss_fetcher,
            rss_validator=rss_validator,
            rss_storage=rss_storage,
            media_extractor=media_extractor,
            translation_service=translation_service,
            duplicate_detector=duplicate_detector,
            translator_queue=translator_queue_service,
            translator_task_queue=translator_queue
        )