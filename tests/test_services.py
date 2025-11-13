# tests/test_services.py
import pytest
from unittest.mock import AsyncMock, MagicMock
from di_container import DIContainer, get_service
from interfaces import IRSSFetcher, IRSSValidator, IRSSStorage, IMediaExtractor, ITranslationService
from services.rss import RSSFetcher, RSSValidator, RSSStorage, MediaExtractor
from services.translation import TranslationService, TranslationCache


class TestDIContainer:
    """Test dependency injection container"""

    def test_register_and_resolve(self):
        """Test basic registration and resolution"""
        container = DIContainer()

        # Register a mock service
        mock_service = MagicMock()
        container.register_instance(str, mock_service)

        # Resolve it
        resolved = container.resolve(str)
        assert resolved == mock_service

    def test_register_factory(self):
        """Test factory registration"""
        container = DIContainer()

        def factory():
            return "test"

        container.register_factory(str, factory)
        resolved = container.resolve(str)
        assert resolved == "test"


class TestRSSServices:
    """Test RSS services"""

    @pytest.fixture
    def mock_media_extractor(self):
        """Mock media extractor"""
        extractor = MagicMock(spec=IMediaExtractor)
        extractor.extract_image.return_value = "http://example.com/image.jpg"
        extractor.extract_video.return_value = None
        return extractor

    @pytest.fixture
    def mock_duplicate_detector(self):
        """Mock duplicate detector"""
        detector = MagicMock()
        detector.is_duplicate.return_value = (False, {})
        return detector

    def test_rss_fetcher_creation(self, mock_media_extractor, mock_duplicate_detector):
        """Test RSS fetcher can be created"""
        fetcher = RSSFetcher(mock_media_extractor, mock_duplicate_detector)
        assert fetcher.media_extractor == mock_media_extractor
        assert fetcher.duplicate_detector == mock_duplicate_detector

    def test_generate_news_id(self, mock_media_extractor, mock_duplicate_detector):
        """Test news ID generation"""
        fetcher = RSSFetcher(mock_media_extractor, mock_duplicate_detector)
        news_id = fetcher.generate_news_id("Title", "Content", "http://link.com", 1)
        assert isinstance(news_id, str)
        assert len(news_id) == 64  # SHA256 hex length

    async def test_check_for_duplicates(self, mock_media_extractor, mock_duplicate_detector):
        """Test duplicate checking"""
        fetcher = RSSFetcher(mock_media_extractor, mock_duplicate_detector)
        result = await fetcher.check_for_duplicates("Title", "Content", "http://link.com", "en")
        assert result is False
        mock_duplicate_detector.is_duplicate.assert_called_once()


class TestTranslationServices:
    """Test translation services"""

    @pytest.fixture
    def mock_model_manager(self):
        """Mock model manager"""
        manager = MagicMock()
        manager.get_model.return_value = (MagicMock(), MagicMock())
        return manager

    @pytest.fixture
    def mock_translator_queue(self):
        """Mock translator queue"""
        queue = MagicMock()
        return queue

    def test_translation_service_creation(self, mock_model_manager, mock_translator_queue):
        """Test translation service can be created"""
        service = TranslationService(mock_model_manager, mock_translator_queue)
        assert service.model_manager == mock_model_manager
        assert service.translator_queue == mock_translator_queue

    def test_translation_cache_creation(self):
        """Test translation cache can be created"""
        cache = TranslationCache()
        assert cache.cache_ttl == 3600  # default TTL
        assert cache.max_cache_size == 10000


class TestMediaExtractor:
    """Test media extractor"""

    def test_extract_image_from_rss_item(self):
        """Test image extraction from RSS item"""
        extractor = MediaExtractor()

        # Test with media_thumbnail
        item = {"media_thumbnail": [{"url": "http://example.com/image.jpg"}]}
        result = extractor.extract_image(item)
        assert result == "http://example.com/image.jpg"

        # Test with enclosure
        item = {"enclosures": [{"type": "image/jpeg", "href": "http://example.com/image2.jpg"}]}
        result = extractor.extract_image(item)
        assert result == "http://example.com/image2.jpg"

        # Test no image
        item = {"title": "Test"}
        result = extractor.extract_image(item)
        assert result is None

    def test_extract_video_from_rss_item(self):
        """Test video extraction from RSS item"""
        extractor = MediaExtractor()

        # Test with enclosure
        item = {"enclosures": [{"type": "video/mp4", "href": "http://example.com/video.mp4"}]}
        result = extractor.extract_video(item)
        assert result == "http://example.com/video.mp4"

        # Test no video
        item = {"title": "Test"}
        result = extractor.extract_video(item)
        assert result is None


class TestIntegration:
    """Integration tests for service interactions"""

    async def test_service_dependencies(self):
        """Test that services can be instantiated with their dependencies"""
        # This test verifies that the service constructors work
        # In a real scenario, we'd use mocks for all dependencies

        # For now, just test that classes can be imported and have correct interfaces
        assert hasattr(RSSFetcher, 'fetch_feed')
        assert hasattr(RSSFetcher, 'fetch_feeds')
        assert hasattr(TranslationService, 'translate_async')
        assert hasattr(TranslationService, 'prepare_translations')
        assert hasattr(MediaExtractor, 'extract_image')
        assert hasattr(MediaExtractor, 'extract_video')