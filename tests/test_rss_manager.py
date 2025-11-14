import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime
from services.rss import RSSManager
from utils.media_extractors import extract_image_from_rss_item, extract_video_from_rss_item


@pytest.mark.asyncio
class TestRSSManager:
    @pytest.fixture
    def rss_manager(self):
        return RSSManager()

    @pytest.fixture
    def mock_pool(self):
        pool = AsyncMock()
        return pool

    @pytest.fixture
    def mock_conn(self):
        conn = AsyncMock()
        return conn

    @pytest.fixture
    def mock_cur(self):
        cur = AsyncMock()
        return cur

    async def test_get_pool(self, rss_manager, mock_pool):
        with patch('rss_manager.get_shared_db_pool', return_value=mock_pool):
            result = await rss_manager.get_pool()
            assert result == mock_pool

    async def test_close_pool(self, rss_manager):
        await rss_manager.close_pool()

    async def test_get_all_active_feeds_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[
            (1, 'http://example.com/rss', 'Test Feed', 'en', 1, 1, 'BBC', 'Tech'),
            None
        ])
        mock_cur.description = [('id',), ('url',), ('name',), ('language',), ('source_id',), ('category_id',), ('source_name',), ('category_name',)]

        result = await rss_manager.get_all_active_feeds()
        assert len(result) == 1
        assert result[0]['name'] == 'Test Feed'

    async def test_get_all_active_feeds_failure(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.execute.side_effect = Exception("DB error")

        result = await rss_manager.get_all_active_feeds()
        assert result == []

    async def test_get_feeds_by_category_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[
            (1, 'http://example.com/rss', 'Test Feed', 'en', 1, 1, 'BBC', 'Tech'),
            None
        ])
        mock_cur.description = [('id',), ('url',), ('name',), ('language',), ('source_id',), ('category_id',), ('source_name',), ('category_name',)]

        result = await rss_manager.get_feeds_by_category('Tech')
        assert len(result) == 1
        assert result[0]['category'] == 'Tech'

    async def test_get_feeds_by_language_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[
            (1, 'http://example.com/rss', 'Test Feed', 'en', 1, 1, 'BBC', 'Tech'),
            None
        ])
        mock_cur.description = [('id',), ('url',), ('name',), ('language',), ('source_id',), ('category_id',), ('source_name',), ('category_name',)]

        result = await rss_manager.get_feeds_by_language('en')
        assert len(result) == 1
        assert result[0]['lang'] == 'en'

    async def test_get_feeds_by_source_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[
            (1, 'http://example.com/rss', 'Test Feed', 'en', 1, 1, 'BBC', 'Tech'),
            None
        ])
        mock_cur.description = [('id',), ('url',), ('name',), ('language',), ('source_id',), ('category_id',), ('source_name',), ('category_name',)]

        result = await rss_manager.get_feeds_by_source('BBC')
        assert len(result) == 1
        assert result[0]['source'] == 'BBC'

    async def test_add_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(1,), (1,)])

        result = await rss_manager.add_feed('http://example.com/rss', 'Tech', 'BBC', 'en')
        assert result is True

    async def test_add_feed_category_not_found(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        result = await rss_manager.add_feed('http://example.com/rss', 'NonExistent', 'BBC', 'en')
        assert result is False

    async def test_update_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(1,), (1,)])
        mock_cur.rowcount = 1

        result = await rss_manager.update_feed(1, name='Updated Feed')
        assert result is True

    async def test_delete_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await rss_manager.delete_feed(1)
        assert result is True

    async def test_get_feed_cooldown_minutes_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (30,)

        result = await rss_manager.get_feed_cooldown_minutes(1)
        assert result == 30

    async def test_get_feed_cooldown_minutes_default(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        result = await rss_manager.get_feed_cooldown_minutes(1)
        assert result == 60

    async def test_get_max_news_per_hour_for_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (5,)

        result = await rss_manager.get_max_news_per_hour_for_feed(1)
        assert result == 5

    async def test_get_last_published_time_for_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (datetime.utcnow(),)

        result = await rss_manager.get_last_published_time_for_feed(1)
        assert isinstance(result, datetime)

    async def test_get_recent_rss_items_count_for_feed_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (5,)

        result = await rss_manager.get_recent_rss_items_count_for_feed(1, 60)
        assert result == 5

    def test_generate_news_id(self, rss_manager):
        title = "Test Title"
        content = "Test Content"
        link = "http://example.com"
        feed_id = 1

        news_id = rss_manager.generate_news_id(title, content, link, feed_id)
        assert isinstance(news_id, str)
        assert len(news_id) == 64  # SHA256 hex length

    async def test_check_for_duplicates_false(self, rss_manager):
        with patch('rss_manager.FireFeedDuplicateDetector') as mock_detector_class:
            mock_detector = AsyncMock()
            mock_detector.is_duplicate_strict.return_value = (False, {})
            mock_detector_class.return_value = mock_detector

            result = await rss_manager.check_for_duplicates("title", "content", "link", "en")
            assert result is False

    async def test_check_for_duplicates_true(self, rss_manager):
        with patch('rss_manager.FireFeedDuplicateDetector') as mock_detector_class:
            mock_detector = AsyncMock()
            mock_detector.is_duplicate_strict.return_value = (True, {"news_id": "duplicate_id"})
            mock_detector_class.return_value = mock_detector

            result = await rss_manager.check_for_duplicates("title", "content", "link", "en")
            assert result is True

    async def test_validate_rss_feed_success(self, rss_manager):
        with patch('feedparser.parse') as mock_parse:
            mock_feed = MagicMock()
            mock_feed.bozo = False
            mock_feed.entries = [{"title": "Test Entry"}]
            mock_parse.return_value = mock_feed

            headers = {"User-Agent": "Test"}
            result = await rss_manager.validate_rss_feed("http://example.com/rss", headers)
            assert result is True

    async def test_validate_rss_feed_no_entries(self, rss_manager):
        with patch('feedparser.parse') as mock_parse:
            mock_feed = MagicMock()
            mock_feed.bozo = False
            mock_feed.entries = []
            mock_parse.return_value = mock_feed

            headers = {"User-Agent": "Test"}
            result = await rss_manager.validate_rss_feed("http://example.com/rss", headers)
            assert result is False

    async def test_save_rss_item_to_db_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1,)  # category_id

        rss_item = {
            "id": "test_news_id",
            "title": "Test Title",
            "content": "Test Content",
            "lang": "en",
            "category": "Tech",
            "source": "BBC",
            "link": "http://example.com",
            "image_filename": "test.jpg"
        }

        result = await rss_manager.save_rss_item_to_db(rss_item, 1)
        assert result == "test_news_id"

    async def test_save_rss_item_to_db_category_not_found(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None  # category not found

        rss_item = {
            "id": "test_news_id",
            "title": "Test Title",
            "content": "Test Content",
            "lang": "en",
            "category": "NonExistent",
            "source": "BBC",
            "link": "http://example.com",
            "image_filename": "test.jpg"
        }

        result = await rss_manager.save_rss_item_to_db(rss_item, 1)
        assert result is None

    async def test_save_translations_to_db_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = ("en", "Original Title", "Original Content")

        translations = {
            "ru": {"title": "Русский заголовок", "content": "Русский контент"},
            "de": {"title": "Deutscher Titel", "content": "Deutscher Inhalt"}
        }

        result = await rss_manager.save_translations_to_db("test_news_id", translations)
        assert result is True

    async def test_save_translations_to_db_empty_translations(self, rss_manager):
        result = await rss_manager.save_translations_to_db("test_news_id", {})
        assert result is True

    async def test_save_translations_to_db_invalid_translations(self, rss_manager):
        result = await rss_manager.save_translations_to_db("test_news_id", "invalid")
        assert result is False

    async def test_extract_image_from_rss_item_media_thumbnail(self):
        item = {
            "media_thumbnail": [{"url": "http://example.com/image.jpg"}]
        }
        result = await extract_image_from_rss_item(item)
        assert result == "http://example.com/image.jpg"

    async def test_extract_image_from_rss_item_enclosure(self):
        item = {
            "enclosures": [{"type": "image/jpeg", "href": "http://example.com/image.jpg"}]
        }
        result = await extract_image_from_rss_item(item)
        assert result == "http://example.com/image.jpg"

    async def test_extract_image_from_rss_item_no_image(self):
        item = {"title": "Test Item"}
        result = await extract_image_from_rss_item(item)
        assert result is None

    async def test_extract_video_from_rss_item_enclosure(self):
        item = {
            "enclosures": [{"type": "video/mp4", "href": "http://example.com/video.mp4"}]
        }
        result = await extract_video_from_rss_item(item)
        assert result == "http://example.com/video.mp4"

    async def test_extract_video_from_rss_item_no_video(self):
        item = {"title": "Test Item"}
        result = await extract_video_from_rss_item(item)
        assert result is None

    async def test_fetch_unprocessed_rss_items_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[
            ("news_id", "Title", "Content", "en", "image.jpg", 1, 1, None, datetime.utcnow(), datetime.utcnow(), "Tech", "BBC", "http://example.com"),
            None
        ])
        mock_cur.description = [('news_id',), ('original_title',), ('original_content',), ('original_language',), ('image_filename',), ('category_id',), ('rss_feed_id',), ('telegram_published_at',), ('created_at',), ('updated_at',), ('category_name',), ('source_name',), ('source_url',)]

        result = await rss_manager.fetch_unprocessed_rss_items()
        assert len(result) == 1
        assert result[0]['news_id'] == 'news_id'

    async def test_cleanup_duplicates_success(self, rss_manager, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 5

        result = await rss_manager.cleanup_duplicates()
        assert result == []