import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bot import (
    PreparedRSSItem,
    mark_translation_as_published,
    mark_original_as_published,
    get_translation_id,
    api_get,
    get_rss_items_list,
    get_rss_item_by_id,
    get_categories,
    get_sources,
    get_languages,
    get_main_menu_keyboard,
    set_current_user_language,
    get_current_user_language,
    process_rss_item,
    monitor_rss_items_task,
    initialize_http_session,
    cleanup_http_session,
)


@pytest.mark.asyncio
class TestBotFunctions:
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

    async def test_mark_translation_as_published_success(self, mock_pool, mock_conn, mock_cur):
        with patch('bot.get_shared_db_pool', return_value=mock_pool):
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

            result = await mark_translation_as_published(1, 12345, 678)
            assert result is True

    async def test_mark_original_as_published_success(self, mock_pool, mock_conn, mock_cur):
        with patch('bot.get_shared_db_pool', return_value=mock_pool):
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

            result = await mark_original_as_published("news123", 12345, 678)
            assert result is True

    async def test_get_translation_id_success(self, mock_pool, mock_conn, mock_cur):
        with patch('bot.get_shared_db_pool', return_value=mock_pool):
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
            mock_cur.fetchone.return_value = (42,)

            result = await get_translation_id("news123", "ru")
            assert result == 42

    async def test_get_translation_id_not_found(self, mock_pool, mock_conn, mock_cur):
        with patch('bot.get_shared_db_pool', return_value=mock_pool):
            mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
            mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
            mock_cur.fetchone.return_value = None

            result = await get_translation_id("news123", "ru")
            assert result is None

    async def test_api_get_success(self):
        with patch('bot.http_session') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"data": "test"}
            mock_session.get.return_value.__aenter__.return_value = mock_response

            result = await api_get("/test")
            assert result == {"data": "test"}

    async def test_api_get_failure(self):
        with patch('bot.http_session') as mock_session:
            mock_response = AsyncMock()
            mock_response.status = 404
            mock_response.text.return_value = "Not Found"
            mock_session.get.return_value.__aenter__.return_value = mock_response

            result = await api_get("/test")
            assert result == {}

    async def test_get_rss_items_list(self):
        with patch('bot.api_get', return_value={"results": []}):
            result = await get_rss_items_list(display_language="en", limit=10)
            assert result == {"results": []}

    async def test_get_rss_item_by_id(self):
        with patch('bot.api_get', return_value={"news_id": "123"}):
            result = await get_rss_item_by_id("123", "en")
            assert result == {"news_id": "123"}

    async def test_get_categories(self):
        with patch('bot.api_get', return_value={"results": ["Tech", "Sports"]}):
            result = await get_categories()
            assert result == ["Tech", "Sports"]

    async def test_get_sources(self):
        with patch('bot.api_get', return_value={"results": ["BBC", "CNN"]}):
            result = await get_sources()
            assert result == ["BBC", "CNN"]

    async def test_get_languages(self):
        with patch('bot.api_get', return_value={"results": ["en", "ru"]}):
            result = await get_languages()
            assert result == ["en", "ru"]

    def test_get_main_menu_keyboard(self):
        keyboard = get_main_menu_keyboard("en")
        assert keyboard is not None
        assert len(keyboard.keyboard) == 2  # 2 rows
        assert len(keyboard.keyboard[0]) == 2  # 2 buttons per row

    async def test_set_current_user_language(self):
        with patch('bot.user_manager') as mock_um:
            mock_um.set_user_language = AsyncMock()
            with patch('bot.USER_LANGUAGES', {}):
                await set_current_user_language(123, "ru")
                assert mock_um.set_user_language.called

    async def test_get_current_user_language_from_memory(self):
        with patch('bot.USER_LANGUAGES', {123: "ru"}):
            result = await get_current_user_language(123)
            assert result == "ru"

    async def test_get_current_user_language_from_db(self):
        with patch('bot.USER_LANGUAGES', {}):
            with patch('bot.user_manager') as mock_um:
                mock_um.get_user_language = AsyncMock(return_value="de")
                result = await get_current_user_language(123)
                assert result == "de"

    async def test_get_current_user_language_default(self):
        with patch('bot.USER_LANGUAGES', {}):
            with patch('bot.user_manager') as mock_um:
                mock_um.get_user_language = AsyncMock(return_value=None)
                result = await get_current_user_language(123)
                assert result == "en"

    async def test_process_rss_item(self):
        prepared_item = PreparedRSSItem(
            original_data={"id": "news123", "title": "Test", "category": "Tech"},
            translations={"ru": {"title": "Тест"}},
            image_filename="test.jpg"
        )

        with patch('bot.post_to_channel') as mock_post:
            with patch('bot.send_personal_rss_items') as mock_send:
                with patch('bot.CHANNEL_CATEGORIES', ["Tech"]):
                    context = MagicMock()
                    rss_item_from_api = {
                        "news_id": "news123",
                        "original_title": "Test",
                        "original_content": "Content",
                        "category": "Tech",
                        "source": "BBC",
                        "original_language": "en",
                        "source_url": "http://example.com",
                        "image_url": "test.jpg",
                        "translations": {"ru": {"title": "Тест"}}
                    }

                    result = await process_rss_item(context, rss_item_from_api)
                    assert result is True
                    assert mock_post.called
                    assert mock_send.called

    async def test_monitor_rss_items_task_success(self):
        with patch('bot.get_rss_items_list', return_value={"results": []}):
            context = MagicMock()
            await monitor_rss_items_task(context)

    async def test_monitor_rss_items_task_with_items(self):
        rss_items = [
            {"news_id": "1", "original_title": "Test 1"},
            {"news_id": "2", "original_title": "Test 2"}
        ]
        with patch('bot.get_rss_items_list', return_value={"results": rss_items}):
            with patch('bot.process_rss_item', return_value=True) as mock_process:
                context = MagicMock()
                await monitor_rss_items_task(context)
                assert mock_process.call_count == 2

    async def test_initialize_http_session(self):
        with patch('bot.http_session', None):
            with patch('aiohttp.ClientSession') as mock_session:
                await initialize_http_session()
                assert mock_session.called

    async def test_cleanup_http_session(self):
        mock_session = AsyncMock()
        with patch('bot.http_session', mock_session):
            await cleanup_http_session()
            assert mock_session.close.called