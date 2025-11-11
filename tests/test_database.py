import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta
from api.database import (
    get_db_pool,
    close_db_pool,
    create_user,
    get_user_by_email,
    get_user_by_id,
    update_user,
    delete_user,
    activate_user,
    update_user_password,
    save_verification_code,
    verify_user_email,
    get_active_verification_code,
    mark_verification_code_used,
    save_password_reset_token,
    get_password_reset_token,
    delete_password_reset_token,
    update_user_categories,
    get_all_category_ids,
    get_user_categories,
    create_user_rss_feed,
    get_user_rss_feeds,
    get_user_rss_feed_by_id,
    update_user_rss_feed,
    delete_user_rss_feed,
    get_user_rss_items_list,
    get_user_rss_items_list_by_feed,
    get_rss_item_by_id,
    get_rss_item_by_id_full,
    get_all_rss_items_list,
    get_all_categories_list,
    activate_user_and_use_verification_code,
    confirm_password_reset_transaction,
    get_all_sources_list,
    get_recent_rss_items_for_broadcast,
)


@pytest.mark.asyncio
class TestDatabaseFunctions:
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

    async def test_get_db_pool_success(self, mock_pool):
        with patch('config.get_shared_db_pool', return_value=mock_pool):
            result = await get_db_pool()
            assert result == mock_pool

    async def test_get_db_pool_failure(self):
        with patch('config.get_shared_db_pool', side_effect=Exception("DB error")):
            result = await get_db_pool()
            assert result is None

    async def test_close_db_pool_success(self):
        with patch('config.close_shared_db_pool', return_value=None):
            await close_db_pool()

    async def test_close_db_pool_failure(self):
        with patch('config.close_shared_db_pool', side_effect=Exception("DB error")):
            await close_db_pool()

    async def test_create_user_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 'test@example.com', 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('email',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await create_user(mock_pool, 'test@example.com', 'hashed_pass', 'en')
        assert result['email'] == 'test@example.com'

    async def test_create_user_failure(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.execute.side_effect = Exception("DB error")

        result = await create_user(mock_pool, 'test@example.com', 'hashed_pass', 'en')
        assert result is None

    async def test_get_user_by_email_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 'test@example.com', 'hashed_pass', 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('email',), ('password_hash',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await get_user_by_email(mock_pool, 'test@example.com')
        assert result['email'] == 'test@example.com'

    async def test_get_user_by_email_not_found(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        result = await get_user_by_email(mock_pool, 'test@example.com')
        assert result is None

    async def test_get_user_by_id_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 'test@example.com', 'hashed_pass', 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('email',), ('password_hash',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await get_user_by_id(mock_pool, 1)
        assert result['id'] == 1

    async def test_update_user_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 'new@example.com', 'hashed_pass', 'es', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('email',), ('password_hash',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await update_user(mock_pool, 1, {'email': 'new@example.com', 'language': 'es'})
        assert result['email'] == 'new@example.com'

    async def test_update_user_no_changes(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 'test@example.com', 'hashed_pass', 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('email',), ('password_hash',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await update_user(mock_pool, 1, {})
        assert result['email'] == 'test@example.com'

    async def test_delete_user_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await delete_user(mock_pool, 1)
        assert result is True

    async def test_delete_user_not_found(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 0

        result = await delete_user(mock_pool, 1)
        assert result is False

    async def test_activate_user_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await activate_user(mock_pool, 1)
        assert result is True

    async def test_update_user_password_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await update_user_password(mock_pool, 1, 'new_hashed_pass')
        assert result is True

    async def test_save_verification_code_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

        result = await save_verification_code(mock_pool, 1, '123456', datetime.utcnow() + timedelta(hours=1))
        assert result is True

    async def test_verify_user_email_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1,)

        result = await verify_user_email(mock_pool, 'test@example.com', '123456')
        assert result == 1

    async def test_verify_user_email_not_found(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        result = await verify_user_email(mock_pool, 'test@example.com', '123456')
        assert result is None

    async def test_get_active_verification_code_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 1, '123456', datetime.utcnow(), datetime.utcnow() + timedelta(hours=1), None)
        mock_cur.description = [('id',), ('user_id',), ('verification_code',), ('created_at',), ('expires_at',), ('used_at',)]

        result = await get_active_verification_code(mock_pool, 1, '123456')
        assert result['verification_code'] == '123456'

    async def test_mark_verification_code_used_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await mark_verification_code_used(mock_pool, 1)
        assert result is True

    async def test_save_password_reset_token_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

        result = await save_password_reset_token(mock_pool, 1, 'token123', datetime.utcnow() + timedelta(hours=1))
        assert result is True

    async def test_get_password_reset_token_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, datetime.utcnow() + timedelta(hours=1))

        result = await get_password_reset_token(mock_pool, 'token123')
        assert result['user_id'] == 1

    async def test_get_password_reset_token_expired(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = None

        result = await get_password_reset_token(mock_pool, 'token123')
        assert result is None

    async def test_delete_password_reset_token_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

        result = await delete_password_reset_token(mock_pool, 'token123')
        assert result is True

    async def test_update_user_categories_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur

        result = await update_user_categories(mock_pool, 1, {1, 2, 3})
        assert result is True

    async def test_get_all_category_ids_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchall.return_value = [(1,), (2,), (3,)]

        result = await get_all_category_ids(mock_pool)
        assert result == {1, 2, 3}

    async def test_get_user_categories_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(1, 'Tech'), (2, 'Sports'), None])

        result = await get_user_categories(mock_pool, 1)
        assert len(result) == 2
        assert result[0]['name'] == 'Tech'

    async def test_create_user_rss_feed_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 1, 'http://example.com/rss', 'Test Feed', 1, 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('user_id',), ('url',), ('name',), ('category_id',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await create_user_rss_feed(mock_pool, 1, 'http://example.com/rss', 'Test Feed', 1, 'en')
        assert result['name'] == 'Test Feed'

    async def test_get_user_rss_feeds_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(1, 1, 'http://example.com/rss', 'Test Feed', 1, 'en', True, datetime.utcnow(), datetime.utcnow()), None])

        result = await get_user_rss_feeds(mock_pool, 1, 10, 0)
        assert len(result) == 1
        assert result[0]['name'] == 'Test Feed'

    async def test_get_user_rss_feed_by_id_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 1, 'http://example.com/rss', 'Test Feed', 1, 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('user_id',), ('url',), ('name',), ('category_id',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await get_user_rss_feed_by_id(mock_pool, 1, 1)
        assert result['id'] == 1

    async def test_update_user_rss_feed_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, 1, 'http://example.com/rss', 'Updated Feed', 1, 'en', True, datetime.utcnow(), datetime.utcnow())
        mock_cur.description = [('id',), ('user_id',), ('url',), ('name',), ('category_id',), ('language',), ('is_active',), ('created_at',), ('updated_at',)]

        result = await update_user_rss_feed(mock_pool, 1, 1, {'name': 'Updated Feed'})
        assert result['name'] == 'Updated Feed'

    async def test_delete_user_rss_feed_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.rowcount = 1

        result = await delete_user_rss_feed(mock_pool, 1, 1)
        assert result is True

    async def test_activate_user_and_use_verification_code_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1,)

        result = await activate_user_and_use_verification_code(mock_pool, 1, '123456')
        assert result is True

    async def test_confirm_password_reset_transaction_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone.return_value = (1, datetime.utcnow() + timedelta(hours=1))
        mock_cur.rowcount = 1

        result = await confirm_password_reset_transaction(mock_pool, 'token123', 'new_hashed_pass')
        assert result is True

    async def test_get_all_categories_list_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(2,), (1, 'Tech'), (2, 'Sports'), None])

        total_count, results = await get_all_categories_list(mock_pool, 10, 0)
        assert total_count == 2
        assert len(results) == 2
        assert results[0]['name'] == 'Tech'

    async def test_get_all_sources_list_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[(2,), (1, 'BBC', 'Description', 'bbc', 'logo.png', 'http://bbc.com'), None])

        total_count, results = await get_all_sources_list(mock_pool, 10, 0)
        assert total_count == 2
        assert len(results) == 1
        assert results[0]['name'] == 'BBC'

    async def test_get_recent_rss_items_for_broadcast_success(self, mock_pool, mock_conn, mock_cur):
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_conn.cursor.return_value.__aenter__.return_value = mock_cur
        mock_cur.fetchone = AsyncMock(side_effect=[('news1', 'Title', 'en', 'Tech', datetime.utcnow(), 'Title RU', 'Content RU', None, None, None, None, None, None), None])
        mock_cur.description = [('news_id',), ('original_title',), ('original_language',), ('category_name',), ('published_at',), ('title_ru',), ('content_ru',), ('title_en',), ('content_en',), ('title_de',), ('content_de',), ('title_fr',), ('content_fr',)]

        result = await get_recent_rss_items_for_broadcast(mock_pool, datetime.utcnow() - timedelta(hours=1))
        assert len(result) == 1
        assert result[0]['news_id'] == 'news1'