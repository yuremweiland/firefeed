import logging
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query, Request

from api.middleware import limiter
from api import database, models
from api.deps import format_datetime, get_full_image_url, build_translations_dict, validate_rss_items_query_params, sanitize_search_phrase
import config

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["rss_items"],
    responses={
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)


def process_rss_items_results(results, columns, display_language, original_language, include_all_translations):
    rss_items_list = []
    for row in results:
        row_dict = dict(zip(columns, row))
        translations = build_translations_dict(row_dict)
        if display_language is not None and original_language and display_language != original_language:
            if not translations or display_language not in translations:
                continue
        if display_language is not None and original_language:
            translations[original_language] = {
                "title": row_dict["original_title"],
                "content": row_dict["original_content"],
            }
        item_data = {
            "news_id": row_dict["news_id"],
            "original_title": row_dict["original_title"],
            "original_content": row_dict["original_content"],
            "original_language": row_dict["original_language"],
            "image_url": get_full_image_url(row_dict["image_filename"]),
            "category": row_dict["category_name"],
            "source": row_dict["source_name"],
            "source_url": row_dict["source_url"],
            "published_at": format_datetime(row_dict["published_at"]),
            "translations": translations,
        }
        rss_items_list.append(models.RSSItem(**item_data))

    return rss_items_list


@router.get(
    "/api/v1/rss-items/",
    summary="Get RSS items with filtering and pagination",
    description="""
    Retrieve a filtered and paginated list of RSS items (news articles).

    This endpoint supports comprehensive filtering by language, category, source, publication status,
    date range, and full-text search. Results can be paginated using offset-based or cursor-based pagination.

    **Filtering Options:**
    - `display_language`: Language for displaying content (ru, en, de, fr)
    - `original_language`: Filter by original article language
    - `category_id`: Filter by news categories (multiple values allowed)
    - `source_id`: Filter by news sources (multiple values allowed)
    - `telegram_published`: Filter by Telegram publication status (true/false)
    - `from_date`: Filter articles published after this timestamp (Unix timestamp)
    - `search_phrase`: Full-text search in titles and content

    **Pagination:**
    - **Offset-based:** Use `limit` and `offset` parameters
    - **Cursor-based:** Use `cursor_published_at` and `cursor_rss_item_id` for keyset pagination

    **Rate limit:** 1000 requests per minute
    """,
    responses={
        200: {
            "description": "List of RSS items",
            "content": {
                "application/json": {
                    "example": {
                        "count": 50,
                        "results": [
                            {
                                "rss_item_id": "abc123",
                                "original_title": "Breaking News",
                                "original_content": "Full article content...",
                                "original_language": "en",
                                "image_url": "https://firefeed.net/data/images/2024/01/01/abc123.jpg",
                                "category": "Technology",
                                "source": "Tech News",
                                "source_url": "https://technews.com/article123",
                                "published_at": "2024-01-01T12:00:00Z",
                                "translations": {
                                    "ru": {"title": "Главные новости", "content": "Полный текст статьи..."},
                                    "de": {"title": "Wichtige Nachrichten", "content": "Vollständiger Artikeltext..."}
                                }
                            }
                        ]
                    }
                }
            }
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("1000/minute")
async def get_rss_items(
    request: Request,
    display_language: Optional[str] = Query(None),
    original_language: Optional[str] = Query(None),
    category_id: Optional[List[int]] = Query(None),
    source_id: Optional[List[int]] = Query(None),
    telegram_published: Optional[bool] = Query(None),
    from_date: Optional[int] = Query(None),
    search_phrase: Optional[str] = Query(None, alias="searchPhrase"),
    include_all_translations: Optional[bool] = Query(None),
    cursor_published_at: Optional[int] = Query(None),
    cursor_rss_item_id: Optional[str] = Query(None),
    limit: Optional[int] = Query(50, le=100, gt=0),
    offset: Optional[int] = Query(0, ge=0),
):
    if display_language is None:
        include_all_translations = True

    # Sanitize search phrase
    if search_phrase:
        search_phrase = sanitize_search_phrase(search_phrase)

    from_datetime, before_published_at = validate_rss_items_query_params(display_language, from_date, cursor_published_at)
    page_offset = 0 if (cursor_published_at is not None or cursor_rss_item_id is not None) else offset

    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")

    try:
        total_count, results, columns = await database.get_all_rss_items_list(
            pool,
            display_language,
            original_language,
            category_id,
            source_id,
            telegram_published,
            from_datetime,
            search_phrase,
            include_all_translations or False,
            before_published_at,
            cursor_rss_item_id,
            limit,
            page_offset,
        )
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_rss_items: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

    rss_items_list = process_rss_items_results(results, columns, display_language, original_language, include_all_translations)
    return {"count": len(rss_items_list), "results": rss_items_list}


@router.get(
    "/api/v1/rss-items/{rss_item_id}",
    response_model=models.RSSItem,
    summary="Get specific RSS item by ID",
    description="""
    Retrieve detailed information about a specific RSS item (news article) by its unique identifier.

    Returns the complete article data including all available translations, metadata, and media URLs.

    **Path parameters:**
    - `rss_item_id`: Unique identifier of the RSS item

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "RSS item details",
            "model": models.RSSItem
        },
        404: {
            "description": "Not Found - RSS item not found",
            "model": models.HTTPError
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_rss_item_by_id(request: Request, rss_item_id: str):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")

    try:
        full_result = await database.get_rss_item_by_id_full(pool, rss_item_id)
        if not full_result or not full_result[0]:
            raise HTTPException(status_code=404, detail="News item not found")
        row, columns = full_result
        row_dict = dict(zip(columns, row))
        item_data = {
            "news_id": row_dict["news_id"],
            "original_title": row_dict["original_title"],
            "original_content": row_dict["original_content"],
            "original_language": row_dict["original_language"],
            "image_url": get_full_image_url(row_dict["image_filename"]),
            "category": row_dict["category_name"],
            "source": row_dict["source_name"],
            "source_url": row_dict["source_url"],
            "published_at": format_datetime(row_dict["published_at"]),
            "translations": build_translations_dict(row_dict),
        }
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_rss_item_by_id: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")
    return models.RSSItem(**item_data)


@router.get(
    "/api/v1/categories/",
    summary="Get available news categories",
    description="""
    Retrieve a paginated list of available news categories.

    Categories are used to classify news articles and can be used for filtering RSS items.
    Results can be filtered by associated source IDs.

    **Query parameters:**
    - `limit`: Number of categories per page (1-1000, default: 100)
    - `offset`: Number of categories to skip (default: 0)
    - `source_ids`: Filter categories by associated news sources (multiple values allowed)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "List of news categories",
            "content": {
                "application/json": {
                    "example": {
                        "count": 8,
                        "results": [
                            {"id": 1, "name": "Technology"},
                            {"id": 2, "name": "Politics"},
                            {"id": 3, "name": "Sports"}
                        ]
                    }
                }
            }
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_categories(
    request: Request,
    limit: Optional[int] = Query(100, le=1000, gt=0),
    offset: Optional[int] = Query(0, ge=0),
    source_ids: Optional[List[int]] = Query(None),
):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")

    try:
        total_count, results = await database.get_all_categories_list(pool, limit, offset, source_ids)
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_categories: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

    return {"count": total_count, "results": results}


@router.get(
    "/api/v1/sources/",
    summary="Get available news sources",
    description="""
    Retrieve a paginated list of available news sources.

    Sources represent the origin of news articles and can be used for filtering RSS items.
    Results can be filtered by associated category IDs.

    **Query parameters:**
    - `limit`: Number of sources per page (1-1000, default: 100)
    - `offset`: Number of sources to skip (default: 0)
    - `category_id`: Filter sources by associated categories (multiple values allowed)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "List of news sources",
            "content": {
                "application/json": {
                    "example": {
                        "count": 25,
                        "results": [
                            {
                                "id": 1,
                                "name": "BBC News",
                                "description": "British Broadcasting Corporation",
                                "alias": "bbc",
                                "logo": "bbc-logo.png",
                                "site_url": "https://bbc.com"
                            }
                        ]
                    }
                }
            }
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_sources(
    request: Request,
    limit: Optional[int] = Query(100, le=1000, gt=0),
    offset: Optional[int] = Query(0, ge=0),
    category_id: Optional[List[int]] = Query(None),
):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Ошибка подключения к базе данных")

    try:
        total_count, results = await database.get_all_sources_list(pool, limit, offset, category_id)
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_sources: {e}")
        raise HTTPException(status_code=500, detail="Внутренняя ошибка сервера")

    return {"count": total_count, "results": results}


@router.get(
    "/api/v1/languages/",
    summary="Get supported languages",
    description="""
    Retrieve the list of languages supported by the FireFeed system.

    These languages are available for content translation, user interface localization,
    and filtering RSS items by original or translated language.

    **Supported languages:**
    - `en`: English
    - `ru`: Russian (Русский)
    - `de`: German (Deutsch)
    - `fr`: French (Français)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "List of supported languages",
            "content": {
                "application/json": {
                    "example": {
                        "results": ["en", "ru", "de", "fr"]
                    }
                }
            }
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_languages(request: Request):
    return {"results": config.SUPPORTED_LANGUAGES}


@router.get(
    "/api/v1/health",
    summary="Health check endpoint",
    description="""
    Check the health status of the FireFeed API and its dependencies.

    This endpoint provides information about the system's operational status,
    including database connectivity and connection pool statistics.

    **Response fields:**
    - `status`: Overall system status ("ok" if healthy)
    - `database`: Database connection status ("ok" or "error")
    - `db_pool`: Database connection pool information
        - `total_connections`: Total number of connections in pool
        - `free_connections`: Number of available connections

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "System health information",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "database": "ok",
                        "db_pool": {
                            "total_connections": 20,
                            "free_connections": 15
                        }
                    }
                }
            }
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {
            "description": "Internal Server Error - System unhealthy",
            "content": {
                "application/json": {
                    "example": {
                        "status": "ok",
                        "database": "error",
                        "db_pool": {
                            "total_connections": 0,
                            "free_connections": 0
                        }
                    }
                }
            }
        }
    }
)
@limiter.limit("300/minute")
async def health_check(request: Request):
    try:
        pool = await database.get_db_pool()
        if pool:
            db_status = "ok"
            pool_total = pool.size
            pool_free = pool.freesize
        else:
            db_status = "error"
            pool_total = 0
            pool_free = 0
    except Exception as e:
        db_status = "error"
        pool_total = 0
        pool_free = 0
        logger.error(f"[Healthcheck] Database connection error: {e}")
    return {
        "status": "ok",
        "database": db_status,
        "db_pool": {"total_connections": pool_total, "free_connections": pool_free},
    }
