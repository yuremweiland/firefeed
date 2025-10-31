import logging
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.middleware import limiter
from api import database, models
from api.deps import get_current_user, validate_rss_url

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/users/me/rss-feeds",
    tags=["user_rss_feeds"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        403: {"description": "Forbidden - Access to resource denied"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)


@router.post(
    "/",
    response_model=models.UserRSSFeedResponse,
    status_code=201,
    summary="Create a new user RSS feed",
    description="""
    Create a new custom RSS feed for the authenticated user.

    This endpoint allows users to add their own RSS feeds to the system.
    The feed will be processed and news items will be available through the RSS items endpoints.

    **Validation:**
    - RSS URL format validation
    - Feed name length (max 255 characters)
    - User ownership verification

    **Rate limit:** 300 requests per minute
    """,
    responses={
        201: {
            "description": "RSS feed successfully created",
            "model": models.UserRSSFeedResponse
        },
        400: {
            "description": "Bad Request - Invalid URL or feed name too long",
            "model": models.HTTPError
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def create_user_rss_feed(request: Request, feed: models.UserRSSFeedCreate, current_user: dict = Depends(get_current_user)):
    # Validate RSS URL
    if not validate_rss_url(feed.url):
        raise HTTPException(status_code=400, detail="Invalid RSS URL format")

    # Validate input lengths
    if feed.name and len(feed.name) > 255:
        raise HTTPException(status_code=400, detail="Feed name too long (max 255 characters)")

    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    new_feed = await database.create_user_rss_feed(
        pool, current_user["id"], feed.url, feed.name, feed.category_id, feed.language
    )
    if not new_feed:
        raise HTTPException(status_code=500, detail="Failed to create RSS feed")
    return models.UserRSSFeedResponse(**new_feed)


@router.get(
    "/",
    response_model=models.PaginatedResponse[models.UserRSSFeedResponse],
    summary="Get user's RSS feeds",
    description="""
    Retrieve a paginated list of the authenticated user's custom RSS feeds.

    Returns all RSS feeds created by the current user, ordered by creation date (newest first).

    **Pagination:**
    - `limit`: Number of feeds per page (1-100, default: 50)
    - `offset`: Number of feeds to skip (default: 0)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "List of user RSS feeds",
            "model": models.PaginatedResponse[models.UserRSSFeedResponse]
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_user_rss_feeds(
    request: Request,
    limit: int = Query(50, le=100, gt=0, description="Number of feeds per page (1-100)"),
    offset: int = Query(0, ge=0, description="Number of feeds to skip"),
    current_user: dict = Depends(get_current_user),
):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")
    feeds = await database.get_user_rss_feeds(pool, current_user["id"], limit, offset)
    feed_models = [models.UserRSSFeedResponse(**feed) for feed in feeds]
    return models.PaginatedResponse[models.UserRSSFeedResponse](count=len(feed_models), results=feed_models)


@router.get(
    "/{feed_id}",
    response_model=models.UserRSSFeedResponse,
    summary="Get specific user RSS feed",
    description="""
    Retrieve details of a specific RSS feed owned by the authenticated user.

    Returns complete information about the RSS feed including URL, name, category, and status.

    **Path parameters:**
    - `feed_id`: Unique identifier of the RSS feed

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "RSS feed details",
            "model": models.UserRSSFeedResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        404: {
            "description": "Not Found - RSS feed not found or doesn't belong to user",
            "model": models.HTTPError
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_user_rss_feed(request: Request, feed_id: int, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")
    feed = await database.get_user_rss_feed_by_id(pool, current_user["id"], feed_id)
    if not feed:
        raise HTTPException(status_code=404, detail="RSS feed not found")
    return models.UserRSSFeedResponse(**feed)


@router.put(
    "/{feed_id}",
    response_model=models.UserRSSFeedResponse,
    summary="Update user RSS feed",
    description="""
    Update an existing RSS feed owned by the authenticated user.

    Allows updating feed name, category assignment, and active status.
    Only provided fields will be updated.

    **Path parameters:**
    - `feed_id`: Unique identifier of the RSS feed to update

    **Validation:**
    - Feed name length (max 255 characters)
    - User ownership verification

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "RSS feed successfully updated",
            "model": models.UserRSSFeedResponse
        },
        400: {
            "description": "Bad Request - Invalid feed name length",
            "model": models.HTTPError
        },
        401: {"description": "Unauthorized - Authentication required"},
        404: {
            "description": "Not Found - RSS feed not found or doesn't belong to user",
            "model": models.HTTPError
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def update_user_rss_feed(request: Request, feed_id: int, feed_update: models.UserRSSFeedUpdate, current_user: dict = Depends(get_current_user)):
    # Validate input lengths
    if feed_update.name is not None and len(feed_update.name) > 255:
        raise HTTPException(status_code=400, detail="Feed name too long (max 255 characters)")

    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")
    update_data = {}
    if feed_update.name is not None:
        update_data["name"] = feed_update.name
    if feed_update.category_id is not None:
        update_data["category_id"] = feed_update.category_id
    if feed_update.is_active is not None:
        update_data["is_active"] = feed_update.is_active
    updated_feed = await database.update_user_rss_feed(pool, current_user["id"], feed_id, update_data)
    if not updated_feed:
        raise HTTPException(status_code=404, detail="RSS feed not found or failed to update")
    return models.UserRSSFeedResponse(**updated_feed)


@router.delete(
    "/{feed_id}",
    status_code=204,
    summary="Delete user RSS feed",
    description="""
    Permanently delete an RSS feed owned by the authenticated user.

    This action cannot be undone. All associated RSS items will remain in the system
    but will no longer be updated from this feed.

    **Path parameters:**
    - `feed_id`: Unique identifier of the RSS feed to delete

    **Rate limit:** 300 requests per minute
    """,
    responses={
        204: {"description": "RSS feed successfully deleted"},
        401: {"description": "Unauthorized - Authentication required"},
        404: {
            "description": "Not Found - RSS feed not found or doesn't belong to user",
            "model": models.HTTPError
        },
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def delete_user_rss_feed(request: Request, feed_id: int, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")
    success = await database.delete_user_rss_feed(pool, current_user["id"], feed_id)
    if not success:
        raise HTTPException(status_code=404, detail="RSS feed not found")
    return
