import logging
from typing import Optional, List, Set
from fastapi import APIRouter, Depends, HTTPException, Query

from api.middleware import limiter
from api import database, models

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/users/me/categories",
    tags=["user_categories"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)


# get_current_user is imported from api.deps


@router.get(
    "/",
    response_model=models.UserCategoriesResponse,
    summary="Get user's subscribed categories",
    description="""
    Retrieve the list of news categories the authenticated user is subscribed to.

    Returns category IDs that the user has selected for personalized news feeds.
    Results can be filtered by associated source IDs.

    **Query parameters:**
    - `source_ids`: Filter categories by associated news sources (multiple values allowed)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "User's subscribed categories",
            "model": models.UserCategoriesResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_user_categories(
    current_user: dict = Depends(get_current_user),
    source_ids: Optional[List[int]] = Query(None, description="Filter by associated source IDs"),
):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    categories = await database.get_user_categories(pool, current_user["id"], source_ids)
    return models.UserCategoriesResponse(category_ids=[cat["id"] for cat in categories])


@router.put(
    "/",
    response_model=models.SuccessResponse,
    summary="Update user's category subscriptions",
    description="""
    Update the list of news categories the authenticated user is subscribed to.

    This replaces all existing category subscriptions with the provided list.
    Categories determine which news articles appear in the user's personalized feeds.

    **Validation:**
    - All provided category IDs must exist in the system
    - Invalid category IDs will result in a 400 error

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "Categories updated successfully",
            "model": models.SuccessResponse
        },
        400: {
            "description": "Bad Request - Invalid category IDs provided",
            "model": models.HTTPError
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def update_user_categories(
    category_update: models.UserCategoriesUpdate, current_user: dict = Depends(get_current_user)
):
    category_ids: Set[int] = category_update.category_ids
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    existing_categories = await database.get_all_category_ids(pool)
    invalid_ids = category_ids - existing_categories
    if invalid_ids:
        raise HTTPException(status_code=400, detail=f"Invalid category IDs: {list(invalid_ids)}")

    success = await database.update_user_categories(pool, current_user["id"], category_ids)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update user categories")

    return models.SuccessResponse(message="User categories successfully updated")
