import logging
import secrets
from fastapi import APIRouter, Depends, HTTPException, Request

from api.middleware import limiter
from api import database, models
from api.deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/api-keys",
    tags=["api-keys"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)


@router.post(
    "/generate-own",
    response_model=models.UserApiKeyGenerateResponse,
    summary="Generate API key for current user",
    description="""
    Generate a new API key for the currently authenticated user.

    The key will have default limits (1000 requests per day, 100 per hour) and no expiration.

    **Rate limit:** 10 requests per minute
    """,
    responses={
        200: {
            "description": "API key generated successfully",
            "model": models.UserApiKeyGenerateResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("10/minute")
async def generate_own_api_key(request: Request, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    # Generate unique key
    key = secrets.token_urlsafe(32)
    limits = {"requests_per_day": 1000, "requests_per_hour": 100}

    api_key = await database.create_user_api_key(pool, current_user["id"], key, limits)
    if not api_key:
        raise HTTPException(status_code=500, detail="Failed to create API key")

    return models.UserApiKeyGenerateResponse(
        id=api_key["id"],
        user_id=api_key["user_id"],
        key=key,
        limits=limits,
        created_at=api_key["created_at"],
        expires_at=api_key.get("expires_at")
    )


@router.get(
    "/list",
    response_model=list[models.UserApiKeyResponse],
    summary="List user's API keys",
    description="""
    Retrieve a list of API keys for the currently authenticated user.

    **Rate limit:** 60 requests per minute
    """,
    responses={
        200: {
            "description": "List of API keys",
            "model": list[models.UserApiKeyResponse]
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("60/minute")
async def list_user_api_keys(request: Request, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    api_keys = await database.get_user_api_keys(pool, current_user["id"])
    return [models.UserApiKeyResponse(**key) for key in api_keys]


@router.patch(
    "/{key_id}",
    response_model=models.UserApiKeyGenerateResponse,
    summary="Update API key",
    description="""
    Update an API key for the currently authenticated user.

    Allows updating active status and limits.

    **Rate limit:** 30 requests per minute
    """,
    responses={
        200: {
            "description": "API key updated successfully",
            "model": models.UserApiKeyResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        404: {"description": "API key not found"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("30/minute")
async def update_api_key(
    request: Request,
    key_id: int,
    key_update: models.UserApiKeyUpdate,
    current_user: dict = Depends(get_current_user)
):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    # Check if key exists and belongs to user
    existing_key = await database.get_user_api_key_by_id(pool, current_user["id"], key_id)
    if not existing_key:
        raise HTTPException(status_code=404, detail="API key not found")

    update_data = {}
    if key_update.is_active is not None:
        update_data["is_active"] = key_update.is_active
    if key_update.limits is not None:
        update_data["limits"] = key_update.limits

    updated_key = await database.update_user_api_key(pool, current_user["id"], key_id, update_data)
    if not updated_key:
        raise HTTPException(status_code=500, detail="Failed to update API key")

    return models.UserApiKeyResponse(**updated_key)


@router.delete(
    "/{key_id}",
    status_code=204,
    summary="Delete API key",
    description="""
    Delete an API key for the currently authenticated user.

    **Rate limit:** 30 requests per minute
    """,
    responses={
        204: {"description": "API key deleted successfully"},
        401: {"description": "Unauthorized - Authentication required"},
        404: {"description": "API key not found"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("30/minute")
async def delete_api_key(request: Request, key_id: int, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    # Check if key exists and belongs to user
    existing_key = await database.get_user_api_key_by_id(pool, current_user["id"], key_id)
    if not existing_key:
        raise HTTPException(status_code=404, detail="API key not found")

    success = await database.delete_user_api_key(pool, current_user["id"], key_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete API key")

    return