import logging
from fastapi import APIRouter, Depends, HTTPException

from api.middleware import limiter
from api import database, models
from api.deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/users",
    tags=["users"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)


@router.get(
    "/me",
    response_model=models.UserResponse,
    summary="Get current user profile",
    description="""
    Retrieve the profile information of the currently authenticated user.

    Returns basic user information including email, language preference, and account status.

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "Current user profile",
            "model": models.UserResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_current_user_profile(current_user: dict = Depends(get_current_user)):
    return models.UserResponse(**current_user)


@router.put(
    "/me",
    response_model=models.UserResponse,
    summary="Update current user profile",
    description="""
    Update the profile information of the currently authenticated user.

    Allows updating email address and language preference. Only provided fields will be updated.

    **Validation:**
    - Email format validation and uniqueness check
    - Email length limit (max 255 characters)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "User profile updated successfully",
            "model": models.UserResponse
        },
        400: {
            "description": "Bad Request - Invalid email format or email already taken",
            "model": models.HTTPError
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def update_current_user(user_update: models.UserUpdate, current_user: dict = Depends(get_current_user)):
    # Validate input lengths
    if user_update.email and len(user_update.email) > 255:
        raise HTTPException(status_code=400, detail="Email too long (max 255 characters)")

    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    if user_update.email and user_update.email != current_user["email"]:
        existing_user = await database.get_user_by_email(pool, user_update.email)
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already registered")

    update_data = {}
    if user_update.email is not None:
        update_data["email"] = user_update.email
    if user_update.language is not None:
        update_data["language"] = user_update.language

    updated_user = await database.update_user(pool, current_user["id"], update_data)
    if not updated_user:
        raise HTTPException(status_code=500, detail="Failed to update user")
    return models.UserResponse(**updated_user)


@router.delete(
    "/me",
    status_code=204,
    summary="Delete current user account",
    description="""
    Permanently deactivate the current user account.

    This action deactivates the user account but preserves the data for compliance purposes.
    The user will no longer be able to authenticate or access protected endpoints.

    **Note:** This action cannot be undone.

    **Rate limit:** 300 requests per minute
    """,
    responses={
        204: {"description": "User account successfully deactivated"},
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def delete_current_user(current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")
    success = await database.delete_user(pool, current_user["id"])
    if not success:
        raise HTTPException(status_code=500, detail="Failed to delete user")
    return
