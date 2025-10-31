import logging
from fastapi import APIRouter, Depends, HTTPException, Request

from api.middleware import limiter
from api import database, models
from api.deps import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/users/me/telegram",
    tags=["telegram_link"],
    responses={
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)




@router.post(
    "/generate-link",
    response_model=models.TelegramLinkResponse,
    summary="Generate Telegram linking code",
    description="""
    Generate a secure code for linking the user's Telegram account to their API account.

    This endpoint creates a unique linking code that can be used in the Telegram bot
    to associate the user's Telegram account with their API account for personalized notifications.

    **Process:**
    1. Generate unique secure linking code
    2. Store code in database with expiration
    3. Return code and instructions for use in Telegram bot

    **Next steps:** Send the code to the Telegram bot using `/link <code>` command.

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "Linking code generated successfully",
            "model": models.TelegramLinkResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def generate_telegram_link_code(request: Request, current_user: dict = Depends(get_current_user)):
    from user_manager import UserManager

    user_manager = UserManager()
    link_code = await user_manager.generate_telegram_link_code(current_user["id"])
    if not link_code:
        raise HTTPException(status_code=500, detail="Failed to generate link code")

    return models.TelegramLinkResponse(
        link_code=link_code, instructions="Отправьте этот код в Telegram бота командой: /link <код>"
    )


@router.delete(
    "/unlink",
    response_model=models.SuccessResponse,
    summary="Unlink Telegram account",
    description="""
    Disconnect the linked Telegram account from the user's API account.

    This removes the association between the user's API account and their Telegram account.
    The user will no longer receive personalized notifications in Telegram.

    **Note:** This does not delete the Telegram account or affect the bot's functionality.

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "Telegram account unlinked successfully",
            "model": models.SuccessResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def unlink_telegram_account(request: Request, current_user: dict = Depends(get_current_user)):
    from user_manager import UserManager

    user_manager = UserManager()
    success = await user_manager.unlink_telegram(current_user["id"])
    if not success:
        raise HTTPException(status_code=500, detail="Failed to unlink Telegram account")

    return models.SuccessResponse(message="Telegram account successfully unlinked")


@router.get(
    "/status",
    response_model=models.TelegramLinkStatusResponse,
    summary="Get Telegram linking status",
    description="""
    Check if the user's API account is linked to a Telegram account.

    Returns the current linking status, including whether an account is linked,
    the Telegram user ID, and when the linking occurred.

    **Response fields:**
    - `is_linked`: Whether a Telegram account is linked
    - `telegram_id`: Telegram user ID (if linked)
    - `linked_at`: ISO timestamp of when the account was linked (if linked)

    **Rate limit:** 300 requests per minute
    """,
    responses={
        200: {
            "description": "Telegram linking status",
            "model": models.TelegramLinkStatusResponse
        },
        401: {"description": "Unauthorized - Authentication required"},
        429: {"description": "Too Many Requests - Rate limit exceeded"},
        500: {"description": "Internal Server Error"}
    }
)
@limiter.limit("300/minute")
async def get_telegram_link_status(request: Request, current_user: dict = Depends(get_current_user)):
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=500, detail="Database error")

    link = await database.get_telegram_link_status(pool, current_user["id"])
    if link:
        return models.TelegramLinkStatusResponse(
            is_linked=True, telegram_id=link.get("telegram_id"), linked_at=link.get("linked_at")
        )
    return models.TelegramLinkStatusResponse(is_linked=False)
