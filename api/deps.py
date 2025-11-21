import logging
import re
import hashlib
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from urllib.parse import urlparse

import bcrypt
import jwt
import redis
from fastapi import Depends, HTTPException, status, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import config

logger = logging.getLogger(__name__)

SECRET_KEY = config.JWT_SECRET_KEY
ALGORITHM = config.JWT_ALGORITHM
ACCESS_TOKEN_EXPIRE_MINUTES = config.JWT_ACCESS_TOKEN_EXPIRE_MINUTES

security = HTTPBearer()


def hash_api_key(api_key: str) -> str:
    """Hash API key with bcrypt for storage"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(api_key.encode("utf-8"), salt).decode("utf-8")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Dependency to get current authenticated user"""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication credentials",
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Get full user data from database
        from api import database
        pool = await database.get_db_pool()
        if pool is None:
            raise HTTPException(status_code=500, detail="Database error")
        user_data = await database.get_user_by_id(pool, int(user_id))
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return user_data
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def sanitize_search_phrase(search_phrase: str) -> str:
    """Sanitize search phrase to prevent injection and limit length"""
    if not search_phrase:
        return ""

    # Remove potentially dangerous characters and limit length
    sanitized = re.sub(r'[^\w\s\-.,!?\'"()\[\]{}]', '', search_phrase)
    return sanitized.strip()[:200]  # Limit to 200 characters


def validate_rss_url(url: str) -> bool:
    """Validate RSS URL format and safety"""
    if not url or len(url) > 2048:
        return False

    try:
        parsed = urlparse(url)
        # Must have scheme and netloc
        if not parsed.scheme or not parsed.netloc:
            return False
        # Only allow http/https
        if parsed.scheme not in ['http', 'https']:
            return False
        # Basic domain validation
        if not re.match(r'^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', parsed.netloc.split(':')[0]):
            return False
        return True
    except Exception:
        return False


def verify_password(plain_password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(plain_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except ValueError:
        return False


def get_password_hash(password: str) -> str:
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def format_datetime(dt_obj):
    return dt_obj.isoformat() if dt_obj else None


def get_full_image_url(image_filename: str) -> str:
    if not image_filename:
        return None
    if image_filename.startswith(("http://", "https://")):
        return image_filename
    base_url = config.HTTP_IMAGES_ROOT_DIR.rstrip("/")
    filename = image_filename.lstrip("/")
    return f"{base_url}/{filename}"


def build_translations_dict(row_dict, display_language=None):
    translations = {}
    languages = ["ru", "en", "de", "fr"]
    original_language = row_dict.get("original_language")

    for lang in languages:
        title_key = f"title_{lang}"
        content_key = f"content_{lang}"
        title = row_dict.get(title_key)
        content = row_dict.get(content_key)
        if (title is None or content is None) and lang == original_language:
            title = row_dict.get("original_title")
            content = row_dict.get("original_content")
        if title is not None or content is not None:
            translations[lang] = {"title": title, "content": content}

    # Handle display_language if provided and not already in translations
    if display_language and display_language not in translations:
        display_title = row_dict.get("display_title")
        display_content = row_dict.get("display_content")
        if display_title is not None or display_content is not None:
            translations[display_language] = {"title": display_title, "content": display_content}

    return translations


def validate_rss_items_query_params(display_language, from_date, cursor_published_at):
    supported_languages = ["ru", "en", "de", "fr"]
    from fastapi import HTTPException, status
    from datetime import datetime

    if display_language is not None and display_language not in supported_languages:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported display language. Valid values: {', '.join(supported_languages)}.",
        )

    from_datetime = None
    if from_date is not None:
        try:
            from_datetime = datetime.fromtimestamp(from_date / 1000.0)
        except (ValueError, OSError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Некорректный формат timestamp в параметре from_date"
            )

    before_published_at = None
    if cursor_published_at is not None:
        try:
            before_published_at = datetime.fromtimestamp(cursor_published_at / 1000.0)
        except (ValueError, OSError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid timestamp format in cursor_published_at parameter",
            )

    return from_datetime, before_published_at


# Redis client for rate limiting
_redis_client = None


def get_redis_client():
    """Get Redis client for rate limiting"""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=config.REDIS_CONFIG["host"],
            port=config.REDIS_CONFIG["port"],
            username=config.REDIS_CONFIG["username"],
            password=config.REDIS_CONFIG["password"],
            db=config.REDIS_CONFIG["db"],
            decode_responses=True
        )
    return _redis_client


async def check_rate_limit(api_key_data: Dict[str, Any]) -> None:
    """Check and increment rate limit for API key"""
    redis_client = get_redis_client()
    key_id = api_key_data["id"]
    limits = api_key_data["limits"]

    now = datetime.utcnow()
    day_key = f"user_api_key:{key_id}:day:{now.strftime('%Y-%m-%d')}"
    hour_key = f"user_api_key:{key_id}:hour:{now.strftime('%Y-%m-%d-%H')}"

    # Check daily limit
    if "requests_per_day" in limits:
        day_count = redis_client.incr(day_key)
        if day_count == 1:
            redis_client.expire(day_key, 86400)  # 24 hours
        if day_count > limits["requests_per_day"]:
            logger.warning(f"API key {key_id}: Daily limit exceeded ({day_count}/{limits['requests_per_day']})")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Daily request limit exceeded",
                headers={"Retry-After": "86400"}
            )
        logger.info(f"API key {key_id}: Daily requests {day_count}/{limits['requests_per_day']}")

    # Check hourly limit
    if "requests_per_hour" in limits:
        hour_count = redis_client.incr(hour_key)
        if hour_count == 1:
            redis_client.expire(hour_key, 3600)  # 1 hour
        if hour_count > limits["requests_per_hour"]:
            logger.warning(f"API key {key_id}: Hourly limit exceeded ({hour_count}/{limits['requests_per_hour']})")
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Hourly request limit exceeded",
                headers={"Retry-After": "3600"}
            )
        logger.info(f"API key {key_id}: Hourly requests {hour_count}/{limits['requests_per_hour']}")


async def get_current_user_by_api_key(request: Request):
    """Dependency to get current user authenticated by API key"""
    try:
        # Extract API key from X-API-Key header
        api_key = request.headers.get("X-API-Key")
        if not api_key:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="X-API-Key header required",
            )

        logger.info(f"[API_KEY_AUTH] Received API key: {api_key}")
        logger.info(f"[API_KEY_AUTH] SITE_API_KEY from config: {config.SITE_API_KEY}")
        logger.info(f"[API_KEY_AUTH] BOT_API_KEY from config: {config.BOT_API_KEY}")

        # Check if it's the site or bot API key
        if config.SITE_API_KEY and api_key == config.SITE_API_KEY:
            logger.info("[API_KEY_AUTH] SITE_API_KEY matched - authenticating as system user")
            # Site key: unlimited access, return system user
            return {
                "id": 0,  # System user ID
                "email": "system@firefeed.net",
                "language": "en",
                "is_active": True,
                "created_at": None,
                "updated_at": None,
                "api_key_data": {"limits": {}}  # No limits
            }
        if config.BOT_API_KEY and api_key == config.BOT_API_KEY:
            logger.info("[API_KEY_AUTH] BOT_API_KEY matched - authenticating as bot user")
            # Bot key: unlimited access, return bot user
            return {
                "id": -1,  # Bot user ID
                "email": "bot@firefeed.net",
                "language": "en",
                "is_active": True,
                "created_at": None,
                "updated_at": None,
                "api_key_data": {"limits": {}}  # No limits
            }

        logger.info("[API_KEY_AUTH] No special API key match, checking user API keys")

        # Get API key data from database
        from api import database
        pool = await database.get_db_pool()
        if pool is None:
            raise HTTPException(status_code=500, detail="Database error")

        api_key_data = await database.get_user_api_key_by_key(pool, api_key)
        if not api_key_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Check rate limits
        await check_rate_limit(api_key_data)

        # Get user data
        user_data = await database.get_user_by_id(pool, api_key_data["user_id"])
        if not user_data:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="User not found",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Check if user is verified and not deleted
        if not user_data.get("is_verified") or user_data.get("is_deleted"):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Account not verified or deactivated",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Return user data with API key info
        user_data["api_key_data"] = api_key_data
        return user_data

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in API key authentication: {e}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"},
        )
