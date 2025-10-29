import logging
import re
from datetime import datetime, timedelta
from typing import Optional
from urllib.parse import urlparse

import bcrypt
import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

import config

logger = logging.getLogger(__name__)

SECRET_KEY = getattr(config, "JWT_SECRET_KEY", "your-secret-key-change-in-production")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

security = HTTPBearer()


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
        return {"id": int(user_id)}
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
    if not url or len(url) > 2048:  # Reasonable URL length limit
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
        try:
            import hashlib

            SECRET_KEY_LOCAL = getattr(config, "JWT_SECRET_KEY", "your-secret-key-change-in-production")
            return (
                hashlib.pbkdf2_hmac(
                    "sha256", plain_password.encode("utf-8"), SECRET_KEY_LOCAL.encode("utf-8"), 100000
                )
                == bytes.fromhex(hashed_password)
            )
        except (ValueError, TypeError):
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


def build_translations_dict(row_dict):
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
    return translations


def validate_rss_items_query_params(display_language, from_date, cursor_published_at):
    supported_languages = ["ru", "en", "de", "fr"]
    from fastapi import HTTPException, status
    from datetime import datetime

    if display_language is not None and display_language not in supported_languages:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Неподдерживаемый язык отображения. Допустимые значения: {', '.join(supported_languages)}.",
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
                detail="Некорректный формат timestamp в параметре cursor_published_at",
            )

    return from_datetime, before_published_at
