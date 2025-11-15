import os
import asyncio
import aiopg
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Default logging level, overridable via env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database connection configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "minsize": int(os.getenv("DB_MINSIZE", 5)),
    "maxsize": int(os.getenv("DB_MAXSIZE", 20)),
}

# SMTP configuration for email sending
SMTP_CONFIG = {
    "server": os.getenv("SMTP_SERVER"),
    "port": int(os.getenv("SMTP_PORT", 465)),
    "email": os.getenv("SMTP_EMAIL"),
    "password": os.getenv("SMTP_PASSWORD"),
    "use_tls": os.getenv("SMTP_USE_TLS", "True").lower() == "true",
}

# One shared pool for all managers
_shared_db_pool = None
# Lock to prevent race conditions during initialization
_pool_init_lock = asyncio.Lock()


async def get_shared_db_pool():
    """Lazily creates and returns shared database connection pool in correct event loop."""
    global _shared_db_pool
    # If pool already created, return it
    if _shared_db_pool is not None:
        return _shared_db_pool

    # Use Lock to avoid creating multiple pools
    async with _pool_init_lock:
        # Double check, might have been created while waiting for Lock
        if _shared_db_pool is not None:
            return _shared_db_pool

        # Create pool inside current (active) event loop
        logger = logging.getLogger(__name__)
        logger.info("[CONFIG] Creating shared database pool...")
        _shared_db_pool = await aiopg.create_pool(**DB_CONFIG)
        logger.info("[CONFIG] Shared database pool created successfully.")
        return _shared_db_pool


async def close_shared_db_pool():
    """Closes shared connection pool."""
    global _shared_db_pool
    if _shared_db_pool is not None:
        _shared_db_pool.close()
        await _shared_db_pool.wait_closed()
        _shared_db_pool = None
        logger = logging.getLogger(__name__)
        logger.info("[DB] Shared connection pool closed.")


# Webhook connection configuration
WEBHOOK_CONFIG = {
    "listen": os.getenv("WEBHOOK_LISTEN", "127.0.0.1"),
    "port": int(os.getenv("WEBHOOK_PORT", 5000)),
    "url_path": os.getenv("WEBHOOK_URL_PATH", "webhook"),
    "webhook_url": os.getenv("WEBHOOK_URL"),
}

# FireFeed Bot Token
BOT_TOKEN = os.getenv("BOT_TOKEN")

# :-)
FIRE_EMOJI = "🔥"

# Default User-Agent for HTTP requests
DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; FireFeed/1.0; +https://firefeed.net)"

# Dictionary of channel IDs for different languages
CHANNEL_IDS = {"ru": "-1002584789230", "de": "-1002959373215", "fr": "-1002910849909", "en": "-1003035894895"}

CHANNEL_CATEGORIES = {"world", "technology", "lifestyle", "politics", "economy", "autos", "sports"}

# Maximum number of news items from one feed in one news monitoring task
MAX_ENTRIES_PER_FEED = 5
# Maximum number of all news items from all feeds in one news monitoring task
MAX_TOTAL_RSS_ITEMS = 15
# Maximum number of RSS feeds processed simultaneously
MAX_CONCURRENT_FEEDS = 10
# RSS item check interval in API
RSS_ITEM_CHECK_INTERVAL_SECONDS = 300
# Maximum number of concurrent WebSocket connections
MAX_WEBSOCKET_CONNECTIONS = 1000

# RSS item uniqueness threshold by meaning (applied for AI model in FireFeedDuplicateDetector)
RSS_ITEM_SIMILARITY_THRESHOLD = 0.9
# Absolute path to images directory on server
IMAGES_ROOT_DIR = "/var/www/firefeed/data/www/firefeed.net/data/images/"
# Absolute path to videos directory on server
VIDEOS_ROOT_DIR = "/var/www/firefeed/data/www/firefeed.net/data/videos/"
# Absolute path to videos directory on website
HTTP_VIDEOS_ROOT_DIR = "https://firefeed.net/data/videos/"
# Absolute path to images directory on website
HTTP_IMAGES_ROOT_DIR = "https://firefeed.net/data/images/"
# RSS parser media type priority (image or video)
RSS_PARSER_MEDIA_TYPE_PRIORITY = os.getenv("RSS_PARSER_MEDIA_TYPE_PRIORITY", "image")
# Allowed image extensions
IMAGE_FILE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".gif", ".webp"]
# Allowed video extensions
VIDEO_FILE_EXTENSIONS = [".mp4", ".avi", ".mkv", ".mov", ".wmv", ".flv", ".webm", ".m4v"]

VERIFICATION_CODE_EXPIRE_HOURS = 1

USER_DEFINED_RSS_CATEGORY_ID = 10

# Supported languages for translations
SUPPORTED_LANGUAGES = ["en", "ru", "de", "fr"]

# Redis configuration
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", 6379)),
    "username": os.getenv("REDIS_USERNAME", "default"),
    "password": os.getenv("REDIS_PASSWORD"),
    "db": int(os.getenv("REDIS_DB", 0)),
}

# API Key configuration
API_KEY_SALT = os.getenv("API_KEY_SALT", "default_salt_change_in_production")
SITE_API_KEY = os.getenv("SITE_API_KEY")  # Special key for website, unlimited access
BOT_API_KEY = os.getenv("BOT_API_KEY")  # Special key for Telegram bot, unlimited access

# JWT configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")
JWT_ALGORITHM = "HS256"
JWT_ACCESS_TOKEN_EXPIRE_MINUTES = 30
