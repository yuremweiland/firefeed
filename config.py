import os
import asyncio
import aiopg
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Уровень логирования по умолчанию, переопределяемый через env var
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Конфигурация подключения к БД
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "minsize": int(os.getenv("DB_MINSIZE", 5)),
    "maxsize": int(os.getenv("DB_MAXSIZE", 20)),
}

# Конфигурация SMTP для отправки email
SMTP_CONFIG = {
    "server": os.getenv("SMTP_SERVER"),
    "port": int(os.getenv("SMTP_PORT", 465)),
    "email": os.getenv("SMTP_EMAIL"),
    "password": os.getenv("SMTP_PASSWORD"),
    "use_tls": os.getenv("SMTP_USE_TLS", "True").lower() == "true",
}

# Один общий пул для всех менеджеров
_shared_db_pool = None
# Lock для предотвращения гонки при инициализации
_pool_init_lock = asyncio.Lock()


async def get_shared_db_pool():
    """Лениво создает и возвращает общий пул подключений к базе данных в правильном event loop."""
    global _shared_db_pool
    # Если пул уже создан, возвращаем его
    if _shared_db_pool is not None:
        return _shared_db_pool

    # Используем Lock, чтобы избежать создания нескольких пулов
    async with _pool_init_lock:
        # Повторная проверка, может быть создан пока ждал Lock
        if _shared_db_pool is not None:
            return _shared_db_pool

        # Создаем пул внутри текущего (активного) event loop
        logger = logging.getLogger(__name__)
        logger.info("[CONFIG] Создание shared database pool...")
        _shared_db_pool = await aiopg.create_pool(**DB_CONFIG)
        logger.info("[CONFIG] Shared database pool успешно создан.")
        return _shared_db_pool


async def close_shared_db_pool():
    """Закрывает общий пул подключений."""
    global _shared_db_pool
    if _shared_db_pool is not None:
        _shared_db_pool.close()
        await _shared_db_pool.wait_closed()
        _shared_db_pool = None
        logger = logging.getLogger(__name__)
        logger.info("[DB] Общий пул подключений закрыт.")


# Конфигурация подключения к webhook
WEBHOOK_CONFIG = {
    "listen": os.getenv("WEBHOOK_LISTEN", "127.0.0.1"),
    "port": int(os.getenv("WEBHOOK_PORT", 5000)),
    "url_path": os.getenv("WEBHOOK_URL_PATH", "webhook"),
    "webhook_url": os.getenv("WEBHOOK_URL"),
}

# Токен FeedFire Bot
BOT_TOKEN = os.getenv("BOT_TOKEN")

# :-)
FIRE_EMOJI = "🔥"

# Словарь ID каналов на разных языках
CHANNEL_IDS = {"ru": "-1002584789230", "de": "-1002959373215", "fr": "-1002910849909", "en": "-1003035894895"}

CHANNEL_CATEGORIES = {"world", "technology", "lifestyle", "politics", "economy", "autos", "sports"}

# Максимальное кол-во новостей из одной ленты в одной задаче мониторинга новостей
MAX_ENTRIES_PER_FEED = 3
# Максимальное кол-во всех новостей со всех лент в одной задаче мониторинга новостей
MAX_TOTAL_NEWS = 15
# Максимальное количество RSS-лент, обрабатываемых одновременно
MAX_CONCURRENT_FEEDS = 3
# Интервал проверки RSS-элементов в API
RSS_ITEM_CHECK_INTERVAL_SECONDS = 300

# Порог уникальности RSS-элементов по смыслу (применяется для AI модели в FireFeedDublicateDetector)
RSS_ITEM_SIMILARITY_THRESHOLD = 0.9
# Абсолютный путь к директории с изображениями на сервере
IMAGES_ROOT_DIR = "/var/www/firefeed/data/www/firefeed.net/data/images/"
# Абсолютный путь к директории с изображениями на сайте
HTTP_IMAGES_ROOT_DIR = "https://firefeed.net/data/images/"
# Допустимые расширения для изображений
IMAGE_FILE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".gif", ".webp"]

# Настройки JWT
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY")

VERIFICATION_CODE_EXPIRE_HOURS = 1

USER_DEFINED_RSS_CATEGORY_ID = 10
