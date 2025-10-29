import logging
import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.middleware import setup_middleware
from api.routers import auth as auth_router
from api.routers import users as users_router
from api.routers import categories as categories_router
from api.routers import rss_feeds as rss_feeds_router
from api.routers import telegram as telegram_router
from api.routers import rss_items as rss_items_router
from api.websocket import router as ws_router, check_for_new_rss_items
from api import database
from logging_config import setup_logging
import config

setup_logging()
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FireFeed API",
    description="""
    # FireFeed News Aggregator API

    A comprehensive AI-powered news aggregation platform that collects, processes, and distributes news from RSS feeds in multiple languages.

    ## Features

    * **AI-Powered Content Processing**: Automatic translation to 4 languages (Russian, German, French, English) using modern ML models
    * **Duplicate Detection**: Semantic analysis using vector embeddings to identify duplicate content
    * **Multi-language Support**: Full localization for Telegram bot and REST API
    * **Personalized Feeds**: User-specific RSS feed subscriptions with category filtering
    * **Real-time Updates**: WebSocket support for live news updates
    * **Secure Authentication**: JWT-based authentication with email verification

    ## Authentication

    All endpoints except `/api/v1/auth/register`, `/api/v1/auth/login`, `/api/v1/auth/reset-password/request`, and `/api/v1/auth/verify` require authentication.

    Include the JWT token in the Authorization header:
    ```
    Authorization: Bearer <your_jwt_token>
    ```

    ## Rate Limiting

    API endpoints are protected with rate limiting:
    - Authentication endpoints: 5-300 requests per minute
    - General endpoints: 100-300 requests per minute

    ## Response Codes

    - `200`: Success
    - `201`: Created
    - `400`: Bad Request
    - `401`: Unauthorized
    - `403`: Forbidden
    - `404`: Not Found
    - `429`: Too Many Requests
    - `500`: Internal Server Error
    """,
    version="1.0.0",
    openapi_url="/api/openapi.json",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    contact={
        "name": "FireFeed Support",
        "url": "https://firefeed.net",
        "email": "support@firefeed.net",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
)

# Middleware & rate limiting
setup_middleware(app)

# CORS configuration
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
    "http://127.0.0.1",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8000",
    "https://firefeed.net",
    "https://www.firefeed.net",
]
if os.getenv("ENVIRONMENT") == "production":
    origins = [
        "https://firefeed.net",
        "https://www.firefeed.net",
    ]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth_router.router)
# Users router has placeholder dependency; keep disabled until wired with auth dep
app.include_router(users_router.router)
# Attach routers with new module names
app.include_router(categories_router.router)
app.include_router(rss_feeds_router.router)
app.include_router(telegram_router.router)
app.include_router(rss_items_router.router)
app.include_router(ws_router)


@app.on_event("startup")
async def startup_event():
    # Start background rss items checking task
    import asyncio

    asyncio.create_task(check_for_new_rss_items())
    logger.info("[Startup] RSS items checking task started")


@app.on_event("shutdown")
async def shutdown_event():
    try:
        await database.close_db_pool()
        logger.info("[Shutdown] Database pool closed")
    except Exception as e:
        logger.error(f"[Shutdown] Error closing DB pool: {e}")
