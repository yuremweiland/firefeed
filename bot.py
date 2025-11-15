# bot.py
import asyncio
import logging
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import aiohttp
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import NetworkError, BadRequest, RetryAfter
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from tenacity import retry, stop_after_attempt, wait_exponential
from utils.text import TextProcessor

from config import WEBHOOK_CONFIG, BOT_TOKEN, CHANNEL_IDS, CHANNEL_CATEGORIES, get_shared_db_pool, RSS_PARSER_MEDIA_TYPE_PRIORITY, HTTP_IMAGES_ROOT_DIR, HTTP_VIDEOS_ROOT_DIR
from firefeed_translations import get_message, LANG_NAMES, TRANSLATED_FROM_LABELS, READ_MORE_LABELS, SOURCE_LABELS
from logging_config import setup_logging
from user_manager import UserManager

# Logging setup
setup_logging()
logger = logging.getLogger(__name__)

# --- API Configuration ---
API_BASE_URL = "http://localhost:8000/api/v1"
BOT_API_KEY = os.getenv("BOT_API_KEY")  # API key for bot authentication

# --- Global variables ---
USER_STATES = {}  # {user_id: {"current_subs": [...], "language": "en", "last_access": timestamp}}
USER_CURRENT_MENUS = {}  # {user_id: "main", "last_access": timestamp}
USER_LANGUAGES = {}  # {user_id: "en", "last_access": timestamp}
SEND_SEMAPHORE = asyncio.Semaphore(5)
RSS_ITEM_PROCESSING_SEMAPHORE = asyncio.Semaphore(10)
FEED_LOCKS = {}  # {feed_id: asyncio.Lock} for feed-level locking

user_manager = None
http_session = None  # Global session for HTTP requests

# TTL for cleaning expired data (24 hours)
USER_DATA_TTL_SECONDS = 24 * 60 * 60


@dataclass
class PreparedRSSItem:
    """Structure for storing prepared RSS item."""

    original_data: Dict[str, Any]
    translations: Dict[str, Dict[str, str]]
    image_filename: Optional[str]
    video_filename: Optional[str]
    feed_id: int


# --- Database functions ---
async def mark_translation_as_published(translation_id: int, channel_id: int, message_id: int = None):
    """Marks translation as published in Telegram channel."""
    try:
        # Get shared connection pool
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = """
                    INSERT INTO rss_items_telegram_published
                    (translation_id, channel_id, message_id, published_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (translation_id, channel_id)
                    DO UPDATE SET
                        message_id = EXCLUDED.message_id,
                        published_at = NOW()
                """
                await cursor.execute(query, (translation_id, channel_id, message_id))
                logger.info(f"Translation {translation_id} marked as published in channel {channel_id}")
                return True
    except Exception as e:
        logger.error(f"Error marking translation {translation_id} as published: {e}")
        return False


async def mark_original_as_published(news_id: str, channel_id: int, message_id: int = None):
    """Marks original news as published in Telegram channel."""
    try:
        # Get shared connection pool
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = """
                    INSERT INTO rss_items_telegram_published_originals
                    (news_id, channel_id, message_id, created_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (news_id, channel_id)
                    DO UPDATE SET
                        message_id = EXCLUDED.message_id,
                        created_at = NOW()
                """
                await cursor.execute(query, (news_id, channel_id, message_id))
                logger.info(f"Original news {news_id} marked as published in channel {channel_id}")
                return True
    except Exception as e:
        logger.error(f"Error marking original news {news_id} as published: {e}")
        return False


async def get_translation_id(news_id: str, language: str) -> int:
    """Gets translation ID from news_translations table."""
    try:
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = """
                    SELECT id FROM news_translations
                    WHERE news_id = %s AND language = %s
                """
                await cursor.execute(query, (news_id, language))
                result = await cursor.fetchone()
                return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting translation ID for {news_id} in {language}: {e}")
        return None


async def get_feed_cooldown_and_max_news(feed_id: int) -> tuple[int, int]:
    """Gets cooldown minutes and max news per hour for feed."""
    try:
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                query = """
                    SELECT COALESCE(cooldown_minutes, 60), COALESCE(max_news_per_hour, 10)
                    FROM rss_feeds WHERE id = %s
                """
                await cursor.execute(query, (feed_id,))
                result = await cursor.fetchone()
                return (result[0], result[1]) if result else (60, 10)
    except Exception as e:
        logger.error(f"Error getting cooldown and max_news for feed {feed_id}: {e}")
        return (60, 10)


async def get_last_telegram_publication_time(feed_id: int) -> Optional[datetime]:
    """Get last Telegram publication time for feed."""
    try:
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                # Get latest publication time from both tables
                query = """
                SELECT GREATEST(
                    COALESCE((
                        SELECT MAX(rtp.published_at)
                        FROM rss_items_telegram_published rtp
                        JOIN news_translations nt ON rtp.translation_id = nt.id
                        JOIN published_news_data pnd ON nt.news_id = pnd.news_id
                        WHERE pnd.rss_feed_id = %s
                    ), '1970-01-01'::timestamp),
                    COALESCE((
                        SELECT MAX(rtpo.created_at)
                        FROM rss_items_telegram_published_originals rtpo
                        JOIN published_news_data pnd ON rtpo.news_id = pnd.news_id
                        WHERE pnd.rss_feed_id = %s
                    ), '1970-01-01'::timestamp)
                ) as last_time
                """
                await cursor.execute(query, (feed_id, feed_id))
                row = await cursor.fetchone()
                if row and row[0] and row[0] > datetime(1970, 1, 1, tzinfo=timezone.utc):
                    return row[0]
                return None
    except Exception as e:
        logger.error(f"Error getting last Telegram publication time for feed {feed_id}: {e}")
        return None


async def get_recent_telegram_publications_count(feed_id: int, minutes: int) -> int:
    """Get count of recent Telegram publications for feed."""
    try:
        db_pool = await get_shared_db_pool()
        async with db_pool.acquire() as connection:
            async with connection.cursor() as cursor:
                time_threshold = datetime.now(timezone.utc) - timedelta(minutes=minutes)
                # Count publications from both tables
                query = """
                SELECT COUNT(*) FROM (
                    SELECT rtp.published_at
                    FROM rss_items_telegram_published rtp
                    JOIN news_translations nt ON rtp.translation_id = nt.id
                    JOIN published_news_data pnd ON nt.news_id = pnd.news_id
                    WHERE pnd.rss_feed_id = %s AND rtp.published_at >= %s
                    UNION ALL
                    SELECT rtpo.created_at as published_at
                    FROM rss_items_telegram_published_originals rtpo
                    JOIN published_news_data pnd ON rtpo.news_id = pnd.news_id
                    WHERE pnd.rss_feed_id = %s AND rtpo.created_at >= %s
                ) as combined_publications
                """
                await cursor.execute(query, (feed_id, time_threshold, feed_id, time_threshold))
                row = await cursor.fetchone()
                return row[0] if row else 0
    except Exception as e:
        logger.error(f"Error getting recent Telegram publications count for feed {feed_id}: {e}")
        return 0


# --- Image validation functions ---
async def validate_image_url(image_url: str) -> bool:
    """Checks availability and correctness of image URL."""
    if not image_url:
        return False

    try:
        # Make HEAD request to check availability
        timeout = aiohttp.ClientTimeout(total=5, connect=2)
        async with http_session.head(image_url, timeout=timeout) as response:
            if response.status != 200:
                logger.debug(f"Image unavailable (status {response.status}): {image_url}")
                return False

            # Check Content-Type
            content_type = response.headers.get('Content-Type', '').lower()
            if not content_type.startswith('image/'):
                logger.debug(f"Incorrect Content-Type '{content_type}' for: {image_url}")
                return False

            # Check size (if specified)
            content_length = response.headers.get('Content-Length')
            if content_length:
                try:
                    size = int(content_length)
                    if size > 10 * 1024 * 1024:  # 10 MB limit
                        logger.debug(f"Image too large ({size} bytes): {image_url}")
                        return False
                except (ValueError, TypeError):
                    pass

            return True

    except asyncio.TimeoutError:
        logger.debug(f"Timeout checking image: {image_url}")
        return False
    except Exception as e:
        logger.debug(f"Error checking image {image_url}: {e}")
        return False


# --- Functions for working with API ---
async def api_get(endpoint: str, params: dict = None) -> dict:
    """Выполняет GET-запрос к API."""
    global http_session
    if http_session is None:
        raise RuntimeError("HTTP session not initialized")

    url = f"{API_BASE_URL}{endpoint}"
    try:
        # Convert boolean parameters to strings
        if params:
            processed_params = {}
            for key, value in params.items():
                if isinstance(value, bool):
                    processed_params[key] = str(value).lower()
                else:
                    processed_params[key] = value
        else:
            processed_params = params

        # Add API key to headers if set
        headers = {}
        if BOT_API_KEY:
            headers["X-API-Key"] = BOT_API_KEY

        timeout = aiohttp.ClientTimeout(total=10, connect=5)  # 10 second timeout for API requests
        async with http_session.get(url, params=processed_params, headers=headers, timeout=timeout) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(f"{endpoint} returned status {response.status}")
                # Attempt to get error text for better understanding of the problem
                error_text = await response.text()
                logger.error(f"Error response body: {error_text}")
                return {}
    except asyncio.TimeoutError:
        logger.error(f"Timeout error calling {endpoint}")
        return {}
    except Exception as e:
        logger.error(f"Failed to call {endpoint}: {e}")
        return {}


async def get_rss_items_list(display_language: str = None, **filters) -> dict:
    """Gets list of RSS items."""
    params = {}
    if display_language is not None:
        params["display_language"] = display_language
    params.update(filters)
    return await api_get("/rss-items/", params)


async def get_rss_item_by_id(rss_item_id: str, display_language: str = "en") -> dict:
    """Gets RSS item by ID."""
    params = {"display_language": display_language}
    return await api_get(f"/rss-items/{rss_item_id}", params)


async def get_categories() -> list:
    """Gets list of categories."""
    result = await api_get("/categories/")
    return result.get("results", [])


async def get_sources() -> list:
    """Gets list of sources."""
    result = await api_get("/sources/")
    return result.get("results", [])


async def get_languages() -> list:
    """Gets list of languages."""
    result = await api_get("/languages/")
    return result.get("results", [])


# --- UI functions ---
def get_main_menu_keyboard(lang="en"):
    """Creates main menu keyboard."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(get_message("menu_settings", lang)), KeyboardButton(get_message("menu_help", lang))],
            [KeyboardButton(get_message("menu_status", lang)), KeyboardButton(get_message("menu_language", lang))],
        ],
        resize_keyboard=True,
        input_field_placeholder=get_message("menu_placeholder", lang),
    )


async def set_current_user_language(user_id: int, lang: str):
    """Sets user language in DB and memory."""
    global user_manager
    try:
        await user_manager.set_user_language(user_id, lang)
        USER_LANGUAGES[user_id] = {"language": lang, "last_access": time.time()}
    except Exception as e:
        logger.error(f"Error setting language for {user_id}: {e}")


async def cleanup_expired_user_data(context=None):
    """Clears expired user data (older than 24 hours)."""
    current_time = time.time()
    expired_threshold = current_time - USER_DATA_TTL_SECONDS

    # Clear USER_STATES
    expired_states = [uid for uid, data in USER_STATES.items()
                      if isinstance(data, dict) and data.get("last_access", 0) < expired_threshold]
    for uid in expired_states:
        del USER_STATES[uid]

    # Clear USER_CURRENT_MENUS
    expired_menus = [uid for uid, data in USER_CURRENT_MENUS.items()
                    if isinstance(data, dict) and data.get("last_access", 0) < expired_threshold]
    for uid in expired_menus:
        del USER_CURRENT_MENUS[uid]

    # Clear USER_LANGUAGES
    expired_langs = [uid for uid, data in USER_LANGUAGES.items()
                    if isinstance(data, dict) and data.get("last_access", 0) < expired_threshold]
    for uid in expired_langs:
        del USER_LANGUAGES[uid]

    if expired_states or expired_menus or expired_langs:
        logger.info(f"[CLEANUP] Cleared expired data: states={len(expired_states)}, menus={len(expired_menus)}, langs={len(expired_langs)}")


async def get_current_user_language(user_id: int) -> str:
    """Gets current user language from memory or DB."""
    if user_id in USER_LANGUAGES:
        data = USER_LANGUAGES[user_id]
        if isinstance(data, dict):
            data["last_access"] = time.time()
            return data["language"]
        else:
            # Update old format
            USER_LANGUAGES[user_id] = {"language": data, "last_access": time.time()}
            return data

    try:
        lang = await user_manager.get_user_language(user_id)
        if lang:
            USER_LANGUAGES[user_id] = {"language": lang, "last_access": time.time()}
        return lang or "en"
    except Exception as e:
        logger.error(f"Error getting language for {user_id}: {e}")
        return "en"


# --- Command handlers ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /start command."""
    user = update.effective_user
    user_id = user.id
    lang = await get_current_user_language(user_id)
    welcome_text = get_message("welcome", lang, user_name=user.first_name)
    await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /settings command."""
    global user_manager
    user_id = update.effective_user.id
    try:
        lang = await get_current_user_language(user_id)
        logger.info(f"Loading settings for user {user_id}")
        settings = await user_manager.get_user_settings(user_id)
        logger.info(f"Loaded settings for user {user_id}: {settings}")
        current_subs = settings["subscriptions"] if isinstance(settings["subscriptions"], list) else []
        USER_STATES[user_id] = {"current_subs": current_subs, "language": settings["language"], "last_access": time.time()}
        await _show_settings_menu(context.bot, update.effective_chat.id, user_id)
        USER_CURRENT_MENUS[user_id] = "settings"
    except Exception as e:
        logger.error(f"Error in /settings command for {user_id}: {e}")
        lang = await get_current_user_language(user_id)
        await update.message.reply_text(get_message("settings_error", lang))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /help command."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    help_text = get_message("help_text", lang)
    await update.message.reply_text(help_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /status command."""
    global user_manager
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    settings = await user_manager.get_user_settings(user_id)
    categories = settings["subscriptions"]
    categories_text = ", ".join(categories) if categories else get_message("no_subscriptions", lang)
    status_text = get_message(
        "status_text", lang, language=LANG_NAMES.get(settings["language"], "English"), categories=categories_text
    )
    await update.message.reply_text(status_text, parse_mode="HTML", reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"


async def change_language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for language change command."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    keyboard = [
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
        [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
        [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")],
    ]
    await update.message.reply_text(get_message("language_select", lang), reply_markup=InlineKeyboardMarkup(keyboard))
    USER_CURRENT_MENUS[user_id] = "language"


async def link_telegram_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for /link command to link Telegram account."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)

    if not context.args:
        await update.message.reply_text(
            "Usage: /link <link_code>\n\n" "Get the link code in your personal account on the site.",
            reply_markup=get_main_menu_keyboard(lang),
        )
        USER_CURRENT_MENUS[user_id] = "main"
        return

    link_code = context.args[0].strip()

    # Check code via UserManager
    success = await user_manager.confirm_telegram_link(user_id, link_code)

    if success:
        await update.message.reply_text(
            "✅ Your Telegram account has been successfully linked to your site account!\n\n"
            "Now you can manage settings through the site or bot.",
            reply_markup=get_main_menu_keyboard(lang),
        )
    else:
        await update.message.reply_text(
            "❌ Link code is invalid or expired.\n\n"
            "Please generate a new code in your personal account on the site.",
            reply_markup=get_main_menu_keyboard(lang),
        )

    USER_CURRENT_MENUS[user_id] = "main"


# --- UI helper functions ---
async def _show_settings_menu(bot, chat_id: int, user_id: int):
    """Displays settings menu."""
    state = USER_STATES.get(user_id)
    if not state:
        return
    current_subs = state["current_subs"]
    current_lang = state["language"]
    try:
        categories = await get_categories()
        keyboard = []
        for category in categories:
            category_name = category.get("name", str(category))
            is_selected = category_name in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category_name.capitalize()}"
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category_name}")])
        keyboard.append([InlineKeyboardButton(get_message("save_button", current_lang), callback_data="save_settings")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await bot.send_message(
            chat_id=chat_id, text=get_message("settings_title", current_lang), reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Error in _show_settings_menu for {user_id}: {e}")


async def _show_settings_menu_from_callback(bot, chat_id: int, user_id: int):
    """Displays settings menu from callback."""
    await _show_settings_menu(bot, chat_id, user_id)


# --- Callback and message handlers ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for callback buttons."""
    global user_manager
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    try:
        if user_id not in USER_STATES:
            subs = await user_manager.get_user_subscriptions(user_id)
            current_subs = subs if isinstance(subs, list) else []
            USER_STATES[user_id] = {"current_subs": current_subs, "language": await get_current_user_language(user_id), "last_access": time.time()}
        state = USER_STATES[user_id]
        current_lang = state["language"]
        if query.data.startswith("toggle_"):
            category = query.data.split("_", 1)[1]
            current_subs = state["current_subs"]
            if category in current_subs:
                current_subs.remove(category)
            else:
                current_subs.append(category)
            state["current_subs"] = current_subs
            try:
                await query.message.delete()
            except Exception:
                pass
            await _show_settings_menu_from_callback(context.bot, query.message.chat_id, user_id)
        elif query.data == "save_settings":
            # Save category names as strings
            logger.info(
                f"Saving settings for user {user_id}: subscriptions={state['current_subs']}, language={state['language']}"
            )
            result = await user_manager.save_user_settings(user_id, state["current_subs"], state["language"])
            logger.info(f"Save result for user {user_id}: {result}")
            USER_STATES.pop(user_id, None)
            try:
                await query.message.delete()
            except Exception:
                pass
            user = await context.bot.get_chat(user_id)
            welcome_text = (
                get_message("settings_saved", current_lang)
                + "\n"
                + get_message("welcome", current_lang, user_name=user.first_name)
            )
            await context.bot.send_message(
                chat_id=user_id, text=welcome_text, reply_markup=get_main_menu_keyboard(current_lang)
            )
            USER_CURRENT_MENUS[user_id] = {"menu": "main", "last_access": time.time()}
        elif query.data.startswith("lang_"):
            lang = query.data.split("_", 1)[1]
            await set_current_user_language(user_id, lang)
            if user_id in USER_STATES:
                USER_STATES[user_id]["language"] = lang
            try:
                await query.message.delete()
            except Exception:
                pass
            user = await context.bot.get_chat(user_id)
            welcome_text = (
                get_message("language_changed", lang, language=LANG_NAMES.get(lang, "English"))
                + "\n"
                + get_message("welcome", lang, user_name=user.first_name)
            )
            await context.bot.send_message(
                chat_id=user_id, text=welcome_text, reply_markup=get_main_menu_keyboard(lang)
            )
            USER_CURRENT_MENUS[user_id] = {"menu": "main", "last_access": time.time()}
        elif query.data == "change_lang":
            current_lang = await get_current_user_language(user_id)
            keyboard = [
                [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
                [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
                [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
                [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")],
            ]
            await query.message.edit_text(
                text=get_message("language_select", current_lang), reply_markup=InlineKeyboardMarkup(keyboard)
            )
            USER_CURRENT_MENUS[user_id] = "language"
    except Exception as e:
        logger.error(f"Error processing button for {user_id}: {e}")
        current_lang = await get_current_user_language(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text=get_message("button_error", current_lang),
            reply_markup=get_main_menu_keyboard(current_lang),
        )
        USER_CURRENT_MENUS[user_id] = {"menu": "main", "last_access": time.time()}


async def handle_menu_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for menu selection."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    text = update.message.text
    menu_actions = {
        get_message("menu_settings", lang): settings_command,
        get_message("menu_help", lang): help_command,
        get_message("menu_status", lang): status_command,
        get_message("menu_language", lang): change_language_command,
    }
    action = menu_actions.get(text)
    if action:
        await action(update, context)
        return
    all_languages = ["en", "ru", "de", "fr"]
    for check_lang in all_languages:
        if text in [get_message(f"menu_{m}", check_lang) for m in ["settings", "help", "status", "language"]]:
            await set_current_user_language(user_id, check_lang)
            new_menu_actions = {
                get_message("menu_settings", check_lang): settings_command,
                get_message("menu_help", check_lang): help_command,
                get_message("menu_status", check_lang): status_command,
                get_message("menu_language", check_lang): change_language_command,
            }
            new_action = new_menu_actions.get(text)
            if new_action:
                await new_action(update, context)
            return
    logger.info(f"Unknown menu selection for {user_id}: {text}")


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handler for debug messages."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    await update.message.reply_text(get_message("bot_active", lang), reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = {"menu": "main", "last_access": time.time()}




@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def send_personal_rss_items(bot, prepared_rss_item: PreparedRSSItem, subscribers_cache=None):
    """Sends personal RSS items to subscribers."""
    news_id = prepared_rss_item.original_data.get("id")
    logger.info(f"Sending personal RSS item: {prepared_rss_item.original_data['title'][:50]}...")
    category = prepared_rss_item.original_data.get("category")
    if not category:
        logger.warning(f"RSS item {news_id} has no category")
        return

    # Use subscribers cache if provided
    if subscribers_cache is not None:
        subscribers = subscribers_cache.get(category, [])
    else:
        # Fallback to old method if cache not provided
        global user_manager
        subscribers = await user_manager.get_subscribers_for_category(category)

    if not subscribers:
        logger.debug(f"No subscribers for category {category}")
        return
    translations_cache = prepared_rss_item.translations
    original_rss_item_lang = prepared_rss_item.original_data.get("lang", "")

    for i, user in enumerate(subscribers):
        try:
            user_id = user["id"]
            user_lang = user.get("language_code", "en")

            # Check if item has content in user's language
            title_to_send = None
            content_to_send = None

            # If user's language matches item's original language
            if user_lang == original_rss_item_lang:
                title_to_send = prepared_rss_item.original_data["title"]
                content_to_send = prepared_rss_item.original_data.get("content", "")
            # Otherwise, look for translation in user's language
            elif user_lang in translations_cache and translations_cache[user_lang]:
                translation_data = translations_cache[user_lang]
                title_to_send = translation_data.get("title", "")
                content_to_send = translation_data.get("content", "")

            # If no suitable content, skip user
            if not title_to_send or not title_to_send.strip():
                logger.debug(f"Skipping user {user_id} - no content in language {user_lang}")
                continue

            title_to_send = TextProcessor.clean(title_to_send)
            content_to_send = TextProcessor.clean(content_to_send)

            lang_note = ""
            if user_lang != original_rss_item_lang:
                lang_note = (
                    f"\n🌐 {TRANSLATED_FROM_LABELS.get(user_lang, 'Translated from')} {original_rss_item_lang.upper()}\n"
                )
            content_text = (
                f"🔥 <b>{title_to_send}</b>\n"
                f"\n\n{content_to_send}\n"
                f"\nFROM: {prepared_rss_item.original_data.get('source', 'Unknown Source')}\n"
                f"CATEGORY: {category}\n{lang_note}\n"
                f"⚡ <a href='{prepared_rss_item.original_data.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
            )
            # Determine media based on priority
            priority = RSS_PARSER_MEDIA_TYPE_PRIORITY.lower()
            media_filename = None
            media_type = None

            if priority == "image":
                if prepared_rss_item.image_filename:
                    media_filename = prepared_rss_item.image_filename
                    media_type = "image"
                elif prepared_rss_item.video_filename:
                    media_filename = prepared_rss_item.video_filename
                    media_type = "video"
            elif priority == "video":
                if prepared_rss_item.video_filename:
                    media_filename = prepared_rss_item.video_filename
                    media_type = "video"
                elif prepared_rss_item.image_filename:
                    media_filename = prepared_rss_item.image_filename
                    media_type = "image"

            logger.debug(f"send_personal_rss_items media_filename = {media_filename}, media_type = {media_type}")

            if media_filename and media_type == "image":
                # Check image availability and correctness
                if await validate_image_url(media_filename):
                    logger.debug(f"Image passed validation: {media_filename}")
                else:
                    logger.warning(f"Image failed validation, sending without it: {media_filename}")
                    media_filename = None  # Reset media
                    continue  # Continue without media
            elif media_filename and media_type == "video":
                # For video, we assume it's already validated during processing
                logger.debug(f"Using video: {media_filename}")

                caption = content_text
                if len(caption) > 1024:
                    base_text = f"🔥 <b>{title_to_send}</b>\nFROM: {prepared_rss_item.original_data.get('source', 'Unknown Source')}\nCATEGORY: {category}{lang_note}\n⚡ <a href='{prepared_rss_item.original_data.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
                    max_content_length = 1024 - len(base_text)
                    if max_content_length > 0:
                        truncated_content = content_to_send[: max_content_length - 3] + "..."
                        caption = f"🔥 <b>{title_to_send}</b>\n{truncated_content}\n{base_text}"
                    else:
                        caption = caption[:1021] + "..."
                try:
                    if media_type == "image":
                        await bot.send_photo(chat_id=user_id, photo=media_filename, caption=caption, parse_mode="HTML")
                    elif media_type == "video":
                        await bot.send_video(chat_id=user_id, video=media_filename, caption=caption, parse_mode="HTML")
                except RetryAfter as e:
                    logger.warning(f"Flood control for user {user_id}, waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after + 1)
                    if media_type == "image":
                        await bot.send_photo(chat_id=user_id, photo=media_filename, caption=caption, parse_mode="HTML")
                    elif media_type == "video":
                        await bot.send_video(chat_id=user_id, video=media_filename, caption=caption, parse_mode="HTML")
                except BadRequest as e:
                    if "Wrong type of the web page content" in str(e):
                        logger.warning(f"Incorrect content type for user {user_id}, sending without media: {media_filename}")
                        # Send without media
                        try:
                            await bot.send_message(
                                chat_id=user_id, text=caption, parse_mode="HTML", disable_web_page_preview=True
                            )
                        except Exception as send_error:
                            logger.error(f"Error sending message to user {user_id}: {send_error}")
                    else:
                        logger.error(f"BadRequest when sending media to user {user_id}: {e}")
                except Exception as e:
                    logger.error(f"Error sending media to user {user_id}: {e}")
            else:
                try:
                    await bot.send_message(
                        chat_id=user_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True
                    )
                except RetryAfter as e:
                    logger.warning(f"Flood control for user {user_id}, waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after + 1)
                    await bot.send_message(
                        chat_id=user_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True
                    )
                except Exception as e:
                    logger.error(f"Error sending message to user {user_id}: {e}")

            if i < len(subscribers) - 1:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Error sending personal RSS item to user {user.get('id', 'Unknown ID')}: {e}")


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def post_to_channel(bot, prepared_rss_item: PreparedRSSItem):
    """Publishes RSS item to Telegram channels."""
    original_title = prepared_rss_item.original_data["title"]
    news_id = prepared_rss_item.original_data.get("id")
    feed_id = prepared_rss_item.feed_id

    # Get or create feed lock
    if feed_id not in FEED_LOCKS:
        FEED_LOCKS[feed_id] = asyncio.Lock()
    feed_lock = FEED_LOCKS[feed_id]

    async with feed_lock:
        logger.info(f"Publishing RSS item to channels: {original_title[:50]}...")

        # Check Telegram publication limits
        cooldown_minutes, max_news_per_hour = await get_feed_cooldown_and_max_news(feed_id)
        recent_telegram_count = await get_recent_telegram_publications_count(feed_id, cooldown_minutes)

        if recent_telegram_count >= max_news_per_hour:
            logger.info(f"[SKIP] Feed {feed_id} reached Telegram publication limit {max_news_per_hour} in {cooldown_minutes} minutes. Published: {recent_telegram_count}")
            return

        # Check time-based limit
        last_telegram_time = await get_last_telegram_publication_time(feed_id)
        if last_telegram_time:
            elapsed = datetime.now(timezone.utc) - last_telegram_time
            min_interval = timedelta(minutes=60 / max_news_per_hour)
            cooldown_limit = timedelta(minutes=cooldown_minutes)
            effective_limit = min(min_interval, cooldown_limit)
            if elapsed < effective_limit:
                remaining_time = effective_limit - elapsed
                logger.info(f"[SKIP] Feed {feed_id} on Telegram cooldown. Remaining: {remaining_time}")
                return

    logger.debug(f"post_to_channel prepared_rss_item = {prepared_rss_item}")
    original_content = prepared_rss_item.original_data.get("content", "")
    category = prepared_rss_item.original_data.get("category", "")
    original_source = prepared_rss_item.original_data.get("source", "UnknownSource")
    original_lang = prepared_rss_item.original_data["lang"]
    translations_cache = prepared_rss_item.translations
    channels_list = list(CHANNEL_IDS.items())

    # Send to channels where translation or original exists
    for target_lang, channel_id in channels_list:
        try:
            # Determine whether to use translation or original
            if target_lang == original_lang:
                # Original language
                title = TextProcessor.clean(original_title)
                content = TextProcessor.clean(original_content)
                lang_note = ""
                translation_id = None  # No translation for original language
            elif target_lang in translations_cache and translations_cache[target_lang]:
                # There is translation
                translation_data = translations_cache[target_lang]
                title = TextProcessor.clean(translation_data.get("title", original_title))
                content = TextProcessor.clean(translation_data.get("content", original_content))
                lang_note = (
                    f"\n{TRANSLATED_FROM_LABELS.get(target_lang, '[AI] Translated from')} {original_lang.upper()}\n"
                )
                # Получаем ID перевода для отслеживания публикации
                translation_id = await get_translation_id(news_id, target_lang)
                if not translation_id:
                    logger.warning(f"Translation ID not found for {news_id} in {target_lang}, skipping publication")
                    continue
            else:
                # No translation, skip
                logger.debug(f"No translation for {news_id} in {target_lang}, skipping publication")
                continue
            hashtags = f"\n#{category} #{original_source}"
            source_url = prepared_rss_item.original_data.get("link", "")
            content_text = f"<b>{title}</b>\n"
            if content and content.strip():
                content_text += f"\n{content}\n"
            if source_url:
                content_text += f"\n🔗 <a href=\"{source_url}\">{SOURCE_LABELS.get(target_lang, 'Source')}</a>\n"
            content_text += f"{lang_note}{hashtags}"
            # Determine media based on priority
            priority = RSS_PARSER_MEDIA_TYPE_PRIORITY.lower()
            media_filename = None
            media_type = None

            if priority == "image":
                if prepared_rss_item.image_filename:
                    media_filename = prepared_rss_item.image_filename
                    media_type = "image"
                elif prepared_rss_item.video_filename:
                    media_filename = prepared_rss_item.video_filename
                    media_type = "video"
            elif priority == "video":
                if prepared_rss_item.video_filename:
                    media_filename = prepared_rss_item.video_filename
                    media_type = "video"
                elif prepared_rss_item.image_filename:
                    media_filename = prepared_rss_item.image_filename
                    media_type = "image"

            logger.debug(f"post_to_channel media_filename = {media_filename}, media_type = {media_type}")

            if media_filename and ((media_type == "image" and await validate_image_url(media_filename)) or media_type == "video"):
                # Media passed validation - send with appropriate method
                logger.debug(f"Media passed validation: {media_filename}")

                caption = content_text
                if len(caption) > 1024:
                    base_text = f"<b>{title}</b>{lang_note}{hashtags}"
                    max_content_length = 1024 - len(base_text)
                    if max_content_length > 0:
                        truncated_content = content[: max_content_length - 3] + "..."
                        caption = f"<b>{title}</b>\n{truncated_content}{lang_note}{hashtags}"
                    else:
                        caption = caption[:1021] + "..."
                try:
                    if media_type == "image":
                        message = await bot.send_photo(
                            chat_id=channel_id, photo=media_filename, caption=caption, parse_mode="HTML"
                        )
                    elif media_type == "video":
                        message = await bot.send_video(
                            chat_id=channel_id, video=media_filename, caption=caption, parse_mode="HTML"
                        )
                    message_id = message.message_id
                except RetryAfter as e:
                    logger.warning(f"Flood control for channel {channel_id}, waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after + 1)
                    if media_type == "image":
                        message = await bot.send_photo(
                            chat_id=channel_id, photo=media_filename, caption=caption, parse_mode="HTML"
                        )
                    elif media_type == "video":
                        message = await bot.send_video(
                            chat_id=channel_id, video=media_filename, caption=caption, parse_mode="HTML"
                        )
                    message_id = message.message_id
                except BadRequest as e:
                    if "Wrong type of the web page content" in str(e):
                        logger.warning(f"Incorrect content type for channel {channel_id}, sending without media: {media_filename}")
                        # Send without media
                        try:
                            message = await bot.send_message(
                                chat_id=channel_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True
                            )
                            message_id = message.message_id
                        except Exception as send_error:
                            logger.error(f"Error sending message to channel {channel_id}: {send_error}")
                            continue
                    else:
                        logger.error(f"BadRequest when sending media to channel {channel_id}: {e}")
                        continue
                except Exception as e:
                    logger.error(f"Error sending media to channel {channel_id}: {e}")
                    continue
            else:
                # No media or it failed validation - send text only
                if media_filename:
                    logger.warning(f"Media failed validation, sending without it: {media_filename}")
                try:
                    message = await bot.send_message(
                        chat_id=channel_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True
                    )
                    message_id = message.message_id
                except RetryAfter as e:
                    logger.warning(f"Flood control for channel {channel_id}, waiting {e.retry_after} seconds")
                    await asyncio.sleep(e.retry_after + 1)
                    message = await bot.send_message(
                        chat_id=channel_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True
                    )
                    message_id = message.message_id
                except Exception as e:
                    logger.error(f"Error sending message to channel {channel_id}: {e}")
                    continue

            # Mark publication in DB
            if translation_id:
                # This is a translation
                await mark_translation_as_published(translation_id, channel_id, message_id)
            else:
                # This is original news
                await mark_original_as_published(news_id, channel_id, message_id)

            logger.info(f"Published to {channel_id}: {title[:50]}...")

            # Add 5 second delay between publications to different channels
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error sending to {channel_id}: {e}")

        # Add delay after publication to enforce time-based limits
        if max_news_per_hour > 0:
            delay_seconds = 60 / max_news_per_hour
            logger.info(f"Adding {delay_seconds} seconds delay after publication for feed {feed_id}")
            await asyncio.sleep(delay_seconds)


# --- Main RSS item processing logic ---
async def process_rss_item(context, rss_item_from_api, subscribers_cache=None, channel_categories_cache=None):
    """Processes RSS item received from API."""
    async with RSS_ITEM_PROCESSING_SEMAPHORE:
        news_id = rss_item_from_api.get("news_id")  # ID remains news_id for compatibility
        logger.debug(f"Starting processing of RSS item {news_id} from API")

        # Convert API data to format expected by the rest of the code
        original_data = {
            "id": rss_item_from_api.get("news_id"),
            "title": rss_item_from_api.get("original_title"),
            "content": rss_item_from_api.get("original_content"),
            "category": rss_item_from_api.get("category"),
            "source": rss_item_from_api.get("source"),
            "lang": rss_item_from_api.get("original_language"),
            "link": rss_item_from_api.get("source_url"),
            "image_url": rss_item_from_api.get("image_url"),
        }

        logger.debug(f"original_data = {original_data}")

        # Translation processing
        translations = {}
        if rss_item_from_api.get("translations"):
            for lang, translation_data in rss_item_from_api["translations"].items():
                translations[lang] = {
                    "title": translation_data.get("title", ""),
                    "content": translation_data.get("content", ""),
                    "category": translation_data.get("category", ""),
                }

        logger.debug(f"Preparation of RSS item {news_id} completed.")

        prepared_rss_item = PreparedRSSItem(
            original_data=original_data,
            translations=translations,
            image_filename=original_data.get("image_url"),  # because that's how API returns
            video_filename=rss_item_from_api.get("video_filename"),
            feed_id=rss_item_from_api.get("feed_id"),
        )

        async def limited_post_to_channel():
            async with SEND_SEMAPHORE:
                await post_to_channel(context.bot, prepared_rss_item)

        async def limited_send_personal_rss_items():
            async with SEND_SEMAPHORE:
                await send_personal_rss_items(context.bot, prepared_rss_item, subscribers_cache)

        tasks_to_await = []
        category = rss_item_from_api.get("category")
        # Use cache to check category suitability for general channel
        if category and channel_categories_cache and channel_categories_cache.get(category, False):
            tasks_to_await.append(limited_post_to_channel())

        # Check if there are subscribers for category before adding personal send task
        if category and subscribers_cache and subscribers_cache.get(category):
            tasks_to_await.append(limited_send_personal_rss_items())
        else:
            logger.debug(f"Skipping personal send for news {news_id} - no subscribers for category {category}")

        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        # Mark RSS item as published in Telegram
        # For channels, publication is already marked in post_to_channel
        # For personal sends, no need to mark publication in DB
        pass

        logger.debug(f"Completion of RSS item {news_id} processing")
        return True


async def monitor_rss_items_task(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Starting RSS items monitoring task")
    try:
        # Get unprocessed RSS items via API
        rss_response = await get_rss_items_list(limit=20, telegram_published="false", include_all_translations="true")
        if not isinstance(rss_response, dict):
            logger.error(f"Invalid API response format: {type(rss_response)}")
            return

        unprocessed_rss_list = rss_response.get("results", [])
        logger.info(f"Received {len(unprocessed_rss_list)} RSS items from API")

        if not unprocessed_rss_list:
            logger.info("No RSS items to process.")
            return

        # Group RSS items by feed_id
        items_by_feed = defaultdict(list)
        for rss_item in unprocessed_rss_list:
            feed_id = rss_item.get("feed_id")
            if feed_id:
                items_by_feed[feed_id].append(rss_item)

        logger.info(f"Grouped into {len(items_by_feed)} feeds")

        # Collect unique categories to optimize subscriber queries
        unique_categories = set()
        for rss_item in unprocessed_rss_list:
            category = rss_item.get("category")
            if category:
                unique_categories.add(category)

        # Preliminary fetching of subscribers for unique categories
        subscribers_cache = {}
        # Cache for checking categories suitability for general channel
        channel_categories_cache = {}
        global user_manager
        for category in unique_categories:
            subscribers = await user_manager.get_subscribers_for_category(category)
            subscribers_cache[category] = subscribers
            channel_categories_cache[category] = category in CHANNEL_CATEGORIES
            if not subscribers:
                logger.info(f"No subscribers for category {category}")
            if channel_categories_cache[category]:
                logger.info(f"Category '{category}' is suitable for general channel")
            else:
                logger.info(f"Category '{category}' is NOT suitable for general channel")

        # Process feeds sequentially
        for feed_id, feed_items in items_by_feed.items():
            logger.info(f"Processing feed {feed_id} with {len(feed_items)} items")
            # Process items within feed sequentially
            for rss_item in feed_items:
                try:
                    await process_rss_item(context, rss_item, subscribers_cache, channel_categories_cache)
                except Exception as e:
                    logger.error(f"Error processing RSS item {rss_item.get('news_id')} from feed {feed_id}: {e}")

        logger.info("All RSS items from current batch processed.")

    except asyncio.TimeoutError:
        logger.error("Timeout getting RSS items")
    except Exception as e:
        logger.error(f"Error in monitoring task: {e}")


async def initialize_http_session():
    """Initializes HTTP session for API work."""
    global http_session
    if http_session is None:
        # Add retries and timeouts for more reliable connection
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=30, keepalive_timeout=30, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        http_session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers={"User-Agent": "TelegramBot/1.0"}
        )
        logger.info("HTTP session for API initialized")


async def cleanup_http_session():
    """Closes HTTP session."""
    global http_session
    if http_session:
        try:
            await http_session.close()
            http_session = None
            logger.info("HTTP session closed")
        except Exception as e:
            logger.error(f"Error closing HTTP session: {e}")


async def post_stop(application: Application) -> None:
    """Called when application stops."""
    logger.info("Stopping application and closing resources...")

    await cleanup_http_session()

    try:
        from config import close_shared_db_pool

        await close_shared_db_pool()
        logger.info("Shared connection pool closed")
    except Exception as e:
        logger.error(f"Error closing shared pool: {e}")

    logger.info("All resources freed")


async def post_init(application: Application) -> None:
    """Called after application initialization."""
    global user_manager
    logger.info("Application initialized")

    # Initialize user_manager
    try:
        user_manager = UserManager()
        logger.info("UserManager initialized")
    except Exception as e:
        logger.error(f"Error initializing UserManager: {e}")

    await initialize_http_session()


# --- Entry point ---
def main():
    logger.info("=== BOT STARTUP BEGINNING ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Bot token configured: {'Yes' if BOT_TOKEN else 'No'}")

    application = Application.builder().token(BOT_TOKEN).post_stop(post_stop).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("status", status_command))
    application.add_handler(CommandHandler("link", link_telegram_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_selection))
    application.add_handler(MessageHandler(filters.ALL, debug))
    application.add_error_handler(error_handler)

    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(monitor_rss_items_task, interval=180, first=1, job_kwargs={"misfire_grace_time": 600})
        job_queue.run_repeating(cleanup_expired_user_data, interval=3600, first=60)
        logger.info("Registered RSS items monitoring task (every 3 minutes)")
        logger.info("Registered task to clean expired user data (every 60 minutes)")

    logger.info("Bot started in Webhook mode")
    try:
        application.run_webhook(**WEBHOOK_CONFIG, close_loop=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Interrupted by user or system...")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Error handler."""
    if isinstance(context.error, NetworkError):
        logger.error("Network error detected. Retrying...")
    elif isinstance(context.error, BadRequest):
        if "Query is too old" in str(context.error):
            logger.error("Ignoring outdated callback query")
            return
        else:
            logger.error(f"Bad request error: {context.error}")
    else:
        logger.error(f"Other error: {context.error}")


if __name__ == "__main__":
    main()
