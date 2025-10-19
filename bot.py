# bot.py
import os
import sys
import asyncio
import aiohttp
import re
import html
import logging
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import NetworkError, BadRequest, RetryAfter
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from config import WEBHOOK_CONFIG, BOT_TOKEN, CHANNEL_IDS, CHANNEL_CATEGORIES, get_shared_db_pool
from user_manager import UserManager
from tenacity import retry, stop_after_attempt, wait_exponential
from firefeed_translations import get_message, LANG_NAMES, TRANSLATED_FROM_LABELS, READ_MORE_LABELS
from dataclasses import dataclass
from typing import Dict, Any, Optional

# Настройка логирования
from logging_config import setup_logging
setup_logging()
logger = logging.getLogger(__name__)

# --- Конфигурация API ---
API_BASE_URL = "http://localhost:8000/api/v1"

# --- Глобальные переменные ---
USER_STATES = {}
USER_CURRENT_MENUS = {}
USER_LANGUAGES = {}
SEND_SEMAPHORE = asyncio.Semaphore(5)
NEWS_PROCESSING_SEMAPHORE = asyncio.Semaphore(10)

user_manager = None
http_session = None  # Глобальная сессия для HTTP-запросов

@dataclass
class PreparedRSSItem:
    """Структура для хранения подготовленного RSS-элемента."""
    original_data: Dict[str, Any]
    translations: Dict[str, Dict[str, str]]
    image_filename: Optional[str]

# --- Функции для работы с БД ---
async def mark_translation_as_published(translation_id: int, channel_id: int, message_id: int = None):
    """Помечает перевод как опубликованный в Telegram-канале."""
    try:
        # Получаем общий пул подключений
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
                logger.info(f"Перевод {translation_id} помечен как опубликованный в канале {channel_id}")
                return True
    except Exception as e:
        logger.error(f"Ошибка при пометке перевода {translation_id} как опубликованного: {e}")
        return False

async def mark_original_as_published(news_id: str, channel_id: int, message_id: int = None):
    """Помечает оригинальную новость как опубликованную в Telegram-канале."""
    try:
        # Получаем общий пул подключений
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
                logger.info(f"Оригинальная новость {news_id} помечена как опубликованная в канале {channel_id}")
                return True
    except Exception as e:
        logger.error(f"Ошибка при пометке оригинальной новости {news_id} как опубликованной: {e}")
        return False

async def get_translation_id(news_id: str, language: str) -> int:
    """Получает ID перевода из таблицы news_translations."""
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
        logger.error(f"Ошибка при получении ID перевода для {news_id} на {language}: {e}")
        return None

# --- Функции для работы с API ---
async def api_get(endpoint: str, params: dict = None) -> dict:
    """Выполняет GET-запрос к API."""
    global http_session
    if http_session is None:
        raise RuntimeError("HTTP session not initialized")
    
    url = f"{API_BASE_URL}{endpoint}"
    try:
        # Преобразуем boolean параметры в строки
        if params:
            processed_params = {}
            for key, value in params.items():
                if isinstance(value, bool):
                    processed_params[key] = str(value).lower()
                else:
                    processed_params[key] = value
        else:
            processed_params = params
            
        timeout = aiohttp.ClientTimeout(total=10, connect=5)  # Таймаут 10 секунд для API запросов
        async with http_session.get(url, params=processed_params, timeout=timeout) as response:
            if response.status == 200:
                return await response.json()
            else:
                logger.error(f"{endpoint} returned status {response.status}")
                # Попытка получить текст ошибки для лучшего понимания проблемы
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
    """Получает список RSS-элементов."""
    params = {}
    if display_language is not None:
        params["display_language"] = display_language
    params.update(filters)
    return await api_get("/rss-items/", params)

async def get_rss_item_by_id(rss_item_id: str, display_language: str = "en") -> dict:
    """Получает RSS-элемент по ID."""
    params = {"display_language": display_language}
    return await api_get(f"/rss-items/{rss_item_id}", params)

async def get_categories() -> list:
    """Получает список категорий."""
    result = await api_get("/categories/")
    return result.get("results", [])

async def get_sources() -> list:
    """Получает список источников."""
    result = await api_get("/sources/")
    return result.get("results", [])

async def get_languages() -> list:
    """Получает список языков."""
    result = await api_get("/languages/")
    return result.get("results", [])

# --- Функции UI ---
def get_main_menu_keyboard(lang="en"):
    """Создает клавиатуру главного меню."""
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(get_message("menu_settings", lang)), KeyboardButton(get_message("menu_help", lang))],
            [KeyboardButton(get_message("menu_status", lang)), KeyboardButton(get_message("menu_language", lang))]
        ],
        resize_keyboard=True,
        input_field_placeholder=get_message("menu_placeholder", lang)
    )

async def set_current_user_language(user_id: int, lang: str):
    """Устанавливает язык пользователя в БД и в памяти."""
    global user_manager
    try:
        await user_manager.set_user_language(user_id, lang)
        USER_LANGUAGES[user_id] = lang
    except Exception as e:
        logger.error(f"Ошибка установки языка для {user_id}: {e}")

async def get_current_user_language(user_id: int) -> str:
    """Получает актуальный язык пользователя из памяти или БД."""
    if user_id in USER_LANGUAGES:
        return USER_LANGUAGES[user_id]
    try:
        lang = await user_manager.get_user_language(user_id)
        if lang:
            USER_LANGUAGES[user_id] = lang
        return lang or "en"
    except Exception as e:
        logger.error(f"Ошибка получения языка для {user_id}: {e}")
        return "en"

# --- Обработчики команд ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start."""
    user = update.effective_user
    user_id = user.id
    lang = await get_current_user_language(user_id)
    welcome_text = get_message("welcome", lang, user_name=user.first_name)
    await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /settings."""
    global user_manager
    user_id = update.effective_user.id
    try:
        lang = await get_current_user_language(user_id)
        logger.info(f"Loading settings for user {user_id}")
        settings = await user_manager.get_user_settings(user_id)
        logger.info(f"Loaded settings for user {user_id}: {settings}")
        current_subs = settings["subscriptions"] if isinstance(settings["subscriptions"], list) else []
        USER_STATES[user_id] = {
            "current_subs": current_subs,
            "language": settings["language"]
        }
        await _show_settings_menu(context.bot, update.effective_chat.id, user_id)
        USER_CURRENT_MENUS[user_id] = "settings"
    except Exception as e:
        logger.error(f"Ошибка команды /settings для {user_id}: {e}")
        lang = await get_current_user_language(user_id)
        await update.message.reply_text(get_message("settings_error", lang))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /help."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    help_text = get_message("help_text", lang)
    await update.message.reply_text(help_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"

async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /status."""
    global user_manager
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    settings = await user_manager.get_user_settings(user_id)
    categories = settings["subscriptions"]
    categories_text = ", ".join(categories) if categories else get_message("no_subscriptions", lang)
    status_text = get_message("status_text", lang,
                             language=LANG_NAMES.get(settings["language"], "English"),
                             categories=categories_text)
    await update.message.reply_text(status_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"

async def change_language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды смены языка."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    keyboard = [
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
        [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
        [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")]
    ]
    await update.message.reply_text(
        get_message("language_select", lang),
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    USER_CURRENT_MENUS[user_id] = "language"

async def link_telegram_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /link для привязки Telegram аккаунта."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)

    if not context.args:
        await update.message.reply_text(
            "Использование: /link <код_привязки>\n\n"
            "Получите код привязки в личном кабинете на сайте.",
            reply_markup=get_main_menu_keyboard(lang)
        )
        USER_CURRENT_MENUS[user_id] = "main"
        return

    link_code = context.args[0].strip()

    # Проверяем код через UserManager
    success = await user_manager.confirm_telegram_link(user_id, link_code)

    if success:
        await update.message.reply_text(
            "✅ Ваш Telegram аккаунт успешно привязан к аккаунту на сайте!\n\n"
            "Теперь вы можете управлять настройками через сайт или бота.",
            reply_markup=get_main_menu_keyboard(lang)
        )
    else:
        await update.message.reply_text(
            "❌ Код привязки недействителен или истек.\n\n"
            "Пожалуйста, сгенерируйте новый код в личном кабинете на сайте.",
            reply_markup=get_main_menu_keyboard(lang)
        )

    USER_CURRENT_MENUS[user_id] = "main"

# --- Вспомогательные функции UI ---
async def _show_settings_menu(bot, chat_id: int, user_id: int):
    """Отображает меню настроек."""
    state = USER_STATES.get(user_id)
    if not state: return
    current_subs = state["current_subs"]
    current_lang = state["language"]
    try:
        categories = await get_categories()
        keyboard = []
        for category in categories:
            category_name = category.get('name', str(category))
            is_selected = category_name in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category_name.capitalize()}"
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category_name}")])
        keyboard.append([InlineKeyboardButton(get_message("save_button", current_lang), callback_data="save_settings")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        await bot.send_message(
            chat_id=chat_id,
            text=get_message("settings_title", current_lang),
            reply_markup=reply_markup
        )
    except Exception as e:
        logger.error(f"Ошибка в _show_settings_menu для {user_id}: {e}")

async def _show_settings_menu_from_callback(bot, chat_id: int, user_id: int):
    """Отображает меню настроек из callback."""
    await _show_settings_menu(bot, chat_id, user_id)

# --- Обработчики callback и сообщений ---
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик callback кнопок."""
    global user_manager
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    try:
        if user_id not in USER_STATES:
            subs = await user_manager.get_user_subscriptions(user_id)
            current_subs = subs if isinstance(subs, list) else []
            USER_STATES[user_id] = {
                "current_subs": current_subs,
                "language": await get_current_user_language(user_id)
            }
        state = USER_STATES[user_id]
        current_lang = state["language"]
        if query.data.startswith("toggle_"):
            category = query.data.split("_", 1)[1]
            current_subs = state['current_subs']
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
            logger.info(f"Saving settings for user {user_id}: subscriptions={state['current_subs']}, language={state['language']}")
            result = await user_manager.save_user_settings(user_id, state["current_subs"], state["language"])
            logger.info(f"Save result for user {user_id}: {result}")
            USER_STATES.pop(user_id, None)
            try:
                await query.message.delete()
            except Exception:
                pass
            user = await context.bot.get_chat(user_id)
            welcome_text = get_message("settings_saved", current_lang) + "\n" + get_message("welcome", current_lang, user_name=user.first_name)
            await context.bot.send_message(chat_id=user_id, text=welcome_text, reply_markup=get_main_menu_keyboard(current_lang))
            USER_CURRENT_MENUS[user_id] = "main"
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
            welcome_text = get_message("language_changed", lang, language=LANG_NAMES.get(lang, "English")) + "\n" + get_message("welcome", lang, user_name=user.first_name)
            await context.bot.send_message(chat_id=user_id, text=welcome_text, reply_markup=get_main_menu_keyboard(lang))
            USER_CURRENT_MENUS[user_id] = "main"
        elif query.data == "change_lang":
            current_lang = await get_current_user_language(user_id)
            keyboard = [
                [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
                [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
                [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
                [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")]
            ]
            await query.message.edit_text(text=get_message("language_select", current_lang), reply_markup=InlineKeyboardMarkup(keyboard))
            USER_CURRENT_MENUS[user_id] = "language"
    except Exception as e:
        logger.error(f"Ошибка обработки кнопки для {user_id}: {e}")
        current_lang = await get_current_user_language(user_id)
        await context.bot.send_message(chat_id=user_id, text=get_message("button_error", current_lang), reply_markup=get_main_menu_keyboard(current_lang))
        USER_CURRENT_MENUS[user_id] = "main"

async def handle_menu_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора пункта меню."""
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
    logger.info(f"Неизвестный выбор меню для {user_id}: {text}")

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отладочных сообщений."""
    user_id = update.effective_user.id
    lang = await get_current_user_language(user_id)
    await update.message.reply_text(get_message("bot_active", lang), reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"

def clean_html(text):
    """Очищает HTML-теги из текста."""
    if not text:
        return ""
    # Используем стандартную функцию для экранирования HTML
    return html.escape(text)

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def send_personal_news(bot, prepared_rss_item: PreparedRSSItem):
    """Отправляет персональные RSS-элементы подписчикам."""
    global user_manager
    news_id = prepared_rss_item.original_data.get('id')
    logger.info(f"Отправка персонального RSS-элемента: {prepared_rss_item.original_data['title'][:50]}...")
    category = prepared_rss_item.original_data.get('category')
    if not category: 
        logger.warning(f"RSS-элемент {news_id} не имеет категории")
        return
    subscribers = await user_manager.get_subscribers_for_category(category)
    if not subscribers: 
        logger.info(f"Нет подписчиков для категории {category}")
        return
    translations_cache = prepared_rss_item.translations
    original_news_lang = prepared_rss_item.original_data.get('lang', '') # Имя переменной изменено для ясности
    
    for i, user in enumerate(subscribers):
        try:
            user_id = user['id']
            user_lang = user.get('language_code', 'en')
            
            # Проверяем, есть ли у элемента контент на языке пользователя
            title_to_send = None
            content_to_send = None

            # Если язык пользователя совпадает с языком оригинала элемента
            if user_lang == original_news_lang:
                title_to_send = prepared_rss_item.original_data['title']
                content_to_send = prepared_rss_item.original_data.get('content', '')
            # Иначе ищем перевод на язык пользователя
            elif user_lang in translations_cache and translations_cache[user_lang]:
                translation_data = translations_cache[user_lang]
                title_to_send = translation_data.get('title', '')
                content_to_send = translation_data.get('content', '')

            # Если нет подходящего контента, пропускаем пользователя
            if not title_to_send or not title_to_send.strip():
                logger.debug(f"Пропуск пользователя {user_id} - нет контента на языке {user_lang}")
                continue

            title_to_send = clean_html(title_to_send)
            content_to_send = clean_html(content_to_send)

            lang_note = ""
            if user_lang != original_news_lang:
                lang_note = f"\n🌐 {TRANSLATED_FROM_LABELS.get(user_lang, 'Translated from')} {original_news_lang.upper()}\n"
            content_text = (
                f"🔥 <b>{title_to_send}</b>\n"
                f"\n\n{content_to_send}\n"
                f"\nFROM: {prepared_rss_item.original_data.get('source', 'Unknown Source')}\n"
                f"CATEGORY: {category}\n{lang_note}\n"
                f"⚡ <a href='{prepared_rss_item.original_data.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
            )
            image_filename = prepared_rss_item.image_filename
            logger.debug(f"send_personal_news image_filename = {image_filename}")

            if image_filename:
                import re
                valid_image_url = re.match(r'^https?://.+\.(jpg|jpeg|png|gif|webp)(\?.*)?$', image_filename, re.IGNORECASE)

                if not valid_image_url:
                    logger.warning(f"Недопустимый URL изображения для Telegram: {image_filename}")
                    return # Выходим из текущей итерации

                caption = content_text
                if len(caption) > 1024:
                    base_text = f"🔥 <b>{title_to_send}</b>\nFROM: {prepared_rss_item.original_data.get('source', 'Unknown Source')}\nCATEGORY: {category}{lang_note}\n⚡ <a href='{prepared_rss_item.original_data.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
                    max_content_length = 1024 - len(base_text)
                    if max_content_length > 0:
                        truncated_content = content_to_send[:max_content_length-3] + "..."
                        caption = f"🔥 <b>{title_to_send}</b>\n{truncated_content}\n{base_text}"
                    else:
                        caption = caption[:1021] + "..."
                try:
                    await bot.send_photo(chat_id=user_id, photo=image_filename, caption=caption, parse_mode="HTML")
                except RetryAfter as e:
                    logger.warning(f"Flood control для пользователя {user_id}, ждем {e.retry_after} секунд")
                    await asyncio.sleep(e.retry_after + 1)
                    await bot.send_photo(chat_id=user_id, photo=image_filename, caption=caption, parse_mode="HTML")
                except Exception as e:
                    logger.error(f"Ошибка отправки фото пользователю {user_id}: {e}")
            else:
                try:
                    await bot.send_message(chat_id=user_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True)
                except RetryAfter as e:
                    logger.warning(f"Flood control для пользователя {user_id}, ждем {e.retry_after} секунд")
                    await asyncio.sleep(e.retry_after + 1)
                    await bot.send_message(chat_id=user_id, text=content_text, parse_mode="HTML", disable_web_page_preview=True)
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения пользователю {user_id}: {e}")
            
            if i < len(subscribers) - 1:
                await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"Ошибка отправки персонального RSS-элемента пользователю {user.get('id', 'Unknown ID')}: {e}")

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
async def post_to_channel(bot, prepared_rss_item: PreparedRSSItem):
    """Публикует RSS-элемент в Telegram-каналы."""
    original_title = prepared_rss_item.original_data['title']
    news_id = prepared_rss_item.original_data.get('id')
    logger.info(f"Публикация RSS-элемента в каналы: {original_title[:50]}...")
    logger.debug(f"post_to_channel prepared_rss_item = {prepared_rss_item}")
    original_content = prepared_rss_item.original_data.get('content', '')
    category = prepared_rss_item.original_data.get('category', '')
    original_source = prepared_rss_item.original_data.get('source', 'UnknownSource')
    original_lang = prepared_rss_item.original_data['lang']
    translations_cache = prepared_rss_item.translations
    channels_list = list(CHANNEL_IDS.items())

    # Отправляем в каналы, где есть перевод или оригинал
    for target_lang, channel_id in channels_list:
        try:
            # Определяем, использовать ли перевод или оригинал
            if target_lang == original_lang:
                # Оригинальный язык
                title = original_title
                content = original_content
                lang_note = ""
                translation_id = None  # Для оригинального языка нет перевода
            elif target_lang in translations_cache and translations_cache[target_lang]:
                # Есть перевод
                translation_data = translations_cache[target_lang]
                title = translation_data.get('title', original_title)
                content = translation_data.get('content', original_content)
                lang_note = f"\n{TRANSLATED_FROM_LABELS.get(target_lang, '[AI] Translated from')} {original_lang.upper()}\n"
                # Получаем ID перевода для отслеживания публикации
                translation_id = await get_translation_id(news_id, target_lang)
                if not translation_id:
                    logger.warning(f"Не найден ID перевода для {news_id} на {target_lang}, пропускаем публикацию")
                    continue
            else:
                # Нет перевода, пропускаем
                logger.debug(f"Нет перевода для {news_id} на {target_lang}, пропускаем публикацию")
                continue
            hashtags = f"\n#{category} #{original_source}"
            content_text = f"<b>{title}</b>\n"
            if content and content.strip():
                content_text += f"\n{content}\n"
            content_text += f"{lang_note}{hashtags}"
            image_filename = prepared_rss_item.image_filename
            logger.debug(f"post_to_channel image_filename = {image_filename}")

            if image_filename:
                valid_image_url = re.match(r'^https?://.+\.(jpg|jpeg|png|gif|webp)(\?.*)?$', image_filename, re.IGNORECASE)

                if not valid_image_url:
                    logger.warning(f"Недопустимый URL изображения для Telegram: {image_filename}")
                    return

                caption = content_text
                if len(caption) > 1024:
                    base_text = f"<b>{title}</b>{lang_note}{hashtags}"
                    max_content_length = 1024 - len(base_text)
                    if max_content_length > 0:
                        truncated_content = content[:max_content_length-3] + "..."
                        caption = f"<b>{title}</b>\n{truncated_content}{lang_note}{hashtags}"
                    else:
                        caption = caption[:1021] + "..."
                try:
                    message = await bot.send_photo(chat_id=channel_id, photo=image_filename, caption=caption, parse_mode='HTML')
                    message_id = message.message_id
                except RetryAfter as e:
                    logger.warning(f"Flood control для канала {channel_id}, ждем {e.retry_after} секунд")
                    await asyncio.sleep(e.retry_after + 1)
                    message = await bot.send_photo(chat_id=channel_id, photo=image_filename, caption=caption, parse_mode='HTML')
                    message_id = message.message_id
                except Exception as e:
                    logger.error(f"Ошибка отправки фото в канал {channel_id}: {e}")
                    continue
            else:
                try:
                    message = await bot.send_message(chat_id=channel_id, text=content_text, parse_mode='HTML', disable_web_page_preview=True)
                    message_id = message.message_id
                except RetryAfter as e:
                    logger.warning(f"Flood control для канала {channel_id}, ждем {e.retry_after} секунд")
                    await asyncio.sleep(e.retry_after + 1)
                    message = await bot.send_message(chat_id=channel_id, text=content_text, parse_mode='HTML', disable_web_page_preview=True)
                    message_id = message.message_id
                except Exception as e:
                    logger.error(f"Ошибка отправки сообщения в канал {channel_id}: {e}")
                    continue

            # Помечаем публикацию в БД
            if translation_id:
                # Это перевод
                await mark_translation_as_published(translation_id, channel_id, message_id)
            else:
                # Это оригинальная новость
                await mark_original_as_published(news_id, channel_id, message_id)

            logger.info(f"Опубликовано в {channel_id}: {title[:50]}...")
            # Не выходим, продолжаем для других каналов, где есть переводы
        except Exception as e:
            logger.error(f"Ошибка отправки в {channel_id}: {e}")

# --- Основная логика обработки RSS-элементов ---
async def process_rss_item(context, rss_item_from_api):
    """Обрабатывает RSS-элемент, полученный из API."""
    async with NEWS_PROCESSING_SEMAPHORE:
        news_id = rss_item_from_api.get('news_id') # ID остается news_id для совместимости
        logger.debug(f"Начало обработки RSS-элемента {news_id} из API")

        # Преобразуем данные из API в формат, ожидаемый остальным кодом
        original_data = {
            'id': rss_item_from_api.get('news_id'),
            'title': rss_item_from_api.get('original_title'),
            'content': rss_item_from_api.get('original_content'),
            'category': rss_item_from_api.get('category'),
            'source': rss_item_from_api.get('source'),
            'lang': rss_item_from_api.get('original_language'),
            'link': rss_item_from_api.get('source_url'),
            'image_url': rss_item_from_api.get('image_url')
        }

        logger.debug(f"original_data = {original_data}")
        
        # Обработка переводов
        translations = {}
        if rss_item_from_api.get('translations'):
            for lang, translation_data in rss_item_from_api['translations'].items():
                translations[lang] = {
                    'title': translation_data.get('title', ''),
                    'content': translation_data.get('content', ''), # Контент в API теперь content
                    'category': translation_data.get('category', '')
                }
        
        logger.debug(f"Подготовка RSS-элемента {news_id} завершена.")
        
        prepared_rss_item = PreparedRSSItem(
            original_data=original_data,
            translations=translations,
            image_filename=original_data.get('image_url') # потому что так возвращает API
        )
        
        async def limited_post_to_channel():
            async with SEND_SEMAPHORE:
                await post_to_channel(context.bot, prepared_rss_item)

        async def limited_send_personal_news():
            async with SEND_SEMAPHORE:
                await send_personal_news(context.bot, prepared_rss_item)

        tasks_to_await = []
        if rss_item_from_api.get('category') in CHANNEL_CATEGORIES:
            logger.info(f"RSS-элемент категории '{rss_item_from_api.get('category')}' подходит для общего канала.")
            tasks_to_await.append(limited_post_to_channel())
        else:
            logger.info(f"RSS-элемент категории '{rss_item_from_api.get('category')}' НЕ подходит для общего канала.")

        tasks_to_await.append(limited_send_personal_news())

        if tasks_to_await:
             await asyncio.gather(*tasks_to_await, return_exceptions=True)

        # Помечаем RSS-элемент как опубликованный в Telegram
        # Для каналов публикация уже отмечена в post_to_channel
        # Для персональных отправок не нужно отмечать публикацию в БД
        pass

        logger.debug(f"Завершение обработки RSS-элемента {news_id}")
        return True

async def monitor_news_task(context: ContextTypes.DEFAULT_TYPE):
    logger.info("Запуск задачи мониторинга RSS-элементов")
    try:
        # Получаем необработанные RSS-элементы через API
        rss_response = await get_rss_items_list(limit=20, telegram_published="false", include_all_translations="true")
        if not isinstance(rss_response, dict):
            logger.error(f"Неверный формат ответа от API: {type(rss_response)}")
            return
            
        unprocessed_rss_list = rss_response.get("results", [])
        logger.info(f"Получено {len(unprocessed_rss_list)} RSS-элементов из API")
        
        if not unprocessed_rss_list:
             logger.info("Нет RSS-элементов для обработки.")
             return

        processing_tasks = [
            process_rss_item(context, rss_item)
            for rss_item in unprocessed_rss_list
        ]
        
        logger.info(f"Запуск обработки {len(processing_tasks)} RSS-элементов...")
        try:
            await asyncio.gather(*processing_tasks, return_exceptions=True)
            logger.info("Все RSS-элементы из текущей партии обработаны.")
        except Exception as e:
             logger.error(f"Ошибка при обработке партии RSS-элементов: {e}")
             
    except asyncio.TimeoutError:
        logger.error("Таймаут получения RSS-элементов")
    except Exception as e:
        logger.error(f"Ошибка в задаче мониторинга: {e}")

async def initialize_http_session():
    """Инициализирует HTTP-сессию для работы с API."""
    global http_session
    if http_session is None:
        # Добавляем повторные попытки и таймауты для более надежного соединения
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=30,
            keepalive_timeout=30,
            enable_cleanup_closed=True
        )
        timeout = aiohttp.ClientTimeout(total=15, connect=5)
        http_session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'TelegramBot/1.0'}
        )
        logger.info("HTTP-сессия для API инициализирована")

async def cleanup_http_session():
    """Закрывает HTTP-сессию."""
    global http_session
    if http_session:
        try:
            await http_session.close()
            http_session = None
            logger.info("HTTP-сессия закрыта")
        except Exception as e:
            logger.error(f"Ошибка при закрытии HTTP-сессии: {e}")

async def post_stop(application: Application) -> None:
    """Вызывается при остановке приложения."""
    logger.info("Остановка приложения и закрытие ресурсов...")
    
    await cleanup_http_session()
    
    try:
        from config import close_shared_db_pool
        await close_shared_db_pool()
        logger.info("Общий пул подключений закрыт")
    except Exception as e:
        logger.error(f"Ошибка при закрытии общего пула: {e}")
    
    logger.info("Все ресурсы освобождены")

async def post_init(application: Application) -> None:
    """Вызывается после инициализации приложения."""
    global user_manager
    logger.info("Приложение инициализировано")
    
    # Инициализируем user_manager
    try:
        user_manager = UserManager()
        logger.info("UserManager инициализирован")
    except Exception as e:
        logger.error(f"Ошибка инициализации UserManager: {e}")
    
    await initialize_http_session()

# --- Точка входа ---
def main():
    logger.info("=== НАЧАЛО ЗАПУСКА БОТА ===")
    logger.info(f"Python version: {sys.version}")
    logger.info(f"Current working directory: {os.getcwd()}")
    logger.info(f"Bot token length: {len(BOT_TOKEN) if BOT_TOKEN else 0}")
    
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
        job_queue.run_repeating(monitor_news_task, interval=300, first=1, job_kwargs={'misfire_grace_time': 600})
        logger.info("Зарегистрирована задача мониторинга RSS-элементов (каждые 5 минут)")
    
    logger.info("Бот запущен в режиме Webhook")
    try:
        application.run_webhook(**WEBHOOK_CONFIG, close_loop=False)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Прервано пользователем или системой...")
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        raise

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик ошибок."""
    if isinstance(context.error, NetworkError):
        logger.error("Network error detected. Retrying...")
    elif isinstance(context.error, BadRequest):
        if "Query is too old" in str(context.error):
            logger.error("Ignoring outdated callback query")
            return
        else:
            logger.error(f"Bad request error: {context.error}")
    else:
        logger.error(f"Другая ошибка: {context.error}")

if __name__ == "__main__":
    main()
