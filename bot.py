import os
import signal
import asyncio
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import NetworkError, BadRequest, TelegramError
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from config import WEBHOOK_CONFIG, BOT_TOKEN, CHANNEL_IDS, CHANNEL_CATEGORIES, IMAGES_ROOT_DIR
from functools import lru_cache
from user_manager import UserManager
from translator import translate_text, prepare_translations
from tenacity import retry, stop_after_attempt, wait_exponential
from rss_manager import RSSManager
from firefeed_utils import clean_html, download_and_save_image, extract_image_from_preview
from firefeed_dublicate_detector import FireFeedDuplicateDetector
from firefeed_translations import get_message, LANG_NAMES, TRANSLATED_FROM_LABELS, READ_MORE_LABELS

import requests
from bs4 import BeautifulSoup

USER_STATES = {}
USER_CURRENT_MENUS = {}
# Храним язык пользователя в памяти для быстрого доступа
USER_LANGUAGES = {}
SEND_SEMAPHORE = asyncio.Semaphore(5)
# --- Переменная для хранения задачи пакетной обработки ---
batch_processor_task = None

# Глобальные переменные для менеджеров
rss_manager = None
user_manager = None
duplicate_detector = None

# Создаем клавиатуру меню
def get_main_menu_keyboard(lang="en"):
    print(f"[LOG] Создание главного меню для языка: {lang}")
    keyboard = ReplyKeyboardMarkup(
        [
            [
                KeyboardButton(get_message("menu_settings", lang)), 
                KeyboardButton(get_message("menu_help", lang))
            ],
            [
                KeyboardButton(get_message("menu_status", lang)), 
                KeyboardButton(get_message("menu_language", lang))
            ]
        ],
        resize_keyboard=True,
        input_field_placeholder=get_message("menu_placeholder", lang)
    )
    return keyboard
# Улучшенная функция установки языка пользователя
async def set_current_user_language(user_id, lang):
    user_manager = UserManager()
    """Устанавливает язык пользователя в БД и в памяти"""
    print(f"[LOG] Установка языка пользователя {user_id} на {lang}")
    try:
        # Сохраняем в БД
        await user_manager.set_user_language(user_id, lang)
        print(f"[LOG] Язык {lang} сохранен в БД для пользователя {user_id}")
        # Сохраняем в памяти
        USER_LANGUAGES[user_id] = lang
        print(f"[LOG] Язык {lang} сохранен в памяти для пользователя {user_id}")
    except Exception as e:
        print(f"[ERROR] Ошибка установки языка для {user_id}: {e}")
# Улучшенная функция получения языка пользователя
async def get_current_user_language(user_id):
    user_manager = UserManager()
    """Получает актуальный язык пользователя из памяти или БД"""
    # Сначала проверяем в памяти
    if user_id in USER_LANGUAGES:
        lang = USER_LANGUAGES[user_id]
        print(f"[LOG] Получен язык пользователя {user_id} из памяти: {lang}")
        return lang
    # Если нет в памяти, получаем из БД
    try:
        lang = await user_manager.get_user_language(user_id)
        print(f"[LOG] Получен язык пользователя {user_id} из БД: {lang}")
        if lang:
            # Сохраняем в памяти для быстрого доступа
            USER_LANGUAGES[user_id] = lang
        return lang or "en"
    except Exception as e:
        print(f"[ERROR] Ошибка получения языка для {user_id}: {e}")
        return "en"
# @lru_cache(maxsize=1000)
def cached_translate(text, source_lang, target_lang):
    return translate_text(text, source_lang, target_lang)
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды /start от пользователя {update.effective_user.id}")
    user = update.effective_user
    user_id = user.id
    lang = await get_current_user_language(user_id)
    print(f"[LOG] Язык пользователя {user_id}: {lang}")
    welcome_text = get_message("welcome", lang, user_name=user.first_name)
    print(f"[LOG] Отправка приветственного сообщения пользователю {user_id}")
    await update.message.reply_text(welcome_text, reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"
    print(f"[LOG] Установлено текущее меню для {user_id}: main")
async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды /settings от пользователя {update.effective_user.id}")
    try:
        user_manager = UserManager()
        user_id = update.effective_user.id
        lang = await get_current_user_language(user_id)
        print(f"[LOG] Язык пользователя {user_id}: {lang}")
        settings = await user_manager.get_user_settings(user_id)
        print(f"[LOG] Настройки пользователя {user_id}: {settings}")
        USER_STATES[user_id] = {
            "current_subs": settings["subscriptions"].copy(),
            "language": settings["language"]
        }
        print(f"[LOG] Сохранено состояние для {user_id}: {USER_STATES[user_id]}")
        await show_settings_menu(update, context, user_id)
        USER_CURRENT_MENUS[user_id] = "settings"
        print(f"[LOG] Установлено текущее меню для {user_id}: settings")
    except Exception as e:
        print(f"[ERROR] Ошибка команды /settings для {update.effective_user.id}: {e}")
        lang = await get_current_user_language(update.effective_user.id)
        await update.message.reply_text(get_message("settings_error", lang))
async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    print(f"[LOG] Отображение меню настроек для пользователя {user_id}")
    try:
        state = USER_STATES.get(user_id)
        if not state:
            print(f"[LOG] Нет состояния для пользователя {user_id}")
            return
        current_subs = state["current_subs"]
        current_lang = state["language"]
        print(f"[LOG] Текущие подписки {user_id}: {current_subs}")
        print(f"[LOG] Текущий язык {user_id}: {current_lang}")
        keyboard = []
        categories = await rss_manager.get_categories()
        print(f"[LOG] Доступные категории: {categories}")
        for category in categories:
            is_selected = category in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category.capitalize()}"
            print(f"[LOG] Категория {category}, выбрана: {is_selected}")
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category}")])
        keyboard.append([InlineKeyboardButton(get_message("save_button", current_lang), callback_data="save_settings")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        print(f"[LOG] Создана клавиатура настроек для {user_id}")
        await update.message.reply_text(
            get_message("settings_title", current_lang),
            reply_markup=reply_markup
        )
        print(f"[LOG] Отправлено меню настроек пользователю {user_id}")
    except Exception as e:
        print(f"[ERROR] Ошибка в show_settings_menu для {user_id}: {e}")
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_manager = UserManager()
    query = update.callback_query
    print(f"[LOG] Получен callback от пользователя {query.from_user.id}: {query.data}")
    await query.answer()
    user_id = query.from_user.id
    try:
        if user_id not in USER_STATES:
            print(f"[LOG] Создание нового состояния для пользователя {user_id}")
            USER_STATES[user_id] = {
                "current_subs": user_manager.get_user_subscriptions(user_id) or [],
                "language": await get_current_user_language(user_id)
            }
            print(f"[LOG] Новое состояние для {user_id}: {USER_STATES[user_id]}")
        state = USER_STATES[user_id]
        current_lang = state["language"]
        print(f"[LOG] Текущий язык для {user_id}: {current_lang}")
        if query.data.startswith("toggle_"):
            print(f"[LOG] Обработка переключения категории для {user_id}")
            category = query.data.split("_")[1]
            current_subs = state['current_subs']
            print(f"[LOG] Переключение категории {category} для {user_id}")
            print(f"[LOG] Текущие подписки до: {current_subs}")
            if category in current_subs:
                current_subs.remove(category)
                print(f"[LOG] Категория {category} удалена из подписок")
            else:
                current_subs.append(category)
                print(f"[LOG] Категория {category} добавлена в подписки")
            state["current_subs"] = current_subs
            print(f"[LOG] Текущие подписки после: {current_subs}")
            try:
                print(f"[LOG] Удаление старого сообщения настроек для {user_id}")
                await query.message.delete()
            except Exception as delete_error:
                print(f"[ERROR] Ошибка удаления сообщения для {user_id}: {delete_error}")
            print(f"[LOG] Отображение обновленного меню настроек для {user_id}")
            await show_settings_menu_from_callback(query, context, user_id)
        elif query.data == "save_settings":
            print(f"[LOG] Сохранение настроек для пользователя {user_id}")
            print(f"[LOG] Сохраняемые данные: подписки={state['current_subs']}, язык={state['language']}")
            await user_manager.save_user_settings(
                user_id,
                state["current_subs"],
                state["language"]
            )
            print(f"[LOG] Настройки сохранены для {user_id}")
            if user_id in USER_STATES:
                del USER_STATES[user_id]
                print(f"[LOG] Состояние удалено для {user_id}")
            try:
                print(f"[LOG] Удаление сообщения настроек для {user_id}")
                await query.message.delete()
            except Exception as delete_error:
                print(f"[ERROR] Ошибка удаления сообщения настроек для {user_id}: {delete_error}")
            user = await context.bot.get_chat(user_id)
            welcome_text = get_message("settings_saved", current_lang) + "\n" + get_message("welcome", current_lang, user_name=user.first_name)
            print(f"[LOG] Отправка подтверждения сохранения и главного меню для {user_id}")
            await context.bot.send_message(
                chat_id=user_id,
                text=welcome_text,
                reply_markup=get_main_menu_keyboard(current_lang)
            )
            USER_CURRENT_MENUS[user_id] = "main"
            print(f"[LOG] Установлено текущее меню для {user_id}: main")
        elif query.data.startswith("lang_"):
            print(f"[LOG] Обработка выбора языка для {user_id}")
            lang = query.data.split("_")[1]
            print(f"[LOG] Выбранный язык: {lang}")
            # Используем улучшенную функцию установки языка
            await set_current_user_language(user_id, lang)
            print(f"[LOG] Язык сохранен для {user_id}: {lang}")
            if user_id in USER_STATES:
                state = USER_STATES[user_id]
                state["language"] = lang
                print(f"[LOG] Язык обновлен в состоянии для {user_id}: {lang}")
            try:
                print(f"[LOG] Удаление сообщения выбора языка для {user_id}")
                await query.message.delete()
            except Exception as delete_error:
                print(f"[ERROR] Ошибка удаления сообщения выбора языка для {user_id}: {delete_error}")
            user = await context.bot.get_chat(user_id)
            welcome_text = get_message("language_changed", lang, language=LANG_NAMES.get(lang, "English")) + "\n" + get_message("welcome", lang, user_name=user.first_name)
            print(f"[LOG] Отправка подтверждения смены языка и главного меню для {user_id}")
            await context.bot.send_message(
                chat_id=user_id,
                text=welcome_text,
                reply_markup=get_main_menu_keyboard(lang)
            )
            USER_CURRENT_MENUS[user_id] = "main"
            print(f"[LOG] Установлено текущее меню для {user_id}: main")
        elif query.data == "change_lang":
            print(f"[LOG] Обработка запроса смены языка для {user_id}")
            # Получаем актуальный язык пользователя
            current_lang = await get_current_user_language(user_id)
            keyboard = [
                [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
                [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
                [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
                [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")]
            ]
            print(f"[LOG] Отправка меню выбора языка для {user_id}")
            await query.message.edit_text(
                text=get_message("language_select", current_lang),
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            USER_CURRENT_MENUS[user_id] = "language"
            print(f"[LOG] Установлено текущее меню для {user_id}: language")
    except Exception as e:
        print(f"[ERROR] Ошибка обработки кнопки для {user_id}: {e}")
        current_lang = await get_current_user_language(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text=get_message("button_error", current_lang),
            reply_markup=get_main_menu_keyboard(current_lang)
        )
        USER_CURRENT_MENUS[user_id] = "main"
async def show_settings_menu_from_callback(query, context, user_id: int):
    print(f"[LOG] Отображение меню настроек из callback для {user_id}")
    try:
        state = USER_STATES.get(user_id)
        if not state:
            print(f"[LOG] Нет состояния для пользователя {user_id}")
            return
        current_subs = state["current_subs"]
        current_lang = state["language"]
        print(f"[LOG] Текущие подписки {user_id}: {current_subs}")
        print(f"[LOG] Текущий язык {user_id}: {current_lang}")
        keyboard = []
        categories = await rss_manager.get_categories()
        print(f"[LOG] Доступные категории: {categories}")
        for category in categories:
            is_selected = category in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category.capitalize()}"
            print(f"[LOG] Категория {category}, выбрана: {is_selected}")
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category}")])
        keyboard.append([InlineKeyboardButton(get_message("save_button", current_lang), callback_data="save_settings")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        print(f"[LOG] Создана клавиатура настроек для {user_id}")
        await context.bot.send_message(
            chat_id=user_id,
            text=get_message("settings_title", current_lang),
            reply_markup=reply_markup
        )
        print(f"[LOG] Отправлено меню настроек пользователю {user_id}")
    except Exception as e:
        print(f"[ERROR] Ошибка в show_settings_menu_from_callback для {user_id}: {e}")
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды /help от пользователя {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = await get_current_user_language(user_id)
    print(f"[LOG] Актуальный язык пользователя {user_id}: {lang}")
    help_text = get_message("help_text", lang)
    print(f"[LOG] Отправка справки пользователю {user_id}")
    await update.message.reply_text(help_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"
    print(f"[LOG] Установлено текущее меню для {user_id}: main")
async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_manager = UserManager()
    print(f"[LOG] Вызов команды /status от пользователя {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = await get_current_user_language(user_id)
    print(f"[LOG] Актуальный язык пользователя {user_id}: {lang}")
    settings = await user_manager.get_user_settings(user_id)
    print(f"[LOG] Настройки пользователя {user_id}: {settings}")
    categories = settings["subscriptions"]
    categories_text = ", ".join(categories) if categories else get_message("no_subscriptions", lang)
    print(f"[LOG] Категории пользователя {user_id}: {categories_text}")
    status_text = get_message("status_text", lang, 
                             language=LANG_NAMES.get(settings["language"], "English"),
                             categories=categories_text)
    print(f"[LOG] Отправка статуса пользователю {user_id}")
    await update.message.reply_text(status_text, parse_mode='HTML', reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"
    print(f"[LOG] Установлено текущее меню для {user_id}: main")
async def handle_menu_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Обработка выбора меню от пользователя {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = await get_current_user_language(user_id)
    text = update.message.text
    print(f"[LOG] Пользователь {user_id} выбрал: {text}")
    print(f"[LOG] Актуальный язык пользователя из памяти/БД: {lang}")
    # Получаем все возможные варианты текста кнопок для текущего языка
    menu_settings = get_message("menu_settings", lang)
    menu_help = get_message("menu_help", lang)
    menu_status = get_message("menu_status", lang)
    menu_language = get_message("menu_language", lang)
    print(f"[LOG] Сравнение с кнопками на языке {lang}: Settings='{menu_settings}', Help='{menu_help}', Status='{menu_status}', Language='{menu_language}'")
    # Также проверяем на всех других языках для надежности
    all_languages = ["en", "ru", "de", "fr"]
    language_matches = {}
    for check_lang in all_languages:
        settings_text = get_message("menu_settings", check_lang)
        help_text = get_message("menu_help", check_lang)
        status_text = get_message("menu_status", check_lang)
        language_text = get_message("menu_language", check_lang)
        if text == settings_text:
            language_matches[check_lang] = "settings"
        elif text == help_text:
            language_matches[check_lang] = "help"
        elif text == status_text:
            language_matches[check_lang] = "status"
        elif text == language_text:
            language_matches[check_lang] = "language"
    print(f"[LOG] Совпадения по языкам: {language_matches}")
    if text == menu_settings:
        print(f"[LOG] Выбрана настройка для {user_id}")
        await settings_command(update, context)
    elif text == menu_help:
        print(f"[LOG] Выбрана помощь для {user_id}")
        await help_command(update, context)
    elif text == menu_status:
        print(f"[LOG] Выбран статус для {user_id}")
        await status_command(update, context)
    elif text == menu_language:
        print(f"[LOG] Выбран язык для {user_id}")
        await change_language_command(update, context)
    elif language_matches:
        # Если есть совпадения на других языках, используем первое найденное
        matched_lang = list(language_matches.keys())[0]
        matched_action = language_matches[matched_lang]
        print(f"[LOG] Найдено совпадение на языке {matched_lang}: {matched_action}")
        # Обновляем язык пользователя
        await set_current_user_language(user_id, matched_lang)
        print(f"[LOG] Обновлен язык пользователя {user_id} на {matched_lang}")
        # Выполняем соответствующее действие с новым языком
        if matched_action == "settings":
            await settings_command(update, context)
        elif matched_action == "help":
            await help_command(update, context)
        elif matched_action == "status":
            await status_command(update, context)
        elif matched_action == "language":
            await change_language_command(update, context)
    else:
        print(f"[LOG] Неизвестный выбор меню для {user_id}: {text}")
async def change_language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды смены языка от пользователя {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = await get_current_user_language(user_id)
    print(f"[LOG] Актуальный язык пользователя {user_id}: {lang}")
    keyboard = [
        [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
        [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
        [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
        [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")]
    ]
    print(f"[LOG] Отправка меню выбора языка пользователю {user_id}")
    await update.message.reply_text(
        get_message("language_select", lang),
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    USER_CURRENT_MENUS[user_id] = "language"
    print(f"[LOG] Установлено текущее меню для {user_id}: language")
@retry(stop=stop_after_attempt(5), 
       wait=wait_exponential(multiplier=1, min=2, max=30))
async def send_personal_news(bot, news_item: dict, translations_dict: dict):
    """
    Отправляет персональные новости подписчикам на основе их подписок и языка.
    Использует заранее подготовленные переводы.
    :param bot: Экземпляр бота python-telegram-bot.
    :param news_item: Словарь с оригинальными данными новости.
    :param translations_dict: Словарь переводов, полученный из prepare_translations.
                              Формат: {'ru': {'title': '...', 'description': '...', 'category': '...'}, ...}
    """
    # Предполагаем, что UserManager доступен
    # Если он находится в другом модуле, импортируйте его
    # from your_user_module import UserManager 
    user_manager = UserManager() 
    original_title = news_item['title']
    news_id = news_item.get('id')  # Получаем ID новости
    print(f"[LOG] Отправка персональной новости: {original_title[:50]}...")
    category = news_item.get('category')
    if not category:
        print("[WARN] Категория новости не указана. Персональная рассылка пропущена.")
        return
    subscribers = await user_manager.get_subscribers_for_category(category)
    print(f"[LOG] Найдено {len(subscribers)} подписчиков для категории {category}")
    if not subscribers:
        print(f"[LOG] Нет подписчиков для категории {category}.")
        return
    # Получаем путь к локальному изображению (если оно было сохранено ранее)
    local_image_path = news_item.get('image_filename')

    for user in subscribers:
        try:
            user_id = user['id']
            user_lang = user.get('language_code', 'en') # Получаем язык пользователя, по умолчанию 'en'
            # --- Получение переведенного контента ---
            # 1. Пытаемся получить перевод для языка пользователя
            translation_data = translations_dict.get(user_lang)
            if translation_data and isinstance(translation_data, dict):
                # Используем готовый перевод
                title_to_send = translation_data.get('title', original_title)
                description_to_send = translation_data.get('description', news_item.get('description', ''))
                # category_to_send = translation_data.get('category', category) # Если нужно
            else:
                # 2. Если перевод для языка пользователя отсутствует или поврежден,
                #    используем оригинальные данные
                print(f"[WARN] Перевод для языка '{user_lang}' не найден или некорректен для новости '{original_title[:30]}...'. Отправляем оригинал.")
                title_to_send = original_title
                description_to_send = news_item.get('description', '')
                # category_to_send = category # Если нужно
            title_to_send = clean_html(title_to_send) 
            description_to_send = clean_html(description_to_send)
            # --- Формирование примечания о переводе ---
            lang_note = ""
            original_news_lang = news_item.get('lang', '')
            if user_lang != original_news_lang:
                 lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS.get(user_lang, 'Translated from')} {original_news_lang.upper()}"
            # --- Формирование сообщения ---
            # Используем .get() с дефолтными значениями для надежности
            content_text = (
                f"🔥 <b>{title_to_send}</b>\n"
                f"{description_to_send}\n"
                f"FROM: {news_item.get('source', 'Unknown Source')}\n"
                f"CATEGORY: {category}{lang_note}\n" # Используем оригинальную категорию или category_to_send
                f"⚡ <a href='{news_item.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
            )
            # --- Отправка в зависимости от наличия изображения ---
            if local_image_path and os.path.exists(local_image_path):
                # Отправляем через send_photo с локальным файлом
                caption = content_text
                if len(caption) > 1024:
                    # Обрезаем description, сохраняя title, остальные элементы
                    max_desc_length = 1024 - len(f"🔥 <b>{title_to_send}</b>\nFROM: {news_item.get('source', 'Unknown Source')}\nCATEGORY: {category}{lang_note}\n⚡ <a href='{news_item.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>")
                    if max_desc_length > 0:
                        truncated_desc = description_to_send[:max_desc_length-3] + "..."
                        caption = f"🔥 <b>{title_to_send}</b>\n{truncated_desc}\nFROM: {news_item.get('source', 'Unknown Source')}\nCATEGORY: {category}{lang_note}\n⚡ <a href='{news_item.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
                    else:
                        # Если даже без description не влезает, обрезаем минимально
                        caption = caption[:1021] + "..."
                await bot.send_photo(
                    chat_id=user_id,
                    photo=local_image_path,
                    caption=caption,
                    parse_mode="HTML",
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30
                )
            else:
                # Отправляем обычное сообщение
                await bot.send_message(
                    chat_id=user_id,
                    text=content_text,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30
                )
            print(f"[LOG] Персональная новость отправлена пользователю {user_id}.")
            await asyncio.sleep(0.1) # Небольшая задержка между отправками
        except KeyError as e:
            print(f"[ERROR] Отсутствует ключ в данных пользователя {user.get('id', 'Unknown ID')}: {e}")
        except Exception as e: # Более общий перехватчик
            print(f"[ERROR] Ошибка отправки персональной новости пользователю {user.get('id', 'Unknown ID')}: {e}")

@retry(stop=stop_after_attempt(5), 
       wait=wait_exponential(multiplier=1, min=2, max=30))
async def post_to_channel(bot, news_item: dict, translations_dict: dict):
    """
    Публикует новость в Telegram-каналы, используя готовые переводы.
    :param bot: Экземпляр бота python-telegram-bot.
    :param news_item: Словарь с оригинальными данными новости 
                      (title, description, lang, category, source, link, id).
    :param translations_dict: Словарь переводов, полученный из prepare_translations.
    """
    original_title = news_item['title']
    news_id = news_item.get('id')  # Получаем ID новости
    print(f"[DEBUG] post_to_channel news_item = {news_item}")
    print(f"[LOG] Публикация новости в каналы: {original_title[:50]}...")

    for target_lang, channel_id in CHANNEL_IDS.items():
        try:
            await asyncio.sleep(0.5) # По-прежнему нужно для соблюдения лимитов Telegram
            # Получаем готовые переводы
            translation_data = translations_dict.get(target_lang, {})
            if not translation_data:
                 print(f"[WARN] Нет данных перевода для языка {target_lang}. Пропущено.")
                 continue
            title = translation_data.get('title', original_title)
            description = translation_data.get('description', news_item.get('description', ''))
            translated_category = translation_data.get('category', news_item.get('category', ''))
            # --- Логика формирования сообщения ---
            original_lang = news_item['lang']
            needs_translation_note = original_lang != target_lang
            lang_note = ""
            if needs_translation_note:
                lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS.get(target_lang, 'Translated from')} {original_lang.upper()}"
            # --- Формирование хэштегов ---
            hashtags = f"\n#{translated_category} #{news_item.get('source', 'UnknownSource')}"
            has_description = bool(description and description.strip())
            # --- Формирование базового контента ---
            content_text = f"<b>{title}</b>"
            if has_description:
                content_text += f"\n\n{description}"
            content_text += f"{lang_note}\n{hashtags}"
            # --- Отправка в зависимости от наличия изображения ---
            image_filename = news_item.get('image_filename')

            print(f"[DEBUG] post_to_channel Путь к изображению: {image_filename}")

            if image_filename and os.path.exists(os.path.join(IMAGES_ROOT_DIR, image_filename)):
                absolute_image_path = os.path.join(IMAGES_ROOT_DIR, image_filename)
                print(f"[DEBUG] post_to_channel Отправляем абсолютный путь к изображению: {absolute_image_path}")
                # Отправляем через send_photo с локальным файлом
                caption = content_text
                if len(caption) > 1024:
                    # Обрезаем description, сохраняя title, lang_note и hashtags
                    max_desc_length = 1024 - len(f"<b>{title}</b>\n{lang_note}\n{hashtags}")
                    if max_desc_length > 0:
                        truncated_desc = description[:max_desc_length-3] + "..."
                        caption = f"<b>{title}</b>\n{truncated_desc}\n{lang_note}\n{hashtags}"
                    else:
                        # Если даже без description не влезает, обрезаем минимально
                        caption = caption[:1021] + "..."
                await bot.send_photo(
                    chat_id=channel_id,
                    photo=absolute_image_path,
                    caption=caption,
                    parse_mode='HTML',
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30
                )
            else:
                # Отправляем обычное сообщение
                await bot.send_message(
                    chat_id=channel_id,
                    text=content_text,
                    parse_mode='HTML',
                    disable_web_page_preview=True,
                    read_timeout=30,
                    write_timeout=30,
                    connect_timeout=30
                )
            print(f"[LOG] Опубликовано в {channel_id}: {title[:50]}...")
        except TelegramError as e:
            print(f"[ERROR] Ошибка отправки в {channel_id}: {e}")
        except KeyError as e:
            print(f"[ERROR] Отсутствует ключ в данных для {target_lang}: {e}")
        except Exception as e:
            print(f"[ERROR] Неожиданная ошибка для {target_lang}: {e}")
async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Получено сообщение: {update.message.text} от {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = await get_current_user_language(user_id)
    await update.message.reply_text(get_message("bot_active", lang), reply_markup=get_main_menu_keyboard(lang))
    USER_CURRENT_MENUS[user_id] = "main"
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, NetworkError):
        print("[ERROR] Network error detected. Retrying...")
    elif isinstance(context.error, BadRequest):
        if "Query is too old" in str(context.error):
            print("[ERROR] Ignoring outdated callback query")
            return
    else:
        print(f"[ERROR] Другая ошибка: {context.error}")

async def process_news_item(context, rss_manager, news):
    """
    Основная функция обработки новости
    """
    news_id = news.get('id')
    news_link = news.get('link')
    rss_feed_id = news.get('rss_feed_id')
    image_url = news.get('image_url')
    image_filename = None
    local_image_path = None
    
    print(f"[DEBUG] process_news_item: Начало обработки новости {news_id}")

    # 1. Готовим переводы
    print(f"[DEBUG] process_news_item: Перед вызовом prepare_translations для {news_id}")
    try:
        translations = await prepare_translations(
            title=news['title'],
            description=news['description'],
            category=news['category'],
            original_lang=news['lang']
        )
        print(f"[DEBUG] process_news_item: После вызова prepare_translations для {news_id}")
    except Exception as e:
        print(f"[ERROR] process_news_item: Ошибка в prepare_translations для {news_id}: {e}")
        import traceback
        traceback.print_exc()
        # Можно решить, продолжать ли обработку без переводов или прервать
        # Пока продолжаем с пустым словарем переводов
        translations = {}

    # 2. Обработка изображений
    print(f"[DEBUG] process_news_item: Перед обработкой изображений для {news_id}")
    if news_link and news_id:
        try:
            if image_url:
                print(f"[DEBUG] process_news_item: Загрузка изображения по URL для {news_id}")
                local_image_path = await download_and_save_image(image_url, news_id)
            else:
                print(f"[DEBUG] process_news_item: Извлечение URL изображения из превью для {news_id}")
                image_url = await extract_image_from_preview(news_link)
                if image_url:
                    print(f"[DEBUG] process_news_item: Загрузка извлеченного изображения для {news_id}")
                    local_image_path = await download_and_save_image(image_url, news_id)

            if local_image_path and os.path.exists(local_image_path):
                # Вычисляем относительный путь от IMAGES_ROOT_DIR
                if local_image_path.startswith(IMAGES_ROOT_DIR):
                    # Убираем базовую директорию и нормализуем путь
                    image_filename = local_image_path[len(IMAGES_ROOT_DIR):].lstrip('/')
                else:
                    # fallback на случай, если путь не соответствует ожидаемой структуре
                    image_filename = os.path.basename(local_image_path)
                print(f"[DEBUG] process_news_item: Изображение сохранено как {image_filename} для {news_id}")
                news['image_filename'] = image_filename

        except Exception as e:
            print(f"[ERROR] process_news_item: Ошибка при обработке изображения для {news_id}: {e}")
            import traceback
            traceback.print_exc()
    print(f"[DEBUG] process_news_item: После обработки изображений для {news_id}")

    print(f"[DEBUG] process_news_items - image_url: {image_url}, image_filename: {image_filename}, local_image_path = {local_image_path}")

    # 3. Сохраняем в БД
    print(f"[DEBUG] process_news_item: Перед вызовом mark_as_published для {news_id}")
    try:
        success_db = await rss_manager.mark_as_published(
            title=news['title'],
            content=news['description'],
            url=news['link'],
            original_language=news['lang'],
            translations_dict=translations,
            category_name=news['category'],
            image_filename=image_filename,
            rss_feed_id=rss_feed_id
        )
        print(f"[DEBUG] process_news_item: После вызова mark_as_published для {news_id}, результат: {success_db}")
    except Exception as e:
        print(f"[ERROR] process_news_item: Ошибка в mark_as_published для {news_id}: {e}")
        import traceback
        traceback.print_exc()
        success_db = False

    # 4. Отправляем в Telegram
    if success_db:
        print("[MAIN] Данные новости успешно обработаны и сохранены в БД.")
        # Создаем обёртки, которые используют семафор
        async def limited_post_to_channel():
            try:
                async with SEND_SEMAPHORE:
                    await post_to_channel(context.bot, news, translations)
            except Exception as e:
                print(f"[ERROR] process_news_item: Ошибка в limited_post_to_channel для {news_id}: {e}")
                import traceback
                traceback.print_exc()

        async def limited_send_personal_news():
            try:
                async with SEND_SEMAPHORE:
                    await send_personal_news(context.bot, news, translations)
            except Exception as e:
                print(f"[ERROR] process_news_item: Ошибка в limited_send_personal_news для {news_id}: {e}")
                import traceback
                traceback.print_exc()

        if news['category'] in CHANNEL_CATEGORIES:
            print(f"[LOG] Новость категории '{news['category']}' подходит для общего канала. Планируем публикацию.")
            asyncio.create_task(limited_post_to_channel())
        else:
            print(f"[LOG] Новость категории '{news['category']}' НЕ подходит для общего канала. Публикация в канал пропущена.")
        # Персональные новости отправляются всегда
        asyncio.create_task(limited_send_personal_news())
    else:
        print("[MAIN] Ошибка обработки и сохранения данных в БД. Публикация в Telegram пропущена.")
    
    print(f"[DEBUG] process_news_item: Завершение обработки новости {news_id}")
    return success_db

async def monitor_news_task(context: ContextTypes.DEFAULT_TYPE):
    """Асинхнхронная задача мониторинга новостей"""
    print("[LOG] Запуск задачи мониторинга новостей")
    try:
        news_list = await asyncio.wait_for(rss_manager.fetch_news(), timeout=120)
        print(f"[LOG] Получено {len(news_list)} новостей")
        # Обрабатываем новости пакетами
        batch_size = 5 # Размер пакета
        delay_between_batches = 10 # Задержка между пакетами в секундах
        for i in range(0, len(news_list[:20]), batch_size):
            batch = news_list[i:i + batch_size]
            print(f"[LOG] Обработка пакета новостей {i//batch_size + 1} (размер: {len(batch)})")
            batch_tasks = []
            for news in batch:
                # Создаем задачи для обработки новостей в пакете
                task = asyncio.create_task(process_news_item(context, rss_manager, news))
                batch_tasks.append(task)
            # Ждем завершения всех задач в пакете
            await asyncio.gather(*batch_tasks, return_exceptions=True)
            # Пауза между пакетами
            if i + batch_size < len(news_list[:20]): # Не делать паузу после последнего пакета
                 print(f"[LOG] Пауза {delay_between_batches} секунд перед следующим пакетом...")
                 await asyncio.sleep(delay_between_batches)
    except asyncio.TimeoutError:
        print("[ERROR] Таймаут получения новостей")
    except Exception as e:
        print(f"[ERROR] Ошибка в задаче мониторинга: {e}")

# --- Функции для пакетной обработки ---
async def schedule_batch_processor(application: Application) -> None:
    """Функция для планирования регулярной пакетной обработки"""
    global rss_manager

    if rss_manager and hasattr(rss_manager, 'dublicate_detector'):
        job_queue = application.job_queue
        if job_queue:
            # Планируем задачу на выполнение каждые 30 минут (1800 секунд)
            # first=60 означает, что первая задача запустится через 1 минуту после старта
            job_queue.run_repeating(
                batch_processor_job, # <-- Новая функция-обработчик задачи
                interval=1800, # 30 минут в секундах
                first=60, # Первая задача через 1 минуту
                job_kwargs={'misfire_grace_time': 600} # 10 минут на выполнение с опозданием
            )
            print("[LOG] Зарегистрирована задача регулярной пакетной обработки (каждые 30 минут)")
        else:
            print("[WARN] JobQueue не доступна для планирования пакетной обработки")
    else:
        print("[WARN] RSSManager или DuplicateDetector не инициализированы для планирования пакетной обработки")

async def batch_processor_job(context: ContextTypes.DEFAULT_TYPE) -> None:
    """Функция, выполняемая по расписанию для пакетной обработки"""
    global rss_manager

    try:
        print("[BATCH] Запуск регулярной пакетной обработки новостей без эмбеддингов...")
        success, errors = await rss_manager.dublicate_detector.process_missing_embeddings_batch(
            batch_size=50,
            delay_between_items=0.1
        )
        print(f"[BATCH] Регулярная пакетная обработка завершена. Успешно: {success}, Ошибок: {errors}")
    except Exception as e:
        print(f"[ERROR] Ошибка в регулярной пакетной обработке: {e}")
        # Можно добавить логирование traceback для отладки
        import traceback
        traceback.print_exc()

async def stop_batch_processor():
    """Останавливает фоновую задачу пакетной обработки."""
    global batch_processor_task
    if batch_processor_task and not batch_processor_task.done():
        print("[LOG] [BATCH_EMBEDDING] Отмена фоновой задачи пакетной обработки...")
        batch_processor_task.cancel()
        try:
            # Дожидаемся завершения задачи (даже если она отменена)
            await batch_processor_task
        except asyncio.CancelledError:
            print("[LOG] [BATCH_EMBEDDING] Фоновая задача пакетной обработки отменена.")
        except Exception as e:
            print(f"[ERROR] [BATCH_EMBEDDING] Ошибка при остановке задачи: {e}")

async def post_stop(application: Application) -> None:
    """Функция, вызываемая при остановке приложения для корректного закрытия ресурсов"""
    global rss_manager, user_manager, duplicate_detector
    
    print("[LOG] Остановка приложения и закрытие пулов подключений...")
    
    # Закрываем пул RSSManager
    if rss_manager and hasattr(rss_manager, 'pool') and rss_manager.pool:
        try:
            rss_manager.pool.close()
            await rss_manager.pool.wait_closed()
            print("[LOG] Пул RSSManager закрыт")
        except Exception as e:
            print(f"[ERROR] Ошибка при закрытии пула RSSManager: {e}")
    
    # Закрываем пул UserManager
    if user_manager and hasattr(user_manager, 'pool') and user_manager.pool:
        try:
            user_manager.pool.close()
            await user_manager.pool.wait_closed()
            print("[LOG] Пул UserManager закрыт")
        except Exception as e:
            print(f"[ERROR] Ошибка при закрытии пула UserManager: {e}")

    # Закрываем пул FireFeedDuplicateDetector (классовый пул)
    try:
        await FireFeedDuplicateDetector.close_pool()
        print("[LOG] Пул FireFeedDuplicateDetector закрыт")
    except Exception as e:
        print(f"[ERROR] Ошибка при закрытии пула FireFeedDuplicateDetector: {e}")
    
    print("[LOG] Все пулы подключений закрыты")

def main():
    global rss_manager, user_manager, duplicate_detector
    
    print("[LOG] Запуск бота")
    
    # Создаем один экземпляр детектора для всего приложения
    duplicate_detector = FireFeedDuplicateDetector()
    
    # Передаем его в RSSManager
    rss_manager = RSSManager(duplicate_detector=duplicate_detector)
    user_manager = UserManager()

    # --- Создаем Application с post_stop ---
    application = Application.builder().token(BOT_TOKEN).post_stop(post_stop).build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu_selection))
    application.add_handler(MessageHandler(filters.ALL, debug))
    application.add_error_handler(error_handler)
    
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_repeating(
            monitor_news_task,
            interval=600,
            first=1,
            job_kwargs={'misfire_grace_time': 600}
        )
        print("[LOG] Зарегистрирована задача мониторинга новостей")

    # --- Добавляем запуск пакетной обработки эмбеддингов ---
    application.post_init = schedule_batch_processor

    def signal_handler(sig, frame):
        print("[LOG] Получен сигнал завершения...")
        # Планируем остановку приложения корректно
        try:
            loop = asyncio.get_running_loop()
            loop.call_soon(asyncio.create_task, application.stop())
        except RuntimeError:
            # Если loop не запущен, останавливаем его напрямую
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.call_soon(asyncio.create_task, application.stop())
            else:
                loop.run_until_complete(application.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("[LOG] Бот запущен в режиме Webhook")
    
    try:
        application.run_webhook(**WEBHOOK_CONFIG)
    except (KeyboardInterrupt, SystemExit):
        print("[LOG] Прервано пользователем или системой...")
    except Exception as e:
        print(f"[ERROR] Ошибка: {e}")
        raise

if __name__ == "__main__":
    main()