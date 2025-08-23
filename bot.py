import os
import signal
import sys
import asyncio
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.error import NetworkError, BadRequest, TelegramError
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from config import WEBHOOK_CONFIG, BOT_TOKEN, FIRE_EMOJI, CHANNEL_IDS
from user_manager import UserManager
from translator import translate_text, prepare_translations
from functools import lru_cache
from tenacity import retry, stop_after_attempt, wait_exponential
from rss_manager import RSSManager
from firefeed_utils import clean_html

LANG_NAMES = {
    "en": "English 🇬🇧",
    "ru": "Русский 🇷🇺",
    "de": "Deutsch 🇩🇪",
    "fr": "Français 🇫🇷"
}

# Тексты сообщений на разных языках
MESSAGES = {
    "welcome": {
        "en": "👋 Hello, {user_name}!\nI am FireFeed - your personal news aggregator.\n\nUse the menu below to navigate:",
        "ru": "👋 Привет, {user_name}!\nЯ бот FireFeed - твой персональный агрегатор новостей.\n\nИспользуй меню ниже для навигации:",
        "de": "👋 Hallo, {user_name}!\nIch bin FireFeed - dein persönlicher News-Aggregator.\n\nVerwende das Menü unten zur Navigation:",
        "fr": "👋 Bonjour, {user_name} !\nJe suis FireFeed - votre agrégateur de nouvelles personnel.\n\nUtilisez le menu ci-dessous pour naviguer :"
    },
    "menu_settings": {
        "en": "⚙️ Settings",
        "ru": "⚙️ Настройки",
        "de": "⚙️ Einstellungen",
        "fr": "⚙️ Paramètres"
    },
    "menu_help": {
        "en": "ℹ️ Help",
        "ru": "ℹ️ Помощь",
        "de": "ℹ️ Hilfe",
        "fr": "ℹ️ Aide"
    },
    "menu_status": {
        "en": "📊 Status",
        "ru": "📊 Статус",
        "de": "📊 Status",
        "fr": "📊 Statut"
    },
    "menu_language": {
        "en": "🌐 Language",
        "ru": "🌐 Язык",
        "de": "🌐 Sprache",
        "fr": "🌐 Langue"
    },
    "menu_placeholder": {
        "en": "Choose an action...",
        "ru": "Выберите действие...",
        "de": "Wählen Sie eine Aktion...",
        "fr": "Choisissez une action..."
    },
    "settings_loading": {
        "en": "⚙️ Loading settings...",
        "ru": "⚙️ Загружаю настройки...",
        "de": "⚙️ Lade Einstellungen...",
        "fr": "⚙️ Chargement des paramètres..."
    },
    "settings_error": {
        "en": "⚠️ Failed to open settings. Please try again later.",
        "ru": "⚠️ Не удалось открыть настройки. Попробуйте позже.",
        "de": "⚠️ Einstellungen konnten nicht geöffnet werden. Bitte versuchen Sie es später erneut.",
        "fr": "⚠️ Impossible d'ouvrir les paramètres. Veuillez réessayer plus tard."
    },
    "settings_saved": {
        "en": "✅ Settings saved!",
        "ru": "✅ Настройки сохранены!",
        "de": "✅ Einstellungen gespeichert!",
        "fr": "✅ Paramètres enregistrés !"
    },
    "save_button": {
        "en": "💾 Save",
        "ru": "💾 Сохранить",
        "de": "💾 Speichern",
        "fr": "💾 Enregistrer"
    },
    "settings_title": {
        "en": "⚙️ Choose the categories you are interested in:",
        "ru": "⚙️ Выберите интересующие вас категории:",
        "de": "⚙️ Wählen Sie die Kategorien aus, die Sie interessieren:",
        "fr": "⚙️ Choisissez les catégories qui vous intéressent :"
    },
    "language_select": {
        "en": "🌐 Choose interface language:",
        "ru": "🌐 Выберите язык интерфейса:",
        "de": "🌐 Wählen Sie die Interface-Sprache:",
        "fr": "🌐 Choisissez la langue de l'interface :"
    },
    "language_changed": {
        "en": "✅ Language changed to {language}",
        "ru": "✅ Язык изменен на {language}",
        "de": "✅ Sprache wurde auf {language} geändert",
        "fr": "✅ Langue changée en {language}"
    },
    "help_text": {
        "en": "🤖 <b>FireFeed Bot Help</b>\n\nI will help you get news according to your subscriptions.\n\nMain commands:\n⚙️ Settings - configure subscriptions\nℹ️ Help - show this help\n📊 Status - information about your subscriptions\n🌐 Language - change interface language\n\nAfter setting up subscriptions, you will receive news of selected categories.",
        "ru": "🤖 <b>Справка по боту FireFeed</b>\n\nЯ помогу вам получать новости по вашим подпискам.\n\nОсновные команды:\n⚙️ Настройки - настройка подписок\nℹ️ Помощь - показать эту справку\n📊 Статус - информация о ваших подписках\n🌐 Язык - изменить язык интерфейса\n\nПосле настройки подписок вы будете получать новости выбранных категорий.",
        "de": "🤖 <b>FireFeed Bot Hilfe</b>\n\nIch werde Ihnen helfen, Nachrichten gemäß Ihren Abonnements zu erhalten.\n\nHauptbefehle:\n⚙️ Einstellungen - Abonnements konfigurieren\nℹ️ Hilfe - diese Hilfe anzeigen\n📊 Status - Informationen zu Ihren Abonnements\n🌐 Sprache - Interface-Sprache ändern\n\nNach dem Einrichten von Abonnements erhalten Sie Nachrichten ausgewählter Kategorien.",
        "fr": "🤖 <b>Aide du bot FireFeed</b>\n\nJe vous aiderai à recevoir des nouvelles selon vos abonnements.\n\nCommandes principales :\n⚙️ Paramètres - configurer les abonnements\nℹ️ Aide - afficher cette aide\n📊 Statut - informations sur vos abonnements\n🌐 Langue - changer la langue de l'interface\n\nAprès avoir configuré les abonnements, vous recevrez des nouvelles des catégories sélectionnées."
    },
    "status_text": {
        "en": "📊 <b>Your current settings:</b>\n\n🌐 Language: {language}\n📋 Categories: {categories}",
        "ru": "📊 <b>Ваши текущие настройки:</b>\n\n🌐 Язык: {language}\n📋 Категории: {categories}",
        "de": "📊 <b>Ihre aktuellen Einstellungen:</b>\n\n🌐 Sprache: {language}\n📋 Kategorien: {categories}",
        "fr": "📊 <b>Vos paramètres actuels :</b>\n\n🌐 Langue: {language}\n📋 Catégories: {categories}"
    },
    "no_subscriptions": {
        "en": "No subscriptions",
        "ru": "Нет подписок",
        "de": "Keine Abonnements",
        "fr": "Aucun abonnement"
    },
    "bot_active": {
        "en": "Bot is active!",
        "ru": "Бот активен!",
        "de": "Bot ist aktiv!",
        "fr": "Le bot est actif !"
    },
    "button_error": {
        "en": "⚠️ An error occurred. Please try again later.",
        "ru": "⚠️ Произошла ошибка. Попробуйте позже.",
        "de": "⚠️ Ein Fehler ist aufgetreten. Bitte versuchen Sie es später erneut.",
        "fr": "⚠️ Une erreur s'est produite. Veuillez réessayer plus tard."
    }
}

TRANSLATED_FROM_LABELS = {
    "en": "[AI] Translated from",
    "ru": "[AI] Переведено с",
    "de": "[AI] Übersetzt aus",
    "fr": "[AI] Traduit de"
}

READ_MORE_LABELS = {
    "en": "Read more",
    "ru": "Подробнее",
    "de": "Mehr lesen",
    "fr": "En savoir plus"
}

SELECT_CATEGORIES_LABELS = {
    "en": "Choose the categories you are interested in",
    "ru": "Выберите категории, которые вам интересны",
    "de": "Wählen Sie die Kategorien aus, die Sie interessieren",
    "fr": "Choisissez les catégories qui vous intéressent"
}

USER_STATES = {}
USER_CURRENT_MENUS = {}
# Храним язык пользователя в памяти для быстрого доступа
USER_LANGUAGES = {}

# Функция для получения сообщения на нужном языке
def get_message(key, lang="en", **kwargs):
    """Возвращает локализованное сообщение"""
    if lang not in MESSAGES.get(key, {}):
        lang = "en"
    
    message = MESSAGES.get(key, {}).get(lang, "")
    
    if kwargs:
        message = message.format(**kwargs)
    
    return message

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
def set_current_user_language(user_id, lang):
    user_manager = UserManager()
    """Устанавливает язык пользователя в БД и в памяти"""
    print(f"[LOG] Установка языка пользователя {user_id} на {lang}")
    try:
        # Сохраняем в БД
        user_manager.set_user_language(user_id, lang)
        print(f"[LOG] Язык {lang} сохранен в БД для пользователя {user_id}")
        # Сохраняем в памяти
        USER_LANGUAGES[user_id] = lang
        print(f"[LOG] Язык {lang} сохранен в памяти для пользователя {user_id}")
    except Exception as e:
        print(f"[ERROR] Ошибка установки языка для {user_id}: {e}")

# Улучшенная функция получения языка пользователя
def get_current_user_language(user_id):
    user_manager = UserManager()
    """Получает актуальный язык пользователя из памяти или БД"""
    # Сначала проверяем в памяти
    if user_id in USER_LANGUAGES:
        lang = USER_LANGUAGES[user_id]
        print(f"[LOG] Получен язык пользователя {user_id} из памяти: {lang}")
        return lang
    
    # Если нет в памяти, получаем из БД
    try:
        lang = user_manager.get_user_language(user_id)
        print(f"[LOG] Получен язык пользователя {user_id} из БД: {lang}")
        if lang:
            # Сохраняем в памяти для быстрого доступа
            USER_LANGUAGES[user_id] = lang
        return lang or "en"
    except Exception as e:
        print(f"[ERROR] Ошибка получения языка для {user_id}: {e}")
        return "en"

@lru_cache(maxsize=1000)
def cached_translate(text, source_lang, target_lang):
    return translate_text(text, source_lang, target_lang)

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды /start от пользователя {update.effective_user.id}")
    user = update.effective_user
    user_id = user.id
    
    lang = get_current_user_language(user_id)
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
        lang = get_current_user_language(user_id)
        print(f"[LOG] Язык пользователя {user_id}: {lang}")
        
        settings = user_manager.get_user_settings(user_id)
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
        lang = get_current_user_language(update.effective_user.id)
        await update.message.reply_text(get_message("settings_error", lang))

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    print(f"[LOG] Отображение меню настроек для пользователя {user_id}")
    rss_manager = RSSManager()
    connection = rss_manager.get_db_connection()

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
        categories = rss_manager.get_categories()
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
    finally:
        rss_manager.close_connection()

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
                "language": get_current_user_language(user_id)
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
            
            user_manager.save_user_settings(
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
            welcome_text = get_message("settings_saved", current_lang) + "\n\n" + get_message("welcome", current_lang, user_name=user.first_name)
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
            set_current_user_language(user_id, lang)
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
            welcome_text = get_message("language_changed", lang, language=LANG_NAMES.get(lang, "English")) + "\n\n" + get_message("welcome", lang, user_name=user.first_name)
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
            current_lang = get_current_user_language(user_id)
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
        current_lang = get_current_user_language(user_id)
        await context.bot.send_message(
            chat_id=user_id,
            text=get_message("button_error", current_lang),
            reply_markup=get_main_menu_keyboard(current_lang)
        )
        USER_CURRENT_MENUS[user_id] = "main"

async def show_settings_menu_from_callback(query, context, user_id: int):
    print(f"[LOG] Отображение меню настроек из callback для {user_id}")
    rss_manager = RSSManager()

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
        categories = rss_manager.get_categories()
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
    finally:
        rss_manager.close_connection()

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"[LOG] Вызов команды /help от пользователя {update.effective_user.id}")
    user_id = update.effective_user.id
    # Используем улучшенную функцию получения языка
    lang = get_current_user_language(user_id)
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
    lang = get_current_user_language(user_id)
    print(f"[LOG] Актуальный язык пользователя {user_id}: {lang}")
    
    settings = user_manager.get_user_settings(user_id)
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
    lang = get_current_user_language(user_id)
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
        set_current_user_language(user_id, matched_lang)
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
    lang = get_current_user_language(user_id)
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
    print(f"[LOG] Отправка персональной новости: {original_title[:50]}...")
    
    category = news_item.get('category')
    if not category:
        print("[WARN] Категория новости не указана. Персональная рассылка пропущена.")
        return

    subscribers = user_manager.get_subscribers_for_category(category)
    print(f"[LOG] Найдено {len(subscribers)} подписчиков для категории {category}")
    
    if not subscribers:
        print(f"[LOG] Нет подписчиков для категории {category}.")
        return

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
            
            # --- Очистка HTML (если еще не очищена на этапе перевода) ---
            # Предполагается, что очистка уже была выполнена в prepare_translations.
            # Если нет, раскомментируйте строки ниже:
            # title_to_send = clean_html(title_to_send) 
            # description_to_send = clean_html(description_to_send)

            # --- Формирование примечания о переводе ---
            lang_note = ""
            original_news_lang = news_item.get('lang', '')
            if user_lang != original_news_lang:
                 lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS.get(user_lang, 'Translated from')} {original_news_lang.upper()}"

            # --- Формирование сообщения ---
            # Используем .get() с дефолтными значениями для надежности
            message = (
                f"🔥 <b>{title_to_send}</b>\n\n"
                f"{description_to_send}\n\n"
                f"FROM: {news_item.get('source', 'Unknown Source')}\n"
                f"CATEGORY: {category}{lang_note}\n\n" # Используем оригинальную категорию или category_to_send
                f"⚡ <a href='{news_item.get('link', '#')}'>{READ_MORE_LABELS.get(user_lang, 'Read more')}</a>"
            )

            # --- Отправка сообщения ---
            await bot.send_message(
                chat_id=user_id,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=False,
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
                      (title, description, lang, category, source, link).
    :param translations_dict: Словарь переводов, полученный из prepare_translations.
    """
    original_title = news_item['title']
    print(f"[LOG] Публикация новости в каналы: {original_title[:50]}...")

    for target_lang, channel_id in CHANNEL_IDS.items():
        try:
            await asyncio.sleep(0.5) # По-прежнему нужно для соблюдения лимитов Telegram

            # Получаем готовые переводы
            translation_data = translations_dict.get(target_lang, {})
            if not translation_data:
                 print(f"[WARN] Нет данных перевода для языка {target_lang}. Пропущено.")
                 continue # Или используем оригинальные данные?

            title = translation_data.get('title', original_title)
            description = translation_data.get('description', news_item.get('description', ''))
            translated_category = translation_data.get('category', news_item.get('category', ''))

            # --- Логика формирования сообщения ---
            original_lang = news_item['lang']
            needs_translation_note = original_lang != target_lang
            
            lang_note = ""
            if needs_translation_note:
                # Получаем название оригинального языка, если нужно
                # Например, TRANSLATED_FROM_LABELS.get(target_lang, "Translated from")
                lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS.get(target_lang, 'Translated from')} {original_lang.upper()}"

            # --- Формирование хэштегов ---
            # Предполагаем, что source и category доступны в news_item
            hashtags = f"\n#{translated_category} #{news_item.get('source', 'UnknownSource')}"
            
            has_description = bool(description and description.strip())
            
            # --- Сборка сообщения ---
            message = f"<b>{title}</b>"
            if has_description:
                message += f"\n\n{description}"
            message += f"{lang_note}\n{hashtags}" # Добавляем всегда, даже без описания?

            # --- Отправка ---
            await bot.send_message(
                chat_id=channel_id,
                text=message,
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
    lang = get_current_user_language(user_id)
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


async def monitor_news_task(context):
        """Асинхронная задача мониторинга новостей"""
        print("[LOG] Запуск задачи мониторинга новостей")
        rss_manager = RSSManager()

        try:
            news_list = await asyncio.wait_for(rss_manager.fetch_news(), timeout=120)
            print(f"[LOG] Получено {len(news_list)} новостей")
            
            for i, news in enumerate(news_list[:20]):
                try:
                    # 2. Готовим переводы
                    translations = await prepare_translations(
                        title=news['title'],
                        description=news['description'],
                        category=news['category'], # Предполагаем, что категория на 'en' или передаем category_lang
                        original_lang=news['lang']
                    )

                    # 3. Сохраняем в БД
                    success_db = rss_manager.mark_as_published(
                        title=news['title'],
                        content=news['description'], # Или другое поле, если есть full_text
                        url=news['link'], # или другой ключ для URL
                        original_language=news['lang'],
                        translations_dict=translations,
                        category=news['category']
                    )

                    if success_db:
                        print("[MAIN] Данные новости успешно сохранены в БД.")
                        # Публикуем в Telegram КАНАЛЫ
                        asyncio.create_task(post_to_channel(context.bot, news, translations))
                        # Отправляем персональные новости ПОЛЬЗОВАТЕЛЯМ
                        asyncio.create_task(send_personal_news(context.bot, news, translations))
                    else:
                        print("[MAIN] Ошибка сохранения данных в БД. Публикация в Telegram пропущена.")
                    
                    if i % 5 == 0:
                        await asyncio.sleep(5)
                        
                except Exception as e:
                    print(f"[ERROR] Ошибка обработки новости: {e}")
                    continue
                            
        except asyncio.TimeoutError:
            print("[ERROR] Таймаут получения новостей")
        except Exception as e:
            print(f"[ERROR] Ошибка в задаче мониторинга: {e}")


def main():
    print("[LOG] Запуск бота")
    rss_manager = RSSManager()
    application = Application.builder().token(BOT_TOKEN).build()
    
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
            callback=monitor_news_task, 
            interval=300,
            first=1,
            job_kwargs={'misfire_grace_time': 600}
        )
        print("[LOG] Зарегистрирована задача мониторинга новостей")

    def signal_handler(sig, frame):
        print("[LOG] Получен сигнал завершения, закрываем соединения...")
        rss_manager = RSSManager()
        rss_manager.close_connection()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    print("[LOG] Бот запущен в режиме Webhook")
    
    try:
        application.run_webhook(**WEBHOOK_CONFIG)
    except KeyboardInterrupt:
        print("[LOG] Прервано пользователем, закрываем соединения...")
        rss_manager = RSSManager()
        rss_manager.close_connection()
    except Exception as e:
        print(f"[ERROR] Ошибка: {e}, закрываем соединения...")
        rss_manager = RSSManager()
        rss_manager.close_connection()
        raise

if __name__ == "__main__":
    main()