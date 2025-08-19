import os
import asyncio
import re
import html
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import (
    NetworkError,
    BadRequest,
    TelegramError
)
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from config import BOT_TOKEN, CHANNEL_ID, FIRE_EMOJI, CATEGORIES
from parser import fetch_news
from database import init_db, is_news_new, mark_as_published, get_user_settings, save_user_settings, get_subscribers_for_category, get_user_preferences, get_user_language, set_user_language
from translator import translate_text
from functools import lru_cache
from tenacity import retry, stop_after_attempt, wait_exponential

LANG_NAMES = {
    "en": "English 🇬🇧",
    "ru": "Русский 🇷🇺",
    "de": "Deutsch 🇩🇪",
    "fr": "Français 🇫🇷"
}

TRANSLATED_FROM_LABELS = {
    "en": "Translated from",
    "ru": "Переведено с",
    "de": "Übersetzt aus",
    "fr": "Traduit de"
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

@lru_cache(maxsize=1000)
def cached_translate(text, target_lang, source_lang='auto'):
    return translate_text(text, target_lang, source_lang)

# Добавляем обработчики команд
def setup_handlers(application):
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CallbackQueryHandler(button_handler))

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    welcome_text = (
        f"👋 Привет, {user.first_name}!\n"
        "Я бот FireFeed - твой персональный агрегатор новостей.\n\n"
        "⚙️ Настрой подписки: /settings\n"
        "ℹ️ Помощь: /help"
    )
    await update.message.reply_text(welcome_text)

async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.effective_user.id
        
        # Используем новую функцию для получения всех настроек
        settings = get_user_settings(user_id)
        
        # Сохраняем состояние в временном хранилище
        USER_STATES[user_id] = {
            "current_subs": settings["subscriptions"].copy(),
            "language": settings["language"],
            "message_id": None
        }
        
        # Создаем сообщение с меню настроек
        message = await update.message.reply_text("⚙️ Загружаю настройки...")
        USER_STATES[user_id]["message_id"] = message.message_id
        
        # Отображаем меню
        await show_settings_menu(update, context, user_id)
        
    except Exception as e:
        print(f"❌ Ошибка команды /settings: {e}")
        await update.message.reply_text("⚠️ Не удалось открыть настройки. Попробуйте позже.")

async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Показывает меню настроек с текущим состоянием"""
    try:

        state = USER_STATES.get(user_id)
        if not state:
            return
            
        current_subs = state["current_subs"]
        current_lang = state["language"]
        
        # Создаем клавиатуру настроек
        keyboard = []
        for category in CATEGORIES.keys():
            is_selected = category in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category.capitalize()}"
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category}")])
        
        # Кнопка смены языка
        keyboard.append([InlineKeyboardButton(
            f"🌐 Язык: {LANG_NAMES.get(current_lang, 'en')}", 
            callback_data="change_lang"
        )])
        
        keyboard.append([InlineKeyboardButton("💾 Сохранить", callback_data="save_settings")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Редактируем существующее сообщение или отправляем новое
        if state.get("message_id"):
            await context.bot.edit_message_text(
                chat_id=user_id,
                message_id=state["message_id"],
                text=f"⚙️ {SELECT_CATEGORIES_LABELS[state["language"]]}:",
                reply_markup=reply_markup
            )
        else:
            message = await context.bot.send_message(
                chat_id=user_id,
                text="⚙️ Выберите интересующие вас категории:",
                reply_markup=reply_markup
            )
            USER_STATES[user_id]["message_id"] = message.message_id
            
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            print(f"❌ Ошибка отображения меню: {e}")
    except Exception as e:
        print(f"❌ Неизвестная ошибка в show_settings_menu: {e}")

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        # Попытка подтвердить обработку callback
        await query.answer(text="Loading...")
    except BadRequest as e:
        if "Query is too old" in str(e):
            print(f"Ignoring outdated callback: {e}")
            return  # Прекращаем обработку
        else:
            raise  # Пробрасываем другие ошибки

    await query.answer()
    user_id = query.from_user.id
    
    try:
        if user_id not in USER_STATES:
            USER_STATES[user_id] = {
                "current_subs": get_user_preferences(user_id) or [],
                "language": get_user_language(user_id)  # Добавляем язык в состояние
            }
            
        state = USER_STATES[user_id]
        
        # Обработка переключения категорий
        if query.data.startswith("toggle_"):
            category = query.data.split("_")[1]
            current_subs = state['current_subs'];
            
            # Переключаем состояние категории
            if category in current_subs:
                current_subs.remove(category)
            else:
                current_subs.append(category)
            
            # Обновляем состояние
            state["current_subs"] = current_subs
            
            # Обновляем меню
            await show_settings_menu(update, context, user_id)
    
        # Обработка сохранения настроек
        elif query.data == "save_settings":
            # Сохраняем ВСЕ настройки: подписки и язык
            save_user_settings(
                user_id,
                state["current_subs"],
                state["language"]  # Используем язык из состояния
            )
            
            # Удаляем временное состояние
            if user_id in USER_STATES:
                del USER_STATES[user_id]
            
            await query.edit_message_text("✅ Настройки сохранены!")
        
        # Обработка выбора языка
        elif query.data.startswith("lang_"):
            lang = query.data.split("_")[1]
            state["language"] = lang  # Обновляем язык в состоянии
            await show_settings_menu(update, context, user_id)
        
        # Обработка смены языка
        elif query.data == "change_lang":
            keyboard = [
                [InlineKeyboardButton("🇬🇧 English", callback_data="lang_en")],
                [InlineKeyboardButton("🇷🇺 Русский", callback_data="lang_ru")],
                [InlineKeyboardButton("🇩🇪 Deutsch", callback_data="lang_de")],
                [InlineKeyboardButton("🇫🇷 Français", callback_data="lang_fr")],
                [InlineKeyboardButton("← Назад", callback_data="back_to_settings")]
            ]
            await query.edit_message_text(
                text="🌐 Выберите язык:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            
        # Обработка возврата в настройки
        elif query.data == "back_to_settings":
            await show_settings_menu(update, context, user_id)
            
    except Exception as e:
        print(f"❌ Ошибка обработки кнопки: {e}")
        await query.edit_message_text("⚠️ Произошла ошибка. Попробуйте позже.")

async def update_settings_menu(query, current_subs):
    """Обновляет меню настроек с текущими параметрами"""
    try:
        user_id = query.from_user.id
        current_lang = get_user_language(user_id)
        
        # Создаем клавиатуру настроек
        keyboard = []
        for category in CATEGORIES.keys():
            is_selected = category in current_subs
            text = f"{'✅ ' if is_selected else '🔲 '}{category.capitalize()}"
            keyboard.append([InlineKeyboardButton(text, callback_data=f"toggle_{category}")])
        
        # Кнопка смены языка
        keyboard.append([InlineKeyboardButton(
            f"🌐 Язык: {LANG_NAMES.get(current_lang, 'en')}", 
            callback_data="change_lang"
        )])
        
        keyboard.append([InlineKeyboardButton("💾 Сохранить", callback_data="save_settings")])
        
        await query.edit_message_text(
            text="⚙️ Выберите интересующие вас категории:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            print(f"❌ Ошибка обновления меню: {e}")
    except Exception as e:
        print(f"❌ Неизвестная ошибка в update_settings_menu: {e}")

def clean_html(raw_html):
    """Удаляет все HTML-теги и преобразует HTML-сущности"""
    if not raw_html:
        return ""
    
    # Удаляем все теги
    clean_text = re.sub(r'<[^>]+>', '', raw_html)
    
    # Заменяем HTML-сущности (например, &amp; → &)
    clean_text = html.unescape(clean_text)
    
    # Удаляем лишние пробелы
    return re.sub(r'\s+', ' ', clean_text).strip()

async def monitor_news_task(context: ContextTypes.DEFAULT_TYPE):
    """Периодическая задача для мониторинга новостей"""
    try:
        news_list = await fetch_news()
        new_news = [news for news in news_list if is_news_new(news['id'])]
        
        # Отправляем всё без задержек, если новостей <= 3
        if len(new_news) <= 3:
            for news in new_news:
                asyncio.create_task(post_to_channel(context.bot, news))
                asyncio.create_task(send_personal_news(context.bot, news))
        else:
            # Для большого количества - отправляем пачкой без задержек
            # ИЛИ увеличиваем интервал между постами
            for news in new_news:
                asyncio.create_task(post_to_channel(context.bot, news))
                asyncio.create_task(send_personal_news(context.bot, news))
                await asyncio.sleep(5)
    except Exception as e:
        print(f"⚠️ Ошибка мониторинга: {e}")

@retry(stop=stop_after_attempt(5), 
       wait=wait_exponential(multiplier=1, min=2, max=30))
async def send_personal_news(bot, news_item):
    # Получаем всех подписчиков для этой категории новостей
    subscribers = get_subscribers_for_category(news_item['category'])
    
    for user in subscribers:
        try:
            # Очищаем HTML
            clean_title = clean_html(news_item['title'])
            clean_description = clean_html(news_item['description'])

            # Переводим если нужно
            if user['language_code'] != news_item['lang']:
                title = translate_text(clean_title, user['language_code'])
                description = translate_text(clean_description, user['language_code'])
                lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS[user['language_code']]} {news_item['lang'].upper()}"
            else:
                title = news_item['title']
                description = news_item['description']
                lang_note = ""
            
            # Форматируем сообщение
            message = (
                f"🔥 <b>{title}</b>\n\n"
                f"{description}\n\n"
                f"FROM: {news_item['source']}\n"
                f"CATEGORY: {news_item['category']}{lang_note}\n\n"
                f"⚡ <a href='{news_item['link']}'>{READ_MORE_LABELS[user['language_code']]}</a>"
            )

            # Отправляем персональное сообщение
            await bot.send_message(
                chat_id=user['id'],
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True
            )
            
            # Небольшая задержка чтобы не превысить лимиты Telegram
            await asyncio.sleep(0.1)
            
        except Exception as e:
            print(f"Error sending news to user {user['id']}: {e}")

@retry(stop=stop_after_attempt(5), 
       wait=wait_exponential(multiplier=1, min=2, max=30))
async def post_to_channel(bot, news_item):
    try:
        DEFAULT_CHANNEL_LANGUAGE = 'ru'

        # Переводим если нужно
        if news_item['lang'] != '':
            title = translate_text(clean_title, DEFAULT_CHANNEL_LANGUAGE)
            description = translate_text(clean_description, DEFAULT_CHANNEL_LANGUAGE)
            lang_note = f"\n\n🌐 {TRANSLATED_FROM_LABELS[DEFAULT_CHANNEL_LANGUAGE]} {news_item['lang'].upper()}"
        else:
            title = news_item['title']
            description = news_item['description']
            lang_note = ""

        hashtags = f"\n#firefeed_{news_item['category']} #firefeed_{news_item['source']}"

        title = clean_title
        description = clean_description
        
        # Проверяем наличие и содержание description у новости
        has_description = description and description.strip()

        message = f"<b>{title}</b>"

        if has_description:
            message += f"\n\n{description}"

        # Всегда добавляем хештеги, но с разным отступом в зависимости от наличия описания
        message += f"\n\n{hashtags}" if has_description else f"\n{hashtags}"
        
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=True
        )
        mark_as_published(news_item['id'])
        print(f"✅ [{news_item['lang']}/{news_item['category']}] Published: {title[:50]}...")
    except TelegramError as e:
        print(f"❌ Ошибка отправки: {e}")

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"⚡ Получено сообщение: {update.message.text}")
    await update.message.reply_text("Бот активен!")

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Используем импортированные классы ошибок
    if isinstance(context.error, NetworkError):
        print("Network error detected. Retrying...")
        # Логика обработки сетевой ошибки

    # Добавьте обработку для BadRequest (особенно для устаревших запросов)
    elif isinstance(context.error, BadRequest):
        if "Query is too old" in str(context.error):
            print("Ignoring outdated callback query")
            return  # Проигнорировать ошибку
    else:
        print(f"Другая ошибка: {context.error}")

def main():
    """Точка входа с использованием Webhooks"""
    application = Application.builder().token(BOT_TOKEN).build()
    init_db()
    
    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.ALL, debug))
    application.add_error_handler(error_handler)
    
    # Регистрируем периодическую задачу
    job_queue = application.job_queue
    job_queue.run_repeating(
        callback=monitor_news_task, 
        interval=60,
        first=1
    )

    print("🟢 Бот запущен в режиме Webhook")
    
    # Запускаем webhook
    application.run_webhook(
        listen='127.0.0.1',
        port=5000,
        url_path='webhook',
        webhook_url='https://firefeed.net/webhook'
    )

if __name__ == "__main__":
    main()