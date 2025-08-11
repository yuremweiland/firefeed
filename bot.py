import asyncio
import re
import html
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import TelegramError, BadRequest
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters
from config import BOT_TOKEN, CHANNEL_ID, FIRE_EMOJI, CATEGORIES
from parser import fetch_news
from database import init_db, is_news_new, mark_as_published, get_user_preferences, save_user_preferences, get_all_users, get_user_language, set_user_language
from translator import translate_text  # Импортируем функцию перевода
from functools import lru_cache

LANG_NAMES = {
    "en": "English 🇬🇧",
    "ru": "Русский 🇷🇺",
    "de": "Deutsch 🇩🇪",
    "fr": "Français 🇫🇷"
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
        # Загружаем текущие настройки из базы
        current_subs = get_user_preferences(user_id) or []
        
        # Сохраняем состояние в временном хранилище
        USER_STATES[user_id] = {
            "current_subs": current_subs.copy(),
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
        state = USER_STATES.get(user_id, {"current_subs": []})
        current_subs = state["current_subs"]
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
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Редактируем существующее сообщение или отправляем новое
        if state.get("message_id"):
            await context.bot.edit_message_text(
                chat_id=user_id,
                message_id=state["message_id"],
                text="⚙️ Выберите интересующие вас категории:",
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
    await query.answer()
    user_id = query.from_user.id
    
    try:
        # Получаем или инициализируем состояние пользователя
        if user_id not in USER_STATES:
            USER_STATES[user_id] = {
                "current_subs": get_user_preferences(user_id) or [],
                "message_id": query.message.message_id
            }
            
        state = USER_STATES[user_id]
        current_subs = state["current_subs"]
        
        # Обработка переключения категорий
        if query.data.startswith("toggle_"):
            category = query.data.split("_")[1]
            
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
            # Сохраняем в базу данных
            save_user_preferences(user_id, current_subs)
            
            # Удаляем временное состояние
            if user_id in USER_STATES:
                del USER_STATES[user_id]
            
            await query.edit_message_text("✅ Настройки сохранены!")
        
        # Обработка выбора языка
        elif query.data.startswith("lang_"):
            lang = query.data.split("_")[1]
            set_user_language(user_id, lang)
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
        print("🔎 Проверка новостей...")
        news_list = await fetch_news()
        for news in news_list:
            if is_news_new(news['id']):
                await post_to_channel(context.bot, news)
    except Exception as e:
        print(f"⚠️ Ошибка мониторинга: {e}")

async def send_news_to_user(user_id, news_item):
    try:
        # Получаем язык пользователя
        user_lang = get_user_language(user_id)
        
        # Переводим если нужно
        if user_lang != news_item['lang']:
            title = translate_text(news_item['title'], user_lang)
            description = translate_text(news_item['description'], user_lang)
            lang_note = f"\n\n🌐 (Переведено с {news_item['lang'].upper()})"
        else:
            title = news_item['title']
            description = news_item['description']
            lang_note = ""
        
        # Очищаем HTML
        clean_description = clean_html(description)
        
        # Форматируем сообщение
        message = (
            f"🔥 *{title}*\n"
            f"_Источник: {news_item['source']}_\n"
            f"_Категория: {news_item['category']}_\n\n"
            f"{clean_description}{lang_note}\n\n"
            f"[Читать полностью]({news_item['link']})"
        )
        
        await context.bot.send_message(
            chat_id=user_id,
            text=message,
            parse_mode='MarkdownV2',
            disable_web_page_preview=False
        )
        print(f"📩 Отправлено пользователю {user_id}: {title[:50]}...")
    except Exception as e:
        print(f"❌ Ошибка отправки пользователю {user_id}: {e}")

async def post_to_channel(bot, news_item):
    try:
        # Очищаем описание от HTML
        clean_description = clean_html(news_item['description'])
        hashtags = f"\n#{news_item['category']}_news #{news_item['source']}"
        
        # Форматируем сообщение с категорией (БЕЗ ПЕРЕВОДА)
        message = (
            f"{FIRE_EMOJI} <b>{html.escape(news_item['title'])}</b>\n"
            f"{clean_description}\n\n"
            f"⚡ <a href='{news_item['link']}'>Читать полностью</a>"
            f"\n\n{hashtags}"
        )
        
        await bot.send_message(
            chat_id=CHANNEL_ID,
            text=message,
            parse_mode='HTML',
            disable_web_page_preview=False
        )
        mark_as_published(news_item['id'])
        print(f"✅ [{news_item['lang']}/{news_item['category']}] Опубликовано: {news_item['title'][:50]}...")
    except TelegramError as e:
        print(f"❌ Ошибка отправки: {e}")

async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    print(f"⚡ Получено сообщение: {update.message.text}")
    await update.message.reply_text("Бот активен!")

def main():
    """Точка входа с использованием JobQueue"""
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Инициализация БД с миграцией
    from database import init_db, migrate_db
    init_db()
    migrate_db()  # <-- Важная строка!
    
    # Регистрируем обработчики команд
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("settings", settings_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.ALL, debug))
    
    # Регистрируем периодическую задачу
    job_queue = application.job_queue
    job_queue.run_repeating(
        callback=monitor_news_task, 
        interval=60,  # проверка каждые 60 секунд
        first=1  # запустить через 1 секунду после старта
    )
    
    print("🟢 Бот запущен. Ожидаем команды и мониторим новости...")
    application.run_polling()

if __name__ == "__main__":
     main()