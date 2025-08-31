from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware # Для CORS
from typing import List, Optional
import sys
import os

# Добавляем корень проекта и папку api в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'api'))

from api import database, models # Импортируем наши модули
import config  # Импортируем конфигурационный файл

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
import traceback

# --- Middleware для принудительной установки UTF-8 ---
class ForceUTF8ResponseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        try:
            response = await call_next(request)
            
            content_type = response.headers.get("content-type", "").lower()
            if content_type.startswith("text/") or content_type.startswith("application/json"):
                if "charset=" not in content_type:
                    new_content_type = f"{content_type}; charset=utf-8"
                    response.headers["content-type"] = new_content_type
                elif "charset=utf-8" not in content_type and "charset=utf8" not in content_type:
                    parts = content_type.split(";")
                    new_parts = [parts[0]]
                    new_parts.append("charset=utf-8")
                    new_content_type = ";".join(new_parts)
                    response.headers["content-type"] = new_content_type
            
            return response
        except Exception as e:
            print(f"[Middleware Error] ForceUTF8: {e}")
            traceback.print_exc()
            return await call_next(request) 

# --- FastAPI приложение ---
app = FastAPI(
    title="News API for Chrome Extension",
    description="API для получения новостей из RSS-лент, обработанных Telegram-ботом.",
    version="1.0.0",
    openapi_url="/api/openapi.json", # Путь к OpenAPI схеме
    docs_url="/api/docs", # Путь к интерактивной документации Swagger UI
    redoc_url="/api/redoc", # Путь к документации ReDoc
)

app.add_middleware(ForceUTF8ResponseMiddleware)

# --- Настройка CORS (важно для расширения Chrome) ---
# (Закомментировано, как в оригинале)
# origins = [...]
# app.add_middleware(CORSMiddleware, ...)

# --- Вспомогательная функция для обработки дат ---
def format_datetime(dt_obj):
    """Форматирует объект datetime в строку ISO."""
    return dt_obj.isoformat() if dt_obj else None

# --- Вспомогательная функция для формирования полного URL изображения ---
def get_full_image_url(image_filename: str) -> str:
    """Формирует полный URL для изображения, добавляя HTTP_IMAGES_ROOT_DIR."""
    if not image_filename:
        return None
    # Убираем слэш в начале HTTP_IMAGES_ROOT_DIR, если он есть, чтобы избежать дублирования
    base_url = config.HTTP_IMAGES_ROOT_DIR.rstrip('/')
    # Убираем слэш в начале image_filename, если он есть
    filename = image_filename.lstrip('/')
    return f"{base_url}/{filename}"

# --- Вспомогательная функция для формирования структуры переводов ---
def build_translations_dict(row_dict):
    """Формирует структуру переводов из данных строки."""
    translations = {}
    languages = ['ru', 'en', 'de', 'fr']
    
    for lang in languages:
        title_key = f'title_{lang}'
        content_key = f'content_{lang}'
        title = row_dict.get(title_key)
        content = row_dict.get(content_key)
        
        # Добавляем в словарь только если есть данные
        if title is not None or content is not None:
            translations[lang] = {
                "title": title,
                "content": content
            }
    
    return translations

# --- Endpoints ---

@app.get("/api/news/", response_model=List[models.NewsItem], summary="Получить список новостей")
async def get_news(
    display_language: str = Query(..., description="Язык, на котором отображать новости (ru, en, de, fr)"),
    original_language: Optional[str] = Query(None, description="Фильтр по оригинальному языку новости"),
    category: Optional[str] = Query(None, description="Фильтр по категории (имя категории)"), # Изменено описание
    source: Optional[str] = Query(None, description="Фильтр по источнику (имя источника)"), # Новый фильтр
    limit: Optional[int] = Query(50, description="Количество новостей", le=100, gt=0)
):
    """
    Получить список новостей, отображая заголовок и содержимое на выбранном языке (display_language).
    """
    supported_languages = ['ru', 'en', 'de', 'fr']
    if display_language not in supported_languages:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Неподдерживаемый язык отображения. Допустимые значения: {', '.join(supported_languages)}.")

    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Начинаем с базовых параметров для JOIN'ов
                query_params = []
                
                # JOIN с rss_feeds, categories и sources для получения имен категории и источника
                query = """
                SELECT 
                    nd.news_id,
                    nd.original_title,
                    nd.original_content,
                    nd.original_language,
                    nd.image_filename,
                    COALESCE(c.name, 'Unknown Category') AS category_name,
                    COALESCE(s.name, 'Unknown Source') AS source_name,
                    pn.source_url,
                    pn.published_at,
                    COALESCE(nt_display.translated_title, nd.original_title) as display_title,
                    COALESCE(nt_display.translated_content, nd.original_content) as display_content,
                    nt_ru.translated_title as title_ru,
                    nt_ru.translated_content as content_ru,
                    nt_en.translated_title as title_en,
                    nt_en.translated_content as content_en,
                    nt_de.translated_title as title_de,
                    nt_de.translated_content as content_de,
                    nt_fr.translated_title as title_fr,
                    nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON pn.source_url LIKE CONCAT(rf.url, %s) OR rf.url LIKE CONCAT(pn.source_url, %s)
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
                WHERE 1=1
                """
                
                # Добавляем параметры: сначала для LIKE условий, потом для языковых JOIN'ов
                query_params.extend(['%%', '%%', 'ru', 'en', 'de', 'fr', display_language])
                
                # Добавляем фильтры в WHERE и соответствующие параметры
                if original_language:
                    query += " AND nd.original_language = %s"
                    query_params.append(original_language)
                if category:
                    query += " AND c.name = %s"
                    query_params.append(category)
                if source:
                    query += " AND s.name = %s"
                    query_params.append(source)
                
                # Добавляем ORDER BY и LIMIT
                query += " ORDER BY pn.published_at DESC LIMIT %s"
                query_params.append(limit)
                
                # Отладочный вывод
                count_percent_s = query.count('%s')
                print(f"[DEBUG] Query has {count_percent_s} %s placeholders")
                print(f"[DEBUG] Query params count: {len(query_params)}")
                print(f"[DEBUG] Query params: {query_params}")

                await cur.execute(query, query_params)
                results = []
                async for row in cur:
                    results.append(row)

                # Получаем имена колонок для создания словарей
                columns = [desc[0] for desc in cur.description]

                news_list = []
                for row in results:
                    # Создаем словарь из результата
                    row_dict = dict(zip(columns, row))
                    
                    # Используем category_name и source_name из результата запроса
                    item_data = {
                        "news_id": row_dict['news_id'],
                        "original_title": row_dict['original_title'],
                        "original_content": row_dict['original_content'],
                        "original_language": row_dict['original_language'],
                        "image_url": get_full_image_url(row_dict['image_filename']),
                        "category": row_dict['category_name'],
                        "source": row_dict['source_name'],
                        "source_url": row_dict['source_url'],
                        "published_at": format_datetime(row_dict['published_at']),
                        "translations": build_translations_dict(row_dict)
                    }
                    news_list.append(models.NewsItem(**item_data))
                     
                return news_list

            except Exception as e:
                print(f"[API] Ошибка при выполнении запроса в get_news: {e}")
                traceback.print_exc()
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")


@app.get("/api/news/{news_id}", response_model=models.NewsItem, summary="Получить новость по ID")
async def get_news_by_id(news_id: str):
    """Получить детали конкретной новости по её ID."""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Аналогично, добавляем JOIN с rss_feeds, categories, sources
                query = """
                SELECT 
                    nd.*,
                    COALESCE(c.name, 'Unknown Category') AS category_name,
                    COALESCE(s.name, 'Unknown Source') AS source_name,
                    pn.source_url,
                    pn.published_at,
                    nt_ru.translated_title as title_ru,
                    nt_ru.translated_content as content_ru,
                    nt_en.translated_title as title_en,
                    nt_en.translated_content as content_en,
                    nt_de.translated_title as title_de,
                    nt_de.translated_content as content_de,
                    nt_fr.translated_title as title_fr,
                    nt_fr.translated_content as content_fr
                FROM published_news_data nd
                LEFT JOIN published_news pn ON nd.news_id = pn.id
                LEFT JOIN rss_feeds rf ON pn.source_url LIKE CONCAT(rf.url, %s) OR rf.url LIKE CONCAT(pn.source_url, %s)
                LEFT JOIN categories c ON nd.category_id = c.id
                LEFT JOIN sources s ON rf.source_id = s.id
                LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = %s
                LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = %s
                LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = %s
                LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = %s
                WHERE nd.news_id = %s
                """
                query_params = ['%%', '%%', 'ru', 'en', 'de', 'fr', news_id]
                
                await cur.execute(query, query_params)
                result = await cur.fetchone()

                if result is None:
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Новость не найдена")

                # Получаем имена колонок для создания словаря
                columns = [desc[0] for desc in cur.description]
                result_dict = dict(zip(columns, result))

                item_data = {
                    "news_id": result_dict['news_id'],
                    "original_title": result_dict['original_title'],
                    "original_content": result_dict['original_content'],
                    "original_language": result_dict['original_language'],
                    "image_url": get_full_image_url(result_dict['image_filename']),
                    "category": result_dict['category_name'],
                    "source": result_dict['source_name'],
                    "source_url": result_dict['source_url'],
                    "published_at": format_datetime(result_dict['published_at']),
                    "translations": build_translations_dict(result_dict)
                }

                return models.NewsItem(**item_data)

            except HTTPException:
                raise
            except Exception as e:
                print(f"[API] Ошибка при выполнении запроса для ID {news_id} в get_news_by_id: {e}")
                traceback.print_exc()
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")


@app.get("/api/categories/", response_model=List[models.CategoryItem], summary="Получить категории")
async def get_categories():
    """
    Получить список всех уникальных категорий.
    Данные в формате id, name из таблицы `categories`.
    """
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = "SELECT id, name FROM categories ORDER BY name"
                await cur.execute(query)
                results = []
                async for row in cur:
                    results.append(row)
                
                return [models.CategoryItem(id=row[0], name=row[1]) for row in results]
            except Exception as e:
                print(f"[API] Ошибка при получении категорий в get_categories: {e}")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")


@app.get("/api/languages/", response_model=List[models.LanguageItem], summary="Получить оригинальные языки")
async def get_original_languages():
    """
    Получить список всех языков активных фидов
    """
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                query = "SELECT DISTINCT language FROM rss_feeds WHERE is_active = TRUE ORDER BY language"
                await cur.execute(query)
                results = []
                async for row in cur:
                    results.append(row)
                
                return [models.LanguageItem(language=row[0]) for row in results]
            except Exception as e:
                print(f"[API] Ошибка при получении языков в get_original_languages: {e}")
                traceback.print_exc()
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")


# --- Healthcheck endpoint ---
@app.get("/api/health", summary="Проверка состояния API")
async def health_check():
    """Проверяет, запущено ли API и доступна ли БД."""
    try:
        pool = await database.get_db_pool()
        if pool:
            db_status = "ok"
        else:
            db_status = "error"
    except Exception:
        db_status = "error"

    return {"status": "ok" if db_status == "ok" else "error", "database": db_status}