# api/main.py
from fastapi import FastAPI, Depends, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware # Для CORS
from typing import List, Optional
import sys
import os

# Добавляем корень проекта и папку api в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'api'))

from api import database, models # Импортируем наши модули

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
import traceback

# --- Middleware для принудительной установки UTF-8 ---
# (Без изменений)
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
# (Без изменений)
def format_datetime(dt_obj):
    """Форматирует объект datetime в строку ISO."""
    return dt_obj.isoformat() if dt_obj else None

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

    with database.get_db() as connection:
        if connection is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

        cursor = connection.cursor(dictionary=True)
        try:
            # --- ОБНОВЛЕНИЕ ЗАПРОСА ---
            # JOIN с rss_feeds, categories и sources для получения имен категории и источника
            query = """
            SELECT 
                nd.news_id,
                nd.original_title,
                nd.original_content,
                nd.original_language,
                COALESCE(c.name, nd.category) AS category_name, -- Имя категории из справочника или из старого поля
                COALESCE(s.name, 'Unknown Source') AS source_name, -- Имя источника
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
            -- JOIN с rss_feeds через любое поле, которое их связывает, если есть точная связь.
            -- Предположим, что URL или часть его может быть связующим звеном.
            -- Или если в published_news_data добавлен feed_id, использовать его.
            -- Пока используем косвенную связь через URL и rss_feeds.
            -- Более надежный способ: добавить feed_id в published_news_data при сохранении.
            -- Для совместимости, делаем LEFT JOIN по URL.
            LEFT JOIN rss_feeds rf ON pn.source_url LIKE CONCAT(rf.url, '%') OR rf.url LIKE CONCAT(pn.source_url, '%') -- Пример косвенной связи
            LEFT JOIN categories c ON rf.category_id = c.id -- Получаем имя категории
            LEFT JOIN sources s ON rf.source_id = s.id -- Получаем имя источника
            LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = 'ru'
            LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = 'en'
            LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = 'de'
            LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = 'fr'
            LEFT JOIN news_translations nt_display ON nd.news_id = nt_display.news_id AND nt_display.language = %s
            WHERE 1=1
            """
            # --- КОНЕЦ ОБНОВЛЕНИЯ ЗАПРОСА ---
            params = [display_language]
            where_params = []

            if original_language:
                query += " AND nd.original_language = %s"
                where_params.append(original_language)
            # --- ИЗМЕНЕНИЕ УСЛОВИЯ ФИЛЬТРАЦИИ ---
            # Фильтруем по имени категории, а не по значению в published_news_data
            if category:
                query += " AND c.name = %s" # Фильтр по имени категории из таблицы categories
                where_params.append(category)
            # --- НОВОЕ УСЛОВИЕ ФИЛЬТРАЦИИ ---
            if source:
                query += " AND s.name = %s" # Фильтр по имени источника из таблицы sources
                where_params.append(source)
            # --- КОНЕЦ ИЗМЕНЕНИЙ ---
            query += " ORDER BY pn.published_at DESC LIMIT %s"
            where_params.append(limit)
            
            full_params = params + where_params

            cursor.execute(query, full_params)
            results = cursor.fetchall()

            news_list = []
            for row in results:
                 # --- ОБНОВЛЕНИЕ ФОРМИРОВАНИЯ ОТВЕТА ---
                 # Используем category_name и source_name из результата запроса
                 item_data = {
                     "news_id": row['news_id'],
                     "original_title": row['original_title'],
                     "original_content": row['original_content'],
                     "original_language": row['original_language'],
                     "category": row['category_name'], # Используем имя категории
                     "source": row['source_name'],     # Добавляем имя источника (если нужно в модели)
                     "source_url": row['source_url'],
                     "published_at": format_datetime(row['published_at']),
                     f"title_{display_language}": row['display_title'],
                     f"content_{display_language}": row['display_content'],
                     "title_ru": row['title_ru'],
                     "content_ru": row['content_ru'],
                     "title_en": row['title_en'],
                     "content_en": row['content_en'],
                     "title_de": row['title_de'],
                     "content_de": row['content_de'],
                     "title_fr": row['title_fr'],
                     "content_fr": row['content_fr'],
                 }
                 # --- КОНЕЦ ОБНОВЛЕНИЯ ---
                 news_list.append(models.NewsItem(**item_data))
                 
            return news_list

        except Exception as e:
            print(f"[API] Ошибка при выполнении запроса в get_news: {e}")
            # traceback.print_exc() # Раскомментируйте для отладки
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")
        finally:
            cursor.close()


@app.get("/api/news/{news_id}", response_model=models.NewsItem, summary="Получить новость по ID")
async def get_news_by_id(news_id: str):
    """Получить детали конкретной новости по её ID."""
    with database.get_db() as connection:
        if connection is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

        cursor = connection.cursor(dictionary=True)
        try:
            # --- ОБНОВЛЕНИЕ ЗАПРОСА ---
            # Аналогично, добавляем JOIN с rss_feeds, categories, sources
            query = """
            SELECT 
                nd.*,
                COALESCE(c.name, nd.category) AS category_name, -- Имя категории
                COALESCE(s.name, 'Unknown Source') AS source_name, -- Имя источника
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
            LEFT JOIN rss_feeds rf ON pn.source_url LIKE CONCAT(rf.url, '%') OR rf.url LIKE CONCAT(pn.source_url, '%')
            LEFT JOIN categories c ON rf.category_id = c.id
            LEFT JOIN sources s ON rf.source_id = s.id
            LEFT JOIN news_translations nt_ru ON nd.news_id = nt_ru.news_id AND nt_ru.language = 'ru'
            LEFT JOIN news_translations nt_en ON nd.news_id = nt_en.news_id AND nt_en.language = 'en'
            LEFT JOIN news_translations nt_de ON nd.news_id = nt_de.news_id AND nt_de.language = 'de'
            LEFT JOIN news_translations nt_fr ON nd.news_id = nt_fr.news_id AND nt_fr.language = 'fr'
            WHERE nd.news_id = %s
            """
            # --- КОНЕЦ ОБНОВЛЕНИЯ ЗАПРОСА ---
            cursor.execute(query, (news_id,))
            result = cursor.fetchone()

            if result is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Новость не найдена")

            # --- ОБНОВЛЕНИЕ ФОРМИРОВАНИЯ ОТВЕТА ---
            item_data = {
                "news_id": result['news_id'],
                "original_title": result['original_title'],
                "original_content": result['original_content'],
                "original_language": result['original_language'],
                "category": result['category_name'], # Используем имя категории
                "source": result['source_name'],     # Добавляем имя источника
                "source_url": result['source_url'],
                "published_at": format_datetime(result['published_at']),
                "title_ru": result['title_ru'],
                "content_ru": result['content_ru'],
                "title_en": result['title_en'],
                "content_en": result['content_en'],
                "title_de": result['title_de'],
                "content_de": result['content_de'],
                "title_fr": result['title_fr'],
                "content_fr": result['content_fr'],
                # Fallback для display_ полей (можно уточнить логику)
                "title_en": result['title_en'] or result['original_title'],
                "content_en": result['content_en'] or result['original_content'],
            }
            # --- КОНЕЦ ОБНОВЛЕНИЯ ---
            return models.NewsItem(**item_data)

        except HTTPException:
            raise
        except Exception as e:
            print(f"[API] Ошибка при выполнении запроса для ID {news_id} в get_news_by_id: {e}")
            # traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")
        finally:
            cursor.close()


@app.get("/api/categories/", response_model=List[models.CategoryItem], summary="Получить категории")
async def get_categories():
    """
    Получить список всех уникальных категорий.
    Данные в формате id, name из таблицы `categories`.
    """
    with database.get_db() as connection:
        if connection is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

        cursor = connection.cursor(dictionary=True) # Убедитесь, что dictionary=True
        try:
            query = "SELECT id, name FROM categories ORDER BY name"
            cursor.execute(query)
            results = cursor.fetchall()
            
            return [models.CategoryItem(id=row['id'], name=row['name']) for row in results]
        except Exception as e:
            print(f"[API] Ошибка при получении категорий в get_categories: {e}")
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")
        finally:
            cursor.close()


@app.get("/api/languages/", response_model=List[models.LanguageItem], summary="Получить оригинальные языки")
async def get_original_languages():
    """
    Получить список всех языков активных фидов
    """
    with database.get_db() as connection:
        if connection is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

        cursor = connection.cursor(dictionary=True)
        try:
            query = "SELECT DISTINCT language FROM rss_feeds WHERE is_active = 1 ORDER BY language"
            cursor.execute(query)
            results = cursor.fetchall()
            
            return [models.LanguageItem(language=row['language']) for row in results]
        except Exception as e:
            print(f"[API] Ошибка при получении языков в get_original_languages: {e}")
            # traceback.print_exc()
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")
        finally:
            cursor.close()


# --- Healthcheck endpoint ---
@app.get("/api/health", summary="Проверка состояния API")
async def health_check():
    """Проверяет, запущено ли API и доступна ли БД."""
    try:
        with database.get_db() as connection:
            if connection and connection.is_connected():
                db_status = "ok"
            else:
                db_status = "error"
    except Exception:
        db_status = "error"

    return {"status": "ok" if db_status == "ok" else "error", "database": db_status}
