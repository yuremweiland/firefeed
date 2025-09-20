from fastapi import FastAPI, APIRouter, Depends, HTTPException, Query, status, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware # Для CORS
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List, Optional, Union, Set
import sys
import os
import asyncio
import json
from datetime import datetime, timedelta
import threading
import logging
import traceback
import hashlib
import secrets
import jwt
import random
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from email_service.sender import send_verification_email

# Настройка логирования для этого модуля
logger = logging.getLogger("api.news")
logger.setLevel(logging.DEBUG) # Установите INFO в продакшене
# Добавляем корень проекта и папку api в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'api'))
from api import database, models # Импортируем наши модули
import config  # Импортируем конфигурационный файл
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
import traceback

# --- Настройки JWT ---
SECRET_KEY = getattr(config, 'JWT_SECRET_KEY', 'your-secret-key-change-in-production')
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# --- OAuth2 схема ---
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")

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

# --- Функции для работы с JWT ---
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt
def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверяет пароль против хэша"""
    return hashlib.pbkdf2_hmac('sha256', 
                               plain_password.encode('utf-8'), 
                               SECRET_KEY.encode('utf-8'), 
                               100000) == bytes.fromhex(hashed_password)
def get_password_hash(password: str) -> str:
    """Создает хэш пароля"""
    pwdhash = hashlib.pbkdf2_hmac('sha256',
                                  password.encode('utf-8'),
                                  SECRET_KEY.encode('utf-8'),
                                  100000)
    return pwdhash.hex()
async def get_current_user(token: str = Depends(oauth2_scheme)):
    """Получает текущего пользователя по токену"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id: int = payload.get("sub")
        if user_id is None:
            raise credentials_exception
    except jwt.PyJWTError:
        raise credentials_exception
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    user = await database.get_user_by_id(pool, user_id)
    if user is None:
        raise credentials_exception
    return user
async def get_current_active_user(current_user: dict = Depends(get_current_user)):
    """Проверяет, что пользователь активен"""
    if not current_user.get("is_active"):
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
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
origins = ["*"] # ИЛИ список конкретных origins как выше
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,           # Разрешенные источники
    allow_credentials=True,          # Разрешить отправку cookies, Authorization headers и т.д.
    allow_methods=["*"],             # Разрешить все HTTP методы (GET, POST, PUT, DELETE и т.д.)
    allow_headers=["*"],             # Разрешить все заголовки
    # expose_headers=["Access-Control-Allow-Origin"] # Опционально: какие заголовки expose
    # max_age=3600 # Опционально: кэширование preflight запросов
)
# --- Вспомогательная функция для обработки дат ---
def format_datetime(dt_obj):
    """Форматирует объект datetime в строку ISO."""
    return dt_obj.isoformat() if dt_obj else None
# --- Вспомогательная функция для формирования полного URL изображения ---
def get_full_image_url(image_filename: str) -> str:
    """Формирует полный URL для изображения, добавляя HTTP_IMAGES_ROOT_DIR."""
    if not image_filename:
        return None
    # Если image_filename уже является полным URL (начинается с http(s)://), возвращаем как есть
    if image_filename.startswith(('http://', 'https://')):
        return image_filename
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
active_connections = []
# --- WebSocket endpoint для реалтайм обновлений ---
@app.websocket("/api/v1/ws/rss-items")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.append(websocket)
    print(f"[WebSocket] New connection. Total connections: {len(active_connections)}")
    try:
        while True:
            # Получаем сообщение от клиента (например, heartbeat)
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                if message.get("type") == "ping":
                    # Отвечаем на ping
                    await websocket.send_text(json.dumps({
                        "type": "pong", 
                        "timestamp": datetime.now().isoformat()
                    }))
            except json.JSONDecodeError:
                # Просто эхо если не JSON
                await websocket.send_text(json.dumps({"type": "echo", "data": data}))
    except WebSocketDisconnect:
        active_connections.remove(websocket)
        print(f"[WebSocket] Connection closed. Total connections: {len(active_connections)}")
    except Exception as e:
        print(f"[WebSocket] Error: {e}")
        if websocket in active_connections:
            active_connections.remove(websocket)

# --- Функция для отправки уведомлений о новых новостях ---
async def broadcast_new_news(news_items: List[dict]):
    """Отправляет уведомление о новых новостях всем подключенным клиентам"""
    if not active_connections:
        return
    message = {
        "type": "new_news",
        "timestamp": datetime.now().isoformat(),
        "count": len(news_items),
        "news": [  # Отправляем только основную информацию для уведомления
            {
                "news_id": item.get("news_id"),
                "title": item.get("original_title", "")[:100] + "..." if item.get("original_title", "") else "Без заголовка",
                "category": item.get("category", "Без категории"),
                "published_at": item.get("published_at")
            }
            for item in news_items[:5]  # Ограничиваем 5 новостями для уведомления
        ]
    }
    disconnected = []
    for connection in active_connections:
        try:
            await connection.send_text(json.dumps(message, ensure_ascii=False))
        except WebSocketDisconnect:
            disconnected.append(connection)
        except Exception as e:
            print(f"[WebSocket] Error sending to connection: {e}")
            disconnected.append(connection)
    # Удаляем отключенные соединения
    for conn in disconnected:
        if conn in active_connections:
            active_connections.remove(conn)
    if disconnected:
        print(f"[WebSocket] Removed {len(disconnected)} disconnected clients")

# --- Фоновая задача для проверки новых новостей ---
last_check_time = datetime.now()

async def check_for_new_news():
    """Фоновая задача для периодической проверки новых новостей"""
    global last_check_time
    pool = await database.get_db_pool()
    if pool is None:
        print("[News Check] Database pool is not available.")
        return
    while True:
        await asyncio.sleep(config.NEWS_CHECK_INTERVAL_SECONDS) # Ждем указанный интервал
        try:
            news_items = await database.get_recent_news_for_broadcast(pool, last_check_time)
            if news_items:
                await broadcast_new_news(news_items)
            last_check_time = datetime.now()
        except Exception as e:
            print(f"[News Check] Error checking for new news: {e}")

# - Запуск фоновой задачи при старте приложения -
@app.on_event("startup")
async def startup_event():
    """Запускает фоновые задачи при старте приложения"""
    # Запускаем задачу проверки новых новостей
    asyncio.create_task(check_for_new_news())
    print("[Startup] News checking task started")

# --- Endpoints для новостей ---
@app.get("/api/v1/rss-items/", summary="Получить список новостей")
async def get_news(
    display_language: str = Query(..., description="Язык, на котором отображать новости (ru, en, de, fr)"),
    original_language: Optional[str] = Query(None, description="Фильтр по оригинальному языку новости"),
    category_id: Optional[List[int]] = Query(None, description="Фильтр по ID категории (можно указать несколько)"),
    source_id: Optional[List[int]] = Query(None, description="Фильтр по ID источника (можно указать несколько)"),
    telegram_published: Optional[bool] = Query(None, description="Фильтр по статусу публикации в Telegram (true/false)"),
    limit: Optional[int] = Query(50, description="Количество новостей на странице", le=100, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных новостей)", ge=0)
):
    """
    Получить список новостей, отображая заголовок и содержимое на выбранном языке (display_language).
    Поддерживает пагинацию через параметры limit и offset.
    Возвращает данные в формате: {"count": общее_количество, "results": [список_новостей]}
    """
    supported_languages = ['ru', 'en', 'de', 'fr']
    if display_language not in supported_languages:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Неподдерживаемый язык отображения. Допустимые значения: {', '.join(supported_languages)}.")
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    try:
        total_count, results, columns = await database.get_all_rss_items_list(
            pool, display_language, original_language, category_id, source_id, telegram_published, limit, offset
        )
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_news: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")

    # Форматируем результаты
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
        news_list.append(models.RSSItem(**item_data))

    # Возвращаем данные в формате с count и results
    return {"count": total_count, "results": news_list}

@app.get("/api/v1/rss-items/{rss_item_id}", response_model=models.RSSItem, summary="Получить новость по ID")
async def get_news_by_id(rss_item_id: str):
    """Получить детали конкретной новости по её ID."""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    result = await database.get_rss_item_by_id(pool, rss_item_id)
    if not result:
         raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News item not found")

    try:
        # Предполагаем, что database.get_rss_item_by_id возвращает кортеж результата и список колонок
        # или изменена сигнатура, чтобы возвращать только кортеж/строку результата.
        # Для совместимости предположим, что она возвращает (row, columns) или просто row.
        # В предоставленном коде database.get_rss_item_by_id возвращала Optional[Tuple].
        # Переделаем вызов и обработку.
        # Пусть database.get_rss_item_by_id возвращает Optional[Tuple]
        row = result
        # Получаем названия колонок внутри database.py или передаем их оттуда.
        # Для простоты, предположим, что database.get_rss_item_by_id теперь возвращает (row, columns) или None
        # Или пусть database.get_rss_item_by_id возвращает row, а columns получаем отдельно.
        # Лучше изменить database.get_rss_item_by_id, чтобы она возвращала row_dict напрямую.
        # Но чтобы не менять сигнатуру кардинально, пусть возвращает row_tuple.
        # Нужно получить columns. Пусть database.py предоставит их.
        # Или сделаем обертку.

        # Предположим, database.get_rss_item_by_id возвращает (row_tuple, column_names) или (None, None)
        # row, columns = result
        # if not row:
        #     raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News item not found")

        # Но оригинальная сигнатура: async def get_rss_item_by_id(pool, news_id: str) -> Optional[Tuple]:
        # Значит, она возвращает row_tuple или None.
        # Нужно получить columns отдельно или внутри database.py обработать.
        # Лучше обернуть в database.py.

        # Пусть database.get_rss_item_by_id_full возвращает (row, columns) или (None, [])
        full_result = await database.get_rss_item_by_id_full(pool, rss_item_id)
        if not full_result or not full_result[0]:
             raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="News item not found")
        row, columns = full_result

        row_dict = dict(zip(columns, row))
        # Формируем данные для модели
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
    except Exception as e:
        print(f"[API] Ошибка при выполнении запроса в get_news_by_id: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")
    return models.RSSItem(**item_data)

@app.get("/api/v1/categories/", summary="Получить категории новостей")
async def get_categories(
    limit: Optional[int] = Query(100, description="Количество категорий на странице", le=1000, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных категорий)", ge=0)
):
    """
    Получить список всех уникальных категорий.
    Данные в формате: {"count": общее_количество, "results": [список_категорий]}
    """
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    try:
        total_count, results = await database.get_all_categories_list(pool, limit, offset)
    except Exception as e:
        print(f"[API] Ошибка при выполнении запроса в get_categories: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")

    return {"count": total_count, "results": results}

@app.get("/api/v1/sources/", summary="Получить источники новостей")
async def get_sources(
    limit: Optional[int] = Query(100, description="Количество источников на странице", le=1000, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных источников)", ge=0),
    category_id: Optional[List[int]] = Query(None, description="Фильтр по ID категорий")
):
    """
    Получить список всех уникальных источников.
    Данные в формате: {"count": общее_количество, "results": [список_источников]}
    """
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    try:
        total_count, results = await database.get_all_sources_list(pool, limit, offset, category_id)
    except Exception as e:
        print(f"[API] Ошибка при выполнении запроса в get_sources: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")

    return {"count": total_count, "results": results}

@app.get("/api/v1/languages/", summary="Получить поддерживаемые языки")
async def get_languages():
    """Получить список поддерживаемых языков."""
    return {"results": config.SUPPORTED_LANGUAGES}

# - Healthcheck endpoint -
@app.get("/api/v1/health", summary="Проверка состояния API")
async def health_check():
    """Проверяет, запущено ли API и доступна ли БД."""
    try:
        pool = await database.get_db_pool()
        if pool:
            db_status = "ok"
            # Получаем информацию о пуле подключений
            pool_total = pool.size # Общее количество подключений в пуле
            pool_free = pool.freesize # Количество свободных подключений
        else:
            db_status = "error"
            pool_total = 0
            pool_free = 0
    except Exception as e:
        db_status = "error"
        pool_total = 0
        pool_free = 0
        print(f"[Healthcheck] Database connection error: {e}")
    return {
        "status": "ok",
        "database": db_status,
        "db_pool": {
            "total_connections": pool_total,
            "free_connections": pool_free
        }
    }

# --- Auth endpoints ---
auth_router = APIRouter(prefix="/api/v1/auth", tags=["authentication"])

@auth_router.post("/register", response_model=models.UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(user: models.UserCreate):
    """Регистрация нового пользователя"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверяем, существует ли пользователь с таким email
    existing_user = await database.get_user_by_email(pool, user.email)
    if existing_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    # Хэшируем пароль
    password_hash = get_password_hash(user.password)
    # Создаем пользователя
    new_user = await database.create_user(pool, user.email, password_hash, user.language)
    if not new_user:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")
    # Генерируем код верификации
    verification_code = ''.join(random.choices('0123456789', k=6))
    expires_at = datetime.utcnow() + timedelta(hours=24)
    # Сохраняем код верификации в БД
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                await cur.execute(
                    "INSERT INTO user_verification_codes (user_id, verification_code, expires_at) VALUES (%s, %s, %s)",
                    (new_user['id'], verification_code, expires_at)
                )
            except Exception as e:
                # Если не удалось сохранить код, удаляем пользователя и возвращаем ошибку
                await database.delete_user(pool, new_user['id'])
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create verification code")
    
    # Отправляем email с кодом подтверждения
    email_sent = send_verification_email(user.email, verification_code, user.language)
    if not email_sent:
        # Если email не отправился, удаляем пользователя и возвращаем ошибку
        await database.delete_user(pool, new_user['id'])
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to send verification email")
    
    # Убираем вывод в логи - теперь код отправляется на email
    # print(f"Verification code for {user.email}: {verification_code}")
    
    return models.UserResponse(**new_user)

@auth_router.post("/verify", response_model=models.SuccessResponse)
async def verify_user(request: models.EmailVerificationRequest):
    """Верификация email пользователя с помощью кода подтверждения"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Находим пользователя по email
    user = await database.get_user_by_email(pool, request.email)
    if not user:
        # В целях безопасности не раскрываем, что пользователь не найден
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid verification code or email")
    # Если пользователь уже активен
    if user.get('is_active'):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="User already verified")
    # Ищем код верификации
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT id, user_id, verification_code, expires_at, used_at FROM user_verification_codes WHERE user_id = %s AND verification_code = %s AND used_at IS NULL AND expires_at > NOW()",
                (user['id'], request.code)
            )
            code_record = await cur.fetchone()
    if not code_record:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid verification code or email")
    # Обновляем пользователя как активного и помечаем код как использованный
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            try:
                # Начинаем транзакцию
                await cur.execute("BEGIN")
                
                # Активируем пользователя
                await cur.execute(
                    "UPDATE users SET is_active = TRUE WHERE id = %s",
                    (user['id'],)
                )
                
                # Помечаем код как использованный
                await cur.execute(
                    "UPDATE user_verification_codes SET used_at = NOW() WHERE id = %s",
                    (code_record[0],)  # code_record[0] - это id записи кода
                )
                
                # Завершаем транзакцию
                await cur.execute("COMMIT")
                
            except Exception as e:
                # Откатываем транзакцию в случае ошибки
                await cur.execute("ROLLBACK")
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to verify user")
    return models.SuccessResponse(message="User successfully verified")

@auth_router.post("/login", response_model=models.Token)
async def login_user(form_data: OAuth2PasswordRequestForm = Depends()):
    """Аутентификация пользователя и выдача токена"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Получаем пользователя по email
    user = await database.get_user_by_email(pool, form_data.username)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password")
    # Проверяем пароль
    if not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Incorrect email or password")
    # Проверяем, активирован ли аккаунт
    if not user.get("is_active"):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Account not verified. Please check your email for verification code.")
    # Создаем токен
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user["id"])}, expires_delta=access_token_expires
    )
    return {
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": ACCESS_TOKEN_EXPIRE_MINUTES * 60
    }
@auth_router.post("/reset-password/request")
async def request_password_reset(request: models.PasswordResetRequest):
    """Запрос на сброс пароля"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверяем, существует ли пользователь
    user = await database.get_user_by_email(pool, request.email)
    if not user:
        # Всегда возвращаем успех для безопасности
        return {"message": "If email exists, reset instructions have been sent"}
    # Генерируем токен
    token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(hours=1)  # Токен действует 1 час
    # Сохраняем токен в БД
    success = await database.create_password_reset_token(pool, user["id"], token, expires_at)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create reset token")
    # Здесь должна быть логика отправки email (в реальной реализации)
    # send_reset_email(request.email, token)
    return {"message": "If email exists, reset instructions have been sent"}
@auth_router.post("/reset-password/confirm")
async def confirm_password_reset(request: models.PasswordResetConfirm):
    """Подтверждение сброса пароля"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Получаем токен
    reset_token = await database.get_password_reset_token(pool, request.token)
    if not reset_token:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired token")
    # Проверяем, не истек ли токен
    if reset_token["expires_at"] < datetime.utcnow():
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Token has expired")
    # Хэшируем новый пароль
    new_password_hash = get_password_hash(request.new_password)
    # Обновляем пароль пользователя
    success = await database.update_user_password(pool, reset_token["user_id"], new_password_hash)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update password")
    # Помечаем токен как использованный
    await database.use_password_reset_token(pool, request.token)
    return {"message": "Password successfully reset"}

# --- User endpoints ---
user_router = APIRouter(prefix="/api/v1/users", tags=["users"])

@user_router.get("/me", response_model=models.UserResponse)
async def get_current_user_profile(current_user: dict = Depends(get_current_active_user)):
    """Получение профиля текущего пользователя"""
    return models.UserResponse(**current_user)
@user_router.put("/me", response_model=models.UserResponse)
async def update_current_user(user_update: models.UserUpdate, current_user: dict = Depends(get_current_active_user)):
    """Обновление профиля текущего пользователя"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверяем, если email изменяется, что он не занят другим пользователем
    if user_update.email and user_update.email != current_user["email"]:
        existing_user = await database.get_user_by_email(pool, user_update.email)
        if existing_user:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    # Подготавливаем данные для обновления
    update_data = {}
    if user_update.email is not None:
        update_data["email"] = user_update.email
    if user_update.language is not None:
        update_data["language"] = user_update.language
    # Обновляем пользователя
    updated_user = await database.update_user(pool, current_user["id"], update_data)
    if not updated_user:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update user")
    return models.UserResponse(**updated_user)
@user_router.delete("/me", status_code=status.HTTP_204_NO_CONTENT)
async def delete_current_user(current_user: dict = Depends(get_current_active_user)):
    """Удаление (деактивация) аккаунта текущего пользователя"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Деактивируем пользователя
    success = await database.delete_user(pool, current_user["id"])
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to delete user")
    return

# --- User Categories endpoints ---
categories_router = APIRouter(prefix="/api/v1/users/me/categories", tags=["user_categories"])

@categories_router.put("/", response_model=models.SuccessResponse)
async def update_user_categories(
    category_update: models.UserCategoriesUpdate,
    current_user: dict = Depends(get_current_active_user)
):
    """Обновление списка пользовательских категорий"""
    category_ids = category_update.category_ids  # Извлекаем Set[int] из модели
    
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    
    # Проверяем, что все указанные категории существуют
    async with pool.acquire() as conn:
        async with conn.cursor() as cur:
            # Получаем все существующие категории
            await cur.execute("SELECT id FROM categories")
            existing_categories = {row[0] for row in await cur.fetchall()}
            
            # Проверяем, что все переданные ID существуют
            invalid_ids = category_ids - existing_categories
            if invalid_ids:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST, 
                    detail=f"Invalid category IDs: {list(invalid_ids)}"
                )
    
    # Обновляем категории пользователя
    success = await database.update_user_categories(pool, current_user["id"], category_ids)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update user categories")
    
    return models.SuccessResponse(message="User categories successfully updated")

@categories_router.get("/", response_model=models.UserCategoriesResponse)  # Используем новую модель ответа
async def get_user_categories(current_user: dict = Depends(get_current_active_user)):
    """Получение списка пользовательских категорий"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    
    categories = await database.get_user_categories(pool, current_user["id"])
    # Возвращаем в формате {"category_ids": [...]}
    return models.UserCategoriesResponse(category_ids=[cat['id'] for cat in categories])

# --- User RSS Feeds endpoints ---
rss_router = APIRouter(prefix="/api/v1/users/me/rss-feeds", tags=["user_rss_feeds"])
@rss_router.post("/", response_model=models.UserRSSFeedResponse, status_code=status.HTTP_201_CREATED)
async def create_user_rss_feed(feed: models.UserRSSFeedCreate, current_user: dict = Depends(get_current_active_user)):
    """Создание пользовательской RSS-ленты"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Создаем RSS-ленту
    new_feed = await database.create_user_rss_feed(
        pool, 
        current_user["id"], 
        feed.url, 
        feed.name, 
        feed.category_id, 
        feed.language
    )
    if not new_feed:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create RSS feed")
    return models.UserRSSFeedResponse(**new_feed)
@rss_router.get("/", response_model=models.PaginatedResponse[models.UserRSSFeedResponse])
async def get_user_rss_feeds(
    limit: int = Query(50, le=100, gt=0),
    offset: int = Query(0, ge=0),
    current_user: dict = Depends(get_current_active_user)
):
    """Получение списка пользовательских RSS-лент"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Получаем RSS-ленты
    feeds = await database.get_user_rss_feeds(pool, current_user["id"], limit, offset)
    # Преобразуем в модели
    feed_models = [models.UserRSSFeedResponse(**feed) for feed in feeds]
    return models.PaginatedResponse[models.UserRSSFeedResponse](
        count=len(feed_models),
        results=feed_models
    )
@rss_router.get("/{feed_id}", response_model=models.UserRSSFeedResponse)
async def get_user_rss_feed(feed_id: int, current_user: dict = Depends(get_current_active_user)):
    """Получение конкретной пользовательской RSS-ленты"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Получаем RSS-ленту
    feed = await database.get_user_rss_feed_by_id(pool, current_user["id"], feed_id)
    if not feed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RSS feed not found")
    return models.UserRSSFeedResponse(**feed)
@rss_router.put("/{feed_id}", response_model=models.UserRSSFeedResponse)
async def update_user_rss_feed(
    feed_id: int, 
    feed_update: models.UserRSSFeedUpdate, 
    current_user: dict = Depends(get_current_active_user)
):
    """Обновление пользовательской RSS-ленты"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Подготавливаем данные для обновления
    update_data = {}
    if feed_update.name is not None:
        update_data["name"] = feed_update.name
    if feed_update.category_id is not None:
        update_data["category_id"] = feed_update.category_id
    if feed_update.is_active is not None:
        update_data["is_active"] = feed_update.is_active
    # Обновляем RSS-ленту
    updated_feed = await database.update_user_rss_feed(pool, current_user["id"], feed_id, update_data)
    if not updated_feed:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RSS feed not found or failed to update")
    return models.UserRSSFeedResponse(**updated_feed)
@rss_router.delete("/{feed_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user_rss_feed(feed_id: int, current_user: dict = Depends(get_current_active_user)):
    """Удаление пользовательской RSS-ленты"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Удаляем RSS-ленту
    success = await database.delete_user_rss_feed(pool, current_user["id"], feed_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="RSS feed not found")
    return
# --- Healthcheck endpoint ---
@app.get("/api/v1/health", summary="Проверка состояния API")
async def health_check():
    """Проверяет, запущено ли API и доступна ли БД."""
    try:
        pool = await database.get_db_pool()
        if pool:
            db_status = "ok"
            # Получаем информацию о пуле подключений
            pool_total = pool.size  # Общее количество подключений в пуле
            pool_free = pool.freesize  # Количество свободных подключений
        else:
            db_status = "error"
            pool_total = None
            pool_free = None
    except Exception as e:
        print(f"[Health Check] Ошибка при проверке БД: {e}")
        db_status = "error"
        pool_total = None
        pool_free = None
    return {
        "status": "ok" if db_status == "ok" else "error", 
        "database": db_status,
        "database_pool": {
            "total_connections": pool_total,
            "free_connections": pool_free
        } if pool_total is not None else None
    }
# --- Подключение роутеров ---
app.include_router(auth_router)
app.include_router(user_router)
app.include_router(categories_router)
app.include_router(rss_router)