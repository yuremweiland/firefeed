# main.py
import logging
import os
import sys
import hashlib
import secrets
import json
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from typing import Optional, Dict, Any, List
import asyncio
import traceback

# Настройка логирования для этого модуля
logger = logging.getLogger("api.news")
logger.setLevel(logging.DEBUG) # Установите INFO в продакшене

# Добавляем корень проекта и папку api в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'api'))

from api import database, models # Импортируем наши модули
import config # Импортируем конфигурационный файл

from fastapi import FastAPI, Depends, HTTPException, status, APIRouter, Query, WebSocket, WebSocketDisconnect
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import jwt
from pydantic import BaseModel, EmailStr
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
from starlette.websockets import WebSocketState

# --- Вспомогательные функции ---
def get_password_hash(password: str) -> str:
    """Хэширует пароль с использованием SHA-256 (простая реализация)"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Проверяет, совпадает ли пароль с хэшем"""
    return get_password_hash(plain_password) == hashed_password

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """Создает JWT токен"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=config.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    # Используем PyJWT
    encoded_jwt = jwt.encode(to_encode, config.SECRET_KEY, algorithm=config.ALGORITHM)
    return encoded_jwt

async def get_current_user(token: str = Depends(OAuth2PasswordBearer(tokenUrl="/api/v1/users/login"))):
    """Получает текущего пользователя по токену"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        # Используем PyJWT
        payload = jwt.decode(token, config.SECRET_KEY, algorithms=[config.ALGORITHM])
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
        # Проверяем, не заблокирован ли пользователь
        pool = await database.get_db_pool()
        if pool is None:
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
        user = await database.get_user_by_id(pool, int(user_id))
        if user is None or not user.get("is_active"):
            raise credentials_exception
        return user
    except jwt.PyJWTError: # Используем конкретное исключение из PyJWT
        raise credentials_exception

async def get_current_active_user(current_user: dict = Depends(get_current_user)):
    """Получает текущего активного пользователя"""
    if not current_user.get("is_active"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    return current_user

def send_verification_email(to_email: str, verification_code: str):
    """Отправляет email с кодом верификации"""
    try:
        msg = MIMEText(f"Your verification code is: {verification_code}")
        msg['Subject'] = "Email Verification"
        msg['From'] = config.EMAIL_ADDRESS
        msg['To'] = to_email
        context = ssl.create_default_context()
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
            server.sendmail(config.EMAIL_ADDRESS, to_email, msg.as_string())
        print(f"[Email] Verification email sent to {to_email}")
    except Exception as e:
        print(f"[Email] Error sending verification email: {e}")

def send_password_reset_email(to_email: str, reset_token: str):
    """Отправляет email с токеном сброса пароля"""
    try:
        reset_link = f"{config.FRONTEND_URL}/reset-password?token={reset_token}"
        msg = MIMEText(f"Click the link to reset your password: {reset_link}")
        msg['Subject'] = "Password Reset Request"
        msg['From'] = config.EMAIL_ADDRESS
        msg['To'] = to_email
        context = ssl.create_default_context()
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            server.starttls(context=context)
            server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
            server.sendmail(config.EMAIL_ADDRESS, to_email, msg.as_string())
        print(f"[Email] Password reset email sent to {to_email}")
    except Exception as e:
        print(f"[Email] Error sending password reset email: {e}")

def format_datetime(dt: datetime) -> str:
    """Форматирует дату и время в строку"""
    if dt:
        return dt.isoformat()
    return None

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

def build_translations_dict(row_dict: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """Строит словарь переводов из результата запроса"""
    translations = {}
    for lang in ['ru', 'en', 'de', 'fr']:
        title_key = f"title_{lang}"
        content_key = f"content_{lang}"
        if title_key in row_dict and content_key in row_dict:
            translations[lang] = {
                "title": row_dict[title_key],
                "content": row_dict[content_key]
            }
    return translations

# --- Middleware ---
class ForceUTF8ResponseMiddleware(BaseHTTPMiddleware):
    """Middleware для принудительной установки кодировки UTF-8 в заголовках ответа"""
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        if isinstance(response, StreamingResponse):
            return response
        if response.headers.get('content-type', '').startswith('text/'):
            response.headers['content-type'] = response.headers['content-type'].split(';')[0] + '; charset=utf-8'
        return response

# --- FastAPI App ---
app = FastAPI(
    title="FireFeed API",
    description="API services for FireFeed RSS AI-aggregator",
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
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# --- Routers ---
user_router = APIRouter(prefix="/api/v1/users", tags=["users"])

@user_router.post("/register", response_model=models.UserResponse, status_code=status.HTTP_201_CREATED)
async def register_user(user: models.UserCreate):
    """Регистрация нового пользователя"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверка, существует ли пользователь с таким email
    existing_user = await database.get_user_by_email(pool, user.email)
    if existing_user:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Email already registered")
    # Хэшируем пароль
    hashed_password = get_password_hash(user.password)
    # Создаем пользователя
    new_user = await database.create_user(pool, user.email, hashed_password, user.language)
    if not new_user:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to create user")
    # Генерируем и сохраняем код верификации
    verification_code = secrets.token_urlsafe(16)
    success = await database.save_verification_code(pool, new_user["id"], verification_code)
    if not success:
        # Можно удалить пользователя, если не удалось сохранить код, или обработать иначе
        logger.warning(f"Failed to save verification code for user {new_user['id']}")
        # Не возвращаем ошибку пользователю, просто не отправляем email
    # Отправляем email с кодом верификации (асинхронно)
    asyncio.create_task(send_verification_email(user.email, verification_code))
    return models.UserResponse(**new_user)

@user_router.post("/login", response_model=models.Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    """Аутентификация пользователя и выдача токена"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    user = await database.get_user_by_email(pool, form_data.username)
    if not user or not verify_password(form_data.password, user["password_hash"]):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.get("is_active"):
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Inactive user")
    access_token_expires = timedelta(minutes=config.ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": str(user["id"])}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@user_router.post("/verify-email", response_model=models.SuccessResponse)
async def verify_email(verification_data: models.EmailVerificationRequest):
    """Верификация email по коду"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверяем код
    user_id = await database.verify_user_email(pool, verification_data.email, verification_data.code)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid verification code or email")
    # Активируем пользователя
    success = await database.activate_user(pool, user_id)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to activate user")
    return models.SuccessResponse(message="Email verified successfully")

@user_router.post("/request-password-reset", response_model=models.SuccessResponse)
async def request_password_reset(request_data: models.PasswordResetRequest):
    """Запрос сброса пароля (отправка токена на email)"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    user = await database.get_user_by_email(pool, request_data.email)
    if not user:
        # Не раскрываем, что email не существует
        return models.SuccessResponse(message="If the email exists, a reset link has been sent.")
    # Генерируем токен сброса
    reset_token = secrets.token_urlsafe(32)
    expires_at = datetime.utcnow() + timedelta(hours=config.PASSWORD_RESET_TOKEN_EXPIRE_HOURS)
    # Сохраняем токен
    success = await database.save_password_reset_token(pool, user["id"], reset_token, expires_at)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to process request")
    # Отправляем email с токеном (асинхронно)
    asyncio.create_task(send_password_reset_email(request_data.email, reset_token))
    return models.SuccessResponse(message="If the email exists, a reset link has been sent.")

@user_router.post("/reset-password", response_model=models.SuccessResponse)
async def reset_password(reset_data: models.PasswordResetConfirm):
    """Сброс пароля по токену"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Проверяем токен
    user_id = await database.get_user_id_by_reset_token(pool, reset_data.token)
    if not user_id:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid or expired token")
    # Хэшируем новый пароль
    new_hashed_password = get_password_hash(reset_data.new_password)
    # Обновляем пароль
    success = await database.update_user_password(pool, user_id, new_hashed_password)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to reset password")
    # Удаляем использованный токен
    await database.delete_password_reset_token(pool, reset_data.token)
    return models.SuccessResponse(message="Password reset successfully")

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
    update_data = {}
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

# - User news endpoints -
user_news_router = APIRouter(prefix="/api/v1/users/me/rss-items", tags=["user_rss_items"])

@user_news_router.get("/", summary="Получить список новостей из пользовательских RSS-лент")
async def get_user_news(
    display_language: str = Query(..., description="Язык, на котором отображать новости (ru, en, de, fr)"),
    original_language: Optional[str] = Query(None, description="Фильтр по оригинальному языку новости"),
    limit: Optional[int] = Query(50, description="Количество новостей на странице", le=100, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных новостей)", ge=0),
    current_user: dict = Depends(get_current_active_user)
):
    """
    Получить список новостей, отображая заголовок и содержимое на выбранном языке (display_language).
    Новости выбираются из лент, принадлежащих пользователю и принадлежащих к категориям,
    на которые он подписан.
    Возвращает данные в формате: {"count": общее_количество, "results": [список_новостей]}
    """
    user_id = current_user["id"]
    supported_languages = ['ru', 'en', 'de', 'fr']
    if display_language not in supported_languages:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Неподдерживаемый язык отображения. Допустимые значения: {', '.join(supported_languages)}.")
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")
    try:
        # Вызов новой функции из database.py
        total_count, results, columns = await database.get_user_rss_items_list(pool, user_id, display_language, original_language, limit, offset)
    except HTTPException:
        raise # Повторно выбрасываем HTTPException из database.py, если она там была
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_user_news: {e}")
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
        # models.RSSItem используется для форматирования и проверки
        news_list.append(models.RSSItem(**item_data))
    # Возвращаем данные в формате с count и results
    return {"count": total_count, "results": news_list}

@user_news_router.get("/{feed_id}", summary="Получить список новостей из конкретной пользовательской RSS-ленты")
async def get_user_news_by_feed(
    feed_id: int,
    display_language: str = Query(..., description="Язык, на котором отображать новости (ru, en, de, fr)"),
    original_language: Optional[str] = Query(None, description="Фильтр по оригинальному языку новости"),
    limit: Optional[int] = Query(50, description="Количество новостей на странице", le=100, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных новостей)", ge=0),
    current_user: dict = Depends(get_current_active_user)
):
    """
    Получить список новостей из конкретной пользовательской RSS-ленты, отображая заголовок и содержимое на выбранном языке (display_language).
    Возвращает данные в формате: {"count": общее_количество, "results": [список_новостей]}
    """
    user_id = current_user["id"]
    supported_languages = ['ru', 'en', 'de', 'fr']
    if display_language not in supported_languages:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Неподдерживаемый язык отображения. Допустимые значения: {', '.join(supported_languages)}.")
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")
    try:
        # Вызов новой функции из database.py
        total_count, results, columns = await database.get_user_rss_items_list_by_feed(pool, user_id, feed_id, display_language, original_language, limit, offset)
        # Если результат пустой, возможно, лента не принадлежит пользователю или не активна
        # В database.py мы возвращаем 0, [] в этом случае
        if total_count == 0 and not results:
            # Проверим существование ленты отдельно.
            feed_check = await database.get_user_rss_feed_by_id(pool, user_id, feed_id)
            if not feed_check:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Пользовательская RSS-лента не найдена или не активна")
    except HTTPException:
        raise # Повторно выбрасываем HTTPException
    except Exception as e:
        logger.error(f"[API] Ошибка при выполнении запроса в get_user_news_by_feed для feed_id {feed_id}: {e}")
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

# - User Categories endpoints -
categories_router = APIRouter(prefix="/api/v1/users/me/categories", tags=["user_categories"])

@categories_router.put("/", response_model=models.SuccessResponse)
async def update_user_categories(category_update: models.UserCategoriesUpdate, current_user: dict = Depends(get_current_active_user)):
    """Обновление списка пользовательских категорий"""
    category_ids = category_update.category_ids # Извлекаем Set[int] из модели
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    success = await database.update_user_categories(pool, current_user["id"], category_ids)
    if not success:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update user categories")
    return models.SuccessResponse(message="User categories successfully updated")

@categories_router.get("/", response_model=models.UserCategoriesResponse) # Используем новую модель ответа
async def get_user_categories(current_user: dict = Depends(get_current_active_user)):
    """Получение списка пользовательских категорий"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    categories = await database.get_user_categories(pool, current_user["id"])
    # Возвращаем в формате {"category_ids": [...]}
    return models.UserCategoriesResponse(category_ids=[cat['id'] for cat in categories])

# - User RSS Feeds endpoints -
rss_router = APIRouter(prefix="/api/v1/users/me/rss-feeds", tags=["user_rss_feeds"])

@rss_router.post("/", response_model=models.UserRSSFeedResponse, status_code=status.HTTP_201_CREATED)
async def create_user_rss_feed(feed: models.UserRSSFeedCreate, current_user: dict = Depends(get_current_active_user)):
    """Создание пользовательской RSS-ленты"""
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error")
    # Создаем RSS-ленту
    new_feed = await database.create_user_rss_feed(pool, current_user["id"], feed.url, feed.name, feed.category_id, feed.language)
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
    return models.PaginatedResponse[models.UserRSSFeedResponse](count=len(feed_models), results=feed_models)

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

# --- WebSocket для реалтайм обновлений ---
# Храним активные соединения
active_connections: List[WebSocket] = []

async def broadcast_new_news(news_items: List[Dict[str, Any]]):
    """Отправляет новые новости всем подключенным клиентам"""
    if not active_connections:
        print("[WebSocket] No active connections to broadcast to.")
        return
    # Форматируем данные для отправки
    message_data = {
        "type": "new_news",
        "data": news_items
    }
    message = json.dumps(message_data, ensure_ascii=False)
    # Отправляем сообщение всем активным соединениям
    for connection in active_connections[:]: # Копируем список, чтобы избежать ошибок при удалении
        if connection.application_state == WebSocketState.CONNECTED:
            try:
                await connection.send_text(message)
                print(f"[WebSocket] Sent news to {connection.client}")
            except Exception as e:
                print(f"[WebSocket] Error sending to {connection.client}: {e}")
                # Удаляем закрытые соединения
                if connection in active_connections:
                    active_connections.remove(connection)
        else:
            # Удаляем неактивные соединения
            if connection in active_connections:
                active_connections.remove(connection)

@app.websocket("/api/v1/ws/rss-items")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint для получения реалтайм обновлений новостей"""
    await websocket.accept()
    active_connections.append(websocket)
    print(f"[WebSocket] Client {websocket.client} connected. Total connections: {len(active_connections)}")
    try:
        while True:
            # Ждем сообщения от клиента (например, ping)
            data = await websocket.receive_text()
            print(f"[WebSocket] Message received from {websocket.client}: {data}")
            # Можно отправить ответ, если нужно
            # await websocket.send_text(f"Echo: {data}")
    except WebSocketDisconnect:
        print(f"[WebSocket] Client {websocket.client} disconnected.")
    except Exception as e:
        print(f"[WebSocket] Unexpected error for {websocket.client}: {e}")
    finally:
        # Удаляем соединение из списка при любом выходе
        if websocket in active_connections:
            active_connections.remove(websocket)
        print(f"[WebSocket] Client {websocket.client} removed. Total connections: {len(active_connections)}")

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
            # Вызываем функцию из database.py
            news_items = await database.get_recent_news_for_broadcast(pool, last_check_time)
            if news_items:
                # Отправляем уведомление
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
    category_id: Optional[int] = Query(None, description="Фильтр по ID категории"),
    source_id: Optional[int] = Query(None, description="Фильтр по ID источника"),
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
        # Вызов новой функции из database.py
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

    # Вызов функции из database.py
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
        # Вызов новой функции из database.py
        total_count, results = await database.get_all_categories_list(pool, limit, offset)
    except Exception as e:
        print(f"[API] Ошибка при выполнении запроса в get_categories: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Внутренняя ошибка сервера")

    return {"count": total_count, "results": results}

@app.get("/api/v1/sources/", summary="Получить источники новостей")
async def get_sources(
    limit: Optional[int] = Query(100, description="Количество источников на странице", le=1000, gt=0),
    offset: Optional[int] = Query(0, description="Смещение (количество пропущенных источников)", ge=0)
):
    """
    Получить список всех уникальных источников.
    Данные в формате: {"count": общее_количество, "results": [список_источников]}
    """
    pool = await database.get_db_pool()
    if pool is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Ошибка подключения к базе данных")

    try:
       # Вызов новой функции из database.py
        total_count, results = await database.get_all_sources_list(pool, limit, offset)
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

# --- Подключение роутеров ---
app.include_router(user_router)
app.include_router(user_news_router)
app.include_router(categories_router)
app.include_router(rss_router)
