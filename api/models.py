from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Generic, TypeVar, Dict, Set
from datetime import datetime

# Определяем типовой параметр для Generic
T = TypeVar("T")


# Модель для представления перевода на конкретный язык
class LanguageTranslation(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None


# Модель для представления новости в API
class RSSItem(BaseModel):
    news_id: str
    original_title: str
    original_content: str
    original_language: str
    image_url: Optional[str] = None
    category: Optional[str] = None
    source: Optional[str] = None  # Имя источника новости
    source_url: Optional[str] = None
    published_at: Optional[str] = None  # ISO формат даты-времени
    translations: Optional[Dict[str, LanguageTranslation]] = None

    class Config:
        from_attributes = True


class CategoryItem(BaseModel):
    id: int
    name: str


class SourceItem(BaseModel):
    id: int
    name: str
    description: Optional[str] = None


class LanguageItem(BaseModel):
    language: str


class PaginatedResponse(BaseModel, Generic[T]):
    count: int
    results: List[T]


# Модель для ответа с ошибкой (опционально, но полезно)
class HTTPError(BaseModel):
    detail: str


# --- Модели для пользователей ---


class UserBase(BaseModel):
    email: EmailStr
    language: str = "en"


class UserCreate(UserBase):
    password: str = Field(..., min_length=8)


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserUpdate(BaseModel):
    email: Optional[EmailStr] = None
    language: Optional[str] = None


class UserResponse(UserBase):
    id: int
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class Token(BaseModel):
    access_token: str
    token_type: str
    expires_in: int


class TokenData(BaseModel):
    user_id: Optional[int] = None


class PasswordResetRequest(BaseModel):
    email: EmailStr


class PasswordResetConfirm(BaseModel):
    token: str
    new_password: str = Field(..., min_length=8)


# --- Модели для верификации пользователей ---


class EmailVerificationRequest(BaseModel):
    """Модель для запроса верификации email пользователя."""

    email: EmailStr
    code: str = Field(..., min_length=6, max_length=6, description="6-digit verification code")


class SuccessResponse(BaseModel):
    """Модель для ответа об успешной операции."""

    message: str


# --- Модели для пользовательских RSS-лент ---


class UserRSSFeedBase(BaseModel):
    url: str
    name: Optional[str] = None
    category_id: Optional[int] = None
    language: str = "en"


class UserRSSFeedCreate(UserRSSFeedBase):
    pass


class UserRSSFeedUpdate(BaseModel):
    name: Optional[str] = None
    category_id: Optional[int] = None
    is_active: Optional[bool] = None


class UserRSSFeedResponse(UserRSSFeedBase):
    id: int
    user_id: int
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserCategoriesUpdate(BaseModel):
    category_ids: Set[int]


class UserCategoriesResponse(BaseModel):
    category_ids: List[int]


# --- Модели для привязки Telegram ---


class TelegramLinkResponse(BaseModel):
    link_code: str
    instructions: str


class TelegramLinkStatusResponse(BaseModel):
    is_linked: bool
    telegram_id: Optional[int] = None
    linked_at: Optional[str] = None


# --- Модели для API-ключей ---


class UserApiKeyBase(BaseModel):
    limits: Dict[str, int] = Field(default_factory=lambda: {"requests_per_day": 1000, "requests_per_hour": 100})


class UserApiKeyCreate(UserApiKeyBase):
    pass


class UserApiKeyUpdate(BaseModel):
    is_active: Optional[bool] = None
    limits: Optional[Dict[str, int]] = None


class UserApiKeyResponse(UserApiKeyBase):
    id: int
    user_id: int
    is_active: bool
    created_at: datetime
    expires_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class UserApiKeyGenerateResponse(BaseModel):
    id: int
    user_id: int
    key: str
    limits: Dict[str, int]
    created_at: datetime
    expires_at: Optional[datetime] = None
