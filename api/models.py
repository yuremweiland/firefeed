from pydantic import BaseModel
from typing import Optional, List, Dict

# Модель для представления перевода на конкретный язык
class LanguageTranslation(BaseModel):
    title: Optional[str] = None
    content: Optional[str] = None

# Модель для представления новости в API
class NewsItem(BaseModel):
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
        from_attributes = True  # Для совместимости с ORM (если будете использовать)

class CategoryItem(BaseModel):
    id: int
    name: str

class LanguageItem(BaseModel):
    language: str

# Модель для ответа с ошибкой (опционально, но полезно)
class HTTPError(BaseModel):
    detail: str