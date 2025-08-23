# api/models.py
from pydantic import BaseModel
from typing import Optional, List

# Модель для представления одного перевода
# (Без изменений, если не требуется отображать источник/категорию для перевода отдельно)
class Translation(BaseModel):
    language: str
    title: str
    content: str

# Модель для представления новости в API
class NewsItem(BaseModel):
    news_id: str
    original_title: str
    original_content: str
    original_language: str
    category: Optional[str] = None
    source: Optional[str] = None # Имя источника новости
    title_ru: Optional[str] = None
    content_ru: Optional[str] = None
    title_en: Optional[str] = None
    content_en: Optional[str] = None
    title_de: Optional[str] = None
    content_de: Optional[str] = None
    title_fr: Optional[str] = None
    content_fr: Optional[str] = None
    source_url: Optional[str] = None
    published_at: Optional[str] = None # ISO формат даты-времени

    class Config:
        from_attributes = True # Для совместимости с ORM (если будете использовать)

class CategoryItem(BaseModel):
    id: int
    name: str

class LanguageItem(BaseModel):
    language: str

# Модель для ответа с ошибкой (опционально, но полезно)
class HTTPError(BaseModel):
    detail: str
