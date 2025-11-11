import re
import html
import logging

logger = logging.getLogger(__name__)


class TextProcessor:
    """Класс для обработки и валидации текста"""

    @staticmethod
    def clean(raw_html: str) -> str:
        """Удаляет все HTML-теги и преобразует HTML-сущности"""
        if not raw_html:
            return ""

        # Работаем с копией
        clean_text = str(raw_html)

        # Заменяем специальные кавычки от NLP моделей
        # Обрабатываем различные варианты: <<, < <, <  < и т.д.
        clean_text = re.sub(r"<\s*<", "«", clean_text)
        clean_text = re.sub(r">\s*>", "»", clean_text)

        # Удаляем HTML-теги
        clean_text = re.sub(r"<[^>]*>", "", clean_text)

        # Декодируем HTML-сущности
        try:
            clean_text = html.unescape(clean_text)
        except Exception:
            # Если html.unescape падает, оставляем как есть
            pass

        # Нормализуем пробелы
        clean_text = re.sub(r"\s+", " ", clean_text)

        return clean_text.strip()

    @staticmethod
    def normalize(text: str) -> str:
        """Нормализует пробелы в тексте"""
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    @staticmethod
    def validate_length(text: str, min_length: int = 1, max_length: int = 10000) -> bool:
        """Проверяет длину текста"""
        if not text:
            return min_length == 0
        return min_length <= len(text) <= max_length

    @staticmethod
    def remove_duplicates(text: str) -> str:
        """Удаляет последовательные дубликаты слов"""
        words = text.split()
        if not words:
            return text

        deduped_words = [words[0]]
        for word in words[1:]:
            # Проверяем не только точное совпадение, но и частичное
            if word.lower() != deduped_words[-1].lower() and not word.lower().startswith(deduped_words[-1].lower()[:3]):
                deduped_words.append(word)

        return " ".join(deduped_words)

    @staticmethod
    def extract_sentences(text: str, lang_code: str = "en") -> list:
        """Разбивает текст на предложения (упрощенная версия без spaCy)"""
        # Простая эвристика для разбиения на предложения
        sentences = re.split(r"[.!?]+", text)
        return [s.strip() for s in sentences if s.strip()]

    @staticmethod
    def is_gibberish(text: str, threshold: float = 0.7) -> bool:
        """Проверяет, является ли текст бессмысленным набором символов"""
        if not text or len(text) < 10:
            return False

        # Проверяем соотношение букв к общему количеству символов
        alphanumeric_chars = len(re.findall(r"[a-zA-Zа-яА-Я0-9]", text))
        total_chars = len(text)

        if total_chars == 0:
            return True

        ratio = alphanumeric_chars / total_chars
        return ratio < threshold
