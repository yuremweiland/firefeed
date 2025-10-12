import re
import spacy
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import List, Optional, Dict, Any
import logging
from firefeed_utils import clean_html

logger = logging.getLogger(__name__)

class FireFeedEmbeddingsProcessor:
    def __init__(self, model_name: str = 'paraphrase-multilingual-mpnet-base-v2', device: str = 'cpu'):
        """
        Инициализация процессора эмбеддингов

        Args:
            model_name: Название модели sentence-transformers
            device: Устройство для модели (cpu/cuda)
        """
        self.model = SentenceTransformer(model_name, device=device)
        self.device = device
        self.embedding_dim = self._get_embedding_dimension()

        # Кэш для spacy моделей
        self.spacy_models = {}

    def _get_embedding_dimension(self) -> int:
        """Получение размерности эмбеддинга модели"""
        sample_text = "test"
        embedding = self.model.encode(sample_text)
        return len(embedding)

    def _get_spacy_model(self, lang_code: str) -> Optional[spacy.Language]:
        """Получает spacy модель для языка"""
        if lang_code in self.spacy_models:
            return self.spacy_models[lang_code]

        spacy_model_map = {
            'en': 'en_core_web_sm',
            'ru': 'ru_core_news_sm',
            'de': 'de_core_news_sm',
            'fr': 'fr_core_news_sm',
        }

        model_name = spacy_model_map.get(lang_code)
        if not model_name:
            logger.warning(f"[EMBEDDINGS] Языковая модель для '{lang_code}' не найдена, используем 'en_core_web_sm'")
            model_name = 'en_core_web_sm'

        try:
            nlp = spacy.load(model_name)
            self.spacy_models[lang_code] = nlp
            logger.info(f"[EMBEDDINGS] Загружена spacy модель для языка '{lang_code}': {model_name}")
            return nlp
        except OSError:
            logger.error(f"[EMBEDDINGS] Модель '{model_name}' не найдена. Установите её командой: python -m spacy download {model_name}")
            return None

    def normalize_text(self, text: str, lang_code: str = 'en') -> str:
        """
        Нормализация текста: удаление HTML, стоп-слов, лемматизация

        Args:
            text: Исходный текст
            lang_code: Код языка

        Returns:
            Нормализованный текст
        """
        # Удаление HTML
        text = clean_html(text)

        # Получение spacy модели
        nlp = self._get_spacy_model(lang_code)
        if nlp is None:
            # Если модель не загружена, просто очистка
            text = re.sub(r'\s+', ' ', text).strip()
            return text

        # Обработка через spacy
        doc = nlp(text)

        # Лемматизация и удаление стоп-слов
        tokens = []
        for token in doc:
            if not token.is_stop and not token.is_punct and not token.is_space:
                tokens.append(token.lemma_.lower())

        normalized = ' '.join(tokens)
        return normalized

    def generate_embedding(self, text: str, lang_code: str = 'en') -> List[float]:
        """
        Генерация эмбеддинга для текста

        Args:
            text: Текст для эмбеддинга
            lang_code: Код языка

        Returns:
            Эмбеддинг как список float
        """
        normalized_text = self.normalize_text(text, lang_code)
        embedding = self.model.encode(normalized_text, show_progress_bar=False)
        return embedding.tolist()

    def calculate_similarity(self, embedding1: List[float], embedding2: List[float]) -> float:
        """
        Расчет косинусного сходства между двумя эмбеддингами

        Args:
            embedding1: Первый эмбеддинг
            embedding2: Второй эмбеддинг

        Returns:
            Сходство (0-1)
        """
        emb1 = np.array(embedding1)
        emb2 = np.array(embedding2)
        similarity = cosine_similarity([emb1], [emb2])[0][0]
        return float(similarity)

    def get_dynamic_threshold(self, text_length: int, text_type: str = 'content') -> float:
        """
        Динамический порог схожести в зависимости от длины и типа текста

        Args:
            text_length: Длина текста (символы)
            text_type: Тип текста ('title' или 'content')

        Returns:
            Порог схожести
        """
        base_threshold = 0.9

        # Корректировка по типу
        if text_type == 'title':
            base_threshold = 0.85  # Мягче для заголовков
        elif text_type == 'content':
            base_threshold = 0.95  # Жестче для статей

        # Корректировка по длине
        if text_length < 50:  # Короткие тексты
            base_threshold -= 0.05
        elif text_length > 1000:  # Длинные тексты
            base_threshold += 0.02

        # Ограничения
        return max(0.7, min(0.98, base_threshold))

    def combine_texts(self, title: str, content: str, lang_code: str = 'en') -> str:
        """
        Комбинирование заголовка и содержания для эмбеддинга

        Args:
            title: Заголовок
            content: Содержание
            lang_code: Код языка

        Returns:
            Комбинированный текст
        """
        normalized_title = self.normalize_text(title, lang_code)
        normalized_content = self.normalize_text(content, lang_code)

        # Ограничиваем длину содержания
        content_preview = normalized_content[:500] if len(normalized_content) > 500 else normalized_content

        return f"{normalized_title} {content_preview}"