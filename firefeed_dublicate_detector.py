import asyncio
import aiopg
import psycopg2
import json
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np
from typing import List, Tuple, Optional, Dict, Any
import logging
from config import DB_CONFIG, NEWS_SIMILARITY_THRESHOLD

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FireFeedDuplicateDetector:
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2', similarity_threshold: float = NEWS_SIMILARITY_THRESHOLD):
        """
        Инициализация асинхронного детектора дубликатов новостей
        
        Args:
            model_name: Название модели sentence-transformers
            similarity_threshold: Порог схожести для определения дубликатов
        """
        self.model = SentenceTransformer(model_name)
        self.similarity_threshold = similarity_threshold
        self.embedding_dim = self._get_embedding_dimension()
    
    def _get_embedding_dimension(self) -> int:
        """Получение размерности эмбеддинга модели"""
        sample_text = "test"
        embedding = self.model.encode(sample_text)
        return len(embedding)
    
    async def _get_db_pool(self):
        """Создание пула соединений с базой данных"""
        try:
            pool = await aiopg.create_pool(**DB_CONFIG)
            return pool
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка создания пула соединений: {e}")
            raise
    
    def _combine_text_fields(self, title: str, content: str) -> str:
        """Комбинирование заголовка и содержания для создания эмбеддинга"""
        return f"{title} {content[:500]}"  # Ограничиваем длину для производительности

    async def _get_embedding_by_id(self, news_id: str) -> Optional[List[float]]:
        """Получение существующего эмбеддинга по ID новости"""
        pool = None
        try:
            pool = await self._get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        SELECT embedding 
                        FROM published_news_data 
                        WHERE news_id = %s AND embedding IS NOT NULL
                    """, (news_id,))
                    
                    result = await cur.fetchone()
                    if result and result[0] is not None:
                        # Преобразуем из строки в список, если нужно
                        if isinstance(result[0], str):
                            return json.loads(result[0])
                        return result[0]
                    return None
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при получении эмбеддинга для новости {news_id}: {e}")
            return None
        finally:
            if pool:
                pool.close()
                await pool.wait_closed()

    async def _is_duplicate_with_embedding(self, news_id: str, embedding: List[float]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Проверка дубликата с уже имеющимся эмбеддингом"""
        try:
            # Ищем похожие новости, исключая текущую
            similar_news = await self.get_similar_news(embedding, current_news_id=news_id, limit=5)

            # Проверяем схожесть
            for news in similar_news:
                if news['embedding'] is not None:
                    # Преобразуем строковое представление эмбеддинга
                    try:
                        if isinstance(news['embedding'], str):
                            stored_embedding_array = np.array(json.loads(news['embedding']))
                        elif isinstance(news['embedding'], (list, np.ndarray)):
                            stored_embedding_array = np.array(news['embedding'], dtype=float)
                        else:
                            continue
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"[DUBLICATE_DETECTOR] Ошибка преобразования эмбеддинга из БД: {e}")
                        continue

                    new_embedding_array = np.array(embedding)
                    
                    similarity = cosine_similarity([stored_embedding_array], [new_embedding_array])[0][0]
                    
                    if similarity > self.similarity_threshold:
                        logger.info(f"[DUBLICATE_DETECTOR] Найден дубликат с схожестью {similarity:.4f}")
                        return True, news

            return False, None
            
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при проверке дубликата с эмбеддингом: {e}")
            raise
    
    async def generate_embedding(self, title: str, content: str) -> List[float]:
        """
        Генерация эмбеддинга для новости
        
        Args:
            title: Заголовок новости
            content: Содержание новости
            
        Returns:
            Эмбеддинг новости в виде списка float
        """
        combined_text = self._combine_text_fields(title, content)
        embedding = self.model.encode(combined_text, show_progress_bar=False)
        return embedding.tolist()
    
    async def save_embedding(self, news_id: str, embedding: List[float]):
        """
        Сохранение эмбеддинга в базу данных
        
        Args:
            news_id: ID новости
            embedding: Эмбеддинг новости
        """
        pool = None
        try:
            pool = await self._get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("""
                        UPDATE published_news_data 
                        SET embedding = %s 
                        WHERE news_id = %s
                    """, (embedding, news_id))
                    # Убираем await conn.commit() - в aiopg транзакции управляются автоматически
                    logger.debug(f"Эмбеддинг для новости {news_id} успешно сохранен")
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при сохранении эмбеддинга для новости {news_id}: {e}")
            raise
        finally:
            if pool:
                pool.close()
                await pool.wait_closed()
    
    async def get_similar_news(self, embedding: List[float], current_news_id: str = None, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Поиск похожих новостей в базе данных
        
        Args:
            embedding: Эмбеддинг для поиска
            current_news_id: ID текущей новости (чтобы исключить её из результатов)
            limit: Максимальное количество результатов
            
        Returns:
            Список похожих новостей
        """
        pool = None
        try:
            pool = await self._get_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if current_news_id:
                        # Исключаем текущую новость из поиска
                        await cur.execute("""
                            SELECT news_id, original_title, original_content, embedding
                            FROM published_news_data 
                            WHERE embedding IS NOT NULL 
                            AND news_id != %s
                            ORDER BY embedding <-> %s::vector 
                            LIMIT %s
                        """, (current_news_id, embedding, limit))
                    else:
                        # Если ID не предоставлен, ищем среди всех новостей
                        await cur.execute("""
                            SELECT news_id, original_title, original_content, embedding
                            FROM published_news_data 
                            WHERE embedding IS NOT NULL
                            ORDER BY embedding <-> %s::vector 
                            LIMIT %s
                        """, (embedding, limit))
                    
                    results = await cur.fetchall()
                    return [dict(zip([column[0] for column in cur.description], row)) for row in results]
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при поиске похожих новостей: {e}")
            raise
        finally:
            if pool:
                pool.close()
                await pool.wait_closed()
    
    async def is_duplicate(self, news_id: str, title: str, content: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Проверка, является ли новость дубликатом

        Args:
            news_id: ID новости (для исключения из поиска)
            title: Заголовок новости
            content: Содержание новости

        Returns:
            Кортеж: (является_дубликатом, информация_о_дубликате)
        """
        pool = None
        try:
            # Генерируем эмбеддинг для новой новости
            embedding = await self.generate_embedding(title, content)

            # Ищем похожие новости, исключая текущую
            similar_news = await self.get_similar_news(embedding, current_news_id=news_id, limit=5)

            # Проверяем схожесть
            for news in similar_news:
                if news['embedding'] is not None:
                    # Преобразуем строковое представление эмбеддинга обратно в список float
                    try:
                        if isinstance(news['embedding'], str):
                            stored_embedding_array = np.array(json.loads(news['embedding']))
                        elif isinstance(news['embedding'], (list, np.ndarray)):
                            # На случай, если данные уже в правильном формате (например, при тестировании)
                            stored_embedding_array = np.array(news['embedding'], dtype=float)
                        else:
                            # Если формат неизвестен, пропускаем эту новость
                            logger.warning(f"[DUBLICATE_DETECTOR] Неизвестный тип данных для эмбеддинга: {type(news['embedding'])}")
                            continue
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"[DUBLICATE_DETECTOR] Ошибка преобразования эмбеддинга из БД: {e}")
                        continue

                    # Генерируем эмбеддинг для текущей новости (новый)
                    new_embedding_array = np.array(embedding)

                    # Вычисляем косинусное сходство
                    similarity = cosine_similarity([stored_embedding_array], [new_embedding_array])[0][0]

                    if similarity > self.similarity_threshold:
                        logger.info(f"[DUBLICATE_DETECTOR] Найден дубликат с схожестью {similarity:.4f}")
                        return True, news

            return False, None

        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при проверке дубликата: {e}")
            raise
        finally:
            if pool: # Переменная pool не используется в этом методе напрямую, 
                    # но если бы использовалась, здесь было бы её закрытие
                # pool.close()
                # await pool.wait_closed()
                pass
    
    async def process_news(self, news_id: str, title: str, content: str) -> bool:
        """
        Полная обработка новости: проверка дубликата и сохранение эмбеддинга
        
        Args:
            news_id: ID новости
            title: Заголовок новости
            content: Содержание новости
            
        Returns:
            True если новость уникальна, False если дубликат
        """
        pool = None
        try:
            # Сначала проверяем, есть ли уже эмбеддинг для этой новости
            existing_embedding = await self._get_embedding_by_id(news_id)
            
            # Если эмбеддинг уже существует, используем его для проверки дубликатов
            if existing_embedding is not None:
                logger.debug(f"[DUBLICATE_DETECTOR] Эмбеддинг для новости {news_id} уже существует")
                # Проверяем на дубликат, используя существующий эмбеддинг
                is_dup, duplicate_info = await self._is_duplicate_with_embedding(
                    news_id, existing_embedding
                )
            else:
                # Если эмбеддинга нет, генерируем новый
                logger.debug(f"[DUBLICATE_DETECTOR] Генерируем новый эмбеддинг для новости {news_id}")
                embedding = await self.generate_embedding(title, content)
                
                # Проверяем на дубликат с новым эмбеддингом
                is_dup, duplicate_info = await self._is_duplicate_with_embedding(
                    news_id, embedding
                )
                
                # Если не дубликат, сохраняем эмбеддинг
                if not is_dup:
                    await self.save_embedding(news_id, embedding)
            
            if is_dup:
                logger.info(f"[DUBLICATE_DETECTOR] Новость {news_id} является дубликатом новости {duplicate_info['news_id']}")
                return False
            
            logger.info(f"[DUBLICATE_DETECTOR] Новость {news_id} уникальна")
            return True
            
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при обработке новости {news_id}: {e}")
            raise
        finally:
            if pool:
                pool.close()
                await pool.wait_closed()