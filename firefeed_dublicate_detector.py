import asyncio
import aiopg
import json
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
    # Классовый пул соединений для предотвращения множественного создания
    _db_pool = None
    _pool_lock = asyncio.Lock()
    
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
    
    async def _initialize_db_pool(self):
        """Инициализация пула соединений с базой данных (один раз для всех экземпляров)"""
        if FireFeedDuplicateDetector._db_pool is None:
            async with FireFeedDuplicateDetector._pool_lock:
                if FireFeedDuplicateDetector._db_pool is None:
                    try:
                        FireFeedDuplicateDetector._db_pool = await aiopg.create_pool(**DB_CONFIG)
                        logger.info("[DUBLICATE_DETECTOR] Пул соединений с БД успешно создан")
                    except Exception as e:
                        logger.error(f"[DUBLICATE_DETECTOR] Ошибка создания пула соединений: {e}")
                        raise
        return FireFeedDuplicateDetector._db_pool
    
    def _get_embedding_dimension(self) -> int:
        """Получение размерности эмбеддинга модели"""
        sample_text = "test"
        embedding = self.model.encode(sample_text)
        return len(embedding)
    
    def _combine_text_fields(self, title: str, content: str) -> str:
        """Комбинирование заголовка и содержания для создания эмбеддинга"""
        return f"{title} {content[:500]}"  # Ограничиваем длину для производительности

    async def _get_embedding_by_id(self, news_id: str) -> Optional[List[float]]:
        """Получение существующего эмбеддинга по ID новости"""
        try:
            pool = await self._initialize_db_pool()
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
        try:
            pool = await self._initialize_db_pool()
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
        try:
            pool = await self._initialize_db_pool()
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
                logger.info(f"[DUBLICATE_DETECTOR] Новость {title[:50]} является дубликатом новости {duplicate_info['news_id']}")
                return False
            
            # logger.info(f"[DUBLICATE_DETECTOR] Новость {news_id} уникальна")
            return True
            
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при обработке новости {news_id}: {e}")
            raise

    # --- Методы для пакетной обработки ---
    
    async def get_news_without_embeddings(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получает список новостей без эмбеддингов из базы данных (асинхронно).

        Args:
            limit: Максимальное количество новостей для получения.

        Returns:
            Список словарей с данными новостей (news_id, original_title, original_content).
        """
        try:
            # Используем существующий метод для получения пула
            pool = await self._initialize_db_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    
                    query = """
                        SELECT news_id, original_title, original_content
                        FROM published_news_data
                        WHERE embedding IS NULL
                        ORDER BY created_at ASC -- Обрабатываем самые старые записи первыми
                        LIMIT %s
                    """
                    await cur.execute(query, (limit,))
                    results = await cur.fetchall()
                    
                    # Получаем имена колонок
                    # cur.description доступен после execute
                    column_names = [desc[0] for desc in cur.description]
                    
                    # Преобразуем результаты в список словарей
                    news_list = [dict(zip(column_names, row)) for row in results]
                    
                    logger.info(f"[BATCH_EMBEDDING] Получено {len(news_list)} новостей без эмбеддингов.")
                    return news_list
                    
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Ошибка при получении новостей без эмбеддингов: {e}")
            raise

    async def process_single_news_batch(self, news_item: Dict[str, Any]) -> bool:
        """
        Асинхронно обрабатывает одну новость в рамках пакетной обработки:
        генерирует и сохраняет эмбеддинг.

        Args:
            news_item: Словарь с данными новости (news_id, original_title, original_content).

        Returns:
            True, если эмбеддинг успешно сохранен, False в случае ошибки.
        """
        news_id = news_item['news_id']
        title = news_item['original_title']
        content = news_item['original_content']
        
        try:
            logger.debug(f"[BATCH_EMBEDDING] Начало обработки новости {news_id}...")
            
            # 1. Генерируем эмбеддинг
            embedding = await self.generate_embedding(title, content)
            logger.debug(f"[BATCH_EMBEDDING] Эмбеддинг для {news_id} сгенерирован.")

            # 2. Сохраняем эмбеддинг
            await self.save_embedding(news_id, embedding)
            logger.info(f"[BATCH_EMBEDDING] Эмбеддинг для новости {news_id} успешно сохранен.")
            return True
                    
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Ошибка при обработке новости {news_id}: {e}", exc_info=True)
            return False

    async def process_missing_embeddings_batch(self, batch_size: int = 50, delay_between_items: float = 0.1) -> Tuple[int, int]:
        """
        Асинхронно обрабатывает одну партию новостей без эмбеддингов.

        Args:
            batch_size: Количество новостей для обработки за один "прогон".
            delay_between_items: Задержка (в секундах) между обработкой каждой новости
                                внутри партии для снижения нагрузки.

        Returns:
            Кортеж (успешно обработано, ошибок).
        """
        logger.info(f"[BATCH_EMBEDDING] Запуск пакетной обработки: размер партии {batch_size}.")

        # 1. Получаем список новостей без эмбеддингов (асинхронно)
        try:
            news_without_embeddings = await self.get_news_without_embeddings(limit=batch_size)
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Не удалось получить список новостей: {e}")
            return 0, 0 # Возвращаем 0, 0 в случае ошибки получения списка

        if not news_without_embeddings:
            logger.info("[BATCH_EMBEDDING] Новости без эмбеддингов не найдены.")
            return 0, 0

        logger.info(f"[BATCH_EMBEDDING] Найдено {len(news_without_embeddings)} новостей для обработки.")

        success_count = 0
        error_count = 0

        # 3. Обрабатываем каждую новость в партии
        for i, news_item in enumerate(news_without_embeddings):
            news_id = news_item['news_id']
            logger.debug(f"[BATCH_EMBEDDING] Обработка новости {i+1}/{len(news_without_embeddings)}: {news_id}")
            
            success = await self.process_single_news_batch(news_item)
            if success:
                success_count += 1
            else:
                error_count += 1

            # Добавляем небольшую задержку между обработками новостей в партии
            if delay_between_items > 0 and (i + 1) < len(news_without_embeddings):
                await asyncio.sleep(delay_between_items)

        logger.info(f"[BATCH_EMBEDDING] Партия обработана. Успешно: {success_count}, Ошибок: {error_count}")
        return success_count, error_count

    async def run_batch_processor_continuously(self, batch_size: int = 50, delay_between_batches: float = 60.0, delay_between_items: float = 0.1):
        """
        Запускает непрерывную пакетную обработку новостей без эмбеддингов по расписанию.

        Args:
            batch_size: Количество новостей для обработки за один "прогон".
            delay_between_batches: Задержка (в секундах) между обработкой партий.
            delay_between_items: Задержка (в секундах) между обработкой каждой новости внутри партии.
        """
        logger.info("[BATCH_EMBEDDING] Запуск непрерывной пакетной обработки...")
        while True:
            try:
                success, errors = await self.process_missing_embeddings_batch(
                    batch_size=batch_size,
                    delay_between_items=delay_between_items
                )
                # Даже если обработано 0 новостей, всё равно ждем перед следующей итерацией
                logger.debug(f"[BATCH_EMBEDDING] Ожидание {delay_between_batches} секунд до следующей партии...")
                await asyncio.sleep(delay_between_batches)
                
            except asyncio.CancelledError:
                logger.info("[BATCH_EMBEDDING] Непрерывная пакетная обработка отменена.")
                break # Выходим из цикла при отмене задачи
            except Exception as e:
                logger.error(f"[BATCH_EMBEDDING] Неожиданная ошибка в непрерывной обработке: {e}", exc_info=True)
                # Ждем перед повторной попыткой в случае ошибки
                logger.debug(f"[BATCH_EMBEDDING] Ожидание {delay_between_batches} секунд перед повторной попыткой...")
                await asyncio.sleep(delay_between_batches)

    async def run_batch_processor_once(self, batch_size: int = 100, delay_between_items: float = 0.1) -> Tuple[int, int]:
        """
        Запускает пакетную обработку один раз.

        Args:
            batch_size: Количество новостей для обработки.
            delay_between_items: Задержка (в секундах) между обработкой каждой новости.

        Returns:
            Кортеж (успешно обработано, ошибок).
        """
        logger.info("[BATCH_EMBEDDING] Запуск однократной пакетной обработки...")
        try:
            success, errors = await self.process_missing_embeddings_batch(
                batch_size=batch_size,
                delay_between_items=delay_between_items
            )
            logger.info(f"[BATCH_EMBEDDING] Однократная обработка завершена. Успешно: {success}, Ошибок: {errors}")
            return success, errors
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Ошибка в однократной обработке: {e}", exc_info=True)
            raise # Повторно выбрасываем исключение, чтобы вызывающая сторона могла его обработать

    @classmethod
    async def close_pool(cls):
        """Закрытие пула соединений при завершении работы (классовый метод)"""
        if cls._db_pool:
            cls._db_pool.close()
            await cls._db_pool.wait_closed()
            cls._db_pool = None
            logger.info("[DUBLICATE_DETECTOR] Пул соединений с БД закрыт")

    async def close(self):
        """Закрытие пула соединений при завершении работы (для совместимости)"""
        # Пул общий для всех экземпляров, поэтому не закрываем его в отдельном экземпляре
        # Вместо этого используйте классовый метод close_pool()
        pass