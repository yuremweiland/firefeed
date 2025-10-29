import asyncio
import json
import numpy as np
from typing import List, Tuple, Optional, Dict, Any
import logging
from config import RSS_ITEM_SIMILARITY_THRESHOLD
from utils.database import DatabaseMixin
from firefeed_embeddings_processor import FireFeedEmbeddingsProcessor

logger = logging.getLogger(__name__)


class FireFeedDuplicateDetector(DatabaseMixin):
    def __init__(
        self,
        model_name: str = "paraphrase-multilingual-MiniLM-L12-v2",
        device: str = "cpu",
        similarity_threshold: float = RSS_ITEM_SIMILARITY_THRESHOLD,
    ):
        """
        Инициализация асинхронного детектора дубликатов новостей

        Args:
            model_name: Название модели sentence-transformers
            device: Устройство для модели
            similarity_threshold: Базовый порог схожести
        """
        self.processor = FireFeedEmbeddingsProcessor(model_name, device)
        self.similarity_threshold = similarity_threshold

    def _combine_text_fields(self, title: str, content: str, lang_code: str = "en") -> str:
        """Комбинирование заголовка и содержания для создания эмбеддинга"""
        return self.processor.combine_texts(title, content, lang_code)

    async def _get_embedding_by_id(self, rss_item_id: str) -> Optional[List[float]]:
        """Получение существующего эмбеддинга по ID RSS-элемента"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT embedding
                    FROM published_news_data
                    WHERE news_id = %s AND embedding IS NOT NULL
                """,
                    (rss_item_id,),
                )

                result = await cur.fetchone()
                if result and result[0] is not None:
                    # Преобразуем из строки в список, если нужно
                    if isinstance(result[0], str):
                        return json.loads(result[0])
                    return result[0]
                return None

    async def _is_duplicate_with_embedding(
        self, rss_item_id: str, embedding: List[float], text_length: int = 0, text_type: str = "content"
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Проверка дубликата с уже имеющимся эмбеддингом"""
        try:
            # Получаем пул один раз и передаем его в get_similar_news
            pool = await self.get_pool()
            # Ищем похожие RSS-элементы, исключая текущий
            similar_rss_items = await self.get_similar_news(embedding, current_news_id=rss_item_id, limit=5, pool=pool)

            # Динамический threshold
            threshold = self.processor.get_dynamic_threshold(text_length, text_type)

            # Проверяем схожесть
            for news in similar_rss_items:
                if news["embedding"] is not None:
                    # Преобразуем эмбеддинг
                    try:
                        if isinstance(news["embedding"], str):
                            stored_embedding = json.loads(news["embedding"])
                        elif isinstance(news["embedding"], (list, np.ndarray)):
                            stored_embedding = (
                                list(news["embedding"])
                                if isinstance(news["embedding"], np.ndarray)
                                else news["embedding"]
                            )
                        else:
                            continue
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"[DUBLICATE_DETECTOR] Ошибка преобразования эмбеддинга из БД: {e}")
                        continue

                    similarity = self.processor.calculate_similarity(stored_embedding, embedding)

                    if similarity > threshold:
                        logger.info(
                            f"[DUBLICATE_DETECTOR] Найден дубликат с схожестью {similarity:.4f} (threshold: {threshold:.4f})"
                        )
                        return True, news

            return False, None

        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при проверке дубликата с эмбеддингом: {e}")
            raise

    async def generate_embedding(self, title: str, content: str, lang_code: str = "en") -> List[float]:
        """
        Генерация эмбеддинга для RSS-элемента

        Args:
            title: Заголовок RSS-элемента
            content: Содержание RSS-элемента
            lang_code: Код языка

        Returns:
            Эмбеддинг RSS-элемента в виде списка float
        """
        combined_text = self._combine_text_fields(title, content, lang_code)
        return self.processor.generate_embedding(combined_text, lang_code)

    async def save_embedding(self, rss_item_id: str, embedding: List[float]):
        """
        Сохранение эмбеддинга в базу данных

        Args:
            rss_item_id: ID RSS-элемента
            embedding: Эмбеддинг RSS-элемента
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    UPDATE published_news_data
                    SET embedding = %s
                    WHERE news_id = %s
                """,
                    (embedding, rss_item_id),
                )
                # Убираем await conn.commit() - в aiopg транзакции управляются автоматически
                logger.debug(f"Эмбеддинг для RSS-элемента {rss_item_id} успешно сохранен")

    async def get_similar_rss_items(
        self, embedding: List[float], current_rss_item_id: str = None, limit: int = 10, pool=None
    ) -> List[Dict[str, Any]]:
        """
        Поиск похожих RSS-элементов в базе данных

        Args:
            embedding: Эмбеддинг для поиска
            current_rss_item_id: ID текущего RSS-элемента (чтобы исключить его из результатов)
            limit: Максимальное количество результатов
            pool: Пул подключений (опционально, для повторного использования)

        Returns:
            Список похожих RSS-элементов
        """
        try:
            # Используем переданный пул или получаем новый
            if pool is None:
                pool = await self.get_pool()

            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    if current_rss_item_id:
                        # Исключаем текущий RSS-элемент из поиска
                        await cur.execute(
                            """
                            SELECT news_id, original_title, original_content, embedding
                            FROM published_news_data
                            WHERE embedding IS NOT NULL
                            AND news_id != %s
                            ORDER BY embedding <-> %s::vector
                            LIMIT %s
                        """,
                            (current_rss_item_id, embedding, limit),
                        )
                    else:
                        # Если ID не предоставлен, ищем среди всех RSS-элементов
                        await cur.execute(
                            """
                            SELECT news_id, original_title, original_content, embedding
                            FROM published_news_data
                            WHERE embedding IS NOT NULL
                            ORDER BY embedding <-> %s::vector
                            LIMIT %s
                        """,
                            (embedding, limit),
                        )

                    results = await cur.fetchall()
                    return [dict(zip([column[0] for column in cur.description], row)) for row in results]
        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при поиске похожих RSS-элементов: {e}")
            raise

    async def is_duplicate(
        self, rss_item_id: str, title: str, content: str, lang_code: str = "en"
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Проверка, является ли RSS-элемент дубликатом

        Args:
            rss_item_id: ID RSS-элемента (для исключения из поиска)
            title: Заголовок RSS-элемента
            content: Содержание RSS-элемента
            lang_code: Код языка

        Returns:
            Кортеж: (является_дубликатом, информация_о_дубликате)
        """
        try:
            # Генерируем эмбеддинг для нового RSS-элемента
            embedding = await self.generate_embedding(title, content, lang_code)

            # Ищем похожие RSS-элементы, исключая текущий
            similar_rss_items = await self.get_similar_news(embedding, current_news_id=rss_item_id, limit=5)

            # Длина текста для динамического threshold
            text_length = len(title) + len(content)
            threshold = self.processor.get_dynamic_threshold(text_length, "content")

            # Проверяем схожесть
            for news in similar_rss_items:
                if news["embedding"] is not None:
                    # Преобразуем эмбеддинг
                    try:
                        if isinstance(news["embedding"], str):
                            stored_embedding = json.loads(news["embedding"])
                        elif isinstance(news["embedding"], (list, np.ndarray)):
                            stored_embedding = (
                                list(news["embedding"])
                                if isinstance(news["embedding"], np.ndarray)
                                else news["embedding"]
                            )
                        else:
                            logger.warning(
                                f"[DUBLICATE_DETECTOR] Неизвестный тип данных для эмбеддинга: {type(news['embedding'])}"
                            )
                            continue
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.error(f"[DUBLICATE_DETECTOR] Ошибка преобразования эмбеддинга из БД: {e}")
                        continue

                    similarity = self.processor.calculate_similarity(stored_embedding, embedding)

                    if similarity > threshold:
                        logger.info(
                            f"[DUBLICATE_DETECTOR] Найден дубликат с схожестью {similarity:.4f} (threshold: {threshold:.4f})"
                        )
                        return True, news

            return False, None

        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при проверке дубликата: {e}")
            raise

    async def is_duplicate_strict(
        self, title: str, content: str, link: str, lang_code: str = "en"
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Строгая проверка на дубликаты с учетом ссылки"""

        # Сначала проверяем по эмбеддингам
        is_dup, dup_info = await self.is_duplicate("temp", title, content, lang_code)
        if is_dup:
            return True, dup_info

        # Дополнительно проверяем по ссылке (если ссылка совпадает - точно дубликат)
        try:
            pool = await self.get_pool()
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        """
                        SELECT news_id, original_title
                        FROM published_news_data
                        WHERE source_url = %s AND source_url IS NOT NULL
                        LIMIT 1
                    """,
                        (link,),
                    )

                    result = await cur.fetchone()
                    if result:
                        return True, {"news_id": result[0], "title": result[1], "reason": "same_url"}

        except Exception as e:
            logger.error(f"Ошибка при проверке по URL: {e}")

        return False, None

    async def process_rss_item(self, rss_item_id: str, title: str, content: str, lang_code: str = "en") -> bool:
        """
        Полная обработка RSS-элемента: проверка дубликата и сохранение эмбеддинга

        Args:
            rss_item_id: ID RSS-элемента
            title: Заголовок RSS-элемента
            content: Содержание RSS-элемента
            lang_code: Код языка

        Returns:
            True если RSS-элемент уникален, False если дубликат
        """
        try:
            # Сначала проверяем, есть ли уже эмбеддинг для этого RSS-элемента
            existing_embedding = await self._get_embedding_by_id(rss_item_id)

            text_length = len(title) + len(content)

            # Если эмбеддинг уже существует, используем его для проверки дубликатов
            if existing_embedding is not None:
                logger.debug(f"[DUBLICATE_DETECTOR] Эмбеддинг для RSS-элемента {rss_item_id} уже существует")
                # Проверяем на дубликат, используя существующий эмбеддинг
                is_dup, duplicate_info = await self._is_duplicate_with_embedding(
                    rss_item_id, existing_embedding, text_length, "content"
                )
            else:
                # Если эмбеддинга нет, генерируем новый
                logger.debug(f"[DUBLICATE_DETECTOR] Генерируем новый эмбеддинг для RSS-элемента {rss_item_id}")
                embedding = await self.generate_embedding(title, content, lang_code)

                # Проверяем на дубликат с новым эмбеддингом
                is_dup, duplicate_info = await self._is_duplicate_with_embedding(
                    rss_item_id, embedding, text_length, "content"
                )

                # Если не дубликат, сохраняем эмбеддинг
                if not is_dup:
                    await self.save_embedding(rss_item_id, embedding)

            if is_dup:
                logger.info(
                    f"[DUBLICATE_DETECTOR] RSS-элемент {title[:50]} является дубликатом RSS-элемента {duplicate_info['news_id']}"
                )
                return False

            # logger.info(f"[DUBLICATE_DETECTOR] RSS-элемент {rss_item_id} уникален")
            return True

        except Exception as e:
            logger.error(f"[DUBLICATE_DETECTOR] Ошибка при обработке RSS-элемента {rss_item_id}: {e}")
            raise

    # --- Методы для пакетной обработки ---

    async def get_rss_items_without_embeddings(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Получает список RSS-элементов без эмбеддингов из базы данных (асинхронно).

        Args:
            limit: Максимальное количество RSS-элементов для получения.

        Returns:
            Список словарей с данными RSS-элементов (news_id, original_title, original_content).
        """
        pool = await self.get_pool()
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
                rss_items_list = [dict(zip(column_names, row)) for row in results]

                logger.info(f"[BATCH_EMBEDDING] Получено {len(rss_items_list)} RSS-элементов без эмбеддингов.")
                return rss_items_list

    async def process_single_rss_item_batch(self, rss_item: Dict[str, Any], lang_code: str = "en") -> bool:
        """
        Асинхронно обрабатывает один RSS-элемент в рамках пакетной обработки:
        генерирует и сохраняет эмбеддинг.

        Args:
            rss_item: Словарь с данными RSS-элемента (news_id, original_title, original_content).
            lang_code: Код языка

        Returns:
            True, если эмбеддинг успешно сохранен, False в случае ошибки.
        """
        rss_item_id = rss_item["news_id"]
        title = rss_item["original_title"]
        content = rss_item["original_content"]

        try:
            logger.debug(f"[BATCH_EMBEDDING] Начало обработки RSS-элемента {rss_item_id}...")

            # 1. Генерируем эмбеддинг
            embedding = await self.generate_embedding(title, content, lang_code)
            logger.debug(f"[BATCH_EMBEDDING] Эмбеддинг для {rss_item_id} сгенерирован.")

            # 2. Сохраняем эмбеддинг
            await self.save_embedding(rss_item_id, embedding)
            logger.info(f"[BATCH_EMBEDDING] Эмбеддинг для RSS-элемента {rss_item_id} успешно сохранен.")
            return True

        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Ошибка при обработке RSS-элемента {rss_item_id}: {e}", exc_info=True)
            return False

    async def process_missing_embeddings_batch(
        self, batch_size: int = 50, delay_between_items: float = 0.1
    ) -> Tuple[int, int]:
        """
        Асинхронно обрабатывает одну партию RSS-элементов без эмбеддингов.

        Args:
            batch_size: Количество RSS-элементов для обработки за один "прогон".
            delay_between_items: Задержка (в секундах) между обработкой каждого RSS-элемента
                                внутри партии для снижения нагрузки.

        Returns:
            Кортеж (успешно обработано, ошибок).
        """
        logger.info(f"[BATCH_EMBEDDING] Запуск пакетной обработки: размер партии {batch_size}.")

        # 1. Получаем список RSS-элементов без эмбеддингов (асинхронно)
        try:
            rss_items_without_embeddings = await self.get_rss_items_without_embeddings(limit=batch_size)
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Не удалось получить список RSS-элементов: {e}")
            return 0, 0  # Возвращаем 0, 0 в случае ошибки получения списка

        if not rss_items_without_embeddings:
            logger.info("[BATCH_EMBEDDING] RSS-элементы без эмбеддингов не найдены.")
            return 0, 0

        logger.info(f"[BATCH_EMBEDDING] Найдено {len(rss_items_without_embeddings)} RSS-элементов для обработки.")

        success_count = 0
        error_count = 0

        # 3. Обрабатываем каждый RSS-элемент в партии
        for i, rss_item in enumerate(rss_items_without_embeddings):
            rss_item_id = rss_item["news_id"]
            logger.debug(f"[BATCH_EMBEDDING] Обработка RSS-элемента {i+1}/{len(rss_items_without_embeddings)}: {rss_item_id}")

            success = await self.process_single_rss_item_batch(rss_item)
            if success:
                success_count += 1
            else:
                error_count += 1

            # Добавляем небольшую задержку между обработками RSS-элементов в партии
            if delay_between_items > 0 and (i + 1) < len(rss_items_without_embeddings):
                await asyncio.sleep(delay_between_items)

        logger.info(f"[BATCH_EMBEDDING] Партия обработана. Успешно: {success_count}, Ошибок: {error_count}")
        return success_count, error_count

    async def run_batch_processor_continuously(
        self, batch_size: int = 50, delay_between_batches: float = 60.0, delay_between_items: float = 0.1
    ):
        """
        Запускает непрерывную пакетную обработку RSS-элементов без эмбеддингов по расписанию.

        Args:
            batch_size: Количество RSS-элементов для обработки за один "прогон".
            delay_between_batches: Задержка (в секундах) между обработкой партий.
            delay_between_items: Задержка (в секундах) между обработкой каждого RSS-элемента внутри партии.
        """
        logger.info("[BATCH_EMBEDDING] Запуск непрерывной пакетной обработки...")
        while True:
            try:
                success, errors = await self.process_missing_embeddings_batch(
                    batch_size=batch_size, delay_between_items=delay_between_items
                )
                # Даже если обработано 0 новостей, всё равно ждем перед следующей итерацией
                logger.debug(f"[BATCH_EMBEDDING] Ожидание {delay_between_batches} секунд до следующей партии...")
                await asyncio.sleep(delay_between_batches)

            except asyncio.CancelledError:
                logger.info("[BATCH_EMBEDDING] Непрерывная пакетная обработка отменена.")
                break  # Выходим из цикла при отмене задачи
            except Exception as e:
                logger.error(f"[BATCH_EMBEDDING] Неожиданная ошибка в непрерывной обработке: {e}", exc_info=True)
                # Ждем перед повторной попыткой в случае ошибки
                logger.debug(f"[BATCH_EMBEDDING] Ожидание {delay_between_batches} секунд перед повторной попыткой...")
                await asyncio.sleep(delay_between_batches)

    async def run_batch_processor_once(
        self, batch_size: int = 100, delay_between_items: float = 0.1
    ) -> Tuple[int, int]:
        """
        Запускает пакетную обработку один раз.

        Args:
            batch_size: Количество RSS-элементов для обработки.
            delay_between_items: Задержка (в секундах) между обработкой каждого RSS-элемента.

        Returns:
            Кортеж (успешно обработано, ошибок).
        """
        logger.info("[BATCH_EMBEDDING] Запуск однократной пакетной обработки...")
        try:
            success, errors = await self.process_missing_embeddings_batch(
                batch_size=batch_size, delay_between_items=delay_between_items
            )
            logger.info(f"[BATCH_EMBEDDING] Однократная обработка завершена. Успешно: {success}, Ошибок: {errors}")
            return success, errors
        except Exception as e:
            logger.error(f"[BATCH_EMBEDDING] Ошибка в однократной обработке: {e}", exc_info=True)
            raise  # Повторно выбрасываем исключение, чтобы вызывающая сторона могла его обработать

    @classmethod
    async def close_pool(cls):
        """Заглушка - пул закрывается глобально"""
        pass

    async def close(self):
        """Заглушка - пул закрывается глобально"""
        pass
