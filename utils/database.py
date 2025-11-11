import logging
from typing import Any, Callable
from functools import wraps
from config import get_shared_db_pool

logger = logging.getLogger(__name__)


class DatabaseMixin:
    """Базовый класс для работы с базой данных"""

    async def get_pool(self):
        """Получает общий пул подключений из config.py"""
        return await get_shared_db_pool()

    async def close_pool(self):
        """Заглушка - пул закрывается глобально"""
        pass


def db_operation(func: Callable) -> Callable:
    """
    Декоратор для операций с базой данных.
    Автоматически получает пул, обрабатывает ошибки и логирует.
    """

    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            pool = await self.get_pool()
            if pool is None:
                logger.error("[DB] Не удалось получить пул подключений")
                return None

            # Вызываем оригинальную функцию с пулом
            return await func(self, pool, *args, **kwargs)

        except Exception as e:
            logger.error(f"[DB] Ошибка в {func.__name__}: {e}")
            return None

    return wrapper
