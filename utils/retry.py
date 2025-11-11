import logging
from functools import wraps
from typing import Callable, Any
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


def retry_operation(max_attempts: int = 5, backoff_multiplier: float = 1.0, max_backoff: float = 30.0) -> Callable:
    """
    Декоратор для повторных попыток выполнения асинхронных операций

    Args:
        max_attempts: Максимальное количество попыток
        backoff_multiplier: Множитель для exponential backoff
        max_backoff: Максимальная задержка между попытками

    Returns:
        Декорированная функция
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        @retry(
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=backoff_multiplier, min=2, max=max_backoff),
            reraise=True,
        )
        async def wrapper(*args, **kwargs) -> Any:
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.warning(f"[RETRY] Попытка выполнения {func.__name__} завершилась ошибкой: {e}")
                raise  # Передаем исключение дальше для tenacity

        return wrapper

    return decorator


# Готовые декораторы для типичных случаев
retry_db_operation = retry_operation(max_attempts=3, backoff_multiplier=0.5, max_backoff=10.0)
retry_api_call = retry_operation(max_attempts=5, backoff_multiplier=1.0, max_backoff=30.0)
retry_file_operation = retry_operation(max_attempts=3, backoff_multiplier=0.1, max_backoff=5.0)
