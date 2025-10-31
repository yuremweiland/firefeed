import asyncio
import signal
import sys
import time
import logging
from logging_config import setup_logging
from rss_manager import RSSManager
from firefeed_dublicate_detector import FireFeedDuplicateDetector
from firefeed_translator import FireFeedTranslator
from firefeed_translator_task_queue import FireFeedTranslatorTaskQueue
from config import close_shared_db_pool

setup_logging()
logger = logging.getLogger(__name__)


class RSSParserService:
    def __init__(self):
        self.duplicate_detector = FireFeedDuplicateDetector()
        # --- Инициализация переводчика ---
        self.translator = FireFeedTranslator(
            device="cpu", max_workers=3, max_concurrent_translations=2, max_cached_models=10
        )
        self.translator_queue = FireFeedTranslatorTaskQueue(self.translator, max_workers=2, queue_size=30)

        self.rss_manager = RSSManager(translator_queue=self.translator_queue)
        self.running = True
        self.parse_task = None
        self.batch_processor_task = None
        self.cleanup_task = None

    async def parse_rss_task(self):
        """Периодическая задача парсинга RSS"""
        while self.running:
            try:
                logger.info("[RSS_PARSER] Начало парсинга RSS лент...")
                await self.rss_manager.fetch_rss_items()
                logger.info("[RSS_PARSER] Парсинг RSS лент завершен")

                # Ждем 15 минут перед следующим парсингом или пока не будет установлен флаг self.running = False
                for _ in range(900):
                    if not self.running:
                        logger.info("[RSS_PARSER] [PARSE_TASK] Получен сигнал остановки, завершение задачи парсинга.")
                        return
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("[RSS_PARSER] [PARSE_TASK] Задача парсинга отменена")
                break
            except Exception as e:
                logger.error(f"[RSS_PARSER] [PARSE_TASK] Ошибка при парсинге: {e}")
                import traceback

                traceback.print_exc()
                # Уменьшаем время ожидания перед повторной попыткой или проверкой флага остановки
                for _ in range(30):  # 30 секунд
                    if not self.running:
                        logger.info(
                            "[RSS_PARSER] [PARSE_TASK] Получен сигнал остановки во время ожидания, завершение задачи парсинга."
                        )
                        return
                    await asyncio.sleep(1)

    async def batch_processor_job(self):
        """Задача регулярной пакетной обработки"""
        try:
            logger.info("[BATCH] Запуск регулярной пакетной обработки новостей без эмбеддингов...")
            success, errors = await self.duplicate_detector.process_missing_embeddings_batch(
                batch_size=20, delay_between_items=0.2
            )
            logger.info(f"[BATCH] Регулярная пакетная обработки завершена. Успешно: {success}, Ошибок: {errors}")
        except Exception as e:
            logger.error(f"[ERROR] [BATCH] Ошибка в регулярной пакетной обработке: {e}")
            import traceback

            traceback.print_exc()

    async def cleanup_duplicates_task(self):
        """Фоновая задача очистки дубликатов (запускается каждый час)"""
        while self.running:
            try:
                logger.info("[CLEANUP] Запуск периодической очистки дубликатов...")
                await self.rss_manager.cleanup_duplicates()
                logger.info("[CLEANUP] Периодическая очистка дубликатов завершена")

                # Ждем 1 час перед следующей очисткой или пока не будет остановка
                for _ in range(3600):
                    if not self.running:
                        logger.info(
                            "[RSS_PARSER] [CLEANUP_TASK] Получен сигнал остановки, завершение задачи очистки дубликатов."
                        )
                        return
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("[CLEANUP] [CLEANUP_TASK] Задача очистки дубликатов отменена")
                break
            except Exception as e:
                logger.error(f"[CLEANUP] [CLEANUP_TASK] Ошибка в фоновой задаче очистки дубликатов: {e}")
                import traceback

                traceback.print_exc()
                # Ждем 5 минут перед повторной попыткой или проверкой флага остановки
                for _ in range(300):
                    if not self.running:
                        logger.info(
                            "[RSS_PARSER] [CLEANUP_TASK] Получен сигнал остановки во время ожидания, завершение задачи очистки дубликатов."
                        )
                        return
                    await asyncio.sleep(1)

    async def batch_processor_task_loop(self):
        """Фоновая задача пакетной обработки"""
        while self.running:
            try:
                await self.batch_processor_job()
                # Ждем 30 минут перед следующей пакетной обработкой или пока не будет остановка
                for _ in range(1800):
                    if not self.running:
                        logger.info(
                            "[RSS_PARSER] [BATCH_TASK] Получен сигнал остановки, завершение задачи пакетной обработки."
                        )
                        return
                    await asyncio.sleep(1)

            except asyncio.CancelledError:
                logger.info("[BATCH] [BATCH_TASK] Задача пакетной обработки отменена")
                break
            except Exception as e:
                logger.error(f"[BATCH] [BATCH_TASK] Ошибка в фоновой задаче пакетной обработки: {e}")
                import traceback

                traceback.print_exc()
                # Ждем минуту перед повторной попыткой или проверкой флага остановки
                for _ in range(60):
                    if not self.running:
                        logger.info(
                            "[RSS_PARSER] [BATCH_TASK] Получен сигнал остановки во время ожидания, завершение задачи пакетной обработки."
                        )
                        return
                    await asyncio.sleep(1)

    async def start(self):
        """Запуск сервиса парсинга"""
        logger.info("[RSS_PARSER] Запуск сервиса парсинга RSS...")

        # Регистрируем асинхронные обработчики сигналов
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._signal_handler(s)))

        # Запускаем очередь переводов в этом же event loop
        await self.translator_queue.start()

        # Создаем задачи
        self.parse_task = asyncio.create_task(self.parse_rss_task())
        self.batch_processor_task = asyncio.create_task(self.batch_processor_task_loop())
        self.cleanup_task = asyncio.create_task(self.cleanup_duplicates_task())

        try:
            # Ждем завершения любой из задач (обычно это не происходит, если running=True)
            # Или завершения по сигналу (который установит running=False и задачи завершатся)
            done, pending = await asyncio.wait(
                [self.parse_task, self.batch_processor_task, self.cleanup_task], return_when=asyncio.FIRST_COMPLETED
            )
            logger.info(f"[RSS_PARSER] Одна из задач завершена. Done: {len(done)}, Pending: {len(pending)}")

            # Отменяем оставшиеся задачи
            for task in pending:
                if not task.done():
                    logger.info(
                        f"[RSS_PARSER] Отмена оставшейся задачи {task.get_name() if hasattr(task, 'get_name') else 'Unknown'}..."
                    )
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.info(
                            f"[RSS_PARSER] Задача {task.get_name() if hasattr(task, 'get_name') else 'Unknown'} успешно отменена"
                        )
                    except Exception as e:
                        logger.error(f"[RSS_PARSER] Ошибка при отмене задачи: {e}")

        except Exception as e:
            logger.error(f"[RSS_PARSER] Критическая ошибка в start(): {e}")
            import traceback

            traceback.print_exc()
        finally:
            await self.cleanup()

    async def _signal_handler(self, signum):
        """Асинхронный обработчик сигналов завершения"""
        sig_name = signal.Signals(signum).name
        logger.info(f"[RSS_PARSER] Получен сигнал {sig_name} ({signum})")
        self.running = False

        # Если сигнал SIGTERM, даем немного времени на корректную остановку
        # Если SIGINT (Ctrl+C), можно остановить быстрее
        # Но в любом случае основная логика остановки в cleanup()
        if signum == signal.SIGTERM:
            logger.info("[RSS_PARSER] Ожидание завершения текущих операций (до 10 секунд)...")
            try:
                # Ждем немного, чтобы задачи могли завершиться по флагу self.running
                await asyncio.wait_for(asyncio.shield(self._wait_for_tasks_to_stop()), timeout=10.0)
            except asyncio.TimeoutError:
                logger.info("[RSS_PARSER] Таймаут ожидания завершения задач. Продолжаем остановку.")
        # Для SIGINT продолжаем

    async def _wait_for_tasks_to_stop(self):
        """Вспомогательная функция для ожидания остановки задач"""
        # Ждем, пока задачи не завершатся сами по self.running=False
        # Это нужно для использования с wait_for
        while (
            (self.parse_task and not self.parse_task.done())
            or (self.batch_processor_task and not self.batch_processor_task.done())
            or (self.cleanup_task and not self.cleanup_task.done())
        ):
            await asyncio.sleep(0.1)
        logger.info("[RSS_PARSER] Все задачи остановлены по флагу running.")

    async def cleanup(self):
        """Очистка ресурсов"""
        logger.info("[RSS_PARSER] Начало очистки ресурсов...")
        self.running = False  # Убедимся, что флаг остановки установлен

        # --- Остановка задач ---
        tasks_to_cancel = []
        if self.parse_task and not self.parse_task.done():
            logger.info("[RSS_PARSER] Отмена активной задачи парсинга...")
            self.parse_task.cancel()
            tasks_to_cancel.append(self.parse_task)

        if self.batch_processor_task and not self.batch_processor_task.done():
            logger.info("[RSS_PARSER] Отмена активной задачи пакетной обработки...")
            self.batch_processor_task.cancel()
            tasks_to_cancel.append(self.batch_processor_task)

        if self.cleanup_task and not self.cleanup_task.done():
            logger.info("[RSS_PARSER] Отмена активной задачи очистки дубликатов...")
            self.cleanup_task.cancel()
            tasks_to_cancel.append(self.cleanup_task)

        # Дожидаемся завершения отмененных задач
        if tasks_to_cancel:
            logger.info(f"[RSS_PARSER] Ожидание завершения {len(tasks_to_cancel)} отмененных задач...")
            done, pending = await asyncio.wait(tasks_to_cancel, timeout=5.0)  # Таймаут 5 секунд
            if pending:
                logger.warning(f"[RSS_PARSER] Предупреждение: {len(pending)} задач не завершились за таймаут.")
            else:
                logger.info("[RSS_PARSER] Все задачи успешно отменены.")

        # --- Остановка очереди переводов и её потока ---
        if hasattr(self, "translator_queue") and self.translator_queue:
            logger.info("[RSS_PARSER] Остановка очереди переводов...")
            try:
                # Останавливаем очередь (это останавливает воркеров)
                await self.translator_queue.stop()
                logger.info("[RSS_PARSER] Очередь переводов остановлена.")
            except Exception as e:
                logger.error(f"[RSS_PARSER] Ошибка при остановке очереди переводов: {e}")
                import traceback

                traceback.print_exc()

            # ------------------------------------

        # Закрываем менеджеры (заглушки, но оставляем)
        managers_to_close = [(self.rss_manager, "RSSManager"), (self.duplicate_detector, "FireFeedDuplicateDetector")]

        for manager, name in managers_to_close:
            try:
                if hasattr(manager, "close_pool"):
                    await manager.close_pool()
                    logger.info(f"[RSS_PARSER] Менеджер {name} закрыт (заглушка)")
            except Exception as e:
                logger.error(f"[RSS_PARSER] Ошибка при закрытии менеджера {name}: {e}")

        # Закрываем общий пул подключений
        try:
            await close_shared_db_pool()
            logger.info("[RSS_PARSER] Общий пул подключений закрыт")
        except Exception as e:
            logger.error(f"[RSS_PARSER] Ошибка при закрытии общего пула: {e}")

        logger.info("[RSS_PARSER] Очистка ресурсов завершена.")


async def main():
    """Асинхронная точка входа"""
    service = None
    try:
        service = RSSParserService()
        await service.start()
    except KeyboardInterrupt:
        logger.info("[RSS_PARSER] [MAIN] Сервис прерван пользователем (Ctrl+C)")
    except Exception as e:
        logger.error(f"[RSS_PARSER] [MAIN] Критическая ошибка в основном цикле: {e}")
        import traceback

        traceback.print_exc()
    # finally не нужен, так как cleanup вызывается внутри start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("[RSS_PARSER] [ENTRY] Приложение остановлено пользователем")
    except Exception as e:
        logger.error(f"[RSS_PARSER] [ENTRY] Фатальная ошибка приложения: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
