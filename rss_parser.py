import asyncio
import signal
import sys
import threading
import time
from rss_manager import RSSManager
from firefeed_dublicate_detector import FireFeedDuplicateDetector
from firefeed_translator import FireFeedTranslator
from firefeed_translator_task_queue import FireFeedTranslatorTaskQueue
from config import close_shared_db_pool

class RSSParserService:
    def __init__(self):
        self.duplicate_detector = FireFeedDuplicateDetector()
        # --- Инициализация переводчика ---
        self.translator = FireFeedTranslator(device="cpu", max_workers=1, max_concurrent_translations=1) 
        self.translator_queue = FireFeedTranslatorTaskQueue(self.translator, max_workers=1, queue_size=30)
        # --- Инициализация потока и loop'а для переводчика ---
        self._start_translator_loop()
        # -------------------------------------------------------
        self.rss_manager = RSSManager(duplicate_detector=self.duplicate_detector, translator_queue=self.translator_queue)
        self.running = True
        self.parse_task = None
        self.translator_thread = None 
        self.translator_thread_loop = None # Сохраняем ссылку на loop потока переводчика
        self.batch_processor_task = None
        
    def _start_translator_loop(self):
        """Запускает цикл очереди переводчика в отдельном потоке и сохраняет ссылку на его loop"""
        def start_translator_loop():
            # Создаем и устанавливаем новый event loop для этого потока
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            # Сохраняем ссылку на loop этого потока в основном объекте сервиса
            self.translator_thread_loop = loop
            try:
                print("[RSS_PARSER] [TRANSLATOR_THREAD] Запуск очереди переводчика в потоке...")
                # Запускаем очередь переводов
                loop.run_until_complete(self.translator_queue.start())
                print("[RSS_PARSER] [TRANSLATOR_THREAD] Очередь переводчика запущена. Цикл событий работает.")
                # run_forever() будет работать, пока loop не будет остановлен
                loop.run_forever() 
                print("[RSS_PARSER] [TRANSLATOR_THREAD] Цикл событий потока переводчика остановлен.")
            except Exception as e:
                print(f"[RSS_PARSER] [TRANSLATOR_THREAD] Ошибка в потоке переводчика: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Важно: закрываем loop при завершении потока
                loop.close()
                print("[RSS_PARSER] [TRANSLATOR_THREAD] Event loop потока переводчика закрыт.")

        # Создаем и запускаем поток
        self.translator_thread = threading.Thread(target=start_translator_loop, daemon=False, name="TranslatorThread")
        self.translator_thread.start()
        print("[RSS_PARSER] Поток очереди переводчика запущен.")

    async def parse_rss_task(self):
        """Периодическая задача парсинга RSS"""
        while self.running:
            try:
                print("[RSS_PARSER] Начало парсинга RSS лент...")
                await self.rss_manager.fetch_news()
                print("[RSS_PARSER] Парсинг RSS лент завершен")
                
                # Ждем 15 минут перед следующим парсингом или пока не будет установлен флаг self.running = False
                for _ in range(900): # 900 секунд / 1 секунда = 15 минут
                    if not self.running:
                        print("[RSS_PARSER] [PARSE_TASK] Получен сигнал остановки, завершение задачи парсинга.")
                        return
                    await asyncio.sleep(1) 
                
            except asyncio.CancelledError:
                print("[RSS_PARSER] [PARSE_TASK] Задача парсинга отменена")
                break
            except Exception as e:
                print(f"[RSS_PARSER] [PARSE_TASK] Ошибка при парсинге: {e}")
                import traceback
                traceback.print_exc()
                # Уменьшаем время ожидания перед повторной попыткой или проверкой флага остановки
                for _ in range(30): # 30 секунд
                    if not self.running:
                        print("[RSS_PARSER] [PARSE_TASK] Получен сигнал остановки во время ожидания, завершение задачи парсинга.")
                        return
                    await asyncio.sleep(1)

    async def batch_processor_job(self):
        """Задача регулярной пакетной обработки"""
        try:
            print("[BATCH] Запуск регулярной пакетной обработки новостей без эмбеддингов...")
            success, errors = await self.duplicate_detector.process_missing_embeddings_batch(batch_size=20, delay_between_items=0.2)
            print(f"[BATCH] Регулярная пакетная обработки завершена. Успешно: {success}, Ошибок: {errors}")
        except Exception as e:
            print(f"[ERROR] [BATCH] Ошибка в регулярной пакетной обработке: {e}")
            import traceback
            traceback.print_exc()

    async def batch_processor_task_loop(self):
        """Фоновая задача пакетной обработки"""
        while self.running:
            try:
                await self.batch_processor_job()
                # Ждем 30 минут перед следующей пакетной обработкой или пока не будет остановка
                for _ in range(1800): # 1800 секунд / 1 секунда = 30 минут
                     if not self.running:
                        print("[RSS_PARSER] [BATCH_TASK] Получен сигнал остановки, завершение задачи пакетной обработки.")
                        return
                     await asyncio.sleep(1)
                     
            except asyncio.CancelledError:
                print("[BATCH] [BATCH_TASK] Задача пакетной обработки отменена")
                break
            except Exception as e:
                print(f"[BATCH] [BATCH_TASK] Ошибка в фоновой задаче пакетной обработки: {e}")
                import traceback
                traceback.print_exc()
                # Ждем минуту перед повторной попыткой или проверкой флага остановки
                for _ in range(60): # 60 секунд
                     if not self.running:
                        print("[RSS_PARSER] [BATCH_TASK] Получен сигнал остановки во время ожидания, завершение задачи пакетной обработки.")
                        return
                     await asyncio.sleep(1)

    async def start(self):
        """Запуск сервиса парсинга"""
        print("[RSS_PARSER] Запуск сервиса парсинга RSS...")
        
        # Регистрируем асинхронные обработчики сигналов
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self._signal_handler(s)))

        # Создаем задачи
        self.parse_task = asyncio.create_task(self.parse_rss_task())
        self.batch_processor_task = asyncio.create_task(self.batch_processor_task_loop())
        
        try:
            # Ждем завершения любой из задач (обычно это не происходит, если running=True)
            # Или завершения по сигналу (который установит running=False и задачи завершатся)
            done, pending = await asyncio.wait(
                [self.parse_task, self.batch_processor_task],
                return_when=asyncio.FIRST_COMPLETED
            )
            print(f"[RSS_PARSER] Одна из задач завершена. Done: {len(done)}, Pending: {len(pending)}")
            
            # Отменяем оставшиеся задачи
            for task in pending:
                if not task.done():
                    print(f"[RSS_PARSER] Отмена оставшейся задачи {task.get_name() if hasattr(task, 'get_name') else 'Unknown'}...")
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        print(f"[RSS_PARSER] Задача {task.get_name() if hasattr(task, 'get_name') else 'Unknown'} успешно отменена")
                    except Exception as e:
                         print(f"[RSS_PARSER] Ошибка при отмене задачи: {e}")
                        
        except Exception as e:
            print(f"[RSS_PARSER] Критическая ошибка в start(): {e}")
            import traceback
            traceback.print_exc()
        finally:
            await self.cleanup()

    async def _signal_handler(self, signum):
        """Асинхронный обработчик сигналов завершения"""
        sig_name = signal.Signals(signum).name
        print(f"[RSS_PARSER] Получен сигнал {sig_name} ({signum})")
        self.running = False

        # Если сигнал SIGTERM, даем немного времени на корректную остановку
        # Если SIGINT (Ctrl+C), можно остановить быстрее
        # Но в любом случае основная логика остановки в cleanup()
        if signum == signal.SIGTERM:
             print("[RSS_PARSER] Ожидание завершения текущих операций (до 10 секунд)...")
             try:
                 # Ждем немного, чтобы задачи могли завершиться по флагу self.running
                 await asyncio.wait_for(asyncio.shield(self._wait_for_tasks_to_stop()), timeout=10.0)
             except asyncio.TimeoutError:
                 print("[RSS_PARSER] Таймаут ожидания завершения задач. Продолжаем остановку.")
        # Для SIGINT просто продолжаем

    async def _wait_for_tasks_to_stop(self):
        """Вспомогательная функция для ожидания остановки задач"""
        # Просто ждем, пока задачи не завершатся сами по self.running=False
        # Это нужно для использования с wait_for
        while self.parse_task and not self.parse_task.done() or \
              self.batch_processor_task and not self.batch_processor_task.done():
            await asyncio.sleep(0.1)
        print("[RSS_PARSER] Все задачи остановлены по флагу running.")

    
    async def cleanup(self):
        """Очистка ресурсов"""
        print("[RSS_PARSER] Начало очистки ресурсов...")
        self.running = False # Убедимся, что флаг остановки установлен

        # --- Остановка задач ---
        tasks_to_cancel = []
        if self.parse_task and not self.parse_task.done():
            print("[RSS_PARSER] Отмена активной задачи парсинга...")
            self.parse_task.cancel()
            tasks_to_cancel.append(self.parse_task)
        
        if self.batch_processor_task and not self.batch_processor_task.done():
            print("[RSS_PARSER] Отмена активной задачи пакетной обработки...")
            self.batch_processor_task.cancel()
            tasks_to_cancel.append(self.batch_processor_task)

        # Дожидаемся завершения отмененных задач
        if tasks_to_cancel:
            print(f"[RSS_PARSER] Ожидание завершения {len(tasks_to_cancel)} отмененных задач...")
            done, pending = await asyncio.wait(tasks_to_cancel, timeout=5.0) # Таймаут 5 секунд
            if pending:
                print(f"[RSS_PARSER] Предупреждение: {len(pending)} задач не завершились за таймаут.")
            else:
                print("[RSS_PARSER] Все задачи успешно отменены.")

        # --- Остановка очереди переводов и её потока ---
        if hasattr(self, 'translator_queue') and self.translator_queue:
            print("[RSS_PARSER] Остановка очереди переводов...")
            try:
                # Останавливаем очередь (это останавливает воркеров)
                await self.translator_queue.stop()
                print("[RSS_PARSER] Очередь переводов остановлена.")
            except Exception as e:
                print(f"[RSS_PARSER] Ошибка при остановке очереди переводов: {e}")
                import traceback
                traceback.print_exc()

            # --- Остановка event loop'а потока переводчика ---
            # Это критически важно для корректного завершения потока
            if self.translator_thread_loop and not self.translator_thread_loop.is_closed():
                print("[RSS_PARSER] Остановка event loop'а потока переводчика...")
                try:
                    # Планируем остановку loop'а в его собственном контексте
                    asyncio.run_coroutine_threadsafe(self.translator_thread_loop.stop(), self.translator_thread_loop)
                    # Ждем завершения потока (с таймаутом)
                    print("[RSS_PARSER] Ожидание завершения потока переводчика...")
                    start_wait = time.time()
                    while self.translator_thread.is_alive() and (time.time() - start_wait) < 10:
                        await asyncio.sleep(0.1)
                    
                    if self.translator_thread.is_alive():
                        print("[RSS_PARSER] Предупреждение: Поток переводчика не завершился за 10 секунд.")
                        # Принудительное завершение потока может быть опасно, лучше дать ему время
                    else:
                        print("[RSS_PARSER] Поток переводчика успешно завершен.")
                        
                except Exception as e:
                    print(f"[RSS_PARSER] Ошибка при остановке loop'а потока переводчика: {e}")
                    import traceback
                    traceback.print_exc()
            else:
                 print("[RSS_PARSER] Loop потока переводчика уже закрыт или недоступен.")
        # ------------------------------------

        # Закрываем менеджеры (заглушки, но оставляем)
        managers_to_close = [
            (self.rss_manager, "RSSManager"),
            (self.duplicate_detector, "FireFeedDuplicateDetector")
        ]
        
        for manager, name in managers_to_close:
            try:
                if hasattr(manager, 'close_pool'):
                    await manager.close_pool()
                    print(f"[RSS_PARSER] Менеджер {name} закрыт (заглушка)")
            except Exception as e:
                print(f"[RSS_PARSER] Ошибка при закрытии менеджера {name}: {e}")
        
        # Закрываем общий пул подключений
        try:
            await close_shared_db_pool()
            print("[RSS_PARSER] Общий пул подключений закрыт")
        except Exception as e:
            print(f"[RSS_PARSER] Ошибка при закрытии общего пула: {e}")

        print("[RSS_PARSER] Очистка ресурсов завершена.")


async def main():
    """Асинхронная точка входа"""
    service = None
    try:
        service = RSSParserService()
        await service.start()
    except KeyboardInterrupt:
        print("[RSS_PARSER] [MAIN] Сервис прерван пользователем (Ctrl+C)")
    except Exception as e:
        print(f"[RSS_PARSER] [MAIN] Критическая ошибка в основном цикле: {e}")
        import traceback
        traceback.print_exc()
    # finally не нужен, так как cleanup вызывается внутри start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("[RSS_PARSER] [ENTRY] Приложение остановлено пользователем")
    except Exception as e:
        print(f"[RSS_PARSER] [ENTRY] Фатальная ошибка приложения: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
