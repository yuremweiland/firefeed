import asyncio
from asyncio import Queue
import time

class FireFeedTranslatorTaskQueue:
    def __init__(self, translator, max_workers=1, queue_size=30):
        self.translator = translator
        self.queue = Queue(maxsize=queue_size)
        self.max_workers = max_workers
        self.workers = []
        self.running = False
        self.stats = {
            'processed': 0,
            'errors': 0,
            'queued': 0
        }
    
    async def start(self):
        """Запуск очереди задач"""
        self.running = True
        for i in range(self.max_workers):
            worker = asyncio.create_task(self._worker(f"worker-{i}"))
            self.workers.append(worker)
        print(f"[QUEUE] 🔧 Запущено {self.max_workers} рабочих потоков перевода")
    
    async def _worker(self, worker_id):
        """Рабочий поток для обработки задач"""
        while self.running:
            try:
                # Получаем задачу с таймаутом
                task = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                start_time = time.time()
                task_id = task.get('task_id', 'unknown')
                print(f"[{worker_id}] 📥 Начало обработки задачи: {task_id[:20]}")

                try:
                    result = await self.translator.prepare_translations(
                        **task['data'],
                        callback=task.get('callback'),
                        error_callback=task.get('error_callback'),
                        task_id=task.get('task_id')
                    )
                    
                    # Статистика
                    self.stats['processed'] += 1

                    duration = time.time() - start_time
                    print(f"[{worker_id}] ✅ Задача {task_id[:20]} завершена за {duration:.2f} сек")
                except Exception as e:
                    # Статистика ошибок
                    self.stats['errors'] += 1
                    print(f"[{worker_id}] ❌ Ошибка перевода для задачи {task_id[:20]}: {e}")
                    
                finally:
                    self.queue.task_done()
            except asyncio.TimeoutError:
                # Продолжаем цикл если таймаут
                continue
            except Exception as e:
                print(f"[{worker_id}] ❌ Критическая ошибка воркера: {e}")
                # traceback.print_exc() # Убрал, так как ошибка выше уже логируется
                if not self.queue.empty():
                    self.queue.task_done()
    
    async def add_task(self, title, description, original_lang,
                       callback=None, error_callback=None, task_id=None):
        """Добавление задачи перевода в очередь"""
        task = {
            'data': {
                'title': title,
                'description': description,
                'original_lang': original_lang
            },
            'callback': callback,
            'error_callback': error_callback,
            'task_id': task_id
        }
        
        try:
            await self.queue.put(task)
            self.stats['queued'] += 1
            print(f"[QUEUE] 📨 Добавлена задача перевода (в очереди: {self.queue.qsize()})")
            return True
        except asyncio.QueueFull:
            print("⚠️ [QUEUE] Очередь перевода переполнена!")
            return False
    
    async def wait_completion(self):
        """Ожидание завершения всех задач в очереди"""
        if self.queue.qsize() > 0:
            print(f"[QUEUE] ⏳ Ожидание завершения {self.queue.qsize()} задач...")
            await self.queue.join()
            print("[QUEUE] ✅ Все задачи завершены")
    
    async def stop(self):
        """Остановка очереди"""
        print("[QUEUE] 🛑 Остановка очереди задач...")
        self.running = False
        
        # Отменяем все рабочие потоки
        for worker in self.workers:
            if not worker.done():
                worker.cancel()
        
        # Ждем завершения с таймаутом
        try:
            await asyncio.wait_for(
                asyncio.gather(*self.workers, return_exceptions=True),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            print("[QUEUE] ⚠️ Принудительная остановка воркеров")
        
        print("[QUEUE] ✅ Очередь задач остановлена")
    
    def get_stats(self):
        """Получение статистики очереди"""
        return self.stats.copy()
    
    def print_stats(self):
        """Вывод статистики"""
        stats = self.get_stats()
        print(f"[QUEUE] 📊 Статистика:")
        print(f"  Обработано: {stats['processed']}")
        print(f"  Ошибок: {stats['errors']}")
        print(f"  В очереди: {stats['queued']}")
        if stats['processed'] + stats['errors'] > 0:
            success_rate = (stats['processed'] / (stats['processed'] + stats['errors'])) * 100
            print(f"  Успешность: {success_rate:.1f}%")