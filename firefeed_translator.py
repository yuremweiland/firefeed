import os
import asyncio
import torch
import spacy
import time
import re
from config import CHANNEL_IDS, CT2_MODELS_DIR
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
import gc
import psutil
import ctranslate2
from collections import OrderedDict
from firefeed_utils import clean_html
from transformers import M2M100Tokenizer
from firefeed_embeddings_processor import FireFeedEmbeddingsProcessor

# Импортируем терминологический словарь
from firefeed_translator_terminology_dict import TERMINOLOGY_DICT

class CachedModel:
    """Класс для хранения модели с метаданными"""
    def __init__(self, model, tokenizer, timestamp):
        self.model = model
        self.tokenizer = tokenizer
        self.timestamp = timestamp
        self.last_used = timestamp

class FireFeedTranslator:
    def __init__(self, device="cpu", max_workers=4, max_concurrent_translations=3, max_cached_models=15):
        """
        Инициализация переводчика
        Args:
            device: устройство для моделей (cpu/cuda)
            max_workers: максимальное количество потоков в пуле
            max_concurrent_translations: максимальное количество одновременных переводов
            max_cached_models: максимальное количество моделей в кэше
        """
        self.device = device
        self.max_cached_models = max_cached_models
        self.model_cache = OrderedDict()
        self.tokenizer_cache = OrderedDict()
        self.translation_cache = {}
        self.model_load_lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.translation_semaphore = asyncio.Semaphore(max_concurrent_translations)
        
        # Кэш для spacy моделей
        self.spacy_models = {}
        
        # Терминологический словарь (из внешнего файла)
        self.terminology_dict = TERMINOLOGY_DICT
        
        # Загрузка процессора эмбеддингов для семантической проверки
        print("[SEMANTIC] Загрузка процессора эмбеддингов для семантической проверки...")
        self.embeddings_processor = FireFeedEmbeddingsProcessor('paraphrase-multilingual-mpnet-base-v2', device)
        print("[SEMANTIC] Процессор загружен")
        
        # Статистика использования
        self.stats = {
            'models_loaded': 0,
            'translations_processed': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'total_translation_time': 0
        }
        
        print(f"[TRANSLATOR] Инициализация FireFeedTranslator на устройстве: {self.device}")
        print(f"[TRANSLATOR] Пул потоков: {max_workers} workers")
        print(f"[TRANSLATOR] Максимум одновременных переводов: {max_concurrent_translations}")
        print(f"[TRANSLATOR] Максимум моделей в кэше: {max_cached_models}")

    def _get_spacy_model(self, lang_code):
        """Получает spacy модель для языка. Потокобезопасна."""
        if lang_code in self.spacy_models:
            return self.spacy_models[lang_code]
        
        with self.model_load_lock:
            if lang_code in self.spacy_models:
                return self.spacy_models[lang_code]
            
            # Сопоставление языкового кода с моделью spacy
            spacy_model_map = {
                'en': 'en_core_web_sm',
                'ru': 'ru_core_news_sm',
                'de': 'de_core_news_sm',
                'fr': 'fr_core_news_sm',
            }
            
            model_name = spacy_model_map.get(lang_code)
            if not model_name:
                print(f"[SPACY] Языковая модель для '{lang_code}' не найдена, используем 'en_core_web_sm'")
                model_name = 'en_core_web_sm'
            
            try:
                nlp = spacy.load(model_name)
                self.spacy_models[lang_code] = nlp
                print(f"[SPACY] Загружена модель для языка '{lang_code}': {model_name}")
                return nlp
            except OSError:
                print(f"[SPACY] Модель '{model_name}' не найдена. Установите её командой:")
                print(f"python -m spacy download {model_name}")
                return None

    def _postprocess_text(self, text, target_lang='ru'):
        """Пост-обработка переведённого текста"""
        # 1. Очистка лишних пробелов
        text = re.sub(r'\s+', ' ', text).strip()

        # 2. Удаление последовательных повторений слов
        words = text.split()
        if words:
            deduped_words = [words[0]]
            for word in words[1:]:
                if word.lower() != deduped_words[-1].lower():
                    deduped_words.append(word)
            text = ' '.join(deduped_words)

        # 3. Удаление слишком коротких слов (кроме предлогов)
        short_words = {'a', 'an', 'the', 'to', 'of', 'in', 'on', 'at', 'by', 'for', 'with', 'as', 'is', 'are', 'was', 'were', 'be', 'been', 'has', 'have', 'had', 'do', 'does', 'did', 'will', 'would', 'can', 'could', 'may', 'might', 'must', 'shall', 'should', 'и', 'в', 'на', 'с', 'по', 'из', 'к', 'от', 'у', 'о', 'а', 'но', 'да', 'или', 'что', 'как', 'где', 'когда', 'почему', 'der', 'die', 'das', 'und', 'mit', 'auf', 'für', 'von', 'zu', 'im', 'am', 'le', 'la', 'les', 'et', 'avec', 'pour', 'dans'}
        filtered_words = []
        for word in text.split():
            if len(word) >= 3 or word.lower() in short_words:
                filtered_words.append(word)
        text = ' '.join(filtered_words)

        # 4. Удаление последовательностей одинаковых символов (более 3 подряд)
        text = re.sub(r'(.)\1{3,}', r'\1\1\1', text)

        # 5. Исправление заглавных букв в начале предложения
        sentences = re.split(r'([.!?]+)', text)
        processed = []
        for i, part in enumerate(sentences):
            if part.strip() and part[0].isalpha():
                processed.append(part[0].upper() + part[1:])
            else:
                processed.append(part)
        text = ''.join(processed)

        # 6. Удаление дубликатов предложений
        lines = text.split('. ')
        unique_lines = []
        seen = set()
        for line in lines:
            line_clean = re.sub(r'\W+', '', line.lower())
            if line_clean not in seen and len(line_clean) > 5:  # Игнорировать слишком короткие
                seen.add(line_clean)
                unique_lines.append(line)
        text = '. '.join(unique_lines)

        # 7. Замена терминов (регистронезависимо)
        for eng, translated in self.terminology_dict.items():
            text = re.sub(r'\b' + re.escape(eng) + r'\b', translated, text, flags=re.IGNORECASE)

        # 8. Удаление лишних символов в конце
        text = text.strip(' .,;')

        # 9. Финальная проверка: если текст слишком короткий или содержит мало букв, вернуть пустую строку
        if len(text) < 10 or len(re.findall(r'[a-zA-Zа-яА-Я]', text)) < len(text) * 0.5:
            return ""

        return text

    def _is_broken_translation(self, text, max_repeats=15):
        """Проверяет, содержит ли текст подозрительные повторы или мусор"""
        words = text.split()
        if len(words) < 5:
            return False
        # Проверяем, нет ли 15 подряд одинаковых слов
        for i in range(len(words) - max_repeats + 1):
            chunk = words[i:i + max_repeats]
            if len(set(chunk)) == 1:
                return True
        # Проверяем на слишком много повторяющихся символов
        if re.search(r'(.)\1{10,}', text):
            return True
        # Проверяем на отсутствие пробелов (сплошной текст)
        if len(text) > 50 and ' ' not in text:
            return True
        # Проверяем на слишком мало уникальных слов
        unique_words = set(words)
        if len(unique_words) < len(words) * 0.3 and len(words) > 10:
            return True
        return False

    def _check_translation_language(self, translated_text, target_lang):
        """Проверяет, что перевод содержит символы целевого языка или не содержит чужих"""
        try:
            translated_lower = translated_text.lower()
            if target_lang == 'en':
                # Для английского: не должен содержать русские, немецкие или французские символы
                return not bool(re.search(r'[а-яёäöüßàâäéèêëïîôöùûüÿç]', translated_lower))
            elif target_lang == 'ru':
                # Должен содержать русские буквы
                return bool(re.search(r'[а-яё]', translated_lower))
            elif target_lang == 'de':
                # Должен содержать немецкие умлауты или специфические буквы
                return bool(re.search(r'[äöüß]', translated_lower))
            elif target_lang == 'fr':
                # Должен содержать французские accents или буквы
                return bool(re.search(r'[àâäéèêëïîôöùûüÿç]', translated_lower))
            else:
                # Для других языков пропускаем проверку
                return True
        except Exception:
            return True  # Если ошибка — считаем, что всё ок

    def _semantic_check(self, original_text, translated_text, lang_code='en'):
        """Проверяет семантическое сходство оригинала и перевода с динамическим threshold"""
        try:
            # Если перевод идентичен оригиналу, считаем плохим
            if original_text.strip() == translated_text.strip():
                print("[SEMANTIC] Перевод идентичен оригиналу")
                return False

            if self._is_broken_translation(translated_text):
                print("[SEMANTIC] Обнаружен битый перевод (повторы слов)")
                return False

            # Длина текста для динамического threshold
            text_length = len(original_text)
            threshold = self.embeddings_processor.get_dynamic_threshold(text_length, 'content')

            # Генерируем эмбеддинги через процессор
            original_embedding = self.embeddings_processor.generate_embedding(original_text, lang_code)
            translated_embedding = self.embeddings_processor.generate_embedding(translated_text, lang_code)

            # Считаем сходство
            similarity = self.embeddings_processor.calculate_similarity(original_embedding, translated_embedding)
            print(f"[SEMANTIC] similarity = {similarity:.4f}, threshold = {threshold:.4f}, original_text = {original_text[:50]}..., translated_text = {translated_text[:50]}...")
            return similarity >= threshold
        except Exception as e:
            print(f"[SEMANTIC] Ошибка в семантической проверке: {e}")
            return True  # Если ошибка — считаем, что всё ок

    def _check_memory_usage(self):
        """Проверка использования памяти"""
        try:
            memory = psutil.virtual_memory()
            return memory.percent
        except:
            return 0

    def _cleanup_old_models(self):
        """Очистка старых моделей при высоком использовании памяти"""
        memory_percent = self._check_memory_usage()
        if memory_percent > 85:  # Если памяти > 85%
            print(f"[MEMORY] Высокое использование памяти: {memory_percent}%. Очистка кэша моделей...")
            # Очищаем половину кэша
            models_to_remove = len(self.model_cache) // 2
            for _ in range(models_to_remove):
                if self.model_cache:
                    old_key, _ = self.model_cache.popitem(last=False)
                    self.tokenizer_cache.pop(old_key, None)
            gc.collect()
            print(f"[MEMORY] Очищено {models_to_remove} моделей из кэша")

    def _enforce_cache_limit(self):
        """Принудительное ограничение размера кэша"""
        while len(self.model_cache) >= self.max_cached_models:
            old_key, _ = self.model_cache.popitem(last=False)
            self.tokenizer_cache.pop(old_key, None)
            print(f"[CACHE] Удалена старая модель из кэша: {old_key}")

    def _get_model(self, direction):
        """
        Получает модель и токенизатор для перевода.
        direction: 'm2m100'
        """
        cache_key = direction
        current_time = time.time()

        # Проверяем наличие в кэше
        if cache_key in self.model_cache:
            cached = self.model_cache[cache_key]
            # Проверяем TTL (2 часа)
            if current_time - cached.timestamp > 7200:
                # Удаляем устаревшую модель
                del self.model_cache[cache_key]
                self.tokenizer_cache.pop(cache_key, None)
                gc.collect()
                print(f"[MODEL] Удалена устаревшая модель: {cache_key}")
            else:
                # Обновляем время последнего использования (LRU)
                cached.last_used = current_time
                self.model_cache.move_to_end(cache_key)
                self.tokenizer_cache.move_to_end(cache_key)
                self.stats['cache_hits'] += 1
                return cached.model, cached.tokenizer

        # Загружаем новую модель
        with self.model_load_lock:
            # Повторная проверка после получения лока
            if cache_key in self.model_cache:
                cached = self.model_cache[cache_key]
                cached.last_used = current_time
                self.stats['cache_hits'] += 1
                return cached.model, cached.tokenizer

            # Ограничиваем размер кэша
            self._enforce_cache_limit()

            try:
                if direction == 'm2m100':
                    model_path = os.path.join(CT2_MODELS_DIR, "m2m100_418M")
                    tokenizer_path = os.path.join(CT2_MODELS_DIR, "m2m100_418M_hf")
                else:
                    raise ValueError(f"Неизвестное направление: {direction}")

                print(f"[TRANSLATOR] [{time.time():.3f}] Начало загрузки модели {model_path} через CTranslate2...")

                # Загрузка токенизатора
                tokenizer = M2M100Tokenizer.from_pretrained(tokenizer_path)

                # Загрузка модели через CTranslate2
                model = ctranslate2.Translator(
                    model_path,
                    device=self.device,
                    compute_type="auto"  # Автоматический выбор типа вычислений
                )

                print(f"[TRANSLATOR] [{time.time():.3f}] Модель {model_path} загружена через CTranslate2 на {self.device}.")

                # Сохраняем в кэш
                cached_model = CachedModel(model, tokenizer, current_time)
                self.model_cache[cache_key] = cached_model
                self.tokenizer_cache[cache_key] = tokenizer
                self.stats['models_loaded'] += 1
                self.stats['cache_misses'] += 1

                # Проверяем память и очищаем при необходимости
                self._cleanup_old_models()

                return model, tokenizer

            except Exception as e:
                print(f"[ERROR] [TRANSLATOR] [{time.time():.3f}] Ошибка загрузки модели {model_path}: {e}")
                traceback.print_exc()
                return None, None

    def _translate_with_context_sync(self, texts, source_lang='en', target_lang='ru', context_window=2):
        """
        Синхронная версия translate_with_context для использования в пуле потоков
        """
        print(f"[TRANSLATOR] [{time.time():.3f}] _translate_with_context_sync: {source_lang} -> {target_lang}, {len(texts)} предложений")

        # Прямой перевод с M2M100
        translated = self._translate_batch_sync(texts, source_lang, target_lang, context_window)

        print(f"[TRANSLATOR] [{time.time():.3f}] _translate_with_context_sync завершена.")
        return translated

    def _translate_text_sync(self, text, source_lang='en', target_lang='ru', context_window=2):
        """Синхронная версия translate_text для использования в пуле потоков"""
        # Отключаем контекст для коротких текстов
        if len(text.split()) < 10:
            context_window = 0

        start_time = time.time()
        print(f"[TRANSLATOR] [{start_time:.3f}] Начало перевода: {source_lang} -> {target_lang}, текст длиной {len(text)} символов")
        
        if source_lang == target_lang:
            result = clean_html(text)
            end_time = time.time()
            print(f"[TRANSLATOR] [{end_time:.3f}] Языки совпадают, возврат без перевода. Время выполнения: {end_time - start_time:.3f} сек")
            return result

        # Используем более надежный ключ для кэширования
        cache_key = f"{source_lang}_{target_lang}_{hash(text)}_{context_window}"
        if cache_key in self.translation_cache:
            cached_result = self.translation_cache[cache_key]
            end_time = time.time()
            print(f"[TRANSLATOR] [{end_time:.3f}] Результат найден в кэше. Время выполнения: {end_time - start_time:.3f} сек")
            return cached_result

        print(f"[TRANSLATOR] [{time.time():.3f}] Прямой перевод {source_lang} -> {target_lang}")
        print(f"[TRANSLATOR] [{time.time():.3f}] Токенизация текста...")

        # Используем spacy для разбиения на предложения
        nlp = self._get_spacy_model(source_lang)
        if nlp is None:
            sentences = [text]  # Если модель не найдена, не разбиваем
        else:
            doc = nlp(text)
            sentences = [sent.text.strip() for sent in doc.sents]
        
        print(f"[TRANSLATOR] [{time.time():.3f}] Получено {len(sentences)} предложений")
        
        print(f"[TRANSLATOR] [{time.time():.3f}] Начало перевода с контекстом (окно: {context_window})")
        translate_start = time.time()
        translated = " ".join(self._translate_with_context_sync(sentences, source_lang, target_lang, context_window))
        translate_time = time.time() - translate_start
        print(f"[TRANSLATOR] [{time.time():.3f}] Перевод завершен за {translate_time:.3f} сек")

        # Применяем пост-обработку
        result = self._postprocess_text(translated, target_lang)
        result = clean_html(result)

        self.translation_cache[cache_key] = result
        end_time = time.time()
        total_time = end_time - start_time
        self.stats['total_translation_time'] += total_time
        print(f"[TRANSLATOR] [{end_time:.3f}] Перевод завершен. Общее время выполнения: {total_time:.3f} сек")
        return result

    def _get_optimal_batch_size(self):
        """Определение оптимального размера батча в зависимости от доступной памяти"""
        try:
            available_memory = psutil.virtual_memory().available / (1024**3)  # ГБ
            if available_memory < 1:
                return 2
            elif available_memory < 2:
                return 4
            elif available_memory < 4:
                return 8
            else:
                return 16
        except:
            return 8  # значение по умолчанию

    def _translate_batch_sync(self, texts, source_lang='en', target_lang='ru', context_window=2, beam_size=None):
        """Синхронная версия translate_batch для использования в пуле потоков"""
        if not texts:
            return []

        print(f"[TRANSLATOR] [BATCH] Начало пакетного перевода: {source_lang} -> {target_lang}, {len(texts)} текстов")

        # Используем мультиязычную модель m2m100
        model, tokenizer = self._get_model('m2m100')
        if model is None or tokenizer is None:
            print(f"[TRANSLATOR] [BATCH] Модель m2m100 не найдена, возврат исходных текстов.")
            return texts

        # Используем spacy для разбиения текстов на предложения
        nlp = self._get_spacy_model(source_lang)
        if nlp is None:
            # Если модель не найдена, не разбиваем
            all_sentences = texts
            text_indices = list(range(len(texts)))
            sentence_counts = [1] * len(texts)
        else:
            all_sentences = []
            text_indices = []
            sentence_counts = []
            for i, text in enumerate(texts):
                doc = nlp(text)
                sentences = [sent.text.strip() for sent in doc.sents]
                sentence_counts.append(len(sentences))
                all_sentences.extend(sentences)
                text_indices.extend([i] * len(sentences))

        if not all_sentences:
            return texts

        print(f"[TRANSLATOR] [BATCH] Всего предложений для перевода: {len(all_sentences)}")

        # Переводим все предложения пакетно
        print(f"[TRANSLATOR] [BATCH] Используемое устройство: {self.device}")

        # Определяем оптимальный размер батча
        batch_size = self._get_optimal_batch_size()
        print(f"[TRANSLATOR] [BATCH] Оптимальный размер батча: {batch_size}")

        translated_batches = []

        for i in range(0, len(all_sentences), batch_size):
            batch_sentences = all_sentences[i:i + batch_size]
            print(f"[TRANSLATOR] [BATCH] Перевод батча {i//batch_size + 1}/{(len(all_sentences)-1)//batch_size + 1}: {len(batch_sentences)} предложений")

            # Токенизация с указанием исходного и целевого языков
            batch_tokenized = [tokenizer(text, src_lang=source_lang, tgt_lang=target_lang)['input_ids'] for text in batch_sentences]
            # Перевод через CTranslate2
            results = model.translate_batch(
                batch_tokenized,
                max_batch_size=batch_size,
                beam_size=beam_size or 10,
                max_decoding_length=256,
                length_penalty=0.8,
                repetition_penalty=1.5,
                return_scores=False
            )
            # Декодирование
            batch_translations = [
                tokenizer.convert_tokens_to_string(res.hypotheses[0])
                for res in results
            ]

            translated_batches.extend(batch_translations)

        # Группируем переводы по исходным текстам
        result_texts = [''] * len(texts)
        current_pos = 0

        for i, (text, sent_count) in enumerate(zip(texts, sentence_counts)):
            if sent_count > 0:
                translated_sentences = translated_batches[current_pos:current_pos + sent_count]
                result_text = ' '.join(translated_sentences)
                # Применяем пост-обработку
                result_text = self._postprocess_text(result_text, target_lang)
                result_texts[i] = result_text
                current_pos += sent_count
            else:
                result_texts[i] = text # Пустой текст остается пустым

        cleaned_results = [clean_html(text) for text in result_texts]
        print(f"[TRANSLATOR] [BATCH] Пакетный перевод завершен. Переведено {len(cleaned_results)} текстов")
        return cleaned_results

    async def translate_async(self, texts, source_lang, target_lang, context_window=2, beam_size=None):
        print(f"[TRANSLATOR] [ASYNC] Начало translate_async для задачи")
        """Асинхронный метод перевода с использованием пула потоков"""
        loop = asyncio.get_event_loop()

        async with self.translation_semaphore:
            try:
                # Создаем Future из run_in_executor
                future = loop.run_in_executor(
                    self.executor,
                    self._translate_batch_sync,
                    texts,
                    source_lang,
                    target_lang,
                    context_window,
                    beam_size
                )

                # Ждем его с таймаутом
                result = await asyncio.wait_for(future, timeout=120.0)
                self.stats['translations_processed'] += len(texts)
                return result
            except asyncio.TimeoutError:
                print(f"[ERROR] [TRANSLATOR] ТАЙМАУТ (120 сек) для '{source_lang}' -> '{target_lang}'!")
                return texts
            except Exception as e:
                print(f"[ERROR] [TRANSLATOR] Ошибка при переводе '{source_lang}' -> '{target_lang}': {e}")
                traceback.print_exc()
                return texts
    
    async def prepare_translations(self, title: str, description: str, original_lang: str, callback=None, error_callback=None, task_id=None) -> dict:
        """Подготавливает переводы заголовка, описания и категории на все целевые языки."""
        start_time = time.time()
        print(f"[TRANSLATOR] prepare_translations начата для языка '{original_lang}' для задачи: {task_id[:20] if task_id else 'Unknown'}")

        try:
            translations = {}
            target_languages = list(CHANNEL_IDS.keys())

            print(f"[TRANSLATOR] Целевые языки для перевода: {target_languages}")
            clean_title = clean_html(title)
            clean_description = clean_html(description)

            # - Обработка оригинального языка -
            if original_lang in target_languages:
                translations[original_lang] = {'title': clean_title, 'description': clean_description}
                print(f"[TRANSLATOR] Оригинальный язык '{original_lang}' включен в результаты без перевода.")

            # - Подготовка и выполнение переводов -
            translation_results = {}
            lang_pairs = []
            for target_lang in target_languages:
                if original_lang == target_lang:
                    continue
                pairs = [
                    (original_lang, target_lang, clean_title, 'title'),
                    (original_lang, target_lang, clean_description, 'description')
                ]
                lang_pairs.append((original_lang, target_lang, pairs))

            print(f"[TRANSLATOR] [BATCH] Подготовлено {len(lang_pairs)} языковых пар для перевода.")

            # Выполняем переводы для каждой языковой пары
            for i, (src_lang, tgt_lang, texts_to_process) in enumerate(lang_pairs):
                print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Перевод '{src_lang}' -> '{tgt_lang}': {len(texts_to_process)} текстов")
                group_start_time = time.time()
                try:
                    # Извлекаем только тексты для перевода
                    texts_only = [text for _, _, text, _ in texts_to_process]
                    
                    # Выполняем асинхронный перевод
                    translated_texts = await self.translate_async(texts_only, src_lang, tgt_lang)
                    
                    group_duration = time.time() - group_start_time
                    print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Группа '{src_lang}' -> '{tgt_lang}' обработана за {group_duration:.2f} сек.")

                    # Сохраняем результаты
                    translation_results[(src_lang, tgt_lang)] = list(zip(
                        [field_type for _, _, _, field_type in texts_to_process],
                        translated_texts
                    ))
                except Exception as e:
                    group_duration = time.time() - group_start_time
                    error_msg = (
                        f"[ERROR] [TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] "
                        f"Критическая ошибка для группы '{src_lang}' -> '{tgt_lang}' "
                        f"за {group_duration:.2f} сек: {e}"
                    )
                    print(error_msg)
                    traceback.print_exc()
                    # В случае критической ошибки используем оригинальные тексты
                    translation_results[(src_lang, tgt_lang)] = [
                        (field_type, original_text) 
                        for _, _, original_text, field_type in texts_to_process
                    ]

            # - Компоновка финальных результатов -
            print("[TRANSLATOR] Начата компоновка финальных результатов переводов.")
            for target_lang in target_languages:
                if original_lang == target_lang:
                    continue

                lang_translations = {}

                src_lang = original_lang
                results = translation_results.get((src_lang, target_lang), [])
                for field_type, translated_text in results:
                    if field_type == 'title':
                        original_text = clean_title
                    else: # description
                        original_text = clean_description

                    # Проверяем, что перевод отличается от оригинала
                    if translated_text.strip() == original_text.strip():
                        print(f"[TRANSLATOR] Перевод идентичен оригиналу для '{field_type}' на '{target_lang}', пропуск")
                        continue

                    # Проверяем, что перевод на правильном языке (содержит символы целевого языка)
                    if not self._check_translation_language(translated_text, target_lang):
                        print(f"[LANG_CHECK] Перевод не на '{target_lang}' для '{field_type}': '{translated_text[:50]}...', пропуск")
                        continue

                    # Семантическая проверка для title и description
                    if not self._semantic_check(original_text, translated_text):
                        warn_msg = (
                            f"[SEMANTIC] [TRANSLATOR] Семантическая проверка не пройдена на '{target_lang}' "
                            f"для поля '{field_type}': '{translated_text[:50]}...'"
                        )
                        print(warn_msg)
                        # Попытка fallback: перевод с beam_size=1
                        fallback_texts = await self.translate_async([original_text], src_lang, tgt_lang, context_window=0, beam_size=1)
                        fallback_text = fallback_texts[0] if fallback_texts else ""
                        if fallback_text and fallback_text.strip() != original_text.strip() and self._check_translation_language(fallback_text, target_lang) and self._semantic_check(original_text, fallback_text):
                            print(f"[FALLBACK] Fallback перевод успешен для '{field_type}'")
                            lang_translations[field_type] = fallback_text
                        else:
                            # Не добавляем поле, если перевод неудачный
                            pass
                    else:
                        lang_translations[field_type] = translated_text

                if 'title' in lang_translations and 'description' in lang_translations:
                    translations[target_lang] = lang_translations
                    print(f"[TRANSLATOR] Перевод на '{target_lang}' успешно добавлен в результаты ({len(lang_translations)} полей).")
                else:
                    warn_msg = (
                        f"[WARN] Перевод на '{target_lang}' не добавлен. "
                        f"Нет заголовка или описания."
                    )
                    print(warn_msg)

            # Удалить языки с одинаковыми переводами (чтобы не сохранять дубликаты битых переводов)
            seen_titles = set()
            to_remove = []
            for lang, data in translations.items():
                title = data.get('title', '')
                if title in seen_titles:
                    print(f"[TRANSLATOR] Удален дубликат перевода для языка '{lang}' (одинаковый title)")
                    to_remove.append(lang)
                else:
                    seen_titles.add(title)
            for lang in to_remove:
                del translations[lang]

            total_duration = time.time() - start_time
            print(f"[TRANSLATOR] prepare_translations завершена за {total_duration:.2f} сек. Всего переводов: {len(translations)}")
            # - ДОПОЛНИТЕЛЬНОЕ ЛОГИРОВАНИЕ ПЕРЕД ВОЗВРАТОМ -
            print(f"[TRANSLATOR] Подготовленный словарь переводов будет возвращен. Размер: {len(translations)} языков.")
            
            # --- ВЫЗОВ CALLBACK ---
            # Если передан callback, вызываем его с результатом
            if callback:
                print(f"[TRANSLATOR] Вызов пользовательского callback для задачи: {task_id[:20] if task_id else 'Unknown'}")
                try:
                    # Проверяем, является ли callback корутиной (async def)
                    if asyncio.iscoroutinefunction(callback):
                        await callback(translations, task_id=task_id)
                    else:
                        # Если это обычная функция, просто вызываем
                        callback(translations, task_id=task_id)
                    print(f"[TRANSLATOR] Пользовательский callback успешно выполнен для задачи: {task_id[:20] if task_id else 'Unknown'}")
                except Exception as cb_error:
                    print(f"[TRANSLATOR] [ERROR] Ошибка в пользовательском callback для задачи {task_id[:20] if task_id else 'Unknown'}: {cb_error}")
                    traceback.print_exc()
                    # Если есть error_callback, уведомляем об ошибке в callback
                    if error_callback:
                        try:
                            error_data = {'error': f"Callback execution failed: {cb_error}", 'task_id': task_id}
                            if asyncio.iscoroutinefunction(error_callback):
                                await error_callback(error_data)
                            else:
                                error_callback(error_data)
                        except Exception as ec_error:
                            print(f"[TRANSLATOR] [ERROR] Ошибка в error_callback при обработке ошибки callback: {ec_error}")
                            traceback.print_exc()
            # -----------------------

            return translations

        except Exception as e:
            # --- ОБРАБОТКА ОШИБОК prepare_translations ---
            error_msg = f"[TRANSLATOR] [CRITICAL ERROR] Критическая ошибка в prepare_translations для задачи {task_id[:20] if task_id else 'Unknown'}: {e}"
            print(error_msg)
            traceback.print_exc()
            
            # Вызываем error_callback, если он передан
            if error_callback:
                print(f"[TRANSLATOR] Вызов пользовательского error_callback для задачи: {task_id[:20] if task_id else 'Unknown'}")
                try:
                    error_data = {'error': str(e), 'task_id': task_id}
                    if asyncio.iscoroutinefunction(error_callback):
                        await error_callback(error_data)
                    else:
                        error_callback(error_data)
                    print(f"[TRANSLATOR] Пользовательский error_callback успешно выполнен для задачи: {task_id[:20] if task_id else 'Unknown'}")
                except Exception as ec_error:
                    print(f"[TRANSLATOR] [ERROR] Ошибка в error_callback: {ec_error}")
                    traceback.print_exc()
            # ---------------------------------------------
            # Возвращаем пустой словарь в случае ошибки
            return {}
            # ---------------------------------------------


    def get_stats(self):
        """Получение статистики использования переводчика"""
        return {
            **self.stats,
            'cached_models': len(self.model_cache),
            'cached_translations': len(self.translation_cache),
            'memory_usage_percent': self._check_memory_usage()
        }

    def clear_cache(self):
        """Очистка всех кэшей"""
        self.model_cache.clear()
        self.tokenizer_cache.clear()
        self.translation_cache.clear()
        gc.collect()
        print("[TRANSLATOR] Все кэши очищены")

    def preload_popular_models(self):
        """Предзагрузка мультиязычной модели"""
        try:
            print("[PRELOAD] Загрузка мультиязычной модели m2m100_418M через CTranslate2...")
            # Загружаем модель
            self._get_model('m2m100')
            print("[PRELOAD] Модель загружена и готова к использованию")
        except Exception as e:
            print(f"[PRELOAD] Ошибка при загрузке модели: {e}")

    def shutdown(self):
        """Корректное завершение работы переводчика"""
        self.executor.shutdown(wait=True)
        self.clear_cache()
        print("[TRANSLATOR] Переводчик корректно завершил работу")