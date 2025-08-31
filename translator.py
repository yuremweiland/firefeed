from transformers import MarianMTModel, MarianTokenizer
from firefeed_utils import clean_html
from functools import lru_cache
import asyncio
import torch
import nltk
import os
import time
import re
from config import CHANNEL_IDS, NLTK_DATA_DIR
import threading # Импортируем threading для RLock
import traceback

# Установка пути для данных NLTK
nltk_data_path = NLTK_DATA_DIR
os.environ['NLTK_DATA'] = nltk_data_path

# Скачивание необходимых ресурсов
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', download_dir=nltk_data_path)

try:
    nltk.data.find('tokenizers/punkt_tab')
except LookupError:
    nltk.download('punkt_tab', download_dir=nltk_data_path)

# Глобальный флаг для однократной инициализации устройства
_device = None

# Создаем семафор для ограничения количества одновременных переводов
TRANSLATION_SEMAPHORE = asyncio.Semaphore(2)

def _get_device():
    """Определяет и кэширует устройство для моделей."""
    global _device
    if _device is None:
        # Проверяем доступность CUDA (если вы планируете использовать GPU)
        # if torch.cuda.is_available():
        #     _device = "cuda"
        # else:
        #     _device = "cpu"
        # Для простоты и избежания проблем с многопоточностью на GPU, используем CPU
        _device = "cpu"
        print(f"[TRANSLATOR] Модели будут загружаться на устройство: {_device}")
    return _device

_model_cache = {}
_tokenizer_cache = {}
_translation_cache = {}
# RLock для потокобезопасной загрузки моделей
_model_load_lock = threading.RLock() 

# Языковые пары, требующие каскадного перевода через английский
CASCADE_TRANSLATIONS = {
    ('ru', 'de'): ('ru', 'en', 'de')
}

def get_translator_model(src_lang, tgt_lang):
    """Получает модель и токенизатор для перевода. Потокобезопасна."""
    cache_key = f"{src_lang}-{tgt_lang}"
    if cache_key not in _model_cache:
        # Используем рекурсивную блокировку, чтобы избежать одновременной загрузки одной и той же модели несколькими потоками
        with _model_load_lock:
            # Повторная проверка, может быть загружена пока ждали лок
            if cache_key not in _model_cache:
                try:
                    model_name = f'Helsinki-NLP/opus-mt-{src_lang}-{tgt_lang}'
                    print(f"[TRANSLATOR] [{time.time():.3f}] Начало загрузки модели {model_name}...")
                    
                    # Оптимизированная загрузка модели
                    model = MarianMTModel.from_pretrained(
                        model_name,
                        # torch_dtype=torch.float16,  # Отключено для CPU совместимости
                        low_cpu_mem_usage=True
                    )
                    
                    tokenizer = MarianTokenizer.from_pretrained(model_name)
                    
                    # Явно перемещаем модель на выбранное устройство (CPU)
                    device = _get_device()
                    model = model.to(device)
                    print(f"[TRANSLATOR] [{time.time():.3f}] Модель {model_name} загружена и перемещена на {device}.")
                    
                    _model_cache[cache_key] = model
                    _tokenizer_cache[cache_key] = tokenizer
                    
                except Exception as e:
                    print(f"[ERROR] [TRANSLATOR] [{time.time():.3f}] Ошибка загрузки модели {model_name}: {e}")
                    traceback.print_exc()
                    return None, None
    return _model_cache[cache_key], _tokenizer_cache[cache_key]

def translate_with_context(texts, source_lang='en', target_lang='ru', context_window=2):
    """
    Переводит список текстов с учётом контекста.
    
    Args:
        texts (list): Список предложений для перевода.
        context_window (int): Количество предыдущих предложений для контекста.
    """
    print(f"[TRANSLATOR] [{time.time():.3f}] translate_with_context: {source_lang} -> {target_lang}, {len(texts)} предложений")
    # Проверяем, нужен ли каскадный перевод
    cascade_key = (source_lang, target_lang)
    if cascade_key in CASCADE_TRANSLATIONS:
        # Используем каскадный перевод через английский
        src_lang, intermediate_lang, tgt_lang = CASCADE_TRANSLATIONS[cascade_key]
        print(f"[TRANSLATOR] [{time.time():.3f}] Каскадный перевод: {src_lang} -> {intermediate_lang} -> {tgt_lang}")
        # Переводим на промежуточный язык (английский)
        intermediate_texts = translate_with_context(texts, src_lang, intermediate_lang, context_window)
        
        # Переводим с промежуточного на целевой язык
        return translate_with_context(intermediate_texts, intermediate_lang, tgt_lang, context_window)
    
    model, tokenizer = get_translator_model(source_lang, target_lang)
    if model is None or tokenizer is None:
        print(f"[TRANSLATOR] [{time.time():.3f}] Модель не найдена, возврат исходного текста.")
        return texts  # Если модель не найдена, возвращаем исходный текст
    
    translated = []
    
    for i in range(len(texts)):
        context = " ".join(texts[max(0, i-context_window):i])
        current_text = texts[i]
        
        combined = f"{context} {current_text}" if context else current_text
        
        inputs = tokenizer(combined, return_tensors="pt", truncation=True, max_length=512)

        # Перемещаем inputs на то же устройство, что и модель
        device = next(model.parameters()).device # Получаем устройство модели
        inputs = {k: v.to(device) for k, v in inputs.items()} # Перемещаем тензоры

        with torch.no_grad():
            outputs = model.generate(**inputs)
        translated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # Удаляем контекст из результата (если нужно)
        if context:
            # Это упрощённый подход - в реальности может потребоваться более сложная постобработка
            translated_text = translated_text.replace(translate_text(context, source_lang, target_lang), "").strip()
        
        translated.append(translated_text)
    
    print(f"[TRANSLATOR] [{time.time():.3f}] translate_with_context завершена.")
    return translated

# @lru_cache(maxsize=1000)
def cached_translate_text(text, source_lang, target_lang):
    return translate_text(text, source_lang, target_lang)

def translate_text(text, source_lang='en', target_lang='ru', context_window=2):
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
    if cache_key in _translation_cache:
        cached_result = _translation_cache[cache_key]
        end_time = time.time()
        print(f"[TRANSLATOR] [{end_time:.3f}] Результат найден в кэше. Время выполнения: {end_time - start_time:.3f} сек")
        return cached_result

    # Проверяем, нужен ли каскадный перевод
    cascade_key = (source_lang, target_lang)
    if cascade_key in CASCADE_TRANSLATIONS:
        print(f"[TRANSLATOR] [{time.time():.3f}] Используется каскадный перевод для {source_lang} -> {target_lang}")
        # Используем каскадный перевод через английский
        src_lang, intermediate_lang, tgt_lang = CASCADE_TRANSLATIONS[cascade_key]
        
        # Переводим на промежуточный язык (английский)
        print(f"[TRANSLATOR] [{time.time():.3f}] Этап 1: Перевод {src_lang} -> {intermediate_lang}")
        intermediate_start = time.time()
        intermediate_text = translate_text(text, src_lang, intermediate_lang, context_window)
        intermediate_time = time.time() - intermediate_start
        print(f"[TRANSLATOR] [{time.time():.3f}] Этап 1 завершен за {intermediate_time:.3f} сек")
        
        # Переводим с промежуточного на целевой язык
        print(f"[TRANSLATOR] [{time.time():.3f}] Этап 2: Перевод {intermediate_lang} -> {tgt_lang}")
        final_start = time.time()
        result = translate_text(intermediate_text, intermediate_lang, tgt_lang, context_window)
        final_time = time.time() - final_start
        print(f"[TRANSLATOR] [{time.time():.3f}] Этап 2 завершен за {final_time:.3f} сек")
    else:
        print(f"[TRANSLATOR] [{time.time():.3f}] Прямой перевод {source_lang} -> {target_lang}")
        print(f"[TRANSLATOR] [{time.time():.3f}] Токенизация текста...")
        sentences = nltk.sent_tokenize(text)
        print(f"[TRANSLATOR] [{time.time():.3f}] Получено {len(sentences)} предложений")
        
        print(f"[TRANSLATOR] [{time.time():.3f}] Начало перевода с контекстом (окно: {context_window})")
        translate_start = time.time()
        translated = " ".join(translate_with_context(sentences, source_lang, target_lang, context_window))
        translate_time = time.time() - translate_start
        print(f"[TRANSLATOR] [{time.time():.3f}] Перевод завершен за {translate_time:.3f} сек")
        
        result = clean_html(translated)

    _translation_cache[cache_key] = result
    end_time = time.time()
    total_time = end_time - start_time
    print(f"[TRANSLATOR] [{end_time:.3f}] Перевод завершен. Общее время выполнения: {total_time:.3f} сек")
    return result

def translate_batch(texts, source_lang='en', target_lang='ru', context_window=2):
    """Переводит список текстов пакетно для лучшей производительности"""
    if not texts:
        return []
    
    print(f"[TRANSLATOR] [BATCH] Начало пакетного перевода: {source_lang} -> {target_lang}, {len(texts)} текстов")
    
    # Проверяем, нужен ли каскадный перевод
    cascade_key = (source_lang, target_lang)
    if cascade_key in CASCADE_TRANSLATIONS:
        src_lang, intermediate_lang, tgt_lang = CASCADE_TRANSLATIONS[cascade_key]
        intermediate_texts = translate_batch(texts, src_lang, intermediate_lang, context_window)
        return translate_batch(intermediate_texts, intermediate_lang, tgt_lang, context_window)
    
    model, tokenizer = get_translator_model(source_lang, target_lang)
    if model is None or tokenizer is None:
        print(f"[TRANSLATOR] [BATCH] Модель не найдена, возврат исходных текстов.")
        return texts
    
    # Разбиваем тексты на предложения
    all_sentences = []
    text_indices = []  # Для отслеживания, к какому тексту относится предложение
    sentence_counts = [] # Для отслеживания количества предложений в каждом тексте
    
    for i, text in enumerate(texts):
        sentences = nltk.sent_tokenize(text)
        sentence_counts.append(len(sentences))
        all_sentences.extend(sentences)
        text_indices.extend([i] * len(sentences))
    
    if not all_sentences:
        return texts
    
    print(f"[TRANSLATOR] [BATCH] Всего предложений для перевода: {len(all_sentences)}")
    
    # Переводим все предложения пакетно
    device = next(model.parameters()).device
    print(f"[TRANSLATOR] [BATCH] Используемое устройство: {device}")
    
    # Создаем батчи
    batch_size = 8  # Настройте под вашу память
    translated_batches = []
    
    for i in range(0, len(all_sentences), batch_size):
        batch_sentences = all_sentences[i:i + batch_size]
        print(f"[TRANSLATOR] [BATCH] Перевод батча {i//batch_size + 1}/{(len(all_sentences)-1)//batch_size + 1}: {len(batch_sentences)} предложений")
        
        # Токенизация батча
        inputs = tokenizer(
            batch_sentences, 
            return_tensors="pt", 
            truncation=True, 
            max_length=512,
            padding=True  # Важно для батчей
        )
        
        inputs = {k: v.to(device) for k, v in inputs.items()}
        
        with torch.no_grad():
            outputs = model.generate(**inputs)
        
        batch_translations = [
            tokenizer.decode(output, skip_special_tokens=True) 
            for output in outputs
        ]
        translated_batches.extend(batch_translations)
    
    # Группируем переводы по исходным текстам
    result_texts = [''] * len(texts)
    current_pos = 0
    
    for i, (text, sent_count) in enumerate(zip(texts, sentence_counts)):
        if sent_count > 0:
            translated_sentences = translated_batches[current_pos:current_pos + sent_count]
            result_texts[i] = ' '.join(translated_sentences)
            current_pos += sent_count
        else:
            result_texts[i] = text # Пустой текст остается пустым
    
    cleaned_results = [clean_html(text) for text in result_texts]
    print(f"[TRANSLATOR] [BATCH] Пакетный перевод завершен. Переведено {len(cleaned_results)} текстов")
    return cleaned_results

def is_broken_translation(text: str, max_repeats: int = 5) -> bool:
    """
    Проверяет, содержит ли текст подозрительное количество повторяющихся символов подряд.
    Например: "......." или "........." или ". . . . ." или "abc abc abc abc abc"
    """
    if not text:
        return True
    
    # Проверяем повторение одного символа (как было)
    if re.search(r'(.)\1{' + str(max_repeats) + ',}', text):
        return True
    
    # Проверяем повторение любых 2 символов подряд
    if re.search(r'(..)\1{' + str(max_repeats-1) + ',}', text):
        return True
    
    return False

async def prepare_translations(title: str, description: str, category: str, original_lang: str) -> dict:
    """
    Подготавливает переводы заголовка, описания и категории на все целевые языки.
    """
    start_time = time.time()
    print(f"[TRANSLATOR] prepare_translations начата для языка '{original_lang}'")

    translations = {}
    target_languages = list(CHANNEL_IDS.keys())
    print(f"[TRANSLATOR] Целевые языки для перевода: {target_languages}")

    clean_title = clean_html(title)
    clean_description = clean_html(description)

    loop = asyncio.get_event_loop()

    # --- Обработка оригинального языка ---
    if original_lang in target_languages:
        translations[original_lang] = {
            'title': clean_title,
            'description': clean_description,
            'category': category
        }
        print(f"[TRANSLATOR] Оригинальный язык '{original_lang}' включен в результаты без перевода.")

    # --- Подготовка и выполнение переводов ---
    translation_tasks = []
    lang_pairs = []

    for target_lang in target_languages:
        if original_lang == target_lang:
            continue
            
        pairs = [
            (original_lang, target_lang, clean_title, 'title'),
            (original_lang, target_lang, clean_description, 'description'),
            ('en', target_lang, category, 'category')
        ]
        lang_pairs.append((original_lang, target_lang, pairs))

    print(f"[TRANSLATOR] [BATCH] Подготовлено {len(lang_pairs)} языковых пар для перевода.")

    # Выполняем переводы для каждой языковой пары
    translation_results = {}
    for i, (src_lang, tgt_lang, texts_to_process) in enumerate(lang_pairs):
        print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Перевод '{src_lang}' -> '{tgt_lang}': {len(texts_to_process)} текстов")
        
        group_start_time = time.time()
        try:
            # Извлекаем только тексты для перевода
            texts_only = [text for _, _, text, _ in texts_to_process]
            
            # --- ВАЖНО: Оборачиваем вызов в try/except внутри lambda ---
            def safe_translate_batch():
                try:
                    print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Начало вызова translate_batch...")
                    result = translate_batch(texts_only, src_lang, tgt_lang)
                    print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] translate_batch вернул результат.")
                    return result
                except Exception as e:
                    print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Исключение в translate_batch: {e}")
                    traceback.print_exc()
                    # Возвращаем оригинальные тексты в случае ошибки
                    return texts_only 
            
            # --- ИСПОЛЬЗУЕМ СЕМАФОР ДЛЯ ОГРАНИЧЕНИЯ КОЛИЧЕСТВА ОДНОВРЕМЕННЫХ ПЕРЕВОДОВ ---
            async with TRANSLATION_SEMAPHORE:
                print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Получен доступ к семафору для '{src_lang}' -> '{tgt_lang}'")
                
                # --- ВАЖНО: Добавляем таймаут ---
                # Создаем Future из run_in_executor
                future = loop.run_in_executor(None, safe_translate_batch)
                
                # Ждем его с таймаутом, например, 120 секунд
                try:
                    translated_texts = await asyncio.wait_for(future, timeout=120.0)
                    print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] await run_in_executor ЗАВЕРШЕН.")
                except asyncio.TimeoutError:
                    print(f"[ERROR] [TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] ТАЙМАУТ (120 сек) для '{src_lang}' -> '{tgt_lang}'!")
                    # В случае таймаута используем оригинальные тексты
                    translated_texts = texts_only

            group_duration = time.time() - group_start_time
            print(f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Группа '{src_lang}' -> '{tgt_lang}' обработана за {group_duration:.2f} сек.")

            # Сохраняем результаты
            translation_results[(src_lang, tgt_lang)] = list(zip(
                [field_type for _, _, _, field_type in texts_to_process],
                translated_texts
            ))
            
        except Exception as e: # Общий except для всей группы
            group_duration = time.time() - group_start_time
            error_msg = (f"[ERROR] [TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] "
                         f"Критическая ошибка для группы '{src_lang}' -> '{tgt_lang}' "
                         f"за {group_duration:.2f} сек: {e}")
            print(error_msg)
            traceback.print_exc()
            
            # В случае критической ошибки используем оригинальные тексты
            translation_results[(src_lang, tgt_lang)] = [
                (field_type, original_text) 
                for _, _, original_text, field_type in texts_to_process
            ]

    # --- Компоновка финальных результатов (без изменений) ---
    print("[TRANSLATOR] Начата компоновка финальных результатов переводов.")
    
    for target_lang in target_languages:
        if original_lang == target_lang:
            continue

        lang_translations = {}
        valid_fields = 0
        
        src_lang = original_lang
        results = translation_results.get((src_lang, target_lang), [])
        
        for field_type, translated_text in results:
            if field_type == 'category':
                original_text = category
            elif field_type == 'title':
                original_text = clean_title
            else:  # description
                original_text = clean_description
                
            if is_broken_translation(translated_text):
                warn_msg = (f"[WARN] [TRANSLATOR] Битый перевод на '{target_lang}' "
                            f"для поля '{field_type}': '{translated_text[:50]}...'")
                print(warn_msg)
                lang_translations[field_type] = original_text
            else:
                lang_translations[field_type] = translated_text
                valid_fields += 1
        
        if valid_fields == 3:
            translations[target_lang] = lang_translations
            print(f"[TRANSLATOR] Перевод на '{target_lang}' успешно добавлен в результаты.")
        else:
            warn_msg = (f"[WARN] Перевод на '{target_lang}' не добавлен. "
                        f"Корректных полей: {valid_fields}/3.")
            print(warn_msg)

    total_duration = time.time() - start_time
    print(f"[TRANSLATOR] prepare_translations завершена за {total_duration:.2f} сек. Всего переводов: {len(translations)}")
    
    # --- ДОПОЛНИТЕЛЬНОЕ ЛОГИРОВАНИЕ ПЕРЕД ВОЗВРАТОМ ---
    print(f"[TRANSLATOR] Подготовленный словарь переводов будет возвращен. Размер: {len(translations)} языков.")
    return translations