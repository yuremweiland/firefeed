import asyncio
import spacy
import time
import re
import logging
from config import CHANNEL_IDS
import threading
from concurrent.futures import ThreadPoolExecutor
import traceback
import gc
import psutil
from collections import OrderedDict
from utils.text import TextProcessor
from utils.cache import SpacyModelCache
from transformers import M2M100Tokenizer, M2M100ForConditionalGeneration
from firefeed_embeddings_processor import FireFeedEmbeddingsProcessor

# Импортируем терминологический словарь
from firefeed_translator_terminology_dict import TERMINOLOGY_DICT

logger = logging.getLogger(__name__)


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

        # Кэш для spacy моделей с ограничением
        self.spacy_cache = SpacyModelCache(max_cache_size=3)
        self.text_processor = TextProcessor()

        # Терминологический словарь (из внешнего файла)
        self.terminology_dict = TERMINOLOGY_DICT

        # Загрузка процессора эмбеддингов для семантической проверки
        logger.info("[SEMANTIC] Загрузка процессора эмбеддингов для семантической проверки...")
        self.embeddings_processor = FireFeedEmbeddingsProcessor("paraphrase-multilingual-mpnet-base-v2", device)
        logger.info("[SEMANTIC] Процессор загружен")

        # Статистика использования
        self.stats = {
            "models_loaded": 0,
            "translations_processed": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "total_translation_time": 0,
        }

        logger.info(f"[TRANSLATOR] Инициализация FireFeedTranslator на устройстве: {self.device}")
        logger.info(f"[TRANSLATOR] Пул потоков: {max_workers} workers")
        logger.info(f"[TRANSLATOR] Максимум одновременных переводов: {max_concurrent_translations}")
        logger.info(f"[TRANSLATOR] Максимум моделей в кэше: {max_cached_models}")

        # Запуск фоновой задачи для выгрузки неиспользуемых моделей
        asyncio.create_task(self._model_cleanup_task())

    def _get_spacy_model(self, lang_code):
        """Получает spacy модель для языка с LRU кэшированием."""
        return self.spacy_cache.get_model(lang_code)

    def _preprocess_text_with_terminology(self, text, target_lang):
        """Предварительная обработка текста: замена терминов на переводы перед переводом"""
        if target_lang not in ["ru", "de", "fr", "en"]:
            return text  # Если язык не поддерживается, возвращаем как есть

        for eng_term, translations in self.terminology_dict.items():
            if target_lang in translations:
                translated_term = translations[target_lang]
                if translated_term != eng_term:  # Заменяем только если перевод отличается
                    text = re.sub(r"\b" + re.escape(eng_term) + r"\b", translated_term, text, flags=re.IGNORECASE)
        return text

    def _postprocess_text(self, text, target_lang="ru"):
        """Пост-обработка переведённого текста"""
        # 1. Очистка лишних пробелов
        text = re.sub(r"\s+", " ", text).strip()

        # 2. Удаление последовательных повторений слов (улучшенная версия)
        words = text.split()
        if words:
            deduped_words = [words[0]]
            for word in words[1:]:
                # Проверяем не только точное совпадение, но и частичное (для слов типа "five-year" -> "five")
                if word.lower() != deduped_words[-1].lower() and not word.lower().startswith(
                    deduped_words[-1].lower()[:3]
                ):
                    deduped_words.append(word)
            text = " ".join(deduped_words)

        # 3. Удаление слишком коротких слов (кроме предлогов и служебных слов)
        short_words = {
            # English
            "a", "an", "the", "to", "of", "in", "on", "at", "by", "for", "with", "as", "is", "are", "was",
            "were", "be", "been", "has", "have", "had", "do", "does", "did", "will", "would", "can",
            "could", "may", "might", "must", "shall", "should", "it", "he", "she", "we", "they", "this",
            "that", "here", "there", "so", "but", "or", "and", "if", "then", "when", "where", "why",
            "how", "all", "some", "any", "no", "yes", "not", "very", "just", "only", "also", "even",
            "too", "much", "many", "few", "more", "most", "less", "least", "good", "bad", "big",
            "small", "new", "old", "first", "last", "next", "now", "then", "up", "down", "out", "over",
            "under", "above", "below", "left", "right", "back", "front", "before", "after", "during",
            "while", "since", "until", "from", "at", "by", "for", "with", "about", "against",
            "between", "into", "through", "across", "along", "around", "behind", "beside", "beyond",
            "inside", "outside", "near", "far",
            # Russian
            "и", "в", "на", "с", "по", "из", "к", "от", "у", "о", "а", "но", "да", "или", "что", "как",
            "где", "когда", "почему", "я", "ты", "он", "она", "оно", "мы", "вы", "они", "это", "тот",
            "та", "то", "те", "мой", "твой", "его", "её", "наш", "ваш", "их", "кто", "что", "где",
            "когда", "почему", "как", "зачем", "ли", "бы", "же", "то", "ни", "нибудь", "либо", "или",
            "да", "нет", "даже", "уже", "ещё", "тоже", "так", "также", "здесь", "там", "тут", "туда",
            "сюда", "оттуда", "отсюда", "везде", "нигде", "всегда", "никогда", "иногда", "часто",
            "редко", "много", "мало", "больше", "меньше", "лучше", "хуже", "хорошо", "плохо",
            "большой", "маленький", "новый", "старый", "первый", "последний", "следующий", "теперь",
            "тогда", "здесь", "там", "вверх", "вниз", "внутри", "снаружи", "спереди", "сзади",
            "слева", "справа", "перед", "после", "во", "со", "изо", "ко", "ото", "до", "без", "для",
            "про", "через", "сквозь", "между", "около", "возле", "против", "ради", "благодаря",
            "согласно", "несмотря", "вопреки", "вследствие", "из-за", "вслед", "вместо", "кроме",
            "помимо", "сверх", "вдоль", "вокруг", "напротив", "рядом", "близко", "далеко",
            # German
            "der", "die", "das", "und", "mit", "auf", "für", "von", "zu", "im", "am", "ich", "du",
            "er", "sie", "es", "wir", "ihr", "sie", "dies", "das", "der", "die", "den", "dem", "des",
            "ein", "eine", "einen", "einem", "eines", "mein", "dein", "sein", "ihr", "unser", "euer",
            "ihr", "wer", "was", "wo", "wann", "warum", "wie", "weshalb", "ob", "wenn", "dann",
            "hier", "da", "dort", "hin", "her", "überall", "nirgendwo", "immer", "nie", "manchmal",
            "oft", "selten", "viel", "wenig", "mehr", "weniger", "besser", "schlechter", "gut",
            "schlecht", "groß", "klein", "neu", "alt", "erster", "letzter", "nächster", "jetzt",
            "dann", "hier", "da", "oben", "unten", "innen", "außen", "vorn", "hinten", "links",
            "rechts", "vor", "nach", "während", "seit", "bis", "von", "zu", "bei", "für", "mit",
            "über", "gegen", "zwischen", "in", "aus", "durch", "quer", "entlang", "um", "hinter",
            "neben", "jenseits", "nahe", "fern",
            # French
            "le", "la", "les", "et", "avec", "pour", "dans", "je", "tu", "il", "elle", "nous", "vous",
            "ils", "elles", "ce", "cet", "cette", "ces", "mon", "ton", "son", "notre", "votre",
            "leur", "qui", "que", "quoi", "où", "quand", "pourquoi", "comment", "si", "quand",
            "alors", "ici", "là", "partout", "nulle", "toujours", "jamais", "parfois", "souvent",
            "rarement", "beaucoup", "peu", "plus", "moins", "mieux", "pire", "bien", "mal", "grand",
            "petit", "nouveau", "vieux", "premier", "dernier", "suivant", "maintenant", "alors",
            "ici", "là", "haut", "bas", "dedans", "dehors", "devant", "derrière", "gauche", "droite",
            "avant", "après", "pendant", "depuis", "jusqu", "de", "à", "chez", "pour", "avec", "sur",
            "contre", "entre", "dans", "hors", "par", "au-dessus", "en-dessous", "à-travers",
            "le-long", "autour", "derrière", "à-côté", "au-delà", "dedans", "dehors", "près", "loin",
        }
        filtered_words = []
        for word in text.split():
            if len(word) >= 3 or word.lower() in short_words:
                filtered_words.append(word)
        text = " ".join(filtered_words)

        # 4. Удаление последовательностей одинаковых символов (более 3 подряд)
        text = re.sub(r"(.)\1{3,}", r"\1\1\1", text)

        # 5. Исправление заглавных букв в начале предложения
        sentences = re.split(r"([.!?]+)", text)
        processed = []
        for i, part in enumerate(sentences):
            if part.strip() and part[0].isalpha():
                processed.append(part[0].upper() + part[1:])
            else:
                processed.append(part)
        text = "".join(processed)

        # 6. Удаление дубликатов предложений
        lines = text.split(". ")
        unique_lines = []
        seen = set()
        for line in lines:
            line_clean = re.sub(r"\W+", "", line.lower())
            if line_clean not in seen and len(line_clean) > 5:  # Игнорировать слишком короткие
                seen.add(line_clean)
                unique_lines.append(line)
        text = ". ".join(unique_lines)

        # 7. Замена терминов (регистронезависимо) - теперь только для случаев, когда перевод не сработал
        for eng, translations in self.terminology_dict.items():
            if target_lang in translations:
                translated_term = translations[target_lang]
                text = re.sub(r"\b" + re.escape(eng) + r"\b", translated_term, text, flags=re.IGNORECASE)

        # 8. Удаление лишних символов в конце
        text = text.strip(" .,;")

        # 9. Финальная проверка: если текст слишком короткий или содержит мало букв, вернуть пустую строку
        if len(text) < 10 or len(re.findall(r"[a-zA-Zа-яА-Я]", text)) < len(text) * 0.5:
            return ""

        return text

    def _is_broken_translation(self, text, max_repeats=15):
        """Проверяет, содержит ли текст подозрительные повторы или мусор"""
        words = text.split()
        if len(words) < 5:
            return False

        # Проверяем, нет ли 15 подряд одинаковых слов
        for i in range(len(words) - max_repeats + 1):
            chunk = words[i : i + max_repeats]
            if len(set(chunk)) == 1:
                return True

        # Проверяем на слишком много повторяющихся символов
        if re.search(r"(.)\1{10,}", text):
            return True

        # Проверяем на отсутствие пробелов (сплошной текст)
        if len(text) > 50 and " " not in text:
            return True

        # Проверяем на слишком мало уникальных слов
        unique_words = set(words)
        if len(unique_words) < len(words) * 0.3 and len(words) > 10:
            return True

        # Дополнительная проверка на gibberish: слишком много слов начинающихся одинаково
        word_starts = [word[:3].lower() for word in words if len(word) >= 3]
        if word_starts:
            most_common_start = max(set(word_starts), key=word_starts.count)
            if word_starts.count(most_common_start) > len(word_starts) * 0.6:
                return True

        # Новая проверка: слишком много слов с цифрами или странными символами
        alphanumeric_ratio = len(re.findall(r"[a-zA-Zа-яА-Я0-9]", text)) / len(text) if text else 0
        if alphanumeric_ratio < 0.7:
            return True

        # Проверка на слишком короткие слова подряд
        short_words_count = sum(1 for word in words if len(word) < 3)
        if len(words) > 5 and short_words_count > len(words) * 0.8:
            return True

        # Проверка на повторяющиеся паттерны типа "five-year five"
        for i in range(len(words) - 1):
            if words[i] in words[i + 1] and len(words[i]) > 3:
                return True

        return False

    def _check_translation_language(self, translated_text, target_lang):
        """Проверяет, что перевод содержит символы целевого языка или не содержит чужих"""
        try:
            translated_lower = translated_text.lower()
            if target_lang == "en":
                # Для английского: не должен содержать русские, немецкие или французские символы
                # Ослабляем проверку: допускаем небольшое количество чужих символов
                foreign_chars = len(re.findall(r"[а-яёäöüßàâäéèêëïîôöùûüÿç]", translated_lower))
                total_chars = len(re.findall(r"[a-zA-Z]", translated_lower))
                if total_chars == 0:
                    return False  # Нет английских букв вообще
                return foreign_chars / total_chars < 0.1  # Менее 10% чужих символов
            elif target_lang == "ru":
                # Должен содержать русские буквы
                return bool(re.search(r"[а-яё]", translated_lower))
            elif target_lang == "de":
                # Должен содержать немецкие умлауты или специфические буквы
                return bool(re.search(r"[äöüß]", translated_lower))
            elif target_lang == "fr":
                # Должен содержать французские accents или буквы
                return bool(re.search(r"[àâäéèêëïîôöùûüÿç]", translated_lower))
            else:
                # Для других языков пропускаем проверку
                return True
        except Exception:
            return True  # Если ошибка — считаем, что всё ок

    def _semantic_check(self, original_text, translated_text, lang_code="en"):
        """Проверяет семантическое сходство оригинала и перевода с динамическим threshold"""
        try:
            # Если перевод идентичен оригиналу, считаем плохим
            if original_text.strip() == translated_text.strip():
                logger.warning("[SEMANTIC] Перевод идентичен оригиналу")
                return False

            if self._is_broken_translation(translated_text):
                logger.warning("[SEMANTIC] Обнаружен битый перевод (повторы слов)")
                return False

            # Длина текста для динамического threshold
            text_length = len(original_text)
            threshold = self.embeddings_processor.get_dynamic_threshold(text_length, "content")

            # Снижаем threshold для более мягкой проверки
            threshold = max(0.2, threshold - 0.2)  # Минимум 0.2, снижаем на 0.2

            # Генерируем эмбеддинги через процессор
            original_embedding = self.embeddings_processor.generate_embedding(original_text, lang_code)
            translated_embedding = self.embeddings_processor.generate_embedding(translated_text, lang_code)

            # Считаем сходство
            similarity = self.embeddings_processor.calculate_similarity(original_embedding, translated_embedding)
            logger.debug(
                f"[SEMANTIC] similarity = {similarity:.4f}, threshold = {threshold:.4f}, original_text = {original_text[:50]}..., translated_text = {translated_text[:50]}..."
            )
            return similarity >= threshold
        except Exception as e:
            logger.error(f"[SEMANTIC] Ошибка в семантической проверке: {e}")
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
            logger.warning(f"[MEMORY] Высокое использование памяти: {memory_percent}%. Очистка кэша моделей...")
            # Очищаем половину кэша
            models_to_remove = len(self.model_cache) // 2
            for _ in range(models_to_remove):
                if self.model_cache:
                    old_key, _ = self.model_cache.popitem(last=False)
                    self.tokenizer_cache.pop(old_key, None)
            gc.collect()
            logger.info(f"[MEMORY] Очищено {models_to_remove} моделей из кэша")

    def _enforce_cache_limit(self):
        """Принудительное ограничение размера кэша"""
        while len(self.model_cache) >= self.max_cached_models:
            old_key, _ = self.model_cache.popitem(last=False)
            self.tokenizer_cache.pop(old_key, None)
            logger.info(f"[CACHE] Удалена старая модель из кэша: {old_key}")

    def _unload_unused_models(self):
        """Выгрузка моделей, не использовавшихся более 30 минут"""
        current_time = time.time()
        unused_threshold = 1800  # 30 минут
        models_to_remove = []

        for cache_key, cached in self.model_cache.items():
            if current_time - cached.last_used > unused_threshold:
                models_to_remove.append(cache_key)

        for cache_key in models_to_remove:
            del self.model_cache[cache_key]
            self.tokenizer_cache.pop(cache_key, None)
            logger.info(f"[MEMORY] Выгружена неиспользуемая модель: {cache_key}")

        if models_to_remove:
            gc.collect()
            logger.info(f"[MEMORY] Выгружено {len(models_to_remove)} неиспользуемых моделей")

    async def _model_cleanup_task(self):
        """Фоновая задача для периодической выгрузки неиспользуемых моделей"""
        while True:
            await asyncio.sleep(600)  # Каждые 10 минут
            try:
                self._unload_unused_models()
            except Exception as e:
                logger.error(f"[MEMORY] Ошибка в задаче очистки моделей: {e}")

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
                logger.info(f"[MODEL] Удалена устаревшая модель: {cache_key}")
            else:
                # Обновляем время последнего использования (LRU)
                cached.last_used = current_time
                self.model_cache.move_to_end(cache_key)
                self.tokenizer_cache.move_to_end(cache_key)
                self.stats["cache_hits"] += 1
                return cached.model, cached.tokenizer

        # Загружаем новую модель
        with self.model_load_lock:
            # Повторная проверка после получения лока
            if cache_key in self.model_cache:
                cached = self.model_cache[cache_key]
                cached.last_used = current_time
                self.stats["cache_hits"] += 1
                return cached.model, cached.tokenizer

            # Ограничиваем размер кэша
            self._enforce_cache_limit()

            try:
                if direction == "m2m100":
                    model_name = "facebook/m2m100_418M"
                else:
                    raise ValueError(f"Неизвестное направление: {direction}")

                logger.info(
                    f"[TRANSLATOR] [{time.time():.3f}] Начало загрузки модели {model_name} через Transformers..."
                )

                # Загрузка токенизатора и модели через Transformers
                tokenizer = M2M100Tokenizer.from_pretrained(model_name)
                model = M2M100ForConditionalGeneration.from_pretrained(model_name).to(self.device)

                logger.info(
                    f"[TRANSLATOR] [{time.time():.3f}] Модель {model_name} загружена через Transformers на {self.device}."
                )

                # Валидация модели: тест на простом переводе
                test_result = self._validate_model(model, tokenizer)
                if not test_result:
                    logger.error(f"[MODEL] Валидация модели {model_name} провалилась, модель может быть повреждена")
                    return None, None

                # Сохраняем в кэш
                cached_model = CachedModel(model, tokenizer, current_time)
                self.model_cache[cache_key] = cached_model
                self.tokenizer_cache[cache_key] = tokenizer
                self.stats["models_loaded"] += 1
                self.stats["cache_misses"] += 1

                # Проверяем память и очищаем при необходимости
                self._cleanup_old_models()

                return model, tokenizer

            except Exception as e:
                logger.error(f"[ERROR] [TRANSLATOR] [{time.time():.3f}] Ошибка загрузки модели {model_name}: {e}")
                traceback.print_exc()
                return None, None

    def _validate_model(self, model, tokenizer):
        """Валидирует модель на простом тестовом переводе"""
        try:
            test_text = "Hello world"
            tokenizer.src_lang = "en"
            encoded = tokenizer(test_text, return_tensors="pt").to(self.device)
            # Переводим с английского на русский для проверки работоспособности
            generated_tokens = model.generate(
                **encoded,
                forced_bos_token_id=tokenizer.get_lang_id("ru"),
                max_length=50,
                num_beams=5,
                repetition_penalty=2.0,
            )
            translation = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)[0]

            # Для M2M100 модель может давать повторения, проверяем только базовую функциональность
            if translation and len(translation.strip()) > 0 and len(translation.split()) >= 1:
                # Проверяем что перевод содержит русские буквы (поскольку переводим на русский)
                if any(char in "абвгдеёжзийклмнопрстуфхцчшщъыьэюя" for char in translation.lower()):
                    return True
                else:
                    logger.warning(f"[MODEL] Валидация: перевод не содержит русских букв '{translation}'")
                    return False
            else:
                logger.warning(f"[MODEL] Валидация провалилась: пустой или слишком короткий перевод '{translation}'")
                return False
        except Exception as e:
            logger.error(f"[MODEL] Ошибка валидации модели: {e}")
            return False

    def _translate_with_context_sync(self, texts, source_lang="en", target_lang="ru", context_window=2):
        """
        Синхронная версия translate_with_context для использования в пуле потоков
        """
        logger.debug(
            f"[TRANSLATOR] [{time.time():.3f}] _translate_with_context_sync: {source_lang} -> {target_lang}, {len(texts)} предложений"
        )

        # Прямой перевод с M2M100
        translated = self._translate_batch_sync(texts, source_lang, target_lang, context_window)

        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] _translate_with_context_sync завершена.")
        return translated

    def _translate_text_sync(self, text, source_lang="en", target_lang="ru", context_window=2):
        """Синхронная версия translate_text для использования в пуле потоков"""
        # Отключаем контекст для коротких текстов
        if len(text.split()) < 10:
            context_window = 0

        start_time = time.time()
        logger.debug(
            f"[TRANSLATOR] [{start_time:.3f}] Начало перевода: {source_lang} -> {target_lang}, текст длиной {len(text)} символов, слов: {len(text.split())}"
        )

        if source_lang == target_lang:
            result = self.text_processor.clean(text)
            end_time = time.time()
            logger.debug(
                f"[TRANSLATOR] [{end_time:.3f}] Языки совпадают, возврат без перевода. Время выполнения: {end_time - start_time:.3f} сек"
            )
            return result

        # Используем более надежный ключ для кэширования
        cache_key = f"{source_lang}_{target_lang}_{hash(text)}_{context_window}"
        if cache_key in self.translation_cache:
            cached_result = self.translation_cache[cache_key]
            end_time = time.time()
            logger.debug(
                f"[TRANSLATOR] [{end_time:.3f}] Результат найден в кэше. Время выполнения: {end_time - start_time:.3f} сек"
            )
            return cached_result

        # Предварительная обработка: замена терминов на переводы
        text = self._preprocess_text_with_terminology(text, target_lang)

        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] Прямой перевод {source_lang} -> {target_lang}")
        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] Токенизация текста...")

        # Используем spacy для разбиения на предложения
        nlp = self._get_spacy_model(source_lang)
        if nlp is None:
            sentences = [text]  # Если модель не найдена, не разбиваем
        else:
            doc = nlp(text)
            sentences = [sent.text.strip() for sent in doc.sents]

        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] Получено {len(sentences)} предложений")

        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] Начало перевода с контекстом (окно: {context_window})")
        translate_start = time.time()
        translated = " ".join(self._translate_with_context_sync(sentences, source_lang, target_lang, context_window))
        translate_time = time.time() - translate_start
        logger.debug(f"[TRANSLATOR] [{time.time():.3f}] Перевод завершен за {translate_time:.3f} сек")

        # Применяем пост-обработку
        result = self._postprocess_text(translated, target_lang)
        result = self.text_processor.clean(result)

        self.translation_cache[cache_key] = result
        end_time = time.time()
        total_time = end_time - start_time
        self.stats["total_translation_time"] += total_time
        logger.debug(f"[TRANSLATOR] [{end_time:.3f}] Перевод завершен. Общее время выполнения: {total_time:.3f} сек")
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

    def _prepare_sentences_for_batch(self, texts, source_lang):
        """Подготавливает предложения для пакетного перевода"""
        nlp = self._get_spacy_model(source_lang)
        if nlp is None:
            # Если модель не найдена, не разбиваем
            return texts, list(range(len(texts))), [1] * len(texts)

        all_sentences = []
        text_indices = []
        sentence_counts = []
        for i, text in enumerate(texts):
            doc = nlp(text)
            sentences = [sent.text.strip() for sent in doc.sents]
            sentence_counts.append(len(sentences))
            all_sentences.extend(sentences)
            text_indices.extend([i] * len(sentences))
        return all_sentences, text_indices, sentence_counts

    def _translate_sentence_batches(self, sentences, model, tokenizer, source_lang, target_lang, batch_size, beam_size):
        """Переводит батчи предложений"""
        translated_batches = []
        for i in range(0, len(sentences), batch_size):
            batch_sentences = sentences[i : i + batch_size]
            logger.debug(
                f"[TRANSLATOR] [BATCH] Перевод батча {i//batch_size + 1}/{(len(sentences)-1)//batch_size + 1}: {len(batch_sentences)} предложений"
            )

            # Адаптируем параметры в зависимости от длины текста
            text_length = len(" ".join(batch_sentences).split())
            if text_length < 20:  # Короткие тексты
                adapted_beam_size = min(beam_size or 5, 3)  # Меньше beam_size для коротких
                adapted_repetition_penalty = 1.5  # Меньше penalty для коротких
                adapted_length_penalty = 0.8  # Меньше length_penalty для коротких
            else:  # Длинные тексты
                adapted_beam_size = beam_size or 5
                adapted_repetition_penalty = 2.0
                adapted_length_penalty = 1.0

            # Токенизация через Transformers
            tokenizer.src_lang = source_lang
            encoded = tokenizer(batch_sentences, return_tensors="pt", padding=True, truncation=True).to(self.device)

            # Перевод через Transformers с target_prefix для M2M100
            generated_tokens = model.generate(
                **encoded,
                forced_bos_token_id=tokenizer.get_lang_id(target_lang),
                max_length=256,
                num_beams=adapted_beam_size,
                repetition_penalty=adapted_repetition_penalty,
            )

            # Декодирование
            batch_translations = tokenizer.batch_decode(generated_tokens, skip_special_tokens=True)

            # Проверяем каждую трансляцию на gibberish и заменяем на fallback если нужно
            for j in range(len(batch_translations)):
                if self._is_broken_translation(batch_translations[j]):
                    logger.warning(
                        f"[GIBBERISH] Обнаружен gibberish в батче, пробуем fallback для: '{batch_sentences[j][:50]}...'"
                    )
                    # Fallback: перевод с beam_size=1
                    fallback_encoded = tokenizer([batch_sentences[j]], return_tensors="pt").to(self.device)
                    fallback_tokens = model.generate(
                        **fallback_encoded,
                        forced_bos_token_id=tokenizer.get_lang_id(target_lang),
                        max_length=256,
                        num_beams=1,
                        repetition_penalty=2.0,
                    )
                    fallback_translation = tokenizer.batch_decode(fallback_tokens, skip_special_tokens=True)[0]
                    if not self._is_broken_translation(fallback_translation):
                        batch_translations[j] = fallback_translation
                        logger.info(f"[FALLBACK] Fallback успешен")
                    else:
                        logger.warning(f"[FALLBACK] Fallback тоже плохой, оставляем оригинал")

            translated_batches.extend(batch_translations)
        return translated_batches

    def _assemble_translated_texts(self, texts, translated_batches, sentence_counts, target_lang):
        """Собирает переведенные тексты из батчей"""
        result_texts = [""] * len(texts)
        current_pos = 0
        for i, (text, sent_count) in enumerate(zip(texts, sentence_counts)):
            if sent_count > 0:
                translated_sentences = translated_batches[current_pos : current_pos + sent_count]
                result_text = " ".join(translated_sentences)
                # Применяем пост-обработку
                result_text = self._postprocess_text(result_text, target_lang)
                result_texts[i] = result_text
                current_pos += sent_count
            else:
                result_texts[i] = text  # Пустой текст остается пустым
        return [self.text_processor.clean(text) for text in result_texts]

    def _translate_batch_sync(self, texts, source_lang="en", target_lang="ru", context_window=2, beam_size=None):
        """Синхронная версия translate_batch для использования в пуле потоков"""
        if not texts:
            return []

        logger.debug(
            f"[TRANSLATOR] [BATCH] Начало пакетного перевода: {source_lang} -> {target_lang}, {len(texts)} текстов"
        )

        # Предварительная обработка: замена терминов на переводы
        texts = [self._preprocess_text_with_terminology(t, target_lang) for t in texts]

        # Используем мультиязычную модель m2m100
        model, tokenizer = self._get_model("m2m100")
        if model is None or tokenizer is None:
            logger.error(f"[TRANSLATOR] [BATCH] Модель m2m100 не найдена или повреждена, возврат исходных текстов.")
            return texts

        # Подготавливаем предложения
        all_sentences, text_indices, sentence_counts = self._prepare_sentences_for_batch(texts, source_lang)
        if not all_sentences:
            return texts

        logger.debug(f"[TRANSLATOR] [BATCH] Всего предложений для перевода: {len(all_sentences)}")
        logger.debug(f"[TRANSLATOR] [BATCH] Используемое устройство: {self.device}")

        # Определяем оптимальный размер батча
        batch_size = self._get_optimal_batch_size()
        logger.debug(f"[TRANSLATOR] [BATCH] Оптимальный размер батча: {batch_size}")

        # Переводим предложения батчами
        translated_batches = self._translate_sentence_batches(
            all_sentences, model, tokenizer, source_lang, target_lang, batch_size, beam_size
        )

        # Собираем результаты
        cleaned_results = self._assemble_translated_texts(texts, translated_batches, sentence_counts, target_lang)
        logger.debug(f"[TRANSLATOR] [BATCH] Пакетный перевод завершен. Переведено {len(cleaned_results)} текстов")
        return cleaned_results

    async def translate_async(self, texts, source_lang, target_lang, context_window=2, beam_size=None):
        logger.debug(f"[TRANSLATOR] [ASYNC] Начало translate_async для задачи")
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
                    beam_size,
                )

                # Ждем его с таймаутом
                result = await asyncio.wait_for(future, timeout=120.0)
                self.stats["translations_processed"] += len(texts)
                return result
            except asyncio.TimeoutError:
                logger.error(f"[ERROR] [TRANSLATOR] ТАЙМАУТ (120 сек) для '{source_lang}' -> '{target_lang}'!")
                return texts
            except Exception as e:
                logger.error(f"[ERROR] [TRANSLATOR] Ошибка при переводе '{source_lang}' -> '{target_lang}': {e}")
                traceback.print_exc()
                return texts

    async def prepare_translations(
        self, title: str, content: str, original_lang: str, callback=None, error_callback=None, task_id=None
    ) -> dict:
        """Подготавливает переводы заголовка, содержания и категории на все целевые языки."""
        start_time = time.time()
        logger.info(
            f"[TRANSLATOR] prepare_translations начата для языка '{original_lang}' для задачи: {task_id[:20] if task_id else 'Unknown'}"
        )

        try:
            translations = {}
            target_languages = list(CHANNEL_IDS.keys())

            logger.debug(f"[TRANSLATOR] Целевые языки для перевода: {target_languages}")
            clean_title = self.text_processor.clean(title)
            clean_content = self.text_processor.clean(content)

            # - Обработка оригинального языка -
            # Всегда включаем оригинальный язык в словарь переводов
            translations[original_lang] = {"title": clean_title, "content": clean_content}
            logger.debug(f"[TRANSLATOR] Оригинальный язык '{original_lang}' включен в результаты без перевода.")

            # - Подготовка и выполнение переводов -
            translation_results = {}
            lang_pairs = []
            for target_lang in target_languages:
                if original_lang == target_lang:
                    continue
                pairs = [
                    (original_lang, target_lang, clean_title, "title"),
                    (original_lang, target_lang, clean_content, "content"),
                ]
                lang_pairs.append((original_lang, target_lang, pairs))

            logger.debug(f"[TRANSLATOR] [BATCH] Подготовлено {len(lang_pairs)} языковых пар для перевода.")

            # Выполняем переводы для каждой языковой пары
            for i, (src_lang, tgt_lang, texts_to_process) in enumerate(lang_pairs):
                logger.debug(
                    f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Перевод '{src_lang}' -> '{tgt_lang}': {len(texts_to_process)} текстов"
                )
                group_start_time = time.time()
                try:
                    # Извлекаем только тексты для перевода
                    texts_only = [text for _, _, text, _ in texts_to_process]

                    # Выполняем асинхронный перевод
                    translated_texts = await self.translate_async(texts_only, src_lang, tgt_lang)

                    group_duration = time.time() - group_start_time
                    logger.debug(
                        f"[TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] Группа '{src_lang}' -> '{tgt_lang}' обработана за {group_duration:.2f} сек."
                    )

                    # Сохраняем результаты
                    translation_results[(src_lang, tgt_lang)] = list(
                        zip([field_type for _, _, _, field_type in texts_to_process], translated_texts)
                    )
                except Exception as e:
                    group_duration = time.time() - group_start_time
                    error_msg = (
                        f"[ERROR] [TRANSLATOR] [BATCH] [{i+1}/{len(lang_pairs)}] "
                        f"Критическая ошибка для группы '{src_lang}' -> '{tgt_lang}' "
                        f"за {group_duration:.2f} сек: {e}"
                    )
                    logger.error(error_msg)
                    traceback.print_exc()
                    # В случае критической ошибки используем оригинальные тексты
                    translation_results[(src_lang, tgt_lang)] = [
                        (field_type, original_text) for _, _, original_text, field_type in texts_to_process
                    ]

            # - Компоновка финальных результатов -
            logger.debug("[TRANSLATOR] Начата компоновка финальных результатов переводов.")
            for target_lang in target_languages:
                if original_lang == target_lang:
                    continue

                lang_translations = {}

                src_lang = original_lang
                results = translation_results.get((src_lang, target_lang), [])
                for field_type, translated_text in results:
                    if field_type == "title":
                        original_text = clean_title
                    else:  # content
                        original_text = clean_content

                    # Проверяем, что перевод отличается от оригинала
                    if translated_text.strip() == original_text.strip():
                        logger.warning(
                            f"[TRANSLATOR] Перевод идентичен оригиналу для '{field_type}' на '{target_lang}', пропуск"
                        )
                        continue

                    # Проверяем, что перевод на правильном языке (содержит символы целевого языка)
                    if not self._check_translation_language(translated_text, target_lang):
                        logger.warning(
                            f"[LANG_CHECK] Перевод не на '{target_lang}' для '{field_type}': '{translated_text[:50]}...', пропуск"
                        )
                        continue

                    # Семантическая проверка для title и content
                    if not self._semantic_check(original_text, translated_text):
                        warn_msg = (
                            f"[SEMANTIC] [TRANSLATOR] Семантическая проверка не пройдена на '{target_lang}' "
                            f"для поля '{field_type}': '{translated_text[:50]}...'"
                        )
                        logger.warning(warn_msg)
                        # Попытка fallback: перевод с beam_size=1
                        fallback_texts = await self.translate_async(
                            [original_text], src_lang, tgt_lang, context_window=0, beam_size=1
                        )
                        fallback_text = fallback_texts[0] if fallback_texts else ""
                        if (
                            fallback_text
                            and fallback_text.strip() != original_text.strip()
                            and self._check_translation_language(fallback_text, target_lang)
                            and self._semantic_check(original_text, fallback_text)
                        ):
                            logger.info(f"[FALLBACK] Fallback перевод успешен для '{field_type}'")
                            lang_translations[field_type] = fallback_text
                        else:
                            # Дополнительный fallback: если gibberish, пробуем beam_size=20 для лучшего качества
                            if self._is_broken_translation(translated_text):
                                logger.warning(
                                    f"[GIBBERISH] Обнаружен gibberish, пробуем beam_size=20 для '{field_type}'"
                                )
                                fallback_texts_2 = await self.translate_async(
                                    [original_text], src_lang, tgt_lang, context_window=0, beam_size=20
                                )
                                fallback_text_2 = fallback_texts_2[0] if fallback_texts_2 else ""
                                if (
                                    fallback_text_2
                                    and fallback_text_2.strip() != original_text.strip()
                                    and self._check_translation_language(fallback_text_2, target_lang)
                                    and self._semantic_check(original_text, fallback_text_2)
                                    and not self._is_broken_translation(fallback_text_2)
                                ):
                                    logger.info(f"[FALLBACK2] Второй fallback успешен для '{field_type}'")
                                    lang_translations[field_type] = fallback_text_2
                                else:
                                    # Не добавляем поле, если перевод неудачный
                                    pass
                            else:
                                # Не добавляем поле, если перевод неудачный
                                pass
                    else:
                        lang_translations[field_type] = translated_text

                if "title" in lang_translations and "content" in lang_translations:
                    translations[target_lang] = lang_translations
                    logger.info(
                        f"[TRANSLATOR] Перевод на '{target_lang}' успешно добавлен в результаты ({len(lang_translations)} полей)."
                    )
                else:
                    warn_msg = f"[WARN] Перевод на '{target_lang}' не добавлен. " f"Нет заголовка или содержания."
                    logger.warning(warn_msg)

            # Удалить языки с одинаковыми переводами (чтобы не сохранять дубликаты битых переводов)
            seen_titles = set()
            to_remove = []
            for lang, data in translations.items():
                title = data.get("title", "")
                if title in seen_titles:
                    logger.warning(f"[TRANSLATOR] Удален дубликат перевода для языка '{lang}' (одинаковый title)")
                    to_remove.append(lang)
                else:
                    seen_titles.add(title)
            for lang in to_remove:
                del translations[lang]

            total_duration = time.time() - start_time
            logger.info(
                f"[TRANSLATOR] prepare_translations завершена за {total_duration:.2f} сек. Всего переводов: {len(translations)}"
            )
            # - ДОПОЛНИТЕЛЬНОЕ ЛОГИРОВАНИЕ ПЕРЕД ВОЗВРАТОМ -
            logger.debug(
                f"[TRANSLATOR] Подготовленный словарь переводов будет возвращен. Размер: {len(translations)} языков."
            )

            # --- ВЫЗОВ CALLBACK ---
            # Если передан callback, вызываем его с результатом
            if callback:
                logger.debug(
                    f"[TRANSLATOR] Вызов пользовательского callback для задачи: {task_id[:20] if task_id else 'Unknown'}"
                )
                try:
                    # Проверяем, является ли callback корутиной (async def)
                    if asyncio.iscoroutinefunction(callback):
                        await callback(translations, task_id=task_id)
                    else:
                        # Если это обычная функция, просто вызываем
                        callback(translations, task_id=task_id)
                    logger.debug(
                        f"[TRANSLATOR] Пользовательский callback успешно выполнен для задачи: {task_id[:20] if task_id else 'Unknown'}"
                    )
                except Exception as cb_error:
                    logger.error(
                        f"[TRANSLATOR] [ERROR] Ошибка в пользовательском callback для задачи {task_id[:20] if task_id else 'Unknown'}: {cb_error}"
                    )
                    traceback.print_exc()
                    # Если есть error_callback, уведомляем об ошибке в callback
                    if error_callback:
                        try:
                            error_data = {"error": f"Callback execution failed: {cb_error}", "task_id": task_id}
                            if asyncio.iscoroutinefunction(error_callback):
                                await error_callback(error_data)
                            else:
                                error_callback(error_data)
                        except Exception as ec_error:
                            logger.error(
                                f"[TRANSLATOR] [ERROR] Ошибка в error_callback при обработке ошибки callback: {ec_error}"
                            )
                            traceback.print_exc()
            # -----------------------

            return translations

        except Exception as e:
            # --- ОБРАБОТКА ОШИБОК prepare_translations ---
            error_msg = f"[TRANSLATOR] [CRITICAL ERROR] Критическая ошибка в prepare_translations для задачи {task_id[:20] if task_id else 'Unknown'}: {e}"
            logger.error(error_msg)
            traceback.print_exc()

            # Вызываем error_callback, если он передан
            if error_callback:
                logger.debug(
                    f"[TRANSLATOR] Вызов пользовательского error_callback для задачи: {task_id[:20] if task_id else 'Unknown'}"
                )
                try:
                    error_data = {"error": str(e), "task_id": task_id}
                    if asyncio.iscoroutinefunction(error_callback):
                        await error_callback(error_data)
                    else:
                        error_callback(error_data)
                    logger.debug(
                        f"[TRANSLATOR] Пользовательский error_callback успешно выполнен для задачи: {task_id[:20] if task_id else 'Unknown'}"
                    )
                except Exception as ec_error:
                    logger.error(f"[TRANSLATOR] [ERROR] Ошибка в error_callback: {ec_error}")
                    traceback.print_exc()
            # ---------------------------------------------
            # Возвращаем пустой словарь в случае ошибки
            return {}
            # ---------------------------------------------

    def get_stats(self):
        """Получение статистики использования переводчика"""
        return {
            **self.stats,
            "cached_models": len(self.model_cache),
            "cached_translations": len(self.translation_cache),
            "memory_usage_percent": self._check_memory_usage(),
        }

    def clear_cache(self):
        """Очистка всех кэшей"""
        self.model_cache.clear()
        self.tokenizer_cache.clear()
        self.translation_cache.clear()
        gc.collect()
        logger.info("[TRANSLATOR] Все кэши очищены")

    def preload_popular_models(self):
        """Предзагрузка мультиязычной модели"""
        try:
            logger.info("[PRELOAD] Загрузка мультиязычной модели m2m100_418M через Transformers...")
            # Загружаем модель
            self._get_model("m2m100")
            logger.info("[PRELOAD] Модель загружена и готова к использованию")
        except Exception as e:
            logger.error(f"[PRELOAD] Ошибка при загрузке модели: {e}")

    def shutdown(self):
        """Корректное завершение работы переводчика"""
        self.executor.shutdown(wait=True)
        self.clear_cache()
        logger.info("[TRANSLATOR] Переводчик корректно завершил работу")
