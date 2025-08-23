from transformers import MarianMTModel, MarianTokenizer
from functools import lru_cache
from firefeed_utils import clean_html
import asyncio
import torch
import nltk
import os
from config import CHANNEL_IDS

# Установка пути для данных NLTK
nltk_data_path = '/var/www/firefeed/data/nltk_data'
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

_model_cache = {}
_tokenizer_cache = {}
_translation_cache = {}

# Языковые пары, требующие каскадного перевода через английский
CASCADE_TRANSLATIONS = {
    ('ru', 'de'): ('ru', 'en', 'de')
}

def get_translator_model(src_lang, tgt_lang):
    cache_key = f"{src_lang}-{tgt_lang}"
    if cache_key not in _model_cache:
        try:
            model_name = f'Helsinki-NLP/opus-mt-{src_lang}-{tgt_lang}'
            model = MarianMTModel.from_pretrained(model_name)
            tokenizer = MarianTokenizer.from_pretrained(model_name)
            _model_cache[cache_key] = model
            _tokenizer_cache[cache_key] = tokenizer
        except Exception as e:
            print(f"Модель {model_name} не найдена: {e}")
            return None, None
    return _model_cache[cache_key], _tokenizer_cache[cache_key]

def translate_with_context(texts, source_lang='en', target_lang='ru', context_window=2):
    """
    Переводит список текстов с учётом контекста.
    
    Args:
        texts (list): Список предложений для перевода.
        context_window (int): Количество предыдущих предложений для контекста.
    """
    # Проверяем, нужен ли каскадный перевод
    cascade_key = (source_lang, target_lang)
    if cascade_key in CASCADE_TRANSLATIONS:
        # Используем каскадный перевод через английский
        src_lang, intermediate_lang, tgt_lang = CASCADE_TRANSLATIONS[cascade_key]
        
        # Переводим на промежуточный язык (английский)
        intermediate_texts = translate_with_context(texts, src_lang, intermediate_lang, context_window)
        
        # Переводим с промежуточного на целевой язык
        return translate_with_context(intermediate_texts, intermediate_lang, tgt_lang, context_window)
    
    model, tokenizer = get_translator_model(source_lang, target_lang)
    if model is None or tokenizer is None:
        return texts  # Если модель не найдена, возвращаем исходный текст
    
    translated = []
    
    for i in range(len(texts)):
        context = " ".join(texts[max(0, i-context_window):i])
        current_text = texts[i]
        
        combined = f"{context} {current_text}" if context else current_text
        
        inputs = tokenizer(combined, return_tensors="pt", truncation=True, max_length=512)
        with torch.no_grad():
            outputs = model.generate(**inputs)
        translated_text = tokenizer.decode(outputs[0], skip_special_tokens=True)
        
        # Удаляем контекст из результата (если нужно)
        if context:
            # Это упрощённый подход - в реальности может потребоваться более сложная постобработка
            translated_text = translated_text.replace(translate_text(context, source_lang, target_lang), "").strip()
        
        translated.append(translated_text)
    
    return translated

@lru_cache(maxsize=1000)
def cached_translate_text(text, source_lang, target_lang):
    return translate_text(text, source_lang, target_lang)

def translate_text(text, source_lang='en', target_lang='ru', context_window=2):
    if source_lang == target_lang:
        return clean_html(text)

    cache_key = f"{source_lang}_{target_lang}_{hash(text)}"
    if cache_key in _translation_cache:
        return _translation_cache[cache_key]

    # Проверяем, нужен ли каскадный перевод
    cascade_key = (source_lang, target_lang)
    if cascade_key in CASCADE_TRANSLATIONS:
        # Используем каскадный перевод через английский
        src_lang, intermediate_lang, tgt_lang = CASCADE_TRANSLATIONS[cascade_key]
        
        # Переводим на промежуточный язык (английский)
        intermediate_text = translate_text(text, src_lang, intermediate_lang, context_window)
        
        # Переводим с промежуточного на целевой язык
        result = translate_text(intermediate_text, intermediate_lang, tgt_lang, context_window)
    else:
        sentences = nltk.sent_tokenize(text)
        translated = " ".join(translate_with_context(sentences, source_lang, target_lang, context_window))
        result = clean_html(translated)

    _translation_cache[cache_key] = result
    return result

async def prepare_translations(title: str, description: str, category: str, original_lang: str) -> dict:
    """
    Подготавливает переводы заголовка, описания и категории на все целевые языки.

    :param title: Оригинальный заголовок.
    :param description: Оригинальное описание.
    :param category: Категория (предположительно на английском).
    :param original_lang: Оригинальный язык новости.
    :return: Словарь переводов вида {
        'ru': {'title': '...', 'description': '...', 'category': '...'},
        'en': {...},
        ...
    }
    """
    translations = {}
    target_languages = list(CHANNEL_IDS.keys()) # ['ru', 'en', 'de', 'fr']

    # Очищаем оригинальный текст один раз
    clean_title = clean_html(title)
    clean_description = clean_html(description)

    tasks = [] # Для параллельного выполнения переводов

    for target_lang in target_languages:
        # Копируем оригинальные данные на случай, если перевод не нужен или произойдет ошибка
        trans_title = clean_title
        trans_description = clean_description
        trans_category = category

        needs_translation = original_lang != target_lang

        if needs_translation:
            # Создаем задачи для асинхронного перевода
            # title_task = asyncio.create_task(translate_text_async(clean_title, original_lang, target_lang))
            # desc_task = asyncio.create_task(translate_text_async(clean_description, original_lang, target_lang))
            # cat_task = asyncio.create_task(translate_text_async(category, 'en', target_lang)) # Предполагаем, что категория на английском
            # tasks.append((target_lang, title_task, desc_task, cat_task))
            
            # --- Если translate_text НЕ асинхронная, делаем последовательно ---
            try:
                trans_title = translate_text(clean_title, original_lang, target_lang)
                trans_description = translate_text(clean_description, original_lang, target_lang)
                # Переводим категорию, если она не на целевом языке (предполагаем en как базовый для категорий)
                trans_category = translate_text(category, 'en', target_lang) 

            except Exception as e:
                print(f"[ERROR] Ошибка перевода на {target_lang}: {e}. Используются оригинальные данные.")
                # В случае ошибки перевода, используем оригинальные данные
                
        # Сохраняем результаты (или оригинальные данные) для этого языка
        translations[target_lang] = {
            'title': trans_title,
            'description': trans_description,
            'category': trans_category
        }
        
    # --- Если бы translate_text была async, использовали бы gather ---
    # results = await asyncio.gather(*(task[1] for task in tasks), return_exceptions=True)
    # for i, (target_lang, _, _, _) in enumerate(tasks):
    #     if isinstance(results[i], Exception):
    #          print(f"[ERROR] Ошибка перевода на {target_lang}: {results[i]}. Используются оригинальные данные.")
    #          # Используем оригинальные данные
    #     else:
    #         # Предполагаем, что результаты возвращаются в том же порядке
    #         title_res, desc_res, cat_res = results[i] # Нужно адаптировать возврат из translate_text_async
    #         translations[target_lang] = {
    #             'title': title_res,
    #             'description': desc_res,
    #             'category': cat_res
    #         }

    return translations