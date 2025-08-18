from deep_translator import GoogleTranslator

def translate_text(text, target_lang, source_lang='auto'):
    try:
        if not text.strip():
            return text
            
        return GoogleTranslator(source=source_lang, target=target_lang).translate(text)
    except Exception as e:
        print(f"Translation error: {e}")
        return text  # Возвращаем оригинал при ошибке