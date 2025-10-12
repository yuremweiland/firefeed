import os
import ctranslate2
from transformers import M2M100Tokenizer, M2M100ForConditionalGeneration
from config import CT2_MODELS_DIR

os.makedirs(CT2_MODELS_DIR, exist_ok=True)

def convert_model(hf_name, local_name):
    hf_path = os.path.join(CT2_MODELS_DIR, local_name + "_hf")
    ct2_path = os.path.join(CT2_MODELS_DIR, local_name)

    print(f"Загрузка {hf_name} → {hf_path}")
    tokenizer = M2M100Tokenizer.from_pretrained(hf_name)
    model = M2M100ForConditionalGeneration.from_pretrained(hf_name)
    tokenizer.save_pretrained(hf_path)
    model.save_pretrained(hf_path)

    print(f"Конвертация в CTranslate2 → {ct2_path}")
    converter = ctranslate2.converters.TransformersConverter(
        model_name_or_path=hf_path
    )
    converter.convert(ct2_path, quantization="float32", force=True)
    print(f"✅ Модель {local_name} успешно сконвертирована")

if __name__ == "__main__":
    convert_model("facebook/m2m100_418M", "m2m100_418M")