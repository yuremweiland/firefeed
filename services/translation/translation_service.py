# services/translation/translation_service.py
import asyncio
import logging
import re
from typing import List, Dict, Any, Optional
from interfaces import ITranslationService, IModelManager, ITranslatorQueue

logger = logging.getLogger(__name__)


class TranslationService(ITranslationService):
    """Service for text translation operations"""

    def __init__(self, model_manager: IModelManager, translator_queue: ITranslatorQueue,
                 max_concurrent_translations: int = 3):
        self.model_manager = model_manager
        self.translator_queue = translator_queue
        self.max_concurrent_translations = max_concurrent_translations
        self.translation_semaphore = asyncio.Semaphore(max_concurrent_translations)

    async def translate_async(self, texts: List[str], source_lang: str, target_lang: str,
                            context_window: int = 2, beam_size: Optional[int] = None) -> List[str]:
        """Translate texts asynchronously"""
        if not texts:
            return []

        async with self.translation_semaphore:
            logger.debug(f"[TRANSLATE] Starting translation: {len(texts)} texts {source_lang} -> {target_lang}")

            try:
                # Get model for this direction
                model, tokenizer = await self.model_manager.get_model(source_lang, target_lang)

                # Prepare texts for translation
                sentences = self._prepare_sentences_for_batch(texts, source_lang)
                sentence_counts = [len(self._split_into_sentences(text)) for text in texts]

                # Determine optimal batch size
                batch_size = self._get_optimal_batch_size()

                # Translate in batches
                translated_sentences = await self._translate_sentence_batches(
                    sentences, model, tokenizer, source_lang, target_lang, batch_size, beam_size
                )

                # Assemble back into full texts
                translated_texts = self._assemble_translated_texts(texts, translated_sentences, sentence_counts, target_lang)

                # Post-process translations
                translated_texts = [self._postprocess_text(text, target_lang) for text in translated_texts]

                logger.debug(f"[TRANSLATE] Translation completed: {len(translated_texts)} texts")
                return translated_texts

            except Exception as e:
                logger.error(f"[TRANSLATE] Error translating {source_lang} -> {target_lang}: {e}")
                # Return original texts on error
                return texts

    async def prepare_translations(self, title: str, content: str, original_lang: str,
                                 target_langs: List[str]) -> Dict[str, Dict[str, str]]:
        """Prepare translations for title and content to multiple languages"""
        translations = {}

        for target_lang in target_langs:
            if target_lang == original_lang:
                continue

            try:
                logger.debug(f"[TRANSLATE] Preparing translation {original_lang} -> {target_lang}")

                # Translate title and content
                translated_title, translated_content = await asyncio.gather(
                    self.translate_async([title], original_lang, target_lang),
                    self.translate_async([content], original_lang, target_lang)
                )

                translations[target_lang] = {
                    "title": translated_title[0] if translated_title else title,
                    "content": translated_content[0] if translated_content else content
                }

            except Exception as e:
                logger.error(f"[TRANSLATE] Error preparing translation for {target_lang}: {e}")
                # Skip this language on error
                continue

        return translations

    def _split_into_sentences(self, text: str) -> List[str]:
        """Split text into sentences"""
        # Simple sentence splitting - can be improved with NLP libraries
        sentences = re.split(r'[.!?]+', text)
        return [s.strip() for s in sentences if s.strip()]

    def _prepare_sentences_for_batch(self, texts: List[str], source_lang: str) -> List[str]:
        """Prepare sentences for batch translation"""
        all_sentences = []
        for text in texts:
            sentences = self._split_into_sentences(text)
            all_sentences.extend(sentences)
        return all_sentences

    async def _translate_sentence_batches(self, sentences: List[str], model, tokenizer,
                                        source_lang: str, target_lang: str, batch_size: int,
                                        beam_size: Optional[int]) -> List[str]:
        """Translate sentences in batches"""
        translated_sentences = []

        for i in range(0, len(sentences), batch_size):
            batch = sentences[i:i + batch_size]

            try:
                # Tokenize batch
                inputs = tokenizer(batch, return_tensors="pt", padding=True, truncation=True, max_length=512)

                # Generate translations
                outputs = model.generate(
                    **inputs,
                    max_length=512,
                    num_beams=beam_size or 4,
                    early_stopping=True
                )

                # Decode translations
                batch_translations = tokenizer.batch_decode(outputs, skip_special_tokens=True)
                translated_sentences.extend(batch_translations)

            except Exception as e:
                logger.error(f"[TRANSLATE] Error in batch translation: {e}")
                # Add original sentences on error
                translated_sentences.extend(batch)

        return translated_sentences

    def _assemble_translated_texts(self, original_texts: List[str], translated_sentences: List[str],
                                 sentence_counts: List[int], target_lang: str) -> List[str]:
        """Assemble translated sentences back into full texts"""
        translated_texts = []
        sentence_idx = 0

        for original_text, count in zip(original_texts, sentence_counts):
            if sentence_idx + count <= len(translated_sentences):
                translated_sentences_for_text = translated_sentences[sentence_idx:sentence_idx + count]
                translated_text = '. '.join(translated_sentences_for_text)
                if original_text.endswith('!'):
                    translated_text = translated_text.replace('.', '!')
                elif original_text.endswith('?'):
                    translated_text = translated_text.replace('.', '?')
                translated_texts.append(translated_text)
            else:
                # Fallback to original if something went wrong
                translated_texts.append(original_text)

            sentence_idx += count

        return translated_texts

    def _postprocess_text(self, text: str, target_lang: str) -> str:
        """Post-process translated text"""
        if not text:
            return text

        # Basic post-processing - can be extended
        text = text.strip()

        # Fix common translation artifacts
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces
        text = re.sub(r'\s*([.!?])', r'\1', text)  # Spaces before punctuation

        return text

    def _get_optimal_batch_size(self) -> int:
        """Determine optimal batch size based on available memory"""
        # Simple heuristic - can be improved with actual memory monitoring
        try:
            import psutil
            available_memory = psutil.virtual_memory().available / (1024 * 1024 * 1024)  # GB

            if available_memory > 8:
                return 16
            elif available_memory > 4:
                return 8
            elif available_memory > 2:
                return 4
            else:
                return 2
        except ImportError:
            # Fallback if psutil not available
            return 4