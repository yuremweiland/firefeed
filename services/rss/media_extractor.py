# services/rss/media_extractor.py
import logging
from typing import Dict, Any, Optional
from interfaces import IMediaExtractor
from utils.media_extractors import extract_image_from_rss_item, extract_video_from_rss_item

logger = logging.getLogger(__name__)


class MediaExtractor(IMediaExtractor):
    """Service for extracting media URLs from RSS items"""

    def extract_image(self, rss_item: Dict[str, Any]) -> Optional[str]:
        """Extract image URL from RSS item"""
        try:
            return extract_image_from_rss_item(rss_item)
        except Exception as e:
            logger.warning(f"Error extracting image from RSS item: {e}")
            return None

    def extract_video(self, rss_item: Dict[str, Any]) -> Optional[str]:
        """Extract video URL from RSS item"""
        try:
            return extract_video_from_rss_item(rss_item)
        except Exception as e:
            logger.warning(f"Error extracting video from RSS item: {e}")
            return None