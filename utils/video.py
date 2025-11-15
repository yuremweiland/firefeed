import os
import hashlib
import logging
from urllib.parse import urlparse
from config import VIDEOS_ROOT_DIR, HTTP_VIDEOS_ROOT_DIR, VIDEO_FILE_EXTENSIONS
from datetime import datetime
import aiohttp
import aiofiles

logger = logging.getLogger(__name__)



class VideoProcessor:
    """Class for processing and downloading videos"""

    @staticmethod
    async def download_and_save_video(url, rss_item_id, save_directory=VIDEOS_ROOT_DIR):
        """
        Downloads the video and saves it locally with a filename based on rss_item_id.
        Saves to path: save_directory/YYYY/MM/DD/{rss_item_id}{ext}

        :param url: Video URL
        :param rss_item_id: Unique RSS item ID for DB
        :param save_directory: Directory for saving videos
        :return: Path to saved file or None
        """
        if not url or not rss_item_id:
            logger.debug(f"[DEBUG] Video saving skipped: no URL ({url}) or rss_item_id ({rss_item_id})")
            return None

        try:
            # Use current time to form path
            created_at = datetime.now()
            date_path = created_at.strftime("%Y/%m/%d")
            full_save_directory = os.path.join(save_directory, date_path)

            logger.debug(f"[DEBUG] Starting to save video from {url} to {full_save_directory}")
            os.makedirs(full_save_directory, exist_ok=True)

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,video/webm,video/ogg,video/*,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
            }

            # Use aiohttp for asynchronous downloading
            timeout = aiohttp.ClientTimeout(total=30)  # Longer timeout for videos
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url, headers=headers) as response:
                    response.raise_for_status()

                    content_type = response.headers.get("Content-Type", "").lower()
                    content_lower = content_type.lower()
                    extension = ".mp4"

                    # Check content_type
                    for ext in VIDEO_FILE_EXTENSIONS:
                        if ext[1:] in content_lower:
                            extension = ext
                            break
                    else:
                        # Check URL
                        parsed_url = urlparse(url)
                        path = parsed_url.path
                        if path.lower().endswith(tuple(VIDEO_FILE_EXTENSIONS)):
                            extension = os.path.splitext(path)[1].lower()

                    safe_rss_item_id = "".join(c for c in str(rss_item_id) if c.isalnum() or c in ("-", "_")).rstrip()
                    if not safe_rss_item_id:
                        safe_rss_item_id = hashlib.md5(url.encode()).hexdigest()

                    filename = f"{safe_rss_item_id}{extension}"
                    file_path = os.path.join(full_save_directory, filename)

                    # Check if file already exists
                    if os.path.exists(file_path):
                        logger.info(f"[LOG] Video already exists on server: {file_path}")
                        # Return relative path from save_directory
                        relative_path = os.path.relpath(file_path, save_directory)
                        return relative_path

                    # Read content asynchronously
                    content = await response.read()

                    # Save file asynchronously
                    async with aiofiles.open(file_path, "wb") as f:
                        await f.write(content)

            logger.info(f"[LOG] Video successfully saved: {file_path}")
            # Return relative path from save_directory
            relative_path = os.path.relpath(file_path, save_directory)
            return relative_path

        except OSError as e:
            logger.warning(
                f"[WARN] Filesystem error when saving video {url} to {full_save_directory}: {e}"
            )
            return None
        except Exception as e:
            logger.warning(f"[WARN] Unexpected error when downloading/saving video {url}: {e}")
            return None

    @staticmethod
    async def process_video_from_url(url, rss_item_id):
        """
        Process video from URL - download and save it locally.

        :param url: URL of the video
        :param rss_item_id: RSS item ID for filename generation
        :return: local file path or None
        """
        if not url or not rss_item_id:
            return None

        return await VideoProcessor.download_and_save_video(url, rss_item_id)