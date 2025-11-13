import os
import logging
import asyncio
import aiohttp
import requests
from ..config import Config

logger = logging.getLogger(__name__)


class ImageDownloadManager:
    """Async image download manager"""

    def __init__(self, config: Config):
        self.max_concurrent = config.max_concurrent_downloads
        self.timeout_seconds = config.request_timeout
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        self.session = None

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def download_image(self, url, dest_folder, filename):
        """
        Asynchronously download a single image
        (Original logic copied)
        """
        async with self.semaphore:
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()
                    file_path = os.path.join(dest_folder, filename)
                    with open(file_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    logger.debug(f"Successfully downloaded image: {filename}")
                    return True
            except aiohttp.ClientError as e:
                logger.error(f"Failed to download image from {url}: {e}")
                return False
            except IOError as e:
                logger.error(f"Failed to save image {filename}: {e}")
                return False
            except Exception as e:
                logger.error(f"Unexpected error downloading {filename}: {e}")
                return False

    async def download_images_batch(self, download_tasks):
        """
        Asynchronously download a batch of images
        (Original logic copied)
        """
        if not download_tasks:
            return {}
        logger.info(f"Starting batch download of {len(download_tasks)} images...")
        tasks = []
        for url, dest_folder, filename in download_tasks:
            task = self.download_image(url, dest_folder, filename)
            tasks.append((filename, task))

        results = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

        for (filename, _), result in zip(tasks, completed_tasks):
            if isinstance(result, Exception):
                logger.error(f"Exception during download of {filename}: {result}")
                results[filename] = False
            else:
                results[filename] = result

        successful_downloads = sum(1 for success in results.values() if success)
        logger.info(f"Completed batch download: {successful_downloads}/{len(download_tasks)} successful")
        return results


def download_image(url: str, dest_folder: str, filename: str, config: Config):
    """
    Synchronously download an image (for fallback)
    """
    try:
        response = requests.get(url, timeout=config.request_timeout)  # Use config
        response.raise_for_status()
        with open(os.path.join(dest_folder, filename), 'wb') as f:
            f.write(response.content)
        logger.debug(f"Successfully downloaded image (sync): {filename}")
        return True
    except requests.RequestException as e:
        logger.error(f"Failed to download image (sync) from {url}: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to save image (sync) {filename}: {e}")
        return False


def collect_image_info(elem, inline_objects, img_count_ref, image_prefix=None, use_relative_path=False):
    """
    Collect image info without downloading
    (Original logic copied)
    """
    object_id = elem['inlineObjectElement']['inlineObjectId']
    inline_obj = inline_objects.get(object_id)
    if not inline_obj:
        return None, None, None

    embedded_obj = inline_obj['inlineObjectProperties']['embeddedObject']
    img_url = None

    if 'imageProperties' in embedded_obj and 'contentUri' in embedded_obj['imageProperties']:
        img_url = embedded_obj['imageProperties']['contentUri']
    elif 'imageProperties' in embedded_obj and 'sourceUri' in embedded_obj['imageProperties']:
        img_url = embedded_obj['imageProperties']['sourceUri']

    if img_url:
        img_ext = os.path.splitext(img_url.split('?')[0])[1]
        if not img_ext or len(img_ext) > 5:
            img_ext = '.png'

        if image_prefix:
            img_filename = f'{image_prefix}_image_{img_count_ref[0]}{img_ext}'
        else:
            img_filename = f'image_{img_count_ref[0]}{img_ext}'
        img_count_ref[0] += 1

        images_path = "../images/" if use_relative_path else "./images/"
        img_tag = f'<img src="{images_path}{img_filename}" />'
        return img_url, img_filename, img_tag

    return None, None, None


def process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref, config: Config, image_prefix=None):
    """
    Synchronously process an image (for simple cases, e.g., in tables)
    """
    img_url, img_filename, img_tag = collect_image_info(elem, inline_objects, img_count_ref, image_prefix)
    if img_url and img_filename:
        if download_image(img_url, images_dir, img_filename, config):  # Pass config
            return img_tag
    return ''


async def collect_all_images_from_document(doc, image_prefix=None):
    """
    Collect info for all images in a document for batch downloading
    (Original logic copied)
    """
    body = doc.get('body', {}).get('content', [])
    inline_objects = doc.get('inlineObjects', {})
    download_tasks = []
    paragraph_image_map = {}
    table_image_map = {}
    url_to_filename = {}
    img_count = [1]  # Use list to pass by reference

    def process_inline_object_element(elem, use_relative_path=False):
        object_id = elem['inlineObjectElement']['inlineObjectId']
        img_url, img_filename, img_tag = collect_image_info(elem, inline_objects, img_count, image_prefix,
                                                            use_relative_path)
        if img_url and img_filename:
            if img_url not in url_to_filename:
                url_to_filename[img_url] = img_filename
                download_tasks.append((img_url, None, img_filename))
            else:
                existing_filename = url_to_filename[img_url]
                images_path = "../images/" if use_relative_path else "./images/"
                # Create alt="Image" for validity
                img_tag = f'<img src="{images_path}{existing_filename}" alt="Image" />'
                img_filename = existing_filename
            return object_id, img_filename, img_tag
        return None, None, None

    for element in body:
        if 'paragraph' in element:
            para = element['paragraph']
            for elem in para.get('elements', []):
                if 'inlineObjectElement' in elem:
                    object_id, img_filename, img_tag = process_inline_object_element(elem, use_relative_path=False)
                    if object_id:
                        paragraph_image_map[object_id] = (img_filename, img_tag)

        elif 'table' in element:
            table = element['table']
            for row in table.get('tableRows', []):
                for cell in row.get('tableCells', []):
                    for cell_content in cell.get('content', []):
                        if 'paragraph' in cell_content:
                            para = cell_content['paragraph']
                            for elem in para.get('elements', []):
                                if 'inlineObjectElement' in elem:
                                    object_id, img_filename, img_tag = process_inline_object_element(elem,
                                                                                                     use_relative_path=True)
                                    if object_id:
                                        table_image_map[object_id] = (img_filename, img_tag)

    logger.info(f"Collected {len(download_tasks)} unique images for download")
    return download_tasks, paragraph_image_map, table_image_map