import os
import requests
import time
import tempfile
import shutil
import re
import logging
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor

# Import Google API client libraries for authentication and service access
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import paramiko

# Import Google Sheets API client for reading spreadsheet data
from googleapiclient.discovery import build as build_google_sheet

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Define required scopes for Google API access
SCOPES = [
    'https://www.googleapis.com/auth/documents.readonly',
    'https://www.googleapis.com/auth/drive.readonly',
    'https://www.googleapis.com/auth/spreadsheets.readonly'
]

# Constants
GRAY_BACKGROUND_RGB = 217.0 / 255.0
GRAY_TOLERANCE = 0.25
REQUEST_TIMEOUT = 120
MAX_CONCURRENT_DOWNLOADS = 3  # Максимальное количество одновременных загрузок
MAX_DOCUMENT_SIZE = 150000  # Максимальный размер документа в символах
CHUNK_SIZE = 50000  # Размер чанка для обработки больших документов

class DocumentSizeError(Exception):
    """Exception raised when document is too large"""
    pass


def check_document_size(doc):
    """
    Проверить размер документа перед обработкой

    Args:
        doc: Google Docs document object

    Returns:
        tuple: (estimated_size, element_count)

    Raises:
        DocumentSizeError: If document is too large
    """
    try:
        # Получаем базовую информацию о документе
        body = doc.get('body', {}).get('content', [])

        # Подсчитываем примерный размер
        estimated_size = 0
        element_count = 0

        for element in body:
            element_count += 1

            # Подсчитываем размер текста в параграфах
            if 'paragraph' in element:
                para = element['paragraph']
                for elem in para.get('elements', []):
                    if 'textRun' in elem:
                        text_content = elem['textRun'].get('content', '')
                        estimated_size += len(text_content)

            # Подсчитываем размер таблиц
            elif 'table' in element:
                table = element['table']
                for row in table.get('tableRows', []):
                    for cell in row.get('tableCells', []):
                        for cell_content in cell.get('content', []):
                            if 'paragraph' in cell_content:
                                para = cell_content['paragraph']
                                for elem in para.get('elements', []):
                                    if 'textRun' in elem:
                                        text_content = elem['textRun'].get('content', '')
                                        estimated_size += len(text_content)

        logger.info(f"Document size estimate: {estimated_size} characters, {element_count} elements")

        # Проверяем лимиты
        if estimated_size > MAX_DOCUMENT_SIZE:
            raise DocumentSizeError(f"Document too large: {estimated_size} characters (max: {MAX_DOCUMENT_SIZE})")

        if element_count > 10000:
            logger.warning(f"Document has many elements: {element_count}. Processing may be slow.")

        return estimated_size, element_count

    except Exception as e:
        logger.error(f"Failed to check document size: {e}")
        raise


def get_document_with_retry(docs_service, document_id, max_retries=3):
    """
    Получить документ с повторными попытками при ошибках

    Args:
        docs_service: Google Docs API service
        document_id: ID документа
        max_retries: Максимальное количество попыток

    Returns:
        dict: Document object

    Raises:
        Exception: If all retries failed
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to fetch document (attempt {attempt + 1}/{max_retries})")

            # Используем частичный запрос для уменьшения нагрузки
            doc = docs_service.documents().get(
                documentId=document_id,
                # Можно ограничить поля, если не нужны все данные
                # fields='body,inlineObjects,lists,namedRanges,bookmarks,title'
            ).execute()

            logger.info(f"Successfully fetched document on attempt {attempt + 1}")
            return doc

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                # Экспоненциальная задержка между попытками
                delay = 2 ** attempt
                logger.info(f"Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise


def process_document_in_chunks(doc, chunk_size=CHUNK_SIZE):
    """
    ИСПРАВЛЕННАЯ: Обработать документ частями для больших документов
    Не разбивает таблицы на части

    Args:
        doc: Google Docs document object
        chunk_size: Размер чанка в символах

    Returns:
        list: Список обработанных частей
    """
    body = doc.get('body', {}).get('content', [])

    chunks = []
    current_chunk = []
    current_size = 0

    for element in body:
        # Примерно оцениваем размер элемента
        element_size = estimate_element_size(element)

        # ИСПРАВЛЕНИЕ: Если это таблица, добавляем её целиком в чанк
        if 'table' in element:
            # Если добавление таблицы превысит размер чанка и в чанке уже есть элементы
            if current_size + element_size > chunk_size and current_chunk:
                # Завершаем текущий чанк
                chunks.append(current_chunk)
                current_chunk = [element]
                current_size = element_size
            else:
                # Добавляем таблицу в текущий чанк
                current_chunk.append(element)
                current_size += element_size
        else:
            # Обычная логика для параграфов и других элементов
            if current_size + element_size > chunk_size and current_chunk:
                # Завершаем текущий чанк
                chunks.append(current_chunk)
                current_chunk = [element]
                current_size = element_size
            else:
                current_chunk.append(element)
                current_size += element_size

    # Добавляем последний чанк
    if current_chunk:
        chunks.append(current_chunk)

    logger.info(f"Document split into {len(chunks)} chunks")
    return chunks

def estimate_element_size(element):
    """
    Примерно оценить размер элемента документа

    Args:
        element: Document element

    Returns:
        int: Estimated size in characters
    """
    size = 0

    if 'paragraph' in element:
        para = element['paragraph']
        for elem in para.get('elements', []):
            if 'textRun' in elem:
                text_content = elem['textRun'].get('content', '')
                size += len(text_content)

    elif 'table' in element:
        table = element['table']
        for row in table.get('tableRows', []):
            for cell in row.get('tableCells', []):
                for cell_content in cell.get('content', []):
                    if 'paragraph' in cell_content:
                        para = cell_content['paragraph']
                        for elem in para.get('elements', []):
                            if 'textRun' in elem:
                                text_content = elem['textRun'].get('content', '')
                                size += len(text_content)

    return size


async def convert_gdoc_to_markdown_large(document_id, output_md_path, images_dir, creds, image_prefix=None):
    """
    УЛУЧШЕННАЯ функция конвертации для больших документов

    Args:
        document_id (str): ID Google документа
        output_md_path (str): Путь для сохранения markdown файла
        images_dir (str): Директория для сохранения изображений
        creds: Google API credentials
        image_prefix (str, optional): Префикс для имен файлов изображений
    """
    try:
        # Build Google Docs API service
        docs_service = build('docs', 'v1', credentials=creds)

        # Получаем документ с повторными попытками
        doc = get_document_with_retry(docs_service, document_id)

        # Проверяем размер документа
        estimated_size, element_count = check_document_size(doc)

        # Создаем директорию для изображений
        os.makedirs(images_dir, exist_ok=True)

        # Извлекаем bookmarks и headers
        bookmarks, headers = extract_bookmarks_and_headers(doc)

        # Для очень больших документов используем обработку по частям
        if estimated_size > CHUNK_SIZE:
            logger.info(f"Large document detected ({estimated_size} chars), using chunked processing")
            await convert_large_document_chunked(doc, output_md_path, images_dir, creds, image_prefix, bookmarks,
                                                 headers)
        else:
            # Обычная обработка для небольших документов
            await convert_gdoc_to_markdown_standard(doc, output_md_path, images_dir, creds, image_prefix, bookmarks,
                                                    headers)

    except DocumentSizeError as e:
        logger.error(f"Document size error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing large document: {e}")
        raise


async def convert_gdoc_to_markdown_standard(doc, output_md_path, images_dir, creds, image_prefix, bookmarks, headers):
    """
    Стандартная обработка документа (выделена из основной функции)
    """
    # Собираем все изображения для пакетной загрузки
    download_tasks, paragraph_image_map, table_image_map = await collect_all_images_from_document(doc, image_prefix)

    # Загружаем все изображения параллельно
    image_download_map = {}
    if download_tasks:
        # Обновляем пути к папке изображений в задачах
        updated_tasks = [(url, images_dir, filename) for url, _, filename in download_tasks]

        async with ImageDownloadManager() as download_manager:
            image_download_map = await download_manager.download_images_batch(updated_tasks)

    # Extract document content and metadata
    body = doc.get('body').get('content')
    inline_objects = doc.get('inlineObjects', {})
    lists = doc.get('lists', {})
    md_lines = []
    img_count = 1
    list_counters = {}
    in_code_block = False
    code_block_lines = []

    # Process each element in the document body
    for idx, element in enumerate(body):
        if 'paragraph' in element:
            para = element['paragraph']
            elements = para.get('elements', [])
            paragraph_style = para.get('paragraphStyle', {})
            named_style = paragraph_style.get('namedStyleType', '')
            bullet = para.get('bullet')
            line = ""
            previous_endswith_alnum = False

            # Detect code block
            is_code_block = detect_code_block(para)

            if is_code_block and not in_code_block:
                in_code_block = True
                code_block_lines = []
            elif not is_code_block and in_code_block:
                if code_block_lines:
                    md_lines.append('```')
                    md_lines.extend(code_block_lines)
                    md_lines.append('```')
                    code_block_lines = []
                in_code_block = False

            # Handle lists
            if bullet:
                list_marker, list_type = process_list_item_in_paragraph(para, lists, list_counters)
                if list_marker:
                    line += list_marker

                    # ИСПРАВЛЕНИЕ: Используем новую функцию для обработки содержимого списка
                    list_content = process_list_content_with_line_breaks(
                        elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers
                    )
                    line += list_content
            elif named_style.startswith("HEADING_"):
                level = int(named_style.replace("HEADING_", ""))
                line += "#" * level + " "

                # Process paragraph elements для заголовков
                for elem in elements:
                    if 'inlineObjectElement' in elem:
                        object_id = elem['inlineObjectElement']['inlineObjectId']
                        if object_id in paragraph_image_map:
                            img_filename, img_tag = paragraph_image_map[object_id]
                            if image_download_map.get(img_filename, False):
                                if line and line[-1].isalnum():
                                    line += " "
                                line += f"![Image](./images/{img_filename})"
                    elif 'textRun' in elem:
                        run_text = elem['textRun'].get('content', '')
                        text_style = elem['textRun'].get('textStyle', {})
                        font_family = text_style.get('fontFamily', '')
                        is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                                      ['courier', 'monaco',
                                                       'monospace']) and not font_family.lower() == 'consolas'
                        if is_inline_code_fallback and not in_code_block:
                            processed = f"`{run_text.strip()}`"
                        else:
                            processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)
                        if line and previous_endswith_alnum and processed and processed[0].isalnum():
                            line += ' '
                        line += processed
                        previous_endswith_alnum = processed[-1].isalnum() if processed else False
            else:
                # Process paragraph elements для обычных параграфов
                for elem in elements:
                    if 'inlineObjectElement' in elem:
                        object_id = elem['inlineObjectElement']['inlineObjectId']
                        if object_id in paragraph_image_map:
                            img_filename, img_tag = paragraph_image_map[object_id]
                            if image_download_map.get(img_filename, False):
                                if line and line[-1].isalnum():
                                    line += " "
                                line += f"![Image](./images/{img_filename})"
                    elif 'textRun' in elem:
                        run_text = elem['textRun'].get('content', '')
                        text_style = elem['textRun'].get('textStyle', {})
                        font_family = text_style.get('fontFamily', '')
                        is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                                      ['courier', 'monaco',
                                                       'monospace']) and not font_family.lower() == 'consolas'
                        if is_inline_code_fallback and not in_code_block:
                            processed = f"`{run_text.strip()}`"
                        else:
                            processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)
                        if line and previous_endswith_alnum and processed and processed[0].isalnum():
                            line += ' '
                        line += processed
                        previous_endswith_alnum = processed[-1].isalnum() if processed else False

            if in_code_block and is_code_block:
                code_text = ""
                for elem in elements:
                    if 'textRun' in elem:
                        code_text += elem['textRun'].get('content', '')
                code_block_lines.append(code_text.rstrip())
            else:
                if line.strip():
                    line_with_anchor = add_header_anchors(line, named_style)
                    md_lines.append(line_with_anchor.rstrip())

        elif 'table' in element:
            if in_code_block and code_block_lines:
                md_lines.append('```')
                md_lines.extend(code_block_lines)
                md_lines.append('```')
                code_block_lines = []
                in_code_block = False

            img_count_ref = [img_count]
            md_table = table_to_markdown(element['table'], inline_objects, images_dir, img_count_ref,
                                         bookmarks, headers, lists, image_prefix, table_image_map, image_download_map)
            img_count = img_count_ref[0]
            if md_table.strip():
                md_lines.append(md_table)

    if in_code_block and code_block_lines:
        md_lines.append('```')
        md_lines.extend(code_block_lines)
        md_lines.append('```')

    # Join markdown content with smart line spacing
    content = join_markdown_lines_smart(md_lines)

    # Write initial markdown content to file
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(content)

    # Apply enhanced post-processing for code block markers
    with open(output_md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Process the markers
    processed_content = post_process_markdown_code_blocks(content)

    # Write back the processed content
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    logger.info(f"Successfully converted document to {output_md_path}")


async def convert_large_document_chunked(doc, output_md_path, images_dir, creds, image_prefix, bookmarks, headers):
    """
    Обработка больших документов по частям
    """
    # Собираем все изображения
    download_tasks, paragraph_image_map, table_image_map = await collect_all_images_from_document(doc, image_prefix)

    # Загружаем изображения небольшими порциями
    image_download_map = {}
    if download_tasks:
        # Разбиваем на меньшие порции
        batch_size = 10
        updated_tasks = [(url, images_dir, filename) for url, _, filename in download_tasks]

        for i in range(0, len(updated_tasks), batch_size):
            batch = updated_tasks[i:i + batch_size]
            logger.info(f"Processing image batch {i // batch_size + 1}")

            async with ImageDownloadManager(max_concurrent=2) as download_manager:
                batch_results = await download_manager.download_images_batch(batch)
                image_download_map.update(batch_results)

            # Небольшая пауза между батчами
            await asyncio.sleep(1)

    # Обрабатываем документ по частям
    chunks = process_document_in_chunks(doc)
    all_md_lines = []

    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i + 1}/{len(chunks)}")

        # Создаем временный документ для чанка
        temp_doc = {
            'body': {'content': chunk},
            'inlineObjects': doc.get('inlineObjects', {}),
            'lists': doc.get('lists', {}),
            'bookmarks': doc.get('bookmarks', {})
        }

        # Обрабатываем чанк
        chunk_md_lines = await process_document_chunk(
            temp_doc, images_dir, bookmarks, headers,
            paragraph_image_map, table_image_map, image_download_map, image_prefix
        )

        all_md_lines.extend(chunk_md_lines)

        # Небольшая пауза между чанками
        await asyncio.sleep(0.5)

    # Объединяем и сохраняем результат
    content = join_markdown_lines_smart(all_md_lines)

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(content)

    # Постобработка
    with open(output_md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    processed_content = post_process_markdown_code_blocks(content)

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    logger.info(f"Successfully converted large document to {output_md_path}")


async def process_document_chunk(doc, images_dir, bookmarks, headers, paragraph_image_map, table_image_map,
                                 image_download_map, image_prefix):
    """
    ИСПРАВЛЕННАЯ: Обработать чанк документа с дополнительными проверками
    """
    body = doc.get('body', {}).get('content', [])
    inline_objects = doc.get('inlineObjects', {})
    lists = doc.get('lists', {})
    md_lines = []
    img_count = 1
    list_counters = {}
    in_code_block = False
    code_block_lines = []

    # Обработка элементов
    for element in body:
        try:
            if 'paragraph' in element:
                para = element['paragraph']
                elements = para.get('elements', [])
                paragraph_style = para.get('paragraphStyle', {})
                named_style = paragraph_style.get('namedStyleType', '')
                bullet = para.get('bullet')
                line = ""
                previous_endswith_alnum = False

                is_code_block = detect_code_block(para)

                if is_code_block and not in_code_block:
                    in_code_block = True
                    code_block_lines = []
                elif not is_code_block and in_code_block:
                    if code_block_lines:
                        md_lines.append('```')
                        md_lines.extend(code_block_lines)
                        md_lines.append('```')
                        code_block_lines = []
                    in_code_block = False

                if bullet:
                    list_marker, list_type = process_list_item_in_paragraph(para, lists, list_counters)
                    if list_marker:
                        line += list_marker

                        # ИСПРАВЛЕНИЕ: Используем новую функцию для обработки содержимого списка
                        list_content = process_list_content_with_line_breaks_for_chunks(
                            elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers
                        )
                        line += list_content
                elif named_style.startswith("HEADING_"):
                    level = int(named_style.replace("HEADING_", ""))
                    line += "#" * level + " "

                    # Process paragraph elements для заголовков
                    for elem in elements:
                        if 'inlineObjectElement' in elem:
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in paragraph_image_map:
                                img_filename, img_tag = paragraph_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    if line and line[-1].isalnum():
                                        line += " "
                                    line += f"![Image](./images/{img_filename})"
                        elif 'textRun' in elem:
                            run_text = elem['textRun'].get('content', '')
                            text_style = elem['textRun'].get('textStyle', {})
                            font_family = text_style.get('fontFamily', '')
                            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                                          ['courier', 'monaco',
                                                           'monospace']) and not font_family.lower() == 'consolas'
                            if is_inline_code_fallback and not in_code_block:
                                processed = f"`{run_text.strip()}`"
                            else:
                                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)
                            if line and previous_endswith_alnum and processed and processed[0].isalnum():
                                line += ' '
                            line += processed
                            previous_endswith_alnum = processed[-1].isalnum() if processed else False
                else:
                    # Process paragraph elements для обычных параграфов
                    for elem in elements:
                        if 'inlineObjectElement' in elem:
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in paragraph_image_map:
                                img_filename, img_tag = paragraph_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    if line and line[-1].isalnum():
                                        line += " "
                                    line += f"![Image](./images/{img_filename})"
                        elif 'textRun' in elem:
                            run_text = elem['textRun'].get('content', '')
                            text_style = elem['textRun'].get('textStyle', {})
                            font_family = text_style.get('fontFamily', '')
                            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                                          ['courier', 'monaco',
                                                           'monospace']) and not font_family.lower() == 'consolas'
                            if is_inline_code_fallback and not in_code_block:
                                processed = f"`{run_text.strip()}`"
                            else:
                                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)
                            if line and previous_endswith_alnum and processed and processed[0].isalnum():
                                line += ' '
                            line += processed
                            previous_endswith_alnum = processed[-1].isalnum() if processed else False

                if in_code_block and is_code_block:
                    code_text = ""
                    for elem in elements:
                        if 'textRun' in elem:
                            code_text += elem['textRun'].get('content', '')
                    code_block_lines.append(code_text.rstrip())
                else:
                    if line.strip():
                        line_with_anchor = add_header_anchors(line, named_style)
                        md_lines.append(line_with_anchor.rstrip())

            elif 'table' in element:
                if in_code_block and code_block_lines:
                    md_lines.append('```')
                    md_lines.extend(code_block_lines)
                    md_lines.append('```')
                    code_block_lines = []
                    in_code_block = False

                # ИСПРАВЛЕНИЕ: Дополнительная проверка таблицы
                table_data = element['table']
                if 'tableRows' in table_data:
                    img_count_ref = [img_count]
                    md_table = table_to_markdown(table_data, inline_objects, images_dir, img_count_ref,
                                                 bookmarks, headers, lists, image_prefix, table_image_map,
                                                 image_download_map)
                    img_count = img_count_ref[0]
                    if md_table.strip():
                        md_lines.append(md_table)
                else:
                    logger.warning("Table element missing 'tableRows' in chunk processing")

        except Exception as e:
            logger.error(f"Error processing element in chunk: {e}")
            continue

    if in_code_block and code_block_lines:
        md_lines.append('```')
        md_lines.extend(code_block_lines)
        md_lines.append('```')

    return md_lines


def run_async_conversion_large(doc_id, md_path, images_dir, creds, image_prefix):
    """
    Запуск асинхронной конвертации для больших документов в синхронном контексте
    """
    try:
        # Проверяем, есть ли уже запущенный event loop
        loop = asyncio.get_running_loop()
        # Если есть, запускаем в отдельном потоке
        with ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run,
                convert_gdoc_to_markdown_large(doc_id, md_path, images_dir, creds, image_prefix)
            )
            future.result()
    except RuntimeError:
        # Нет запущенного event loop, можем запустить напрямую
        asyncio.run(convert_gdoc_to_markdown_large(doc_id, md_path, images_dir, creds, image_prefix))

class ConfigurationError(Exception):
    """Exception raised for configuration errors"""
    pass


class SFTPConfig:
    """SFTP configuration data class"""

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def validate(self):
        """Validate SFTP configuration"""
        if not all([self.host, self.user, self.password]):
            raise ConfigurationError("SFTP configuration is incomplete")
        if not isinstance(self.port, int) or self.port <= 0:
            raise ConfigurationError("SFTP port must be a positive integer")


class ImageDownloadManager:
    """Менеджер асинхронной загрузки изображений"""

    def __init__(self, max_concurrent=MAX_CONCURRENT_DOWNLOADS, timeout=REQUEST_TIMEOUT):
        self.max_concurrent = max_concurrent
        self.timeout = timeout
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.session = None

    async def __aenter__(self):
        """Async context manager entry"""
        connector = aiohttp.TCPConnector(limit=self.max_concurrent)
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()

    async def download_image(self, url, dest_folder, filename):
        """
        Асинхронная загрузка одного изображения

        Args:
            url (str): URL изображения
            dest_folder (str): Папка назначения
            filename (str): Имя файла

        Returns:
            bool: True если успешно, False если ошибка
        """
        async with self.semaphore:  # Ограничиваем количество одновременных загрузок
            try:
                async with self.session.get(url) as response:
                    response.raise_for_status()

                    # Создаем путь к файлу
                    file_path = os.path.join(dest_folder, filename)

                    # Записываем содержимое файла
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
        Асинхронная загрузка пакета изображений

        Args:
            download_tasks (list): Список задач загрузки [(url, dest_folder, filename), ...]

        Returns:
            dict: Словарь результатов {filename: success_status}
        """
        if not download_tasks:
            return {}

        logger.info(f"Starting batch download of {len(download_tasks)} images")

        # Создаем корутины для всех задач загрузки
        tasks = []
        for url, dest_folder, filename in download_tasks:
            task = self.download_image(url, dest_folder, filename)
            tasks.append((filename, task))

        # Выполняем все задачи параллельно
        results = {}
        completed_tasks = await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

        # Обрабатываем результаты
        for (filename, _), result in zip(tasks, completed_tasks):
            if isinstance(result, Exception):
                logger.error(f"Exception during download of {filename}: {result}")
                results[filename] = False
            else:
                results[filename] = result

        successful_downloads = sum(1 for success in results.values() if success)
        logger.info(f"Completed batch download: {successful_downloads}/{len(download_tasks)} successful")

        return results


def download_image(url, dest_folder, filename, timeout=REQUEST_TIMEOUT):
    """
    Синхронная загрузка изображения (для обратной совместимости)

    Args:
        url (str): Image URL
        dest_folder (str): Destination folder path
        filename (str): Target filename
        timeout (int): Request timeout in seconds

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        # Write image binary content to file
        with open(os.path.join(dest_folder, filename), 'wb') as f:
            f.write(response.content)

        logger.debug(f"Successfully downloaded image: {filename}")
        return True

    except requests.RequestException as e:
        logger.error(f"Failed to download image from {url}: {e}")
        return False
    except IOError as e:
        logger.error(f"Failed to save image {filename}: {e}")
        return False


def get_credentials_from_sheet(sheet_id, creds):
    """
    Получить учетные данные из листа Credentials в Google Sheet

    Args:
        sheet_id (str): ID Google Sheet
        creds: Google API credentials

    Returns:
        SFTPConfig: Конфигурация SFTP

    Raises:
        ConfigurationError: Если не удается получить конфигурацию
    """
    try:
        service = build_google_sheet('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Получаем данные из листа Credentials, ячейки B1:B4
        result = sheet.values().get(
            spreadsheetId=sheet_id,
            range="Credentials!B1:B4"
        ).execute()

        values = result.get('values', [])

        if len(values) < 4:
            raise ConfigurationError("Insufficient data in Credentials sheet. Expected values in B1:B4")

        sftp_user = values[0][0] if values[0] else None
        sftp_pass = values[1][0] if values[1] else None
        sftp_host = values[2][0] if len(values) > 2 and values[2] else None
        sftp_port_str = values[3][0] if len(values) > 3 and values[3] else None

        if not sftp_user or not sftp_pass:
            raise ConfigurationError("SFTP credentials are empty in Credentials sheet")

        if not sftp_host:
            raise ConfigurationError("SFTP host is empty in Credentials sheet (cell B3)")

        if not sftp_port_str:
            raise ConfigurationError("SFTP port is empty in Credentials sheet (cell B4)")

        # Преобразуем порт в число
        try:
            sftp_port = int(sftp_port_str)
        except ValueError:
            raise ConfigurationError(f"SFTP port must be a number, got: {sftp_port_str}")

        # Создаем конфигурацию SFTP
        sftp_config = SFTPConfig(sftp_host, sftp_port, sftp_user, sftp_pass)
        sftp_config.validate()

        logger.info(f"Successfully loaded SFTP configuration: {sftp_user}@{sftp_host}:{sftp_port}")
        return sftp_config

    except Exception as e:
        logger.error(f"Failed to get credentials from sheet: {e}")
        raise ConfigurationError(f"Cannot load SFTP configuration: {e}")


def authenticate():
    """Authenticate with Google APIs using OAuth2 credentials"""
    creds = None

    try:
        # Check if token file exists for stored credentials
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)

        # Validate and refresh credentials if needed
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                logger.info("Refreshing expired credentials")
                creds.refresh(Request())
            else:
                # Run OAuth flow to get new credentials
                logger.info("Starting OAuth flow for new credentials")
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)

            # Save credentials to token file for future use
            with open('token.json', 'w') as token:
                token.write(creds.to_json())

        logger.info("Successfully authenticated with Google APIs")
        return creds

    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        raise ConfigurationError(f"Google API authentication failed: {e}")


def extract_bookmarks_and_headers(doc):
    """Extract bookmarks and headers from document for link processing"""
    bookmarks = {}
    headers = {}

    # Extract bookmarks from document
    doc_bookmarks = doc.get('bookmarks', {})
    for bookmark_id, bookmark_data in doc_bookmarks.items():
        text_content = bookmark_data.get('textContent', '')
        if text_content:
            # Create slug from bookmark text
            slug = text_content.strip().lower().replace(' ', '-').replace('_', '-')
            slug = ''.join(c for c in slug if c.isalnum() or c == '-')
            bookmarks[bookmark_id] = slug

    # Extract headers from document body
    body = doc.get('body', {}).get('content', [])
    for element in body:
        if 'paragraph' in element:
            para = element['paragraph']
            paragraph_style = para.get('paragraphStyle', {})
            named_style = paragraph_style.get('namedStyleType', '')

            if named_style.startswith("HEADING_"):
                # Extract header text
                header_text = ""
                for elem in para.get('elements', []):
                    if 'textRun' in elem:
                        header_text += elem['textRun'].get('content', '')

                if header_text.strip():
                    # Create slug from header text for anchor links
                    slug = header_text.strip().lower().replace(' ', '-').replace('_', '-')
                    slug = ''.join(c for c in slug if c.isalnum() or c == '-')
                    headers[header_text.strip()] = slug

    return bookmarks, headers


def process_text_run_enhanced(text_run, bookmarks, headers):
    """
    ИСПРАВЛЕНО: Process Google Docs text run with correct line break handling
    Мягкие переводы строк (\n) обрабатываются ТОЛЬКО внутри текста, не в конце
    """
    text = text_run.get('content', '')
    if not text:
        return text

    style = text_run.get('textStyle', {})

    # Check if the text has gray background color #d9d9d9
    background_color = style.get('backgroundColor', {})
    if background_color:
        rgb = background_color.get('color', {}).get('rgbColor', {})
        if rgb:
            red = rgb.get('red', 0)
            green = rgb.get('green', 0)
            blue = rgb.get('blue', 0)

            # Check for #d9d9d9 background with lenient tolerance
            target_value = GRAY_BACKGROUND_RGB
            tolerance = GRAY_TOLERANCE

            is_d9d9d9_gray_background = (
                    abs(red - target_value) < tolerance and
                    abs(green - target_value) < tolerance and
                    abs(blue - target_value) < tolerance and
                    red > 0.5 and green > 0.5 and blue > 0.5
            )

            if is_d9d9d9_gray_background:
                # В инлайн коде обрабатываем переводы строк
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

        # Check for any light gray background as fallback
        if rgb:
            red = float(rgb.get('red', 0))
            green = float(rgb.get('green', 0))
            blue = float(rgb.get('blue', 0))

            avg_rgb = (red + green + blue) / 3
            max_diff = max(abs(red - avg_rgb), abs(green - avg_rgb), abs(blue - avg_rgb))

            if max_diff < 0.1 and 0.7 < avg_rgb < 0.95:
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

    # ИСПРАВЛЕНИЕ: Обрабатываем мягкие переводы строк только внутри текста
    text = process_line_breaks_in_text(text)

    # Apply strikethrough
    if style.get('strikethrough'):
        text = f"~~{text.strip()}~~"
    elif style.get('underline'):
        text = f"<u>{text.strip()}</u>"

    # Apply bold/italic styles
    if style.get('bold') and style.get('italic'):
        text = f"***{text.strip()}***"
    elif style.get('bold'):
        text = f"**{text.strip()}**"
    elif style.get('italic'):
        text = f"*{text.strip()}*"

    # Process link if present
    if 'link' in style:
        link = style['link']
        url = link.get('url')
        bookmark_id = link.get('bookmarkId')
        heading_id = link.get('headingId')

        if url:
            return f"[{text.strip()}]({url})"
        elif bookmark_id and bookmark_id in bookmarks:
            return f"[{text.strip()}](#{bookmarks[bookmark_id]})"
        elif heading_id:
            for header_text, slug in headers.items():
                if heading_id in header_text or header_text in text:
                    return f"[{text.strip()}](#{slug})"
            slug = ''.join(c for c in heading_id.lower().replace('_', '-').replace(' ', '-') if c.isalnum() or c == '-')
            return f"[{text.strip()}](#{slug})"
        else:
            for header_text, slug in headers.items():
                if text.strip().lower() in header_text.lower():
                    return f"[{text.strip()}](#{slug})"

    return text.replace('\u00A0', ' ')


def process_text_run_enhanced_for_table(text_run, bookmarks, headers, is_header=False):
    """
    ИСПРАВЛЕНО: Process Google Docs text run for tables with correct line break handling
    """
    text = text_run.get('content', '')
    if not text:
        return text

    style = text_run.get('textStyle', {})

    # Check if the text has gray background color #d9d9d9
    background_color = style.get('backgroundColor', {})
    if background_color:
        rgb = background_color.get('color', {}).get('rgbColor', {})
        if rgb:
            red = rgb.get('red', 0)
            green = rgb.get('green', 0)
            blue = rgb.get('blue', 0)

            target_value = GRAY_BACKGROUND_RGB
            tolerance = GRAY_TOLERANCE

            is_d9d9d9_gray_background = (
                    abs(red - target_value) < tolerance and
                    abs(green - target_value) < tolerance and
                    abs(blue - target_value) < tolerance and
                    red > 0.5 and green > 0.5 and blue > 0.5
            )

            if is_d9d9d9_gray_background:
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

        if rgb:
            red = float(rgb.get('red', 0))
            green = float(rgb.get('green', 0))
            blue = float(rgb.get('blue', 0))

            avg_rgb = (red + green + blue) / 3
            max_diff = max(abs(red - avg_rgb), abs(green - avg_rgb), abs(blue - avg_rgb))

            if max_diff < 0.1 and 0.7 < avg_rgb < 0.95:
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

    # ИСПРАВЛЕНИЕ: Обрабатываем мягкие переводы строк только внутри текста
    text = process_line_breaks_in_text(text)

    # Process link if present (HTML format for tables)
    if 'link' in style:
        link = style['link']
        url = link.get('url')
        bookmark_id = link.get('bookmarkId')
        heading_id = link.get('headingId')

        if url:
            return f' <a href="{escape_html_attribute(url)}">{escape_html_text(text.strip())}</a>'
        elif bookmark_id and bookmark_id in bookmarks:
            return f' <a href="#{bookmarks[bookmark_id]}">{escape_html_text(text.strip())}</a>'
        elif heading_id:
            for header_text, slug in headers.items():
                if heading_id in header_text or header_text in text:
                    return f' <a href="#{slug}">{escape_html_text(text.strip())}</a>'
            slug = ''.join(c for c in heading_id.lower().replace('_', '-').replace(' ', '-') if c.isalnum() or c == '-')
            return f' <a href="#{slug}">{escape_html_text(text.strip())}</a>'
        else:
            for header_text, slug in headers.items():
                if text.strip().lower() in header_text.lower():
                    return f' <a href="#{slug}">{escape_html_text(text.strip())}</a>'

    # Apply strikethrough
    if style.get('strikethrough'):
        text = f"<del>{escape_html_text(text.strip())}</del>"
    elif style.get('underline'):
        text = f"<u>{escape_html_text(text.strip())}</u>"
    else:
        # Apply bold/italic styles - Skip bold formatting for header cells
        if style.get('bold') and style.get('italic'):
            if is_header:
                text = f"<em>{escape_html_text(text.strip())}</em>"
            else:
                text = f"<strong><em>{escape_html_text(text.strip())}</em></strong>"
        elif style.get('bold'):
            if not is_header:
                text = f"<strong>{escape_html_text(text.strip())}</strong>"
            else:
                text = escape_html_text(text.strip())
        elif style.get('italic'):
            text = f"<em>{escape_html_text(text.strip())}</em>"
        else:
            text = escape_html_text(text.strip())

    return text.replace('\u00A0', ' ')


def process_line_breaks_in_text(text):
    """
    НОВАЯ ФУНКЦИЯ: Правильная обработка мягких переводов строк

    Правила:
    1. \n в середине текста → <br />
    2. \n в начале или конце → удаляются (не нужен <br />)
    3. Только \n без другого текста → удаляется

    Args:
        text (str): Исходный текст

    Returns:
        str: Обработанный текст
    """
    import re

    if not text:
        return text

    # Если текст состоит только из переводов строк и пробелов - возвращаем пустую строку
    if text.strip().replace('\n', '').strip() == '':
        return ''

    # Заменяем все \n на <br />
    text = text.replace('\n', '<br />')

    # Удаляем <br /> в начале и в конце строки
    text = re.sub(r'^(<br\s*/?>)+', '', text)
    text = re.sub(r'(<br\s*/?>)+$', '', text)

    return text


def process_list_content_with_line_breaks(elements, inline_objects, paragraph_image_map, image_download_map, bookmarks,
                                          headers):
    """
    НОВАЯ ФУНКЦИЯ: Обработка содержимого элементов списка с правильными переводами строк

    Args:
        elements: Элементы параграфа
        inline_objects: Объекты изображений
        paragraph_image_map: Карта изображений в параграфах
        image_download_map: Карта загруженных изображений
        bookmarks: Закладки
        headers: Заголовки

    Returns:
        str: Обработанное содержимое элемента списка
    """
    content = ""
    previous_endswith_alnum = False

    for elem in elements:
        if 'inlineObjectElement' in elem:
            object_id = elem['inlineObjectElement']['inlineObjectId']
            if object_id in paragraph_image_map:
                img_filename, img_tag = paragraph_image_map[object_id]
                if image_download_map.get(img_filename, False):
                    if content and content[-1].isalnum():
                        content += " "
                    content += f"![Image](./images/{img_filename})"
        elif 'textRun' in elem:
            run_text = elem['textRun'].get('content', '')
            text_style = elem['textRun'].get('textStyle', {})
            font_family = text_style.get('fontFamily', '')

            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                          ['courier', 'monaco', 'monospace']) and not font_family.lower() == 'consolas'

            if is_inline_code_fallback:
                processed_text = process_line_breaks_in_text(run_text)
                processed = f"`{processed_text.strip()}`"
            else:
                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)

            if content and previous_endswith_alnum and processed and processed[0].isalnum():
                content += ' '
            content += processed
            previous_endswith_alnum = processed[-1].isalnum() if processed else False

    return content.strip()


def process_list_content_with_line_breaks_for_chunks(elements, inline_objects, paragraph_image_map, image_download_map,
                                                     bookmarks, headers):
    """
    Обработка содержимого элементов списка с правильными переводами строк для чанков
    """
    content = ""
    previous_endswith_alnum = False

    for elem in elements:
        if 'inlineObjectElement' in elem:
            object_id = elem['inlineObjectElement']['inlineObjectId']
            if object_id in paragraph_image_map:
                img_filename, img_tag = paragraph_image_map[object_id]
                if image_download_map.get(img_filename, False):
                    if content and content[-1].isalnum():
                        content += " "
                    content += f"![Image](./images/{img_filename})"
        elif 'textRun' in elem:
            run_text = elem['textRun'].get('content', '')
            text_style = elem['textRun'].get('textStyle', {})
            font_family = text_style.get('fontFamily', '')

            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                          ['courier', 'monaco', 'monospace']) and not font_family.lower() == 'consolas'

            if is_inline_code_fallback:
                processed_text = process_line_breaks_in_text(run_text)
                processed = f"`{processed_text.strip()}`"
            else:
                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)

            if content and previous_endswith_alnum and processed and processed[0].isalnum():
                content += ' '
            content += processed
            previous_endswith_alnum = processed[-1].isalnum() if processed else False

    return content.strip()


def escape_html_text(text):
    """Escape HTML special characters in text content"""
    if not text:
        return text

    text = str(text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&#x27;')
    return text


def escape_html_attribute(attr):
    """Escape HTML special characters in attribute values"""
    if not attr:
        return attr

    attr = str(attr)
    attr = attr.replace('&', '&amp;')
    attr = attr.replace('<', '&lt;')
    attr = attr.replace('>', '&gt;')
    attr = attr.replace('"', '&quot;')
    attr = attr.replace("'", '&#x27;')
    return attr


def get_list_marker(list_props, nesting_level, list_counters, list_id):
    """
    ИСПРАВЛЕННАЯ функция получения маркера списка
    Использует только стандартную markdown-разметку
    """
    if not list_props or 'nestingLevels' not in list_props:
        # Значение по умолчанию для ненумерованного списка
        indent = '    ' * nesting_level
        return indent + '- '

    if nesting_level >= len(list_props['nestingLevels']):
        nesting_level = len(list_props['nestingLevels']) - 1

    nesting_info = list_props['nestingLevels'][nesting_level]
    indent = '    ' * nesting_level

    # Используем исправленную функцию для определения типа
    if is_ordered_list(list_props, nesting_level):
        # Нумерованный список - стандартная нумерация
        list_key = f"{list_id}_{nesting_level}"
        if list_key not in list_counters:
            start_number = nesting_info.get('startNumber', 1)
            list_counters[list_key] = start_number
        else:
            list_counters[list_key] += 1
        return indent + f"{list_counters[list_key]}. "
    else:
        # Ненумерованный список - всегда дефис
        return indent + '- '


def is_ordered_list(list_props, nesting_level):
    """
    ИСПРАВЛЕННАЯ функция определения нумерованного списка
    Более строгие критерии для определения нумерованных списков
    """
    if not list_props or 'nestingLevels' not in list_props:
        return False

    if nesting_level >= len(list_props['nestingLevels']):
        nesting_level = len(list_props['nestingLevels']) - 1

    nesting_info = list_props['nestingLevels'][nesting_level]
    glyph_format = nesting_info.get('glyphFormat', '')
    glyph_type = nesting_info.get('glyphType', '')
    glyph_symbol = nesting_info.get('glyphSymbol', '')

    # СТРОГАЯ ПРОВЕРКА: список нумерованный только если:
    # 1. Есть glyphFormat с подстановкой (например, "%0.", "%1)", "%0 -")
    # 2. И glyphType указывает на нумерацию
    # 3. И НЕТ символов маркеров (●, ○, ■, ▪)

    # Проверяем наличие символов маркеров - если есть, это точно НЕ нумерованный список
    bullet_symbols = ['●', '○', '■', '▪', '▫', '◦', '‣', '⁃', '-', '*', '+', '•']
    if glyph_symbol and glyph_symbol in bullet_symbols:
        return False

    # Проверяем glyph_format - должен содержать подстановку для номера
    has_number_format = glyph_format and '%' in glyph_format

    # Проверяем glyph_type - должен быть типом нумерации
    ordered_glyph_types = ['DECIMAL', 'ALPHA', 'UPPER_ALPHA', 'ROMAN', 'UPPER_ROMAN']
    has_ordered_type = glyph_type in ordered_glyph_types

    # Список нумерованный ТОЛЬКО если есть И формат подстановки, И тип нумерации
    return has_number_format and has_ordered_type


def process_list_item_in_paragraph(para, lists, list_counters):
    """
    ИСПРАВЛЕННАЯ функция обработки элементов списка в параграфах
    Использует только стандартную markdown-разметку
    """
    bullet = para.get('bullet')
    if not bullet:
        return None, None

    list_id = bullet['listId']
    nesting_level = bullet.get('nestingLevel', 0)

    if lists and list_id in lists:
        list_props = lists[list_id]['listProperties']

        # Используем исправленную функцию определения типа
        is_ordered = is_ordered_list(list_props, nesting_level)

        if is_ordered:
            # Нумерованный список
            list_key = f"{list_id}_{nesting_level}"
            if list_key not in list_counters:
                if ('nestingLevels' in list_props and
                        nesting_level < len(list_props['nestingLevels'])):
                    start_number = list_props['nestingLevels'][nesting_level].get('startNumber', 1)
                else:
                    start_number = 1
                list_counters[list_key] = start_number
            else:
                list_counters[list_key] += 1

            indent = '    ' * nesting_level
            marker = f"{list_counters[list_key]}. "
            return indent + marker, 'ordered'
        else:
            # Ненумерованный список - всегда дефис
            indent = '    ' * nesting_level
            marker = '- '
            return indent + marker, 'unordered'

    # Значение по умолчанию для ненумерованного списка
    indent = '    ' * nesting_level
    return indent + '- ', 'unordered'


def detect_code_block(paragraph):
    """Detect if paragraph represents a code block"""
    # Check if paragraph uses monospace font or has specific formatting
    paragraph_style = paragraph.get('paragraphStyle', {})
    named_style = paragraph_style.get('namedStyleType', '')

    # Check for code-like named styles
    if 'CODE' in named_style.upper():
        return True

    # Check if all text runs use monospace font
    elements = paragraph.get('elements', [])
    for elem in elements:
        if 'textRun' in elem:
            text_style = elem['textRun'].get('textStyle', {})
            font_family = text_style.get('fontFamily', '')
            if any(font_name in font_family.lower() for font_name in ['consolas', 'courier', 'monaco', 'monospace']):
                return True

    return False


def add_header_anchors(line, named_style):
    """
    ИСПРАВЛЕНО: Add anchor tags to headers without adding extra <br/> tags
    """
    if named_style.startswith("HEADING_"):
        # Extract header text from line (remove markdown # and any trailing whitespace)
        header_text = line.lstrip('#').strip()
        if header_text:
            # Удаляем лишние <br /> теги из заголовков
            header_text = header_text.replace('<br />', ' ').strip()

            # Create slug for anchor
            slug = header_text.lower().replace(' ', '-').replace('_', '-')
            slug = ''.join(c for c in slug if c.isalnum() or c == '-')

            # Восстанавливаем заголовок без лишних <br />
            level = named_style.replace("HEADING_", "")
            clean_header = "#" * int(level) + " " + header_text

            return f"{clean_header} {{#{slug}}}"
    return line


def clean_line_breaks_at_end(text):
    """
    НОВАЯ ФУНКЦИЯ: Удаление лишних <br /> тегов в конце строк

    Args:
        text (str): Текст для очистки

    Returns:
        str: Очищенный текст
    """
    if not text:
        return text

    # Удаляем <br /> в конце строк
    text = re.sub(r'<br\s*/>\s*$', '', text.strip())

    return text

def format_paragraphs(paragraphs):
    """Format paragraphs with proper <p> tags"""
    if not paragraphs:
        return []

    formatted_paragraphs = []

    # If there's only one paragraph and it's short, don't wrap in <p> tags
    if len(paragraphs) == 1 and len(paragraphs[0]) < 100:
        formatted_paragraphs.append(paragraphs[0])
    else:
        # Multiple paragraphs or long single paragraph - use <p> tags
        for para in paragraphs:
            if para.strip():
                formatted_paragraphs.append(f"<p>{para}</p>")

    return formatted_paragraphs


def build_list_html_fixed(list_type, items):
    """
    ИСПРАВЛЕННАЯ функция построения HTML списков
    """
    if not items:
        return ''

    html_lines = []
    current_level = 0
    open_lists = []

    for item in items:
        level = item['level']
        text = item['text']
        item_type = item['type']  # 'ordered' или 'unordered'

        # Преобразуем тип в HTML теги
        html_tag = 'ol' if item_type == 'ordered' else 'ul'

        # Обработка уровней вложенности
        while current_level < level:
            html_lines.append('  ' * len(open_lists) + f'<{html_tag}>')
            open_lists.append(html_tag)
            current_level += 1

        while current_level > level:
            if open_lists:
                closed_type = open_lists.pop()
                html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')
            current_level -= 1

        # Если тип списка изменился на том же уровне, закрываем старый и открываем новый
        if open_lists and open_lists[-1] != html_tag:
            closed_type = open_lists.pop()
            html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')
            html_lines.append('  ' * len(open_lists) + f'<{html_tag}>')
            open_lists.append(html_tag)

        # Добавляем элемент списка
        indent = '  ' * len(open_lists)
        html_lines.append(f'{indent}<li>{text}</li>')

    # Закрываем все открытые списки
    while open_lists:
        closed_type = open_lists.pop()
        html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')

    return '\n'.join(html_lines)


def collect_image_info(elem, inline_objects, img_count_ref, image_prefix=None, use_relative_path=False):
    """
    Собрать информацию об изображении без загрузки

    Args:
        elem: Inline object element
        inline_objects: Dictionary of inline objects
        img_count_ref: Reference to image counter (list with single int)
        image_prefix: Optional prefix for image filename
        use_relative_path: If True, use ../images/ instead of ./images/

    Returns:
        tuple: (img_url, img_filename, img_tag) or (None, None, None) if no image
    """
    object_id = elem['inlineObjectElement']['inlineObjectId']
    inline_obj = inline_objects.get(object_id)
    if not inline_obj:
        return None, None, None

    embedded_obj = inline_obj['inlineObjectProperties']['embeddedObject']
    img_url = None

    # Extract image URL from different possible locations
    if 'imageProperties' in embedded_obj and 'contentUri' in embedded_obj['imageProperties']:
        img_url = embedded_obj['imageProperties']['contentUri']
    elif 'imageProperties' in embedded_obj and 'sourceUri' in embedded_obj['imageProperties']:
        img_url = embedded_obj['imageProperties']['sourceUri']

    if img_url:
        # Determine image file extension
        img_ext = os.path.splitext(img_url.split('?')[0])[1]
        if not img_ext or len(img_ext) > 5:
            img_ext = '.png'

        # Generate unique filename with optional custom prefix
        if image_prefix:
            img_filename = f'{image_prefix}_image_{img_count_ref[0]}{img_ext}'
        else:
            img_filename = f'image_{img_count_ref[0]}{img_ext}'

        img_count_ref[0] += 1

        # Choose correct path based on context and create img tag without alt attribute
        images_path = "../images/" if use_relative_path else "./images/"
        img_tag = f'<img src="{images_path}{img_filename}" />'

        return img_url, img_filename, img_tag

    return None, None, None


def process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref, image_prefix=None):
    """
    Синхронная обработка изображения (для простых случаев)

    Args:
        elem: Inline object element
        inline_objects: Dictionary of inline objects
        images_dir: Directory to save images
        img_count_ref: Reference to image counter (list with single int)
        image_prefix: Optional prefix for image filename

    Returns:
        str: HTML img tag or empty string
    """
    img_url, img_filename, img_tag = collect_image_info(elem, inline_objects, img_count_ref, image_prefix)

    if img_url and img_filename:
        if download_image(img_url, images_dir, img_filename):
            return img_tag

    return ''


def escape_html_content(text):
    """
    Escape HTML special characters but preserve intentional HTML tags like <br>, <img>, <a>, lists, paragraphs, etc.
    """
    import re

    # First, protect our intentional HTML tags by replacing them with placeholders
    protected_tags = {}
    tag_counter = 0

    # List of HTML tags we want to preserve (expanded for all HTML elements we generate)
    preserve_patterns = [
        r'<br\s*/?>',
        r'<img[^>]+>',
        r'<a[^>]+>.*?</a>',  # Complete links
        r'<strong>.*?</strong>',  # Bold text
        r'<em>.*?</em>',  # Italic text
        r'<u>.*?</u>',  # Underlined text
        r'<del>.*?</del>',  # Strikethrough text
        r'<code>.*?</code>',  # Inline code
        r'<p>.*?</p>',  # Paragraphs
        r'<ul>.*?</ul>',  # Unordered lists (complete)
        r'<ol>.*?</ol>',  # Ordered lists (complete)
        r'<li>.*?</li>',  # List items (complete)
        # Individual tags for partial matching
        r'<a[^>]+>',
        r'</a>',
        r'<strong>',
        r'</strong>',
        r'<em>',
        r'</em>',
        r'<u>',
        r'</u>',
        r'<del>',
        r'</del>',
        r'<code>',
        r'</code>',
        r'<p>',
        r'</p>',
        r'<ul>',
        r'</ul>',
        r'<ol>',
        r'</ol>',
        r'<li>',
        r'</li>'
    ]

    for pattern in preserve_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        for match in matches:
            placeholder = f"__PRESERVE_TAG_{tag_counter}__"
            protected_tags[placeholder] = match
            text = text.replace(match, placeholder, 1)
            tag_counter += 1

    # Now escape HTML special characters
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&#x27;')

    # Restore protected tags
    for placeholder, original_tag in protected_tags.items():
        text = text.replace(placeholder, original_tag)

    return text


def process_cell_content(cell, inline_objects, images_dir, img_count_ref, bookmarks, headers, lists, is_header,
                         image_prefix=None, table_image_map=None, image_download_map=None):
    """
    ИСПРАВЛЕННАЯ обработка содержимого ячеек с правильной обработкой списков
    """
    cell_elements = []
    current_list_type = None
    list_items = []
    list_counters = {}
    paragraphs = []

    # Обрабатываем содержимое ячейки
    for cell_content in cell.get('content', []):
        if 'paragraph' in cell_content:
            para = cell_content['paragraph']
            bullet = para.get('bullet')

            if bullet:
                # Это элемент списка - сначала закрываем открытые параграфы
                if paragraphs:
                    cell_elements.extend(format_paragraphs(paragraphs))
                    paragraphs = []

                # Обрабатываем элемент списка
                list_id = bullet['listId']
                nesting_level = bullet.get('nestingLevel', 0)

                if lists and list_id in lists:
                    list_props = lists[list_id]['listProperties']
                    is_ordered = is_ordered_list(list_props, nesting_level)
                    list_type = 'ordered' if is_ordered else 'unordered'
                else:
                    list_type = 'unordered'  # По умолчанию ненумерованный
                    is_ordered = False

                # Обрабатываем содержимое элемента списка
                item_text = ""
                for elem in para.get('elements', []):
                    if 'inlineObjectElement' in elem:
                        if table_image_map and image_download_map:
                            # Используем уже загруженные изображения для таблиц
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in table_image_map:
                                img_filename, img_tag = table_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    item_text += img_tag
                        else:
                            # Синхронная загрузка (fallback)
                            item_text += process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref,
                                                                   image_prefix)
                    elif 'textRun' in elem:
                        item_text += process_text_run_enhanced_for_table(elem['textRun'], bookmarks, headers, is_header)

                item_text = item_text.strip()

                # Управление непрерывностью списков
                if current_list_type != list_type or current_list_type is None:
                    # Закрываем предыдущий список, если тип отличается
                    if current_list_type and list_items:
                        cell_elements.append(build_list_html_fixed(current_list_type, list_items))
                        list_items = []
                    current_list_type = list_type

                # Добавляем элемент в текущий список
                list_items.append({
                    'text': item_text,
                    'level': nesting_level,
                    'type': list_type  # Используем 'ordered'/'unordered' вместо 'ol'/'ul'
                })

            else:
                # Это обычный параграф
                # Сначала закрываем открытый список
                if current_list_type and list_items:
                    cell_elements.append(build_list_html_fixed(current_list_type, list_items))
                    list_items = []
                    current_list_type = None

                # Обрабатываем содержимое параграфа
                para_text = ""
                for elem in para.get('elements', []):
                    if 'inlineObjectElement' in elem:
                        if table_image_map and image_download_map:
                            # Используем уже загруженные изображения для таблиц
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in table_image_map:
                                img_filename, img_tag = table_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    para_text += img_tag
                        else:
                            # Синхронная загрузка (fallback)
                            para_text += process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref,
                                                                   image_prefix)
                    elif 'textRun' in elem:
                        para_text += process_text_run_enhanced_for_table(elem['textRun'], bookmarks, headers, is_header)

                para_text = para_text.strip()
                if para_text:
                    paragraphs.append(para_text)

    # Закрываем любой оставшийся открытый список
    if current_list_type and list_items:
        cell_elements.append(build_list_html_fixed(current_list_type, list_items))

    # Добавляем оставшиеся параграфы
    if paragraphs:
        cell_elements.extend(format_paragraphs(paragraphs))

    # Объединяем все элементы ячейки
    cell_html = ''.join(cell_elements) if cell_elements else ''

    # Экранируем HTML специальные символы (кроме наших намеренных HTML тегов)
    cell_html = escape_html_content(cell_html)

    return cell_html


def table_to_markdown(table, inline_objects, images_dir, img_count_ref, bookmarks, headers, lists=None,
                      image_prefix=None, table_image_map=None, image_download_map=None):
    """
    Convert Google Docs table to HTML format with corrected span detection including rowspan and paragraph support
    ИСПРАВЛЕНО: добавлена проверка на существование tableRows

    Args:
        table: Table object from Google Docs
        inline_objects: Dictionary of inline objects
        images_dir: Directory to save images
        img_count_ref: Reference to image counter
        bookmarks: Dictionary of bookmarks
        headers: Dictionary of headers
        lists: Dictionary of lists
        image_prefix: Optional prefix for image filenames
        table_image_map: Optional dict mapping object_id to (filename, img_tag) for table images
        image_download_map: Optional dict mapping filenames to download status

    Returns:
        str: HTML table markup
    """
    # ИСПРАВЛЕНИЕ: проверяем существование tableRows
    if 'tableRows' not in table:
        logger.warning("Table object missing 'tableRows' key, skipping table")
        return ''

    rows = table['tableRows']
    html_lines = []

    if not rows:
        return ''

    # Extract all cell contents
    cell_contents = []
    raw_cells = []  # Keep original cell objects for processing

    for row_idx, row in enumerate(rows):
        row_contents = []
        row_cells = []
        cells = row.get('tableCells', [])

        for cell_idx, cell in enumerate(cells):
            content = extract_cell_text_content(cell)
            row_contents.append(content)
            row_cells.append(cell)

        cell_contents.append(row_contents)
        raw_cells.append(row_cells)

    # Normalize table dimensions
    max_cols = max(len(row) for row in cell_contents) if cell_contents else 0
    for i, row in enumerate(cell_contents):
        while len(row) < max_cols:
            row.append('')
            # Add empty cell object for consistency
            if i < len(raw_cells):
                raw_cells[i].append({'content': []})

    # Detect spans with improved logic including rowspan
    spans = detect_corrected_spans(cell_contents)

    # Generate HTML
    html_lines.append('<table>')

    for row_idx in range(len(cell_contents)):
        html_lines.append('  <tr>')

        is_header_row = (row_idx == 0)
        cell_tag = 'th' if is_header_row else 'td'

        col_idx = 0

        while col_idx < max_cols:
            # Skip cells that are covered by previous spans
            if should_skip_cell(row_idx, col_idx, spans):
                col_idx += 1
                continue

            # Get cell content
            if (row_idx < len(raw_cells) and col_idx < len(raw_cells[row_idx]) and
                    col_idx < len(cell_contents[row_idx])):

                cell = raw_cells[row_idx][col_idx]
                cell_text = cell_contents[row_idx][col_idx]

                # Process cell content with unified function
                cell_html = process_cell_content(cell, inline_objects, images_dir, img_count_ref,
                                                 bookmarks, headers, lists, is_header_row, image_prefix,
                                                 table_image_map, image_download_map)

                # Get span attributes
                cell_attrs = []
                span_info = spans.get((row_idx, col_idx))

                if span_info:
                    row_span, col_span = span_info
                    if col_span > 1:
                        cell_attrs.append(f'colspan="{col_span}"')
                    if row_span > 1:
                        cell_attrs.append(f'rowspan="{row_span}"')

                # Build cell HTML
                attrs_str = ' ' + ' '.join(cell_attrs) if cell_attrs else ''
                html_lines.append(f'    <{cell_tag}{attrs_str}>{cell_html}</{cell_tag}>')

            col_idx += 1

        html_lines.append('  </tr>')

    html_lines.append('</table>')

    result = '\n'.join(html_lines)
    return result


def should_skip_cell(row_idx, col_idx, spans):
    """Check if a cell should be skipped due to spanning from another cell"""
    for (span_row, span_col), (row_span, col_span) in spans.items():
        # Check if current cell is covered by this span
        if (span_row <= row_idx < span_row + row_span and
                span_col <= col_idx < span_col + col_span and
                not (span_row == row_idx and span_col == col_idx)):  # Don't skip the span origin
            return True
    return False


def detect_corrected_spans(cell_contents):
    """
    Improved span detection logic with rowspan support
    """
    spans = {}
    rows = len(cell_contents)
    cols = len(cell_contents[0]) if rows > 0 else 0

    # Phase 1: Detect colspan (horizontal merging)
    for row_idx in range(rows):
        col_idx = 0
        while col_idx < cols:
            cell_content = cell_contents[row_idx][col_idx]

            if cell_content.strip():  # Non-empty cell
                # Count consecutive empty cells to the right
                colspan = 1
                next_col = col_idx + 1

                while next_col < cols and not cell_contents[row_idx][next_col].strip():
                    # But stop if we find content in the next row at this position
                    # (indicating it's not truly merged)
                    if (row_idx + 1 < rows and
                            cell_contents[row_idx + 1][next_col].strip()):
                        break
                    colspan += 1
                    next_col += 1

                # Also check if there's meaningful content that suggests merging
                # Look for cases where empty cells are followed by content in next row
                if colspan == 1 and row_idx + 1 < rows:
                    # Check if there are empty cells to the right that have content below
                    temp_colspan = 1
                    temp_next_col = col_idx + 1

                    while (temp_next_col < cols and
                           not cell_contents[row_idx][temp_next_col].strip() and
                           cell_contents[row_idx + 1][temp_next_col].strip()):
                        temp_colspan += 1
                        temp_next_col += 1

                    if temp_colspan > colspan:
                        colspan = temp_colspan

                # Only register as span if we have multiple columns AND reasonable content
                if colspan > 1 and (len(cell_content) > 2 or
                                    cell_content.lower() in ['player', 'type', 'description']):
                    spans[(row_idx, col_idx)] = (1, colspan)

            col_idx += 1

    # Phase 2: Detect rowspan (vertical merging)
    for col_idx in range(cols):
        row_idx = 0
        while row_idx < rows:
            cell_content = cell_contents[row_idx][col_idx]

            if cell_content.strip():  # Non-empty cell
                # Get existing colspan if any
                existing_span = spans.get((row_idx, col_idx))
                if existing_span:
                    current_rowspan, colspan = existing_span
                else:
                    current_rowspan = 1
                    colspan = 1

                # Count consecutive empty cells below
                rowspan = 1
                next_row = row_idx + 1

                while next_row < rows and not cell_contents[next_row][col_idx].strip():
                    # Check if there's content to the right that would indicate separate cells
                    has_content_right = False
                    for check_col in range(col_idx + 1, min(col_idx + colspan, cols)):
                        if cell_contents[next_row][check_col].strip():
                            has_content_right = True
                            break

                    # If there's content to the right, this might not be a true rowspan
                    if has_content_right:
                        break

                    rowspan += 1
                    next_row += 1

                # Enhanced rowspan detection: look for patterns where content repeats
                # or where there are systematic empty cells
                if rowspan == 1:
                    # Check for pattern-based rowspan detection
                    temp_rowspan = 1
                    temp_next_row = row_idx + 1

                    while (temp_next_row < rows and
                           not cell_contents[temp_next_row][col_idx].strip()):

                        # Check if the entire row section is empty (indicating a span)
                        section_empty = True
                        for check_col in range(col_idx, min(col_idx + colspan, cols)):
                            if cell_contents[temp_next_row][check_col].strip():
                                section_empty = False
                                break

                        if section_empty:
                            temp_rowspan += 1
                            temp_next_row += 1
                        else:
                            break

                    if temp_rowspan > rowspan:
                        rowspan = temp_rowspan

                # Only register rowspan if we have multiple rows AND substantial content
                if rowspan > 1 and (len(cell_content) > 3 or
                                    any(keyword in cell_content.lower()
                                        for keyword in ['parameter', 'type', 'description', 'api', 'request'])):
                    spans[(row_idx, col_idx)] = (rowspan, colspan)

                row_idx += rowspan
            else:
                row_idx += 1

    # Phase 3: Special handling for typical table patterns
    # Pattern: Header row with category spanning multiple subheaders
    if rows >= 2:
        # Look for pattern where row 0 has content, empty cells, then row 1 has content in those positions
        for col_idx in range(cols):
            if (col_idx < len(cell_contents[0]) and
                    cell_contents[0][col_idx].strip()):

                # Count empty cells to the right in row 0
                empty_count = 0
                check_col = col_idx + 1

                while (check_col < cols and
                       check_col < len(cell_contents[0]) and
                       not cell_contents[0][check_col].strip()):

                    # Check if row 1 has content at this position
                    if (1 < len(cell_contents) and
                            check_col < len(cell_contents[1]) and
                            cell_contents[1][check_col].strip()):
                        empty_count += 1

                    check_col += 1

                if empty_count > 0:
                    total_span = empty_count + 1
                    # Don't override if we already detected a span here
                    if (0, col_idx) not in spans:
                        spans[(0, col_idx)] = (1, total_span)

    # Phase 4: Detect multi-row headers (common pattern)
    # Look for cases where first few rows have similar structure
    if rows >= 3:
        for col_idx in range(min(3, cols)):  # Usually first 3 columns are headers
            # Check if first cell has content and subsequent cells in same column are empty
            if (cell_contents[0][col_idx].strip() and
                    len(cell_contents[0][col_idx]) > 3):  # Substantial content

                # Count how many rows below are empty in this column
                empty_rows = 0
                for check_row in range(1, min(rows, 4)):  # Check up to 4 rows
                    if not cell_contents[check_row][col_idx].strip():
                        empty_rows += 1
                    else:
                        break

                # If we have empty rows below and this looks like a header
                if empty_rows > 0 and empty_rows < rows - 1:
                    # Check if there's actual content further down
                    has_content_below = False
                    for check_row in range(empty_rows + 1, rows):
                        if cell_contents[check_row][col_idx].strip():
                            has_content_below = True
                            break

                    # Only create rowspan if there's content below (indicating it's not just empty)
                    if has_content_below:
                        total_rowspan = empty_rows + 1
                        # Don't override existing spans
                        if (0, col_idx) not in spans:
                            spans[(0, col_idx)] = (total_rowspan, 1)

    return spans


def extract_cell_text_content(cell):
    """Extract plain text content from a cell for analysis"""
    content = ""
    for cell_content in cell.get('content', []):
        if 'paragraph' in cell_content:
            para = cell_content['paragraph']
            for elem in para.get('elements', []):
                if 'textRun' in elem:
                    content += elem['textRun'].get('content', '')
    return content.strip()


def post_process_markdown_code_blocks(content):
    """
    ИСПРАВЛЕНО: Enhanced post-processing with line break cleanup
    """
    import re

    # Сначала обрабатываем code block markers
    marker = ""
    if marker not in content:
        # Если нет маркеров, только очищаем лишние <br />
        return clean_excessive_line_breaks(content)

    # Count markers before processing
    marker_count = content.count(marker)
    lines = content.splitlines()
    processed_lines = []
    in_code_block = False
    code_block_counter = 0

    for i, line in enumerate(lines):
        original_line = line
        line_has_markers = False

        # Check if line contains the special marker
        if marker in line:
            line_has_markers = True

            while marker in line:
                if not in_code_block:
                    processed_lines.append("```")
                    in_code_block = True
                    code_block_counter += 1
                else:
                    processed_lines.append("```")
                    in_code_block = False

                line = line.replace(marker, "", 1)

        if line_has_markers:
            if line.strip():
                processed_lines.append(line)
        else:
            processed_lines.append(line)

    if in_code_block:
        processed_lines.append("```")
        logger.warning("Auto-closed unclosed code block at end of document")

    processed_content = '\n'.join(processed_lines)
    processed_content = re.sub(r'```\n\n```', '```\n```', processed_content)
    processed_content = ensure_proper_code_block_spacing(processed_content)

    # ИСПРАВЛЕНИЕ: Очищаем лишние <br /> теги
    processed_content = clean_excessive_line_breaks(processed_content)

    return processed_content


def clean_excessive_line_breaks(content):
    """
    НОВАЯ ФУНКЦИЯ: Очистка лишних <br /> тегов

    Удаляет <br /> теги в неподходящих местах:
    - После заголовков
    - В конце параграфов
    - После элементов списков
    - В конце строк перед пустыми строками
    """
    import re

    lines = content.splitlines()
    cleaned_lines = []

    for i, line in enumerate(lines):
        cleaned_line = line

        # Удаляем <br /> после заголовков
        if re.match(r'^#+\s.*<br\s*/>\s*{#.*}?\s*$', line):
            cleaned_line = re.sub(r'<br\s*/>\s*({#.*}?\s*)$', r'\1', line)

        # Удаляем <br /> в конце элементов списков
        elif re.match(r'^\s*[-*+]\s.*<br\s*/>\s*$', line) or re.match(r'^\s*\d+\.\s.*<br\s*/>\s*$', line):
            cleaned_line = re.sub(r'<br\s*/>\s*$', '', line)

        # Удаляем <br /> в конце строк, если следующая строка пустая или это конец документа
        elif line.endswith('<br />') or line.endswith('<br/>'):
            next_line_empty = (i + 1 >= len(lines)) or (i + 1 < len(lines) and not lines[i + 1].strip())
            if next_line_empty:
                cleaned_line = re.sub(r'<br\s*/>\s*$', '', line)

        cleaned_lines.append(cleaned_line)

    return '\n'.join(cleaned_lines)


def join_markdown_lines_smart(md_lines):
    """
    ИСПРАВЛЕНО: Умное объединение строк Markdown с правильной обработкой переводов строк
    """
    if not md_lines:
        return ""

    # Сначала очищаем лишние <br /> теги в конце каждой строки
    cleaned_lines = []
    for line in md_lines:
        cleaned_line = clean_line_breaks_at_end(line)
        cleaned_lines.append(cleaned_line)

    result_lines = []
    prev_line_is_list = False

    for i, line in enumerate(cleaned_lines):
        current_line_is_list = is_list_item(line)

        # Если это первая строка, просто добавляем
        if i == 0:
            result_lines.append(line)
        else:
            # Определяем, нужно ли добавить пустую строку
            if should_add_empty_line(cleaned_lines[i - 1], line, prev_line_is_list, current_line_is_list):
                result_lines.append("")  # Добавляем пустую строку

            result_lines.append(line)

        prev_line_is_list = current_line_is_list

    return '\n'.join(result_lines)


def ensure_proper_code_block_spacing(content):
    """
    Ensure proper spacing around code blocks:
    - Remove empty lines after opening code blocks (```)
    - Ensure empty line after closing code blocks (```)
    unless it's at the end of the document or followed by another code block
    """
    lines = content.splitlines()
    processed_lines = []
    in_code_block = False

    i = 0
    while i < len(lines):
        line = lines[i]
        processed_lines.append(line)

        # Check if current line is opening or closing code block
        if line.strip() == "```":
            if not in_code_block:
                # This is an opening code block
                in_code_block = True

                # Check if next line is empty and remove it
                next_line_index = i + 1
                if (next_line_index < len(lines) and
                        lines[next_line_index].strip() == ""):
                    # Skip the empty line after opening ```
                    i += 1  # Skip the empty line

            else:
                # This is a closing code block
                in_code_block = False

                # Look ahead to see what comes next
                next_line_index = i + 1

                # Check if this is the end of document
                if next_line_index >= len(lines):
                    # End of document, no need to add empty line
                    pass
                else:
                    next_line = lines[next_line_index]

                    # If next line is not empty and not another code block
                    if next_line.strip() != "" and next_line.strip() != "```":
                        # Add empty line after closing code block
                        processed_lines.append("")

        i += 1

    return '\n'.join(processed_lines)


def is_list_item(line):
    """Проверяет, является ли строка элементом списка"""
    stripped = line.strip()

    # Нумерованный список (1. 2. 3. и т.д.) с любым количеством ведущих пробелов
    if re.match(r'^\s*\d+\.\s', line):
        return True

    # Ненумерованный список (-, *, +) с любым количеством ведущих пробелов
    if re.match(r'^\s*[-*+]\s', line):
        return True

    return False


def should_add_empty_line(prev_line, current_line, prev_is_list, current_is_list):
    """
    Определяет, нужно ли добавить пустую строку между элементами
    """
    # Если оба элемента списка, не добавляем пустую строку
    if prev_is_list and current_is_list:
        return False

    # Если предыдущий элемент список, а текущий нет - добавляем пустую строку
    if prev_is_list and not current_is_list:
        return True

    # Если текущий элемент список, а предыдущий нет - добавляем пустую строку
    if not prev_is_list and current_is_list:
        return True

    # Специальные случаи для заголовков
    if current_line.strip().startswith('#'):
        return True

    # Специальные случаи для таблиц
    if current_line.strip().startswith('<table>'):
        return True

    # Специальные случаи для блоков кода
    if current_line.strip() == '```':
        return True

    # Для всех остальных случаев добавляем пустую строку
    return True


async def collect_all_images_from_document(doc, image_prefix=None):
    """
    Собрать информацию о всех изображениях в документе для пакетной загрузки

    Args:
        doc: Google Docs document object
        image_prefix: Optional prefix for image filenames

    Returns:
        tuple: (download_tasks, paragraph_image_map, table_image_map) где:
               - download_tasks: список задач загрузки
               - paragraph_image_map: словарь object_id -> (filename, img_tag) для параграфов
               - table_image_map: словарь object_id -> (filename, img_tag) для таблиц
    """
    body = doc.get('body', {}).get('content', [])
    inline_objects = doc.get('inlineObjects', {})
    download_tasks = []
    paragraph_image_map = {}  # object_id -> (filename, img_tag) для параграфов
    table_image_map = {}  # object_id -> (filename, img_tag) для таблиц
    url_to_filename = {}  # url -> filename для дедупликации
    img_count = [1]  # Используем список для передачи по ссылке

    def process_inline_object_element(elem, use_relative_path=False):
        """Обработать элемент изображения"""
        object_id = elem['inlineObjectElement']['inlineObjectId']
        img_url, img_filename, img_tag = collect_image_info(elem, inline_objects, img_count, image_prefix,
                                                            use_relative_path)

        if img_url and img_filename:
            # Проверяем, не загружали ли мы уже это изображение
            if img_url not in url_to_filename:
                url_to_filename[img_url] = img_filename
                download_tasks.append((img_url, None, img_filename))
            else:
                # Используем уже существующий filename, но создаем правильный тег
                existing_filename = url_to_filename[img_url]
                images_path = "../images/" if use_relative_path else "./images/"
                img_tag = f'<img src="{images_path}{existing_filename}" alt="Image" />'
                img_filename = existing_filename

            return object_id, img_filename, img_tag
        return None, None, None

    # Сканируем все элементы документа
    for element in body:
        if 'paragraph' in element:
            para = element['paragraph']
            elements = para.get('elements', [])

            for elem in elements:
                if 'inlineObjectElement' in elem:
                    object_id, img_filename, img_tag = process_inline_object_element(elem, use_relative_path=False)
                    if object_id:
                        paragraph_image_map[object_id] = (img_filename, img_tag)

        elif 'table' in element:
            table = element['table']
            rows = table.get('tableRows', [])

            for row in rows:
                cells = row.get('tableCells', [])
                for cell in cells:
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
    logger.info(f"Paragraph images: {len(paragraph_image_map)}, Table images: {len(table_image_map)}")
    return download_tasks, paragraph_image_map, table_image_map


async def convert_gdoc_to_markdown(document_id, output_md_path, images_dir, creds, image_prefix=None):
    """
    Асинхронная конвертация Google Doc в markdown с параллельной загрузкой изображений

    Args:
        document_id (str): ID Google документа
        output_md_path (str): Путь для сохранения markdown файла
        images_dir (str): Директория для сохранения изображений
        creds: Google API credentials
        image_prefix (str, optional): Префикс для имен файлов изображений
    """
    # Build Google Docs API service
    docs_service = build('docs', 'v1', credentials=creds)
    doc = docs_service.documents().get(documentId=document_id).execute()

    # Create images directory if it doesn't exist
    os.makedirs(images_dir, exist_ok=True)

    # Extract bookmarks and headers for link processing
    bookmarks, headers = extract_bookmarks_and_headers(doc)

    # Собираем все изображения для пакетной загрузки
    download_tasks, paragraph_image_map, table_image_map = await collect_all_images_from_document(doc, image_prefix)

    # Загружаем все изображения параллельно
    image_download_map = {}
    if download_tasks:
        # Обновляем пути к папке изображений в задачах
        updated_tasks = [(url, images_dir, filename) for url, _, filename in download_tasks]

        async with ImageDownloadManager() as download_manager:
            image_download_map = await download_manager.download_images_batch(updated_tasks)

    # Extract document content and metadata
    body = doc.get('body').get('content')
    inline_objects = doc.get('inlineObjects', {})
    lists = doc.get('lists', {})
    md_lines = []
    img_count = 1
    list_counters = {}
    in_code_block = False
    code_block_lines = []

    # Process each element in the document body
    for idx, element in enumerate(body):
        if 'paragraph' in element:
            para = element['paragraph']
            elements = para.get('elements', [])
            paragraph_style = para.get('paragraphStyle', {})
            named_style = paragraph_style.get('namedStyleType', '')
            bullet = para.get('bullet')
            line = ""
            previous_endswith_alnum = False

            # Detect code block
            is_code_block = detect_code_block(para)

            if is_code_block and not in_code_block:
                in_code_block = True
                code_block_lines = []
            elif not is_code_block and in_code_block:
                if code_block_lines:
                    md_lines.append('```')
                    md_lines.extend(code_block_lines)
                    md_lines.append('```')
                    code_block_lines = []
                in_code_block = False

            # Handle lists
            if bullet:
                list_marker, list_type = process_list_item_in_paragraph(para, lists, list_counters)
                if list_marker:
                    line += list_marker
            elif named_style.startswith("HEADING_"):
                level = int(named_style.replace("HEADING_", ""))
                line += "#" * level + " "

            # Process paragraph elements
            for elem in elements:
                if 'inlineObjectElement' in elem:
                    # Используем уже загруженные изображения
                    object_id = elem['inlineObjectElement']['inlineObjectId']
                    if object_id in paragraph_image_map:
                        img_filename, img_tag = paragraph_image_map[object_id]
                        if image_download_map.get(img_filename, False):
                            if line and line[-1].isalnum():
                                line += " "
                            line += f"![Image](./images/{img_filename})"
                elif 'textRun' in elem:
                    run_text = elem['textRun'].get('content', '')
                    text_style = elem['textRun'].get('textStyle', {})
                    font_family = text_style.get('fontFamily', '')
                    is_inline_code_fallback = any(font_name in font_family.lower() for font_name in
                                                  ['courier', 'monaco',
                                                   'monospace']) and not font_family.lower() == 'consolas'
                    if is_inline_code_fallback and not in_code_block:
                        processed = f"`{run_text.strip()}`"
                    else:
                        processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers)
                    if line and previous_endswith_alnum and processed and processed[0].isalnum():
                        line += ' '
                    line += processed
                    previous_endswith_alnum = processed[-1].isalnum() if processed else False

            if in_code_block and is_code_block:
                code_text = ""
                for elem in elements:
                    if 'textRun' in elem:
                        code_text += elem['textRun'].get('content', '')
                code_block_lines.append(code_text.rstrip())
            else:
                if line.strip():
                    line_with_anchor = add_header_anchors(line, named_style)
                    md_lines.append(line_with_anchor.rstrip())

        elif 'table' in element:
            if in_code_block and code_block_lines:
                md_lines.append('```')
                md_lines.extend(code_block_lines)
                md_lines.append('```')
                code_block_lines = []
                in_code_block = False

            img_count_ref = [img_count]
            md_table = table_to_markdown(element['table'], inline_objects, images_dir, img_count_ref,
                                         bookmarks, headers, lists, image_prefix, table_image_map, image_download_map)
            img_count = img_count_ref[0]
            if md_table.strip():
                md_lines.append(md_table)

    if in_code_block and code_block_lines:
        md_lines.append('```')
        md_lines.extend(code_block_lines)
        md_lines.append('```')

    # Join markdown content with smart line spacing
    content = join_markdown_lines_smart(md_lines)

    # Write initial markdown content to file
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(content)

    # Apply enhanced post-processing for code block markers
    with open(output_md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Process the markers
    processed_content = post_process_markdown_code_blocks(content)

    # Write back the processed content
    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    logger.info(f"Successfully converted document to {output_md_path}")


def sftp_upload_file(local_path, remote_path, sftp_config):
    """
    Upload file to remote server via SFTP

    Args:
        local_path (str): Local file path
        remote_path (str): Remote file path
        sftp_config (SFTPConfig): SFTP configuration
    """
    transport = None
    sftp = None

    try:
        # Establish SFTP connection
        transport = paramiko.Transport((sftp_config.host, sftp_config.port))
        transport.connect(username=sftp_config.user, password=sftp_config.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create remote directory structure if it doesn't exist
        remote_dir = os.path.dirname(remote_path)
        _create_remote_directory(sftp, remote_dir)

        # Upload file to remote server
        sftp.put(local_path, remote_path)
        logger.info(f"Successfully uploaded {local_path} to {remote_path}")

    except Exception as e:
        logger.error(f"SFTP upload failed for {local_path}: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def sftp_upload_directory(local_dir, remote_dir, sftp_config):
    """
    Upload entire directory to remote server via SFTP

    Args:
        local_dir (str): Local directory path
        remote_dir (str): Remote directory path
        sftp_config (SFTPConfig): SFTP configuration
    """
    transport = None
    sftp = None

    try:
        # Establish SFTP connection
        transport = paramiko.Transport((sftp_config.host, sftp_config.port))
        transport.connect(username=sftp_config.user, password=sftp_config.password)
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Create remote directory structure if it doesn't exist
        _create_remote_directory(sftp, remote_dir)

        # Upload all files in the local directory
        for root, dirs, files in os.walk(local_dir):
            for file in files:
                local_file_path = os.path.join(root, file)
                # Calculate relative path from local_dir
                relative_path = os.path.relpath(local_file_path, local_dir)
                remote_file_path = os.path.join(remote_dir, relative_path).replace("\\", "/")

                # Create subdirectories if needed
                remote_file_dir = os.path.dirname(remote_file_path)
                if remote_file_dir != remote_dir:
                    _create_remote_directory(sftp, remote_file_dir)

                # Upload the file
                sftp.put(local_file_path, remote_file_path)
                logger.debug(f"Uploaded {local_file_path} to {remote_file_path}")

        logger.info(f"Successfully uploaded directory {local_dir} to {remote_dir}")

    except Exception as e:
        logger.error(f"SFTP directory upload failed for {local_dir}: {e}")
        raise
    finally:
        if sftp:
            sftp.close()
        if transport:
            transport.close()


def _create_remote_directory(sftp, remote_dir):
    """
    Create remote directory structure recursively

    Args:
        sftp: SFTP client
        remote_dir (str): Remote directory path
    """
    try:
        sftp.chdir(remote_dir)
    except IOError:
        # Create directory path recursively
        dirs = remote_dir.strip('/').split('/')
        current_dir = ''
        for dir_name in dirs:
            current_dir += '/' + dir_name
            try:
                sftp.chdir(current_dir)
            except IOError:
                sftp.mkdir(current_dir)
                sftp.chdir(current_dir)


def extract_gdoc_id_from_url(url):
    """
    Extract Google Doc ID from URL

    Args:
        url (str): Google Docs URL or document ID

    Returns:
        str: Document ID
    """
    if "/d/" in url:
        start = url.index("/d/") + 3
        end = url.find("/", start)
        if end == -1:
            end = url.find("?", start)
        if end == -1:
            end = len(url)
        return url[start:end]
    return url


def get_gdocs_batch_from_sheet(sheet_id, creds):
    """
    Получить batch Google Docs из листа Links в Google Sheet

    Args:
        sheet_id (str): ID Google Sheet
        creds: Google API credentials

    Returns:
        list: Список кортежей (doc_url, remote_dir, custom_filename)
    """
    try:
        service = build_google_sheet('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        # Получаем значения из листа Links, колонки A, B, C, D
        result = sheet.values().get(
            spreadsheetId=sheet_id,
            range="Links!A:D"
        ).execute()

        values = result.get('values', [])
        output = []

        if not values:
            logger.warning("No data found in Links sheet")
            return output

        # Обрабатываем каждую строку (пропускаем заголовок)
        for row_index, row in enumerate(values[1:], start=2):
            try:
                if len(row) >= 2:
                    # Проверяем флажок в колонке D (индекс 3)
                    publish_flag = row[3] if len(row) >= 4 and row[3].strip() else False

                    # Обрабатываем только если флажок установлен
                    if publish_flag and publish_flag.upper() in ['TRUE', 'YES', '1', 'ON', '✓']:
                        doc_url = row[0].strip() if row[0] else None
                        remote_dir = row[1].strip() if row[1] else None
                        file_name = row[2].strip() if len(row) >= 3 and row[2] else None

                        if doc_url and remote_dir:
                            output.append((doc_url, remote_dir, file_name))
                            logger.info(f"Added document from row {row_index}: {doc_url}")
                        else:
                            logger.warning(f"Incomplete data in row {row_index}: missing URL or remote directory")

            except Exception as e:
                logger.error(f"Error processing row {row_index}: {e}")
                continue

        logger.info(f"Successfully loaded {len(output)} documents from Links sheet")
        return output

    except Exception as e:
        logger.error(f"Failed to get documents from Links sheet: {e}")
        raise ConfigurationError(f"Cannot load documents list: {e}")


def get_document_name(document_id, creds):
    """
    Get document title from Google Doc

    Args:
        document_id (str): Google Doc ID
        creds: Google API credentials

    Returns:
        str: Document title
    """
    try:
        docs_service = build('docs', 'v1', credentials=creds)
        doc = docs_service.documents().get(documentId=document_id, fields='title').execute()
        title = doc.get('title', 'output')
        logger.debug(f"Retrieved document title: {title}")
        return title
    except Exception as e:
        logger.error(f"Failed to get document title for {document_id}: {e}")
        raise


def normalize_filename(name):
    """
    Normalize filename by removing special characters and spaces

    Args:
        name (str): Original filename

    Returns:
        str: Normalized filename
    """
    name = name.strip().lower().replace(" ", "_")
    normalized = "".join([c for c in name if c.isalnum() or c in ('_', '-')])
    logger.debug(f"Normalized filename: {name} -> {normalized}")
    return normalized


async def process_document_async(doc_id, md_path, images_dir, creds, image_prefix):
    """
    Асинхронная обработка одного документа
    """
    await convert_gdoc_to_markdown(doc_id, md_path, images_dir, creds, image_prefix)


def run_async_conversion(doc_id, md_path, images_dir, creds, image_prefix):
    """
    Запуск асинхронной конвертации в синхронном контексте
    """
    try:
        # Проверяем, есть ли уже запущенный event loop
        loop = asyncio.get_running_loop()
        # Если есть, запускаем в отдельном потоке
        with ThreadPoolExecutor() as executor:
            future = executor.submit(
                asyncio.run,
                convert_gdoc_to_markdown(doc_id, md_path, images_dir, creds, image_prefix)
            )
            future.result()
    except RuntimeError:
        # Нет запущенного event loop, можем запустить напрямую
        asyncio.run(convert_gdoc_to_markdown(doc_id, md_path, images_dir, creds, image_prefix))


def main():
    """Main function to process Google Docs batch conversion"""
    # Конфигурация
    SHEET_ID = '1I2fJDAXP2ZyOLJA4iVxX7BbLNBEO_5j7AmYtcuxjtlw'

    try:
        logger.info("=== Starting Google Docs to Markdown conversion ===")

        # Authenticate with Google APIs
        logger.info("Authenticating with Google APIs...")
        creds = authenticate()

        # Get SFTP configuration from Google Sheet
        logger.info("Loading SFTP configuration from Google Sheet...")
        sftp_config = get_credentials_from_sheet(SHEET_ID, creds)

        # Get batch jobs from Google Sheet
        logger.info("Loading document list from Google Sheet...")
        jobs = get_gdocs_batch_from_sheet(SHEET_ID, creds)

        if not jobs:
            logger.warning("No documents found for processing")
            return

        logger.info(f"Found {len(jobs)} documents to process")

        # Process each document
        for job_index, (doc_url, remote_dir, custom_filename) in enumerate(jobs, start=1):
            logger.info(f"\n=== Processing document {job_index}/{len(jobs)} ===")
            logger.info(f"URL: {doc_url}")
            logger.info(f"Remote directory: {remote_dir}")
            logger.info(f"Custom filename: {custom_filename}")

            try:
                # Validate that we have a proper Google Docs URL
                if not doc_url or not ("docs.google.com" in doc_url or len(doc_url) == 44):
                    logger.error(f"Invalid Google Docs URL or ID: {doc_url}")
                    continue

                # Extract document ID from URL
                doc_id = extract_gdoc_id_from_url(doc_url)
                logger.info(f"Document ID: {doc_id}")

                # Validate document ID
                if not doc_id or len(doc_id) < 20:
                    logger.error(f"Could not extract valid document ID from: {doc_url}")
                    continue

                # Determine markdown filename and image prefix
                if custom_filename and custom_filename.strip():
                    # Use custom filename from column C
                    md_filename = custom_filename.strip()
                    # Ensure it has .md extension
                    if not md_filename.endswith('.md'):
                        md_filename += '.md'
                    # Extract base name for image prefix (without .md extension)
                    image_prefix = md_filename.replace('.md', '')
                else:
                    # Fall back to document title if no custom filename provided
                    title = get_document_name(doc_id, creds)
                    md_filename = normalize_filename(title) + ".md"
                    image_prefix = normalize_filename(title)

                # Create temporary directory for processing
                temp_dir = tempfile.mkdtemp()
                images_dir = os.path.join(temp_dir, "images")
                md_path = os.path.join(temp_dir, md_filename)

                logger.info(f"Processing document with filename: {md_filename}")
                logger.info(f"Image prefix: {image_prefix}")

                # Convert Google Doc to markdown with async image downloading (with large document support)
                run_async_conversion_large(doc_id, md_path, images_dir, creds, image_prefix)

                # Upload markdown file to remote server
                remote_md_path = os.path.join(remote_dir, md_filename).replace("\\", "/")
                sftp_upload_file(md_path, remote_md_path, sftp_config)

                # Upload images directory to remote server if it contains files
                if os.path.exists(images_dir) and os.listdir(images_dir):
                    remote_images_dir = os.path.join(remote_dir, "images").replace("\\", "/")
                    sftp_upload_directory(images_dir, remote_images_dir, sftp_config)
                    logger.info(f"Uploaded images directory to {remote_images_dir}")

                logger.info(f"✅ Successfully processed document {job_index}")

                # Clean up temporary directory
                shutil.rmtree(temp_dir)

                # Wait between processing to avoid rate limits
                time.sleep(2)

            except Exception as e:
                logger.error(f"❌ Failed to process document {job_index}: {e}")
                continue

        logger.info("=== Batch processing complete! ===")

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1

    return 0


if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)