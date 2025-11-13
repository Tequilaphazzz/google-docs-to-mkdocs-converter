import os
import logging
import asyncio
import time
from googleapiclient.discovery import build
from ..config import Config
from ..exceptions import DocumentSizeError
from .images import ImageDownloadManager, collect_all_images_from_document
from .text import process_text_run_enhanced
from .lists import (
    process_list_item_in_paragraph,
    process_list_content_with_line_breaks,
    process_list_content_with_line_breaks_for_chunks
)
from .tables import table_to_markdown
from .post_processor import post_process_markdown_code_blocks, join_markdown_lines_smart

logger = logging.getLogger(__name__)


# --- Size Check and Chunking ---

def check_document_size(doc, config: Config):
    """
    Check document size before processing
    """
    try:
        body = doc.get('body', {}).get('content', [])
        estimated_size = 0
        element_count = 0

        for element in body:
            element_count += 1
            # (Original size estimation code)
            if 'paragraph' in element:
                para = element['paragraph']
                for elem in para.get('elements', []):
                    if 'textRun' in elem:
                        estimated_size += len(elem['textRun'].get('content', ''))
            elif 'table' in element:
                table = element['table']
                for row in table.get('tableRows', []):
                    for cell in row.get('tableCells', []):
                        for cell_content in cell.get('content', []):
                            if 'paragraph' in cell_content:
                                para = cell_content['paragraph']
                                for elem in para.get('elements', []):
                                    if 'textRun' in elem:
                                        estimated_size += len(elem['textRun'].get('content', ''))

        logger.info(f"Document size estimate: {estimated_size} characters, {element_count} elements")

        max_size = config.max_doc_size
        if estimated_size > max_size:
            raise DocumentSizeError(f"Document too large: {estimated_size} characters (max: {max_size})")

        if element_count > 10000:
            logger.warning(f"Document has many elements: {element_count}. Processing may be slow.")

        return estimated_size, element_count

    except Exception as e:
        logger.error(f"Failed to check document size: {e}")
        raise


def get_document_with_retry(docs_service, document_id: str, max_retries=3):
    """
    Get document with retries on failure
    """
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries} to fetch document...")
            doc = docs_service.documents().get(
                documentId=document_id,
            ).execute()
            logger.info(f"Successfully fetched document on attempt {attempt + 1}")
            return doc

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                delay = 2 ** attempt
                logger.info(f"Waiting {delay} seconds before retry...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed")
                raise


def process_document_in_chunks(doc, config: Config):
    """
    FIXED: Process document in chunks for large documents
    (Original logic copied)
    """
    body = doc.get('body', {}).get('content', [])
    chunk_size = config.chunk_size  # Use config

    chunks = []
    current_chunk = []
    current_size = 0

    for element in body:
        element_size = estimate_element_size(element)

        if 'table' in element:
            if current_size + element_size > chunk_size and current_chunk:
                chunks.append(current_chunk)
                current_chunk = [element]
                current_size = element_size
            else:
                current_chunk.append(element)
                current_size += element_size
        else:
            if current_size + element_size > chunk_size and current_chunk:
                chunks.append(current_chunk)
                current_chunk = [element]
                current_size = element_size
            else:
                current_chunk.append(element)
                current_size += element_size

    if current_chunk:
        chunks.append(current_chunk)

    logger.info(f"Document split into {len(chunks)} chunks")
    return chunks


def estimate_element_size(element):
    """
    Roughly estimate the size of a document element
    (Original logic copied)
    """
    size = 0
    if 'paragraph' in element:
        para = element['paragraph']
        for elem in para.get('elements', []):
            if 'textRun' in elem:
                size += len(elem['textRun'].get('content', ''))
    elif 'table' in element:
        table = element['table']
        for row in table.get('tableRows', []):
            for cell in row.get('tableCells', []):
                for cell_content in cell.get('content', []):
                    if 'paragraph' in cell_content:
                        para = cell_content['paragraph']
                        for elem in para.get('elements', []):
                            if 'textRun' in elem:
                                size += len(elem['textRun'].get('content', ''))
    return size


# --- Bookmarks and Headers ---

def extract_bookmarks_and_headers(doc):
    """Extract bookmarks and headers from document for link processing"""
    # (Original logic copied)
    bookmarks = {}
    headers = {}
    doc_bookmarks = doc.get('bookmarks', {})
    for bookmark_id, bookmark_data in doc_bookmarks.items():
        text_content = bookmark_data.get('textContent', '')
        if text_content:
            slug = text_content.strip().lower().replace(' ', '-').replace('_', '-')
            slug = ''.join(c for c in slug if c.isalnum() or c == '-')
            bookmarks[bookmark_id] = slug

    body = doc.get('body', {}).get('content', [])
    for element in body:
        if 'paragraph' in element:
            para = element['paragraph']
            paragraph_style = para.get('paragraphStyle', {})
            named_style = paragraph_style.get('namedStyleType', '')
            if named_style.startswith("HEADING_"):
                header_text = ""
                for elem in para.get('elements', []):
                    if 'textRun' in elem:
                        header_text += elem['textRun'].get('content', '')
                if header_text.strip():
                    slug = header_text.strip().lower().replace(' ', '-').replace('_', '-')
                    slug = ''.join(c for c in slug if c.isalnum() or c == '-')
                    headers[header_text.strip()] = slug
    return bookmarks, headers


def detect_code_block(paragraph, config: Config):
    """Detect if paragraph represents a code block"""
    # (Original logic copied)
    paragraph_style = paragraph.get('paragraphStyle', {})
    named_style = paragraph_style.get('namedStyleType', '')
    if 'CODE' in named_style.upper():
        return True

    elements = paragraph.get('elements', [])
    code_fonts = config.code_fonts  # Use config
    for elem in elements:
        if 'textRun' in elem:
            text_style = elem['textRun'].get('textStyle', {})
            font_family = elem['textRun'].get('textStyle', {}).get('fontFamily', '')
            if any(font_name in font_family.lower() for font_name in code_fonts):
                return True
    return False


def add_header_anchors(line, named_style):
    """
    FIXED: Add anchor tags to headers without adding extra <br/> tags
    (Original logic copied)
    """
    if named_style.startswith("HEADING_"):
        header_text = line.lstrip('#').strip()
        if header_text:
            header_text = header_text.replace('<br />', ' ').strip()
            slug = header_text.lower().replace(' ', '-').replace('_', '-')
            slug = ''.join(c for c in slug if c.isalnum() or c == '-')
            level = named_style.replace("HEADING_", "")
            clean_header = "#" * int(level) + " " + header_text
            return f"{clean_header} {{#{slug}}}"
    return line


# --- Main Conversion Functions ---

async def convert_gdoc_to_markdown_large(document_id, output_md_path, images_dir, creds, config: Config,
                                         image_prefix=None):
    """
    IMPROVED conversion function for large documents
    """
    try:
        docs_service = build('docs', 'v1', credentials=creds)
        doc = get_document_with_retry(docs_service, document_id)
        estimated_size, element_count = check_document_size(doc, config)  # Pass config
        os.makedirs(images_dir, exist_ok=True)
        bookmarks, headers = extract_bookmarks_and_headers(doc)

        if estimated_size > config.chunk_size:  # Use config
            logger.info(f"Large document detected ({estimated_size} chars), using chunked processing")
            await convert_large_document_chunked(doc, output_md_path, images_dir, creds, config, image_prefix,
                                                 bookmarks, headers)
        else:
            logger.info("Standard size document, using standard processing")
            await convert_gdoc_to_markdown_standard(doc, output_md_path, images_dir, creds, config, image_prefix,
                                                    bookmarks, headers)

    except DocumentSizeError as e:
        logger.error(f"Document size error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error processing large document: {e}")
        raise


async def convert_gdoc_to_markdown_standard(doc, output_md_path, images_dir, creds, config: Config, image_prefix,
                                            bookmarks, headers):
    """
    Standard document processing
    """
    # Collect all images
    download_tasks, paragraph_image_map, table_image_map = await collect_all_images_from_document(doc, image_prefix)

    # Download images
    image_download_map = {}
    if download_tasks:
        updated_tasks = [(url, images_dir, filename) for url, _, filename in download_tasks]
        async with ImageDownloadManager(config) as download_manager:  # Pass config
            image_download_map = await download_manager.download_images_batch(updated_tasks)

    # (Original processing logic, passing config where needed)
    body = doc.get('body').get('content')
    inline_objects = doc.get('inlineObjects', {})
    lists = doc.get('lists', {})
    md_lines = []
    list_counters = {}
    in_code_block = False
    code_block_lines = []
    code_fonts = config.code_fonts  # Use config

    for idx, element in enumerate(body):
        if 'paragraph' in element:
            para = element['paragraph']
            elements = para.get('elements', [])
            paragraph_style = para.get('paragraphStyle', {})
            named_style = paragraph_style.get('namedStyleType', '')
            bullet = para.get('bullet')
            line = ""
            previous_endswith_alnum = False

            is_code_block = detect_code_block(para, config)  # Pass config

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
                    list_content = process_list_content_with_line_breaks(
                        elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers, config
                        # Pass config
                    )
                    line += list_content
            elif named_style.startswith("HEADING_"):
                level = int(named_style.replace("HEADING_", ""))
                line += "#" * level + " "
                # (Header element processing...)
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
                                                      code_fonts) and not font_family.lower() == 'consolas'
                        if is_inline_code_fallback and not in_code_block:
                            processed = f"`{run_text.strip()}`"
                        else:
                            processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers,
                                                                  config)  # Pass config
                        if line and previous_endswith_alnum and processed and processed[0].isalnum():
                            line += ' '
                        line += processed
                        previous_endswith_alnum = processed[-1].isalnum() if processed else False
            else:
                # (Regular paragraph processing...)
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
                                                      code_fonts) and not font_family.lower() == 'consolas'
                        if is_inline_code_fallback and not in_code_block:
                            processed = f"`{run_text.strip()}`"
                        else:
                            processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers,
                                                                  config)  # Pass config
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

            img_count_ref = [1]  # Local counter for this table
            md_table = table_to_markdown(element['table'], inline_objects, images_dir, img_count_ref,
                                         bookmarks, headers, lists, config, image_prefix, table_image_map,
                                         image_download_map)  # Pass config
            if md_table.strip():
                md_lines.append(md_table)

    if in_code_block and code_block_lines:
        md_lines.append('```')
        md_lines.extend(code_block_lines)
        md_lines.append('```')

    content = join_markdown_lines_smart(md_lines)

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(content)

    with open(output_md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    processed_content = post_process_markdown_code_blocks(content, config)  # Pass config

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    logger.info(f"Document converted successfully to {output_md_path}")


async def convert_large_document_chunked(doc, output_md_path, images_dir, creds, config: Config, image_prefix,
                                         bookmarks, headers):
    """
    Process large documents in chunks
    """
    download_tasks, paragraph_image_map, table_image_map = await collect_all_images_from_document(doc, image_prefix)

    image_download_map = {}
    if download_tasks:
        batch_size = 10
        updated_tasks = [(url, images_dir, filename) for url, _, filename in download_tasks]

        for i in range(0, len(updated_tasks), batch_size):
            batch = updated_tasks[i:i + batch_size]
            logger.info(f"Processing image batch {i // batch_size + 1}")

            # Create a new manager with fewer concurrent downloads for chunks
            chunk_config_data = config.data.copy()
            chunk_config_data['network']['max_concurrent_image_downloads'] = 2
            chunk_config = Config(config_path="")  # Dummy path
            chunk_config.data = chunk_config_data

            async with ImageDownloadManager(chunk_config) as download_manager:  # Pass modified config
                batch_results = await download_manager.download_images_batch(batch)
                image_download_map.update(batch_results)
            await asyncio.sleep(1)

    chunks = process_document_in_chunks(doc, config)  # Pass config
    all_md_lines = []

    for i, chunk in enumerate(chunks):
        logger.info(f"Processing chunk {i + 1}/{len(chunks)}")
        temp_doc = {
            'body': {'content': chunk},
            'inlineObjects': doc.get('inlineObjects', {}),
            'lists': doc.get('lists', {}),
            'bookmarks': doc.get('bookmarks', {})
        }

        chunk_md_lines = await process_document_chunk(
            temp_doc, images_dir, bookmarks, headers,
            paragraph_image_map, table_image_map, image_download_map, config, image_prefix  # Pass config
        )
        all_md_lines.extend(chunk_md_lines)
        await asyncio.sleep(0.5)

    content = join_markdown_lines_smart(all_md_lines)

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(content)

    with open(output_md_path, 'r', encoding='utf-8') as f:
        content = f.read()

    processed_content = post_process_markdown_code_blocks(content, config)  # Pass config

    with open(output_md_path, 'w', encoding='utf-8') as f:
        f.write(processed_content)

    logger.info(f"Large document converted successfully to {output_md_path}")


async def process_document_chunk(doc, images_dir, bookmarks, headers, paragraph_image_map, table_image_map,
                                 image_download_map, config: Config, image_prefix):
    """
    FIXED: Process a document chunk
    """
    # (Logic is almost identical to convert_gdoc_to_markdown_standard, but without image downloading)
    # (Original logic copied, with config passing added)

    body = doc.get('body', {}).get('content', [])
    inline_objects = doc.get('inlineObjects', {})
    lists = doc.get('lists', {})
    md_lines = []
    list_counters = {}
    in_code_block = False
    code_block_lines = []
    code_fonts = config.code_fonts  # Use config

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

                is_code_block = detect_code_block(para, config)  # Pass config

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
                        list_content = process_list_content_with_line_breaks_for_chunks(
                            elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers,
                            config  # Pass config
                        )
                        line += list_content
                elif named_style.startswith("HEADING_"):
                    level = int(named_style.replace("HEADING_", ""))
                    line += "#" * level + " "
                    # (Header element processing...)
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
                                                          code_fonts) and not font_family.lower() == 'consolas'
                            if is_inline_code_fallback and not in_code_block:
                                processed = f"`{run_text.strip()}`"
                            else:
                                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers,
                                                                      config)  # Pass config
                            if line and previous_endswith_alnum and processed and processed[0].isalnum():
                                line += ' '
                            line += processed
                            previous_endswith_alnum = processed[-1].isalnum() if processed else False
                else:
                    # (Regular paragraph processing...)
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
                                                          code_fonts) and not font_family.lower() == 'consolas'
                            if is_inline_code_fallback and not in_code_block:
                                processed = f"`{run_text.strip()}`"
                            else:
                                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers,
                                                                      config)  # Pass config
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

                table_data = element['table']
                if 'tableRows' in table_data:
                    img_count_ref = [1]
                    md_table = table_to_markdown(table_data, inline_objects, images_dir, img_count_ref,
                                                 bookmarks, headers, lists, config, image_prefix, table_image_map,
                                                 # Pass config
                                                 image_download_map)
                    if md_table.strip():
                        md_lines.append(md_table)
                else:
                    logger.warning("Table element in chunk missing 'tableRows'")

        except Exception as e:
            logger.error(f"Error processing element in chunk: {e}")
            continue

    if in_code_block and code_block_lines:
        md_lines.append('```')
        md_lines.extend(code_block_lines)
        md_lines.append('```')

    return md_lines