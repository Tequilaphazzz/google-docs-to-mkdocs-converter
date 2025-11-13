import logging
from ..config import Config
from .text import process_text_run_enhanced, process_line_breaks_in_text

logger = logging.getLogger(__name__)

def process_list_content_with_line_breaks(elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers, config: Config):
    """
    NEW FUNCTION: Process list item content
    (Original logic copied, with config passing)
    """
    content = ""
    previous_endswith_alnum = False
    code_fonts = config.code_fonts # Use config

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

            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in code_fonts) and not font_family.lower() == 'consolas'

            if is_inline_code_fallback:
                processed_text = process_line_breaks_in_text(run_text)
                processed = f"`{processed_text.strip()}`"
            else:
                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers, config) # Pass config

            if content and previous_endswith_alnum and processed and processed[0].isalnum():
                content += ' '
            content += processed
            previous_endswith_alnum = processed[-1].isalnum() if processed else False
    return content.strip()


def process_list_content_with_line_breaks_for_chunks(elements, inline_objects, paragraph_image_map, image_download_map, bookmarks, headers, config: Config):
    """
    Process list item content for chunks
    (Original logic copied, with config passing)
    """
    content = ""
    previous_endswith_alnum = False
    code_fonts = config.code_fonts # Use config

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
            is_inline_code_fallback = any(font_name in font_family.lower() for font_name in code_fonts) and not font_family.lower() == 'consolas'

            if is_inline_code_fallback:
                processed_text = process_line_breaks_in_text(run_text)
                processed = f"`{processed_text.strip()}`"
            else:
                processed = process_text_run_enhanced(elem['textRun'], bookmarks, headers, config) # Pass config

            if content and previous_endswith_alnum and processed and processed[0].isalnum():
                content += ' '
            content += processed
            previous_endswith_alnum = processed[-1].isalnum() if processed else False
    return content.strip()


def get_list_marker(list_props, nesting_level, list_counters, list_id):
    """
    FIXED function to get the list marker
    (Original logic copied)
    """
    if not list_props or 'nestingLevels' not in list_props:
        indent = '    ' * nesting_level
        return indent + '- '

    if nesting_level >= len(list_props['nestingLevels']):
        nesting_level = len(list_props['nestingLevels']) - 1

    nesting_info = list_props['nestingLevels'][nesting_level]
    indent = '    ' * nesting_level

    if is_ordered_list(list_props, nesting_level):
        list_key = f"{list_id}_{nesting_level}"
        if list_key not in list_counters:
            start_number = nesting_info.get('startNumber', 1)
            list_counters[list_key] = start_number
        else:
            list_counters[list_key] += 1
        return indent + f"{list_counters[list_key]}. "
    else:
        return indent + '- '


def is_ordered_list(list_props, nesting_level):
    """
    FIXED function to determine if a list is ordered
    (Original logic copied)
    """
    if not list_props or 'nestingLevels' not in list_props:
        return False
    if nesting_level >= len(list_props['nestingLevels']):
        nesting_level = len(list_props['nestingLevels']) - 1

    nesting_info = list_props['nestingLevels'][nesting_level]
    glyph_format = nesting_info.get('glyphFormat', '')
    glyph_type = nesting_info.get('glyphType', '')
    glyph_symbol = nesting_info.get('glyphSymbol', '')

    bullet_symbols = ['●', '○', '■', '▪', '▫', '◦', '‣', '⁃', '-', '*', '+', '•']
    if glyph_symbol and glyph_symbol in bullet_symbols:
        return False

    has_number_format = glyph_format and '%' in glyph_format
    ordered_glyph_types = ['DECIMAL', 'ALPHA', 'UPPER_ALPHA', 'ROMAN', 'UPPER_ROMAN']
    has_ordered_type = glyph_type in ordered_glyph_types
    return has_number_format and has_ordered_type


def process_list_item_in_paragraph(para, lists, list_counters):
    """
    FIXED function to process list items in paragraphs
    (Original logic copied)
    """
    bullet = para.get('bullet')
    if not bullet:
        return None, None

    list_id = bullet['listId']
    nesting_level = bullet.get('nestingLevel', 0)

    if lists and list_id in lists:
        list_props = lists[list_id]['listProperties']
        is_ordered = is_ordered_list(list_props, nesting_level)

        if is_ordered:
            list_key = f"{list_id}_{nesting_level}"
            if list_key not in list_counters:
                start_number = 1
                if ('nestingLevels' in list_props and
                        nesting_level < len(list_props['nestingLevels'])):
                    start_number = list_props['nestingLevels'][nesting_level].get('startNumber', 1)
                list_counters[list_key] = start_number
            else:
                list_counters[list_key] += 1
            indent = '    ' * nesting_level
            marker = f"{list_counters[list_key]}. "
            return indent + marker, 'ordered'
        else:
            indent = '    ' * nesting_level
            marker = '- '
            return indent + marker, 'unordered'

    indent = '    ' * nesting_level
    return indent + '- ', 'unordered'


def build_list_html_fixed(list_type, items):
    """
    FIXED function to build list HTML
    (Original logic copied)
    """
    if not items:
        return ''
    html_lines = []
    current_level = 0
    open_lists = []

    for item in items:
        level = item['level']
        text = item['text']
        item_type = item['type']
        html_tag = 'ol' if item_type == 'ordered' else 'ul'

        while current_level < level:
            html_lines.append('  ' * len(open_lists) + f'<{html_tag}>')
            open_lists.append(html_tag)
            current_level += 1
        while current_level > level:
            if open_lists:
                closed_type = open_lists.pop()
                html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')
            current_level -= 1
        if open_lists and open_lists[-1] != html_tag:
            closed_type = open_lists.pop()
            html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')
            html_lines.append('  ' * len(open_lists) + f'<{html_tag}>')
            open_lists.append(html_tag)

        indent = '  ' * len(open_lists)
        html_lines.append(f'{indent}<li>{text}</li>')

    while open_lists:
        closed_type = open_lists.pop()
        html_lines.append('  ' * len(open_lists) + f'</{closed_type}>')
    return '\n'.join(html_lines)