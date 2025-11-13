import logging
from ..config import Config
from .text import process_text_run_enhanced_for_table, escape_html_content, escape_html_text, escape_html_attribute
from .lists import build_list_html_fixed, is_ordered_list
from .images import process_inline_image_sync

logger = logging.getLogger(__name__)


def format_paragraphs(paragraphs):
    """Format paragraphs with proper <p> tags"""
    # (Original logic copied)
    if not paragraphs:
        return []
    formatted_paragraphs = []
    if len(paragraphs) == 1 and len(paragraphs[0]) < 100:
        formatted_paragraphs.append(paragraphs[0])
    else:
        for para in paragraphs:
            if para.strip():
                formatted_paragraphs.append(f"<p>{para}</p>")
    return formatted_paragraphs


def process_cell_content(cell, inline_objects, images_dir, img_count_ref, bookmarks, headers, lists, config: Config,
                         is_header, image_prefix=None, table_image_map=None, image_download_map=None):
    """
    FIXED cell content processing
    (Original logic copied, with config passing)
    """
    cell_elements = []
    current_list_type = None
    list_items = []
    paragraphs = []

    for cell_content in cell.get('content', []):
        if 'paragraph' in cell_content:
            para = cell_content['paragraph']
            bullet = para.get('bullet')

            if bullet:
                if paragraphs:
                    cell_elements.extend(format_paragraphs(paragraphs))
                    paragraphs = []

                list_id = bullet['listId']
                nesting_level = bullet.get('nestingLevel', 0)
                list_type = 'unordered'
                if lists and list_id in lists:
                    list_props = lists[list_id]['listProperties']
                    is_ordered = is_ordered_list(list_props, nesting_level)
                    list_type = 'ordered' if is_ordered else 'unordered'

                item_text = ""
                for elem in para.get('elements', []):
                    if 'inlineObjectElement' in elem:
                        if table_image_map and image_download_map:
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in table_image_map:
                                img_filename, img_tag = table_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    item_text += img_tag
                        else:
                            item_text += process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref,
                                                                   config, image_prefix)  # Pass config
                    elif 'textRun' in elem:
                        item_text += process_text_run_enhanced_for_table(elem['textRun'], bookmarks, headers, config,
                                                                         is_header)  # Pass config
                item_text = item_text.strip()

                if current_list_type != list_type or current_list_type is None:
                    if current_list_type and list_items:
                        cell_elements.append(build_list_html_fixed(current_list_type, list_items))
                        list_items = []
                    current_list_type = list_type

                list_items.append({'text': item_text, 'level': nesting_level, 'type': list_type})

            else:
                if current_list_type and list_items:
                    cell_elements.append(build_list_html_fixed(current_list_type, list_items))
                    list_items = []
                    current_list_type = None

                para_text = ""
                for elem in para.get('elements', []):
                    if 'inlineObjectElement' in elem:
                        if table_image_map and image_download_map:
                            object_id = elem['inlineObjectElement']['inlineObjectId']
                            if object_id in table_image_map:
                                img_filename, img_tag = table_image_map[object_id]
                                if image_download_map.get(img_filename, False):
                                    para_text += img_tag
                        else:
                            para_text += process_inline_image_sync(elem, inline_objects, images_dir, img_count_ref,
                                                                   config, image_prefix)  # Pass config
                    elif 'textRun' in elem:
                        para_text += process_text_run_enhanced_for_table(elem['textRun'], bookmarks, headers, config,
                                                                         is_header)  # Pass config

                para_text = para_text.strip()
                if para_text:
                    paragraphs.append(para_text)

    if current_list_type and list_items:
        cell_elements.append(build_list_html_fixed(current_list_type, list_items))
    if paragraphs:
        cell_elements.extend(format_paragraphs(paragraphs))

    cell_html = ''.join(cell_elements) if cell_elements else ''
    cell_html = escape_html_content(cell_html)
    return cell_html


def table_to_markdown(table, inline_objects, images_dir, img_count_ref, bookmarks, headers, lists, config: Config,
                      image_prefix=None, table_image_map=None, image_download_map=None):
    """
    Convert Google Docs table to HTML format
    (Original logic copied, with config passing)
    """
    if 'tableRows' not in table:
        logger.warning("Table object missing 'tableRows', skipping table")
        return ''

    rows = table['tableRows']
    html_lines = []
    if not rows:
        return ''

    cell_contents = []
    raw_cells = []
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

    max_cols = max(len(row) for row in cell_contents) if cell_contents else 0
    for i, row in enumerate(cell_contents):
        while len(row) < max_cols:
            row.append('')
            if i < len(raw_cells):
                raw_cells[i].append({'content': []})

    spans = detect_corrected_spans(cell_contents)
    html_lines.append('<table>')

    for row_idx in range(len(cell_contents)):
        html_lines.append('  <tr>')
        is_header_row = (row_idx == 0)
        cell_tag = 'th' if is_header_row else 'td'
        col_idx = 0

        while col_idx < max_cols:
            if should_skip_cell(row_idx, col_idx, spans):
                col_idx += 1
                continue

            if (row_idx < len(raw_cells) and col_idx < len(raw_cells[row_idx]) and
                    col_idx < len(cell_contents[row_idx])):

                cell = raw_cells[row_idx][col_idx]

                cell_html = process_cell_content(
                    cell, inline_objects, images_dir, img_count_ref,
                    bookmarks, headers, lists, config, is_header_row, image_prefix,  # Pass config
                    table_image_map, image_download_map
                )

                cell_attrs = []
                span_info = spans.get((row_idx, col_idx))
                if span_info:
                    row_span, col_span = span_info
                    if col_span > 1:
                        cell_attrs.append(f'colspan="{col_span}"')
                    if row_span > 1:
                        cell_attrs.append(f'rowspan="{row_span}"')

                attrs_str = ' ' + ' '.join(cell_attrs) if cell_attrs else ''
                html_lines.append(f'    <{cell_tag}{attrs_str}>{cell_html}</{cell_tag}>')

            col_idx += 1
        html_lines.append('  </tr>')

    html_lines.append('</table>')
    return '\n'.join(html_lines)


def should_skip_cell(row_idx, col_idx, spans):
    """Check if a cell should be skipped due to spanning"""
    # (Original logic copied)
    for (span_row, span_col), (row_span, col_span) in spans.items():
        if (span_row <= row_idx < span_row + row_span and
                span_col <= col_idx < span_col + col_span and
                not (span_row == row_idx and span_col == col_idx)):
            return True
    return False


def detect_corrected_spans(cell_contents):
    """
    Improved span detection logic with rowspan support
    (Original logic copied)
    """
    spans = {}
    rows = len(cell_contents)
    cols = len(cell_contents[0]) if rows > 0 else 0
    if cols == 0:
        return spans

    # (Original logic copied...)
    # Phase 1: Detect colspan
    for row_idx in range(rows):
        col_idx = 0
        while col_idx < cols:
            cell_content = cell_contents[row_idx][col_idx]
            if cell_content.strip():
                colspan = 1
                next_col = col_idx + 1
                while next_col < cols and not cell_contents[row_idx][next_col].strip():
                    if (row_idx + 1 < rows and cell_contents[row_idx + 1][next_col].strip()):
                        break
                    colspan += 1
                    next_col += 1

                if colspan == 1 and row_idx + 1 < rows:
                    temp_colspan = 1
                    temp_next_col = col_idx + 1
                    while (temp_next_col < cols and
                           not cell_contents[row_idx][temp_next_col].strip() and
                           cell_contents[row_idx + 1][temp_next_col].strip()):
                        temp_colspan += 1
                        temp_next_col += 1
                    if temp_colspan > colspan:
                        colspan = temp_colspan

                if colspan > 1 and (len(cell_content) > 2 or cell_content.lower() in ['player', 'type', 'description']):
                    spans[(row_idx, col_idx)] = (1, colspan)
            col_idx += 1

    # Phase 2: Detect rowspan
    for col_idx in range(cols):
        row_idx = 0
        while row_idx < rows:
            cell_content = cell_contents[row_idx][col_idx]
            if cell_content.strip():
                existing_span = spans.get((row_idx, col_idx))
                current_rowspan, colspan = existing_span if existing_span else (1, 1)

                rowspan = 1
                next_row = row_idx + 1
                while next_row < rows and not cell_contents[next_row][col_idx].strip():
                    has_content_right = False
                    for check_col in range(col_idx + 1, min(col_idx + colspan, cols)):
                        if cell_contents[next_row][check_col].strip():
                            has_content_right = True
                            break
                    if has_content_right:
                        break
                    rowspan += 1
                    next_row += 1

                if rowspan == 1:
                    temp_rowspan = 1
                    temp_next_row = row_idx + 1
                    while (temp_next_row < rows and not cell_contents[temp_next_row][col_idx].strip()):
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

                if rowspan > 1 and (len(cell_content) > 3 or any(
                        kw in cell_content.lower() for kw in ['parameter', 'type', 'description', 'api', 'request'])):
                    spans[(row_idx, col_idx)] = (rowspan, colspan)
                row_idx += rowspan
            else:
                row_idx += 1

    # (Phases 3 and 4 from original...)
    if rows >= 2:
        for col_idx in range(cols):
            if (col_idx < len(cell_contents[0]) and cell_contents[0][col_idx].strip()):
                empty_count = 0
                check_col = col_idx + 1
                while (check_col < cols and
                       check_col < len(cell_contents[0]) and
                       not cell_contents[0][check_col].strip()):
                    if (1 < len(cell_contents) and
                            check_col < len(cell_contents[1]) and
                            cell_contents[1][check_col].strip()):
                        empty_count += 1
                    check_col += 1
                if empty_count > 0:
                    total_span = empty_count + 1
                    if (0, col_idx) not in spans:
                        spans[(0, col_idx)] = (1, total_span)
    if rows >= 3:
        for col_idx in range(min(3, cols)):
            if (cell_contents[0][col_idx].strip() and len(cell_contents[0][col_idx]) > 3):
                empty_rows = 0
                for check_row in range(1, min(rows, 4)):
                    if not cell_contents[check_row][col_idx].strip():
                        empty_rows += 1
                    else:
                        break
                if empty_rows > 0 and empty_rows < rows - 1:
                    has_content_below = False
                    for check_row in range(empty_rows + 1, rows):
                        if cell_contents[check_row][col_idx].strip():
                            has_content_below = True
                            break
                    if has_content_below:
                        total_rowspan = empty_rows + 1
                        if (0, col_idx) not in spans:
                            spans[(0, col_idx)] = (total_rowspan, 1)
    return spans


def extract_cell_text_content(cell):
    """Extract plain text content from a cell for analysis"""
    # (Original logic copied)
    content = ""
    for cell_content in cell.get('content', []):
        if 'paragraph' in cell_content:
            para = cell_content['paragraph']
            for elem in para.get('elements', []):
                if 'textRun' in elem:
                    content += elem['textRun'].get('content', '')
    return content.strip()