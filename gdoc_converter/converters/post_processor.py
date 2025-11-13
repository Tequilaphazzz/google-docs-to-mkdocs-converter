import re
import logging
from ..config import Config

logger = logging.getLogger(__name__)


def post_process_markdown_code_blocks(content: str, config: Config) -> str:
    """
    FIXED: Enhanced post-processing with line break cleanup
    (Original logic copied, with config passing)
    """
    marker = config.code_block_marker  # Use config
    if marker not in content:
        return clean_excessive_line_breaks(content)

    lines = content.splitlines()
    processed_lines = []
    in_code_block = False

    for i, line in enumerate(lines):
        if marker in line:
            while marker in line:
                if not in_code_block:
                    processed_lines.append("```")
                    in_code_block = True
                else:
                    processed_lines.append("```")
                    in_code_block = False
                line = line.replace(marker, "", 1)

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
    processed_content = clean_excessive_line_breaks(processed_content)
    return processed_content


def clean_excessive_line_breaks(content: str) -> str:
    """
    NEW FUNCTION: Clean up excessive <br /> tags
    (Original logic copied)
    """
    lines = content.splitlines()
    cleaned_lines = []
    for i, line in enumerate(lines):
        cleaned_line = line
        # Remove <br /> after headers
        if re.match(r'^#+\s.*<br\s*/>\s*{#.*}?\s*$', line):
            cleaned_line = re.sub(r'<br\s*/>\s*({#.*}?\s*)$', r'\1', line)
        # Remove <br /> at the end of list items
        elif re.match(r'^\s*[-*+]\s.*<br\s*/>\s*$', line) or re.match(r'^\s*\d+\.\s.*<br\s*/>\s*$', line):
            cleaned_line = re.sub(r'<br\s*/>\s*$', '', line)
        # Remove <br /> at the end of lines if the next line is empty
        elif line.endswith('<br />') or line.endswith('<br/>'):
            next_line_empty = (i + 1 >= len(lines)) or (i + 1 < len(lines) and not lines[i + 1].strip())
            if next_line_empty:
                cleaned_line = re.sub(r'<br\s*/>\s*$', '', line)
        cleaned_lines.append(cleaned_line)
    return '\n'.join(cleaned_lines)


def join_markdown_lines_smart(md_lines: list) -> str:
    """
    FIXED: Smartly join Markdown lines
    (Original logic copied)
    """
    if not md_lines:
        return ""

    cleaned_lines = [clean_line_breaks_at_end(line) for line in md_lines]
    result_lines = []
    prev_line_is_list = False

    for i, line in enumerate(cleaned_lines):
        current_line_is_list = is_list_item(line)
        if i == 0:
            result_lines.append(line)
        else:
            if should_add_empty_line(cleaned_lines[i - 1], line, prev_line_is_list, current_line_is_list):
                result_lines.append("")
            result_lines.append(line)
        prev_line_is_list = current_line_is_list
    return '\n'.join(result_lines)


def ensure_proper_code_block_spacing(content: str) -> str:
    """
    Ensure proper spacing around code blocks
    (Original logic copied)
    """
    lines = content.splitlines()
    processed_lines = []
    in_code_block = False
    i = 0
    while i < len(lines):
        line = lines[i]
        processed_lines.append(line)
        if line.strip() == "```":
            if not in_code_block:
                in_code_block = True
                next_line_index = i + 1
                if (next_line_index < len(lines) and lines[next_line_index].strip() == ""):
                    i += 1  # Skip empty line after opening
            else:
                in_code_block = False
                next_line_index = i + 1
                if next_line_index < len(lines):
                    next_line = lines[next_line_index]
                    if next_line.strip() != "" and next_line.strip() != "```":
                        processed_lines.append("")  # Add empty line after closing
        i += 1
    return '\n'.join(processed_lines)


def is_list_item(line: str) -> bool:
    """Checks if a line is a list item"""
    # (Original logic copied)
    if re.match(r'^\s*\d+\.\s', line):
        return True
    if re.match(r'^\s*[-*+]\s', line):
        return True
    return False


def should_add_empty_line(prev_line, current_line, prev_is_list, current_is_list):
    """
    Determines if an empty line should be added between elements
    (Original logic copied)
    """
    if prev_is_list and current_is_list:
        return False
    if prev_is_list and not current_is_list:
        return True
    if not prev_is_list and current_is_list:
        return True
    if current_line.strip().startswith('#'):
        return True
    if current_line.strip().startswith('<table>'):
        return True
    if current_line.strip() == '```':
        return True
    return True


def clean_line_breaks_at_end(text: str) -> str:
    """
    NEW FUNCTION: Remove trailing <br /> tags at the end of lines
    (Original logic copied)
    """
    if not text:
        return text
    text = re.sub(r'<br\s*/>\s*$', '', text.strip())
    return text