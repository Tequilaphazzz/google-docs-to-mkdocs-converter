import re
import logging
from ..config import Config

logger = logging.getLogger(__name__)


def process_text_run_enhanced(text_run, bookmarks, headers, config: Config):
    """
    FIXED: Process Google Docs text run with correct line break handling
    (Original logic copied, with config passing)
    """
    text = text_run.get('content', '')
    if not text:
        return text

    style = text_run.get('textStyle', {})

    # Check for #d9d9d9 background for inline code
    background_color = style.get('backgroundColor', {})
    if background_color:
        rgb = background_color.get('color', {}).get('rgbColor', {})
        if rgb:
            red = rgb.get('red', 0)
            green = rgb.get('green', 0)
            blue = rgb.get('blue', 0)

            target_value = config.inline_code_rgb  # Use config
            tolerance = config.inline_code_tolerance  # Use config

            is_d9d9d9_gray_background = (
                    abs(red - target_value) < tolerance and
                    abs(green - target_value) < tolerance and
                    abs(blue - target_value) < tolerance and
                    red > 0.5 and green > 0.5 and blue > 0.5
            )

            if is_d9d9d9_gray_background:
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

        # Fallback for any light gray background
        if rgb:
            red = float(rgb.get('red', 0))
            green = float(rgb.get('green', 0))
            blue = float(rgb.get('blue', 0))
            avg_rgb = (red + green + blue) / 3
            max_diff = max(abs(red - avg_rgb), abs(green - avg_rgb), abs(blue - avg_rgb))
            if max_diff < 0.1 and 0.7 < avg_rgb < 0.95:
                processed_text = process_line_breaks_in_text(text)
                return f"`{processed_text.strip()}`"

    text = process_line_breaks_in_text(text)

    if style.get('strikethrough'):
        text = f"~~{text.strip()}~~"
    elif style.get('underline'):
        text = f"<u>{text.strip()}</u>"

    if style.get('bold') and style.get('italic'):
        text = f"***{text.strip()}***"
    elif style.get('bold'):
        text = f"**{text.strip()}**"
    elif style.get('italic'):
        text = f"*{text.strip()}*"

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


def process_text_run_enhanced_for_table(text_run, bookmarks, headers, config: Config, is_header=False):
    """
    FIXED: Process Google Docs text run for tables
    (Original logic copied, with config passing)
    """
    text = text_run.get('content', '')
    if not text:
        return text

    style = text_run.get('textStyle', {})

    background_color = style.get('backgroundColor', {})
    if background_color:
        rgb = background_color.get('color', {}).get('rgbColor', {})
        if rgb:
            red = rgb.get('red', 0)
            green = rgb.get('green', 0)
            blue = rgb.get('blue', 0)

            target_value = config.inline_code_rgb  # Use config
            tolerance = config.inline_code_tolerance  # Use config

            is_d9d9d9_gray_background = (
                    abs(red - target_value) < tolerance and
                    abs(green - target_value) < tolerance and
                    abs(blue - target_value) < tolerance
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

    text = process_line_breaks_in_text(text)

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

    if style.get('strikethrough'):
        text = f"<del>{escape_html_text(text.strip())}</del>"
    elif style.get('underline'):
        text = f"<u>{escape_html_text(text.strip())}</u>"
    else:
        if style.get('bold') and style.get('italic'):
            text = f"<em>{escape_html_text(text.strip())}" if is_header else f"<strong><em>{escape_html_text(text.strip())}</em></strong>"
        elif style.get('bold'):
            text = escape_html_text(text.strip()) if is_header else f"<strong>{escape_html_text(text.strip())}</strong>"
        elif style.get('italic'):
            text = f"<em>{escape_html_text(text.strip())}</em>"
        else:
            text = escape_html_text(text.strip())

    return text.replace('\u00A0', ' ')


def process_line_breaks_in_text(text: str) -> str:
    """
    NEW FUNCTION: Correctly handle soft line breaks
    (Original logic copied)
    """
    if not text:
        return text
    if text.strip().replace('\n', '').strip() == '':
        return ''
    text = text.replace('\n', '<br />')
    text = re.sub(r'^(<br\s*/?>)+', '', text)
    text = re.sub(r'(<br\s*/?>)+$', '', text)
    return text


def escape_html_text(text: str) -> str:
    """Escape HTML special characters in text content"""
    # (Original logic copied)
    if not text:
        return text
    text = str(text)
    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&#x27;')
    return text


def escape_html_attribute(attr: str) -> str:
    """Escape HTML special characters in attribute values"""
    # (Original logic copied)
    if not attr:
        return attr
    attr = str(attr)
    attr = attr.replace('&', '&amp;')
    attr = attr.replace('<', '&lt;')
    attr = attr.replace('>', '&gt;')
    attr = attr.replace('"', '&quot;')
    attr = attr.replace("'", '&#x27;')
    return attr


def escape_html_content(text: str) -> str:
    """
    Escape HTML special characters but preserve intentional HTML tags
    (Original logic copied)
    """
    protected_tags = {}
    tag_counter = 0
    preserve_patterns = [
        r'<br\s*/?>', r'<img[^>]+>', r'<a[^>]+>.*?</a>',
        r'<strong>.*?</strong>', r'<em>.*?</em>', r'<u>.*?</u>', r'<del>.*?</del>',
        r'<code>.*?</code>', r'<p>.*?</p>', r'<ul>.*?</ul>', r'<ol>.*?</ol>',
        r'<li>.*?</li>', r'<a[^>]+>', r'</a>', r'<strong>', r'</strong>',
        r'<em>', r'</em>', r'<u>', r'</u>', r'<del>', r'</del>',
        r'<code>', r'</code>', r'<p>', r'</p>', r'<ul>', r'</ul>',
        r'<ol>', r'</ol>', r'<li>', r'</li>'
    ]
    for pattern in preserve_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE | re.DOTALL)
        for match in matches:
            placeholder = f"__PRESERVE_TAG_{tag_counter}__"
            protected_tags[placeholder] = match
            text = text.replace(match, placeholder, 1)
            tag_counter += 1

    text = text.replace('&', '&amp;')
    text = text.replace('<', '&lt;')
    text = text.replace('>', '&gt;')
    text = text.replace('"', '&quot;')
    text = text.replace("'", '&#x27;')

    for placeholder, original_tag in protected_tags.items():
        text = text.replace(placeholder, original_tag)
    return text