"""
Giữ lại để tương thích ngược; mã chính nằm ở src/nlp/nlp_cleaner.py.
"""
from src.nlp.nlp_cleaner import HTML_REGEX, WHITESPACE_REGEX, clean_text, clean_text_spark


def clean_html_regex(text: str) -> str:
    return clean_text(text) or ""
