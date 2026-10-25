"""
Làm sạch văn bản tiếng Việt cho tầng Silver:
  1. Giải mã HTML entity (&amp;, &nbsp;, ...) và loại thẻ HTML.
  2. Chuẩn hóa Unicode về dạng NFC (dữ liệu báo điện tử trộn lẫn NFC và NFD,
     cùng một chữ "ạ" có thể là 1 hoặc 2 code point -> làm sai dedup và đặc trưng TF-IDF).
  3. Gộp khoảng trắng thừa.
"""
import html
import re
import unicodedata

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

HTML_REGEX = r"<[^>]+>"
WHITESPACE_REGEX = r"\s+"

_HTML_RE = re.compile(HTML_REGEX)
_WS_RE = re.compile(WHITESPACE_REGEX)


def normalize_unicode_nfc(text):
    if text is None:
        return None
    return unicodedata.normalize("NFC", text)


def clean_text(text):
    """Bản Python thuần (dùng cho test, streaming và làm thân UDF)."""
    if text is None:
        return None
    text = html.unescape(text)
    text = _HTML_RE.sub(" ", text)
    text = unicodedata.normalize("NFC", text)
    text = _WS_RE.sub(" ", text)
    return text.strip()


nfc_udf = F.udf(normalize_unicode_nfc, StringType())


def clean_text_spark(df, input_col, output_col, use_nfc=True):
    """
    Làm sạch cột văn bản trên Spark DataFrame.
    Loại HTML và khoảng trắng bằng hàm nội tại của Spark (chạy trong JVM, nhanh);
    chỉ bước chuẩn hóa NFC cần UDF Python vì Spark SQL không có hàm normalize.
    """
    col = F.regexp_replace(F.col(input_col), HTML_REGEX, " ")
    col = F.regexp_replace(col, "&nbsp;", " ")
    col = F.regexp_replace(col, "&amp;", "&")
    if use_nfc:
        col = nfc_udf(col)
    col = F.trim(F.regexp_replace(col, WHITESPACE_REGEX, " "))
    return df.withColumn(output_col, col)
