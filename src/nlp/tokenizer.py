from pyspark.sql import functions as F
from pyspark.sql.types import StringType

try:
    from pyvi import ViTokenizer
except ImportError:
    ViTokenizer = None


def tokenize_vietnamese(text: str) -> str:
    """
    Tách từ tiếng Việt bằng thư viện pyvi (ghép từ ghép bằng dấu '_').
    Ví dụ: 'Dữ liệu lớn' -> 'Dữ_liệu lớn'
    Nếu worker chưa cài pyvi thì trả nguyên văn (tách theo âm tiết).
    """
    if not text:
        return ""
    if ViTokenizer is None:
        return text
    try:
        return ViTokenizer.tokenize(text)
    except Exception:
        return text


tokenize_udf = F.udf(tokenize_vietnamese, StringType())


def add_tokenized_text(df, input_col="text", output_col="text_tok"):
    """
    Thêm cột văn bản đã tách từ, dùng chung cho cả huấn luyện và suy luận
    để hai bước có cùng cách tách từ.
    """
    return df.withColumn(output_col, tokenize_udf(F.col(input_col)))
