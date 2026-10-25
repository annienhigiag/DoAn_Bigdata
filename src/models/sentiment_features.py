"""
Chuẩn bị văn bản đầu vào cho mô hình cảm xúc, dùng chung cho huấn luyện, đánh giá và suy luận
để ba bước luôn biến đổi văn bản giống hệt nhau.
"""
import os

from pyspark.sql import functions as F

from src.nlp.tokenizer import add_tokenized_text

LABEL_NAMES = {0: "Tiêu cực", 1: "Trung tính", 2: "Tích cực"}
SAPO_CHARS = 250


def build_article_text(df, title_col="title", content_col="content", output_col="text"):
    """Ghép tiêu đề + 250 ký tự đầu (sapo) theo nguyên lý "kim tự tháp ngược" của báo chí."""
    return df.withColumn(output_col, F.concat_ws(" . ", F.col(title_col), F.substring(F.col(content_col), 1, SAPO_CHARS)))


def prepare_text(df, input_col="text", output_col="text_tok"):
    """
    Tách từ tiếng Việt bằng pyvi. Đặt SENTIMENT_USE_PYVI=0 để bỏ qua (tách theo âm tiết),
    ví dụ khi chạy thử trên máy không cài pyvi.
    """
    if os.environ.get("SENTIMENT_USE_PYVI", "1") == "1":
        return add_tokenized_text(df, input_col, output_col)
    return df.withColumn(output_col, F.col(input_col))
