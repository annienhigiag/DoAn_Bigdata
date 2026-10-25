"""
Khử trùng lặp 2 tầng cho tầng Silver:

Tầng 1 - Exact Dedup:
  - Trùng URL.
  - Trùng nội dung: SHA-256 của (tiêu đề + nội dung) đã chuẩn hóa chữ thường.

Tầng 2 - Near-duplicate bằng SimHash 64-bit:
  - Mỗi bài được băm thành vân tay 64 bit từ các shingle 2 từ (xxhash64).
  - Hai bài là near-duplicate khi khoảng cách Hamming <= 3 bit.
  - Để không phải so sánh mọi cặp (O(n^2)), vân tay được chia thành (ngưỡng + 1) band
    (ngưỡng 3 -> 4 band x 16 bit). Theo nguyên lý Dirichlet, 2 vân tay lệch <= ngưỡng bit
    thì chắc chắn trùng ít nhất 1 band, nên chỉ cần so sánh các cặp cùng band (self-join theo band).
  - Trong mỗi cặp, giữ bài đăng sớm hơn (bài gốc) và loại bài sau ("tin xào").

Toàn bộ chạy bằng hàm nội tại của Spark (không UDF Python) nên chạy hoàn toàn trong JVM.
"""
from functools import reduce

from pyspark.sql import functions as F

SIMHASH_BITS = 64
DEFAULT_HAMMING_THRESHOLD = 3


# ---------------------------------------------------------------------------
# Tầng 1: Exact Dedup
# ---------------------------------------------------------------------------
def add_content_hash(df, title_col="title", content_col="content", output_col="content_hash"):
    normalized = F.lower(F.concat_ws(" ", F.col(title_col), F.col(content_col)))
    return df.withColumn(output_col, F.sha2(normalized, 256))


def drop_exact_duplicates(df, url_col="url", title_col="title", content_col="content"):
    """
    Trả về (df_đã_khử_trùng, thống_kê). Thống kê gồm số bài bị loại ở từng bước.
    """
    before = df.count()
    by_url = df.dropDuplicates([url_col])
    after_url = by_url.count()

    by_hash = add_content_hash(by_url, title_col, content_col).dropDuplicates(["content_hash"])
    after_hash = by_hash.count()

    stats = {
        "exact_url_removed": before - after_url,
        "exact_content_removed": after_url - after_hash,
    }
    return by_hash.drop("content_hash"), stats


# ---------------------------------------------------------------------------
# Tầng 2: SimHash 64-bit + Hamming
# ---------------------------------------------------------------------------
def add_simhash(df, id_col, text_col, output_col="simhash", max_chars=2000):
    """
    Thêm cột vân tay SimHash 64 bit (kiểu long) cho từng bài.
    Chỉ dùng max_chars ký tự đầu (tiêu đề + sapo + đoạn mở đầu) vì "tin xào"
    thường giữ nguyên phần này; giúp giảm số shingle cần băm.
    """
    words = F.split(F.trim(F.lower(F.substring(F.col(text_col), 1, max_chars))), r"\s+")
    tmp = df.select(F.col(id_col).alias("_sid"), words.alias("_w"))

    bigrams = F.expr(
        "zip_with(slice(_w, 1, size(_w) - 1), slice(_w, 2, size(_w) - 1), (a, b) -> concat(a, ' ', b))"
    )
    shingles = F.when(F.size("_w") >= 2, bigrams).otherwise(F.col("_w"))

    hashed = tmp.select("_sid", F.explode(shingles).alias("_sh")) \
        .filter(F.length("_sh") > 0) \
        .select("_sid", F.xxhash64("_sh").alias("_h"))

    # Với mỗi bit i: cộng +1 nếu bit i của hash shingle là 1, ngược lại -1
    bit_votes = [
        F.sum(F.when(F.shiftrightunsigned("_h", i).bitwiseAND(F.lit(1)) == 1, 1).otherwise(-1)).alias(f"_b{i}")
        for i in range(SIMHASH_BITS)
    ]
    votes = hashed.groupBy("_sid").agg(*bit_votes)

    # Bit i của vân tay = 1 nếu tổng phiếu > 0
    fingerprint = reduce(
        lambda acc, i: acc.bitwiseOR(
            F.when(F.col(f"_b{i}") > 0, F.shiftleft(F.lit(1).cast("long"), i)).otherwise(F.lit(0).cast("long"))
        ),
        range(SIMHASH_BITS),
        F.lit(0).cast("long"),
    )
    fp_df = votes.select(F.col("_sid"), fingerprint.alias(output_col))

    return df.join(fp_df, df[id_col] == fp_df["_sid"], "left").drop("_sid")


def find_near_duplicates(df, key_col, order_cols, simhash_col="simhash", threshold=DEFAULT_HAMMING_THRESHOLD):
    """
    Trả về DataFrame các cặp near-duplicate (keep_key, drop_key, hamming).
    order_cols: các cột xác định bài nào "sớm hơn" (bài sớm hơn được giữ lại).
    """
    base = df.filter(F.col(simhash_col).isNotNull()).select(
        F.col(key_col).alias("_k"), F.col(simhash_col).alias("_sh"),
        *[F.col(c).alias(f"_o{i}") for i, c in enumerate(order_cols)]
    )

    num_bands = threshold + 1
    band_bits = SIMHASH_BITS // num_bands
    band_specs = []
    for b in range(num_bands):
        start = b * band_bits
        width = SIMHASH_BITS - start if b == num_bands - 1 else band_bits
        band_specs.append((b, start, width))

    bands = base.select(
        "*",
        F.explode(F.array(*[
            F.struct(
                F.lit(b).alias("idx"),
                F.shiftrightunsigned("_sh", start).bitwiseAND(F.lit((1 << width) - 1).cast("long")).alias("val"),
            )
            for b, start, width in band_specs
        ])).alias("_band")
    ).select("*", F.col("_band.idx").alias("_bidx"), F.col("_band.val").alias("_bval")).drop("_band")

    a = bands.alias("a")
    b = bands.alias("b")

    # a "sớm hơn" b theo thứ tự (order_cols..., key)
    a_cols = [F.col(f"a._o{i}") for i in range(len(order_cols))] + [F.col("a._k")]
    b_cols = [F.col(f"b._o{i}") for i in range(len(order_cols))] + [F.col("b._k")]
    a_before_b = F.struct(*a_cols) < F.struct(*b_cols)

    pairs = a.join(
        b,
        (F.col("a._bidx") == F.col("b._bidx")) & (F.col("a._bval") == F.col("b._bval")) & a_before_b,
    ).select(
        F.col("a._k").alias("keep_key"),
        F.col("b._k").alias("drop_key"),
        F.expr("bit_count(a._sh ^ b._sh)").alias("hamming"),
    ).filter(F.col("hamming") <= threshold).dropDuplicates(["keep_key", "drop_key"])

    return pairs


def drop_near_duplicates(df, key_col="url", text_col="content", order_cols=("crawled_at",),
                         threshold=DEFAULT_HAMMING_THRESHOLD):
    """
    Trả về (df_đã_khử_trùng, thống_kê). df phải có key_col duy nhất (ví dụ url sau Exact Dedup).
    """
    with_fp = add_simhash(df, key_col, text_col).cache()
    pairs = find_near_duplicates(with_fp, key_col, list(order_cols), threshold=threshold).cache()

    to_drop = pairs.select(F.col("drop_key").alias(key_col)).distinct()
    result = with_fp.join(to_drop, key_col, "left_anti")

    stats = {
        "near_dup_pairs": pairs.count(),
        "near_dup_removed": to_drop.count(),
        "hamming_threshold": threshold,
    }
    return result, stats, pairs
