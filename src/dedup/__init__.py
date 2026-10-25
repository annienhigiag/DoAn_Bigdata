"""
Gói thuật toán khử trùng lặp dữ liệu: Exact Dedup + SimHash 64-bit/Hamming.
"""
from src.dedup.dedup import drop_exact_duplicates, add_simhash, find_near_duplicates, drop_near_duplicates

__all__ = ["drop_exact_duplicates", "add_simhash", "find_near_duplicates", "drop_near_duplicates"]
