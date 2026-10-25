"""
Đo độ trễ các endpoint FastAPI (yêu cầu < 200 ms).
Chạy API trước (scripts/run_api.ps1), rồi: python scripts/benchmark_api.py [--n 50] [--base http://localhost:8000]
"""
import argparse
import statistics
import time
import urllib.request

ENDPOINTS = [
    "/api/overview",
    "/api/trends?group_by=day",
    "/api/trends?group_by=day_topic",
    "/api/sentiment",
    "/api/alerts?min_z=1.5",
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--n", type=int, default=50)
    args = ap.parse_args()

    urllib.request.urlopen(args.base + "/api/overview").read()  # làm nóng cache
    print(f"{'Endpoint':38} {'p50 ms':>8} {'p95 ms':>8} {'max ms':>8}  < 200ms?")
    for ep in ENDPOINTS:
        times = []
        for _ in range(args.n):
            t0 = time.perf_counter()
            urllib.request.urlopen(args.base + ep).read()
            times.append((time.perf_counter() - t0) * 1000)
        times.sort()
        p95 = times[int(len(times) * 0.95) - 1]
        print(f"{ep:38} {statistics.median(times):8.1f} {p95:8.1f} {max(times):8.1f}  {'ĐẠT' if p95 < 200 else 'CHƯA'}")


if __name__ == "__main__":
    main()
