# ==============================================================================
# KHỞI CHẠY FASTAPI PHỤC VỤ DỮ LIỆU TẦNG GOLD
# ==============================================================================

$uvicornPath = ".venv\Scripts\uvicorn.exe"

if (-not (Test-Path $uvicornPath)) {
    Write-Host "Chưa tìm thấy uvicorn trong .venv. Đang cài đặt requirements.txt..." -ForegroundColor Yellow
    if (-not (Test-Path ".venv")) { python -m venv .venv }
    .venv\Scripts\pip install -r requirements.txt
}

Write-Host "API tại: http://localhost:8000  (tài liệu: http://localhost:8000/docs)" -ForegroundColor Green
& $uvicornPath src.api.main:app --host 0.0.0.0 --port 8000
