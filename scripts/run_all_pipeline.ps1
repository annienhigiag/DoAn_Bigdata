# ==============================================================================
# PIPELINE ĐIỀU PHỐI TOÀN BỘ HỆ THỐNG LAKEHOUSE (END-TO-END MASTER ORCHESTRATOR)
# Đồ án: Theo dõi Xu hướng và Cảm xúc Tin tức Tiếng Việt
#
#   powershell -ExecutionPolicy Bypass -File scripts/run_all_pipeline.ps1
#   powershell -ExecutionPolicy Bypass -File scripts/run_all_pipeline.ps1 -SkipTrain
# ==============================================================================
param(
    [switch]$SkipTrain,
    [switch]$SkipAdvanced,
    [string]$LocalBronze = "data/filtered_2021_2022/news_2021_2022.parquet"
)

$HdfsRoot = "/lakehouse/bd05_tin_tuc"
$BronzeFile = "$HdfsRoot/bronze/news_2021_2022.parquet"

function Invoke-SparkJob([string]$Title, [string]$Script) {
    Write-Host "`n$Title" -ForegroundColor Green
    docker exec spark-master spark-submit `
        --master spark://spark-master:7077 `
        --conf "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8" `
        --conf "spark.executorEnv.PYTHONPATH=/opt/spark-apps" `
        --conf "spark.executorEnv.PYTHONIOENCODING=utf-8" `
        --conf "spark.cores.max=2" `
        $Script
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Lỗi tại bước: $Title. Dừng pipeline!" -ForegroundColor Red
        exit 1
    }
}

Write-Host "==========================================================================" -ForegroundColor Cyan
Write-Host "   BẮT ĐẦU CHẠY TOÀN BỘ PIPELINE BIG DATA LAKEHOUSE (1-CLICK RUNNER)      " -ForegroundColor Yellow
Write-Host "==========================================================================" -ForegroundColor Cyan

# 1. KIỂM TRA TRẠNG THÁI CONTAINER
Write-Host "`n[Bước 1/9] Kiểm tra hạ tầng Docker HDFS & Spark..." -ForegroundColor Green
$runningContainers = docker ps --format "{{.Names}}"
if ($runningContainers -notmatch "namenode" -or $runningContainers -notmatch "spark-master") {
    Write-Host "Hạ tầng chưa chạy. Đang kích hoạt docker compose up -d..." -ForegroundColor Yellow
    docker compose up -d
    Start-Sleep -Seconds 20
}
docker exec namenode hdfs dfsadmin -safemode wait | Out-Null
Write-Host "✓ Cụm HDFS (3 DataNodes) và Apache Spark đã sẵn sàng!" -ForegroundColor Green

# 2. BATCH INGESTION -> BRONZE
Write-Host "`n[Bước 2/9] Batch ingestion: nạp dữ liệu thô vào HDFS Bronze..." -ForegroundColor Green
foreach ($dir in @("bronze", "silver", "gold", "models", "labels", "checkpoints", "_quarantine")) {
    docker exec namenode hdfs dfs -mkdir -p "$HdfsRoot/$dir"
}
docker exec namenode hdfs dfs -test -e $BronzeFile
if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Bronze đã có trên HDFS: $BronzeFile (bỏ qua nạp lại)" -ForegroundColor Green
} elseif (Test-Path $LocalBronze) {
    docker cp $LocalBronze namenode:/tmp/news_2021_2022.parquet
    docker exec namenode hdfs dfs -D dfs.replication=3 -put -f /tmp/news_2021_2022.parquet $BronzeFile
    docker exec namenode rm -f /tmp/news_2021_2022.parquet
    Write-Host "✓ Đã nạp $LocalBronze -> $BronzeFile (replication = 3)" -ForegroundColor Green
} else {
    Write-Host "Không tìm thấy $LocalBronze. Chạy scripts/filter_kaggle_data.py trước." -ForegroundColor Red
    exit 1
}
docker exec namenode hdfs dfs -setrep 3 $BronzeFile | Out-Null

# Nhãn cảm xúc thật (UIT-VSFC), nhãn tay 200 bài và nhãn sửa từ Dashboard (write-back)
if (-not (Test-Path "data/labels/sentiment_train.csv")) { python scripts/prepare_sentiment_labels.py }
foreach ($f in @("data/labels/sentiment_train.csv", "data/labels/manual_200.csv", "data/writeback/feedback.csv")) {
    if (Test-Path $f) {
        $name = Split-Path $f -Leaf
        docker cp $f "namenode:/tmp/$name"
        docker exec namenode hdfs dfs -put -f "/tmp/$name" "$HdfsRoot/labels/$name"
        Write-Host "✓ Đã đưa $f lên $HdfsRoot/labels/" -ForegroundColor Green
    }
}

# 3-7. CÁC SPARK JOB
Invoke-SparkJob "[Bước 3/9] Job ETL: Làm sạch, Unicode NFC, Exact + SimHash Dedup (Bronze -> Silver)..." `
    "/opt/spark-apps/src/jobs/bronze_to_silver_job.py"

if ($SkipTrain) {
    Write-Host "`n[Bước 4/9] Bỏ qua huấn luyện (-SkipTrain), dùng model sẵn có trên HDFS." -ForegroundColor Yellow
} else {
    Invoke-SparkJob "[Bước 4/9] Job ML: Huấn luyện & đánh giá mô hình cảm xúc Spark MLlib..." `
        "/opt/spark-apps/src/models/train_sentiment_mllib.py"
}

Invoke-SparkJob "[Bước 5/9] Job Scoring: Gán nhãn cảm xúc toàn bộ tầng Silver..." `
    "/opt/spark-apps/src/jobs/silver_sentiment_inference.py"

Invoke-SparkJob "[Bước 6/9] Job SQL: Phân tích Spark SQL..." `
    "/opt/spark-apps/src/jobs/spark_sql_analytics.py"

Invoke-SparkJob "[Bước 7/9] Job Gold: Tổng hợp xu hướng & Z-score đột biến..." `
    "/opt/spark-apps/src/jobs/silver_to_gold_job.py"

# 8. PHÂN TÍCH NÂNG CAO: DELTA LAKE, THỰC THỂ, LDA
if ($SkipAdvanced) {
    Write-Host "`n[Bước 8/9] Bỏ qua phân tích nâng cao (-SkipAdvanced)." -ForegroundColor Yellow
} else {
    Invoke-SparkJob "[Bước 8/9] Delta Lake (MERGE write-back, time-travel, OPTIMIZE, VACUUM)..." `
        "/opt/spark-apps/src/jobs/delta_lake_job.py"
    Invoke-SparkJob "[Bước 8/9] Đột biến theo thực thể / thương hiệu..." `
        "/opt/spark-apps/src/jobs/entity_trend_job.py"
    Invoke-SparkJob "[Bước 8/9] Mô hình chủ đề LDA..." `
        "/opt/spark-apps/src/jobs/lda_topic_job.py"
}

# 9. ĐỒNG BỘ DỮ LIỆU GOLD PHỤC VỤ API & DASHBOARD
Write-Host "`n[Bước 9/9] Đang đồng bộ bảng Gold từ HDFS sang tầng phục vụ API/Dashboard..." -ForegroundColor Green
docker exec namenode rm -rf /tmp/gold
docker exec namenode hdfs dfs -get "$HdfsRoot/gold" /tmp/gold
if (Test-Path "data/lakehouse/gold") { Remove-Item -Recurse -Force "data/lakehouse/gold" }
New-Item -ItemType Directory -Path "data/lakehouse/gold" -Force | Out-Null
docker cp namenode:/tmp/gold/. data/lakehouse/gold/

Write-Host "`n==========================================================================" -ForegroundColor Cyan
Write-Host "✓ TOÀN BỘ PIPELINE BIG DATA ĐÃ CHẠY HOÀN TẤT!" -ForegroundColor Green
Write-Host "✓ API     : powershell -ExecutionPolicy Bypass -File scripts/run_api.ps1" -ForegroundColor White
Write-Host "✓ Dashboard: powershell -ExecutionPolicy Bypass -File scripts/run_dashboard.ps1" -ForegroundColor White
Write-Host "==========================================================================" -ForegroundColor Cyan
