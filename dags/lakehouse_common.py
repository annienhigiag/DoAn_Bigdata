"""
Hàm dùng chung cho các DAG của Lakehouse.

Mỗi task chạy job thật bằng `spark-submit` bên trong container spark-master (hoặc lệnh `hdfs`
trong container namenode) thông qua Docker Engine API. Container airflow mount
/var/run/docker.sock và thư mục dự án tại /opt/spark-apps (xem docker-compose.yml).
"""
import io
import os
import shutil
import tarfile
from datetime import datetime, timedelta

HDFS_ROOT = "/lakehouse/bd05_tin_tuc"
BRONZE_FILE = f"{HDFS_ROOT}/bronze/news_2021_2022.parquet"
APPS_DIR = "/opt/spark-apps"
LOCAL_GOLD_DIR = f"{APPS_DIR}/data/lakehouse/gold"
LAKEHOUSE_DIRS = ["bronze", "silver", "gold", "models", "labels", "checkpoints", "_quarantine"]

SPARK_SUBMIT = [
    "spark-submit",
    "--master", "spark://spark-master:7077",
    "--conf", "spark.driver.extraJavaOptions=-Dfile.encoding=UTF-8",
    "--conf", f"spark.executorEnv.PYTHONPATH={APPS_DIR}",
    "--conf", "spark.executorEnv.PYTHONIOENCODING=utf-8",
    # Mỗi job tối đa 2 core để job streaming và job batch chạy song song được trên worker 4 core
    "--conf", "spark.cores.max=2",
]

DEFAULT_ARGS = {
    "owner": "bd05_lakehouse_team",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


def _docker():
    import docker  # có sẵn trong image apache/airflow (provider docker)
    return docker.from_env()


def docker_exec(container, cmd, check=True, environment=None):
    """Chạy lệnh trong container, in log trực tiếp vào log Airflow, trả về exit code."""
    client = _docker()
    api = client.api
    env = {"PYTHONIOENCODING": "utf-8", **(environment or {})}
    exec_id = api.exec_create(client.containers.get(container).id, cmd, environment=env)
    print(f"$ docker exec {container} {' '.join(cmd)}")
    for chunk in api.exec_start(exec_id, stream=True):
        print(chunk.decode("utf-8", errors="replace"), end="")
    code = api.exec_inspect(exec_id)["ExitCode"]
    if check and code != 0:
        raise RuntimeError(f"Lệnh thất bại (exit {code}): {' '.join(cmd)}")
    return code


def spark_job(script, environment=None):
    """Tạo callable chạy một Spark job của dự án."""
    def _run():
        docker_exec("spark-master", SPARK_SUBMIT + [f"{APPS_DIR}/{script}"], environment=environment)
    return _run


def hdfs_put(local_file, hdfs_path, replication=3):
    """Copy file từ thư mục dự án vào namenode qua Docker API rồi đẩy lên HDFS."""
    name = os.path.basename(local_file)
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tar:
        tar.add(local_file, arcname=name)
    buf.seek(0)
    _docker().containers.get("namenode").put_archive("/tmp", buf.getvalue())
    docker_exec("namenode", ["hdfs", "dfs", "-D", f"dfs.replication={replication}", "-put", "-f",
                             f"/tmp/{name}", hdfs_path])
    docker_exec("namenode", ["rm", "-f", f"/tmp/{name}"])


def task_ingest_bronze():
    for d in LAKEHOUSE_DIRS:
        docker_exec("namenode", ["hdfs", "dfs", "-mkdir", "-p", f"{HDFS_ROOT}/{d}"])
    if docker_exec("namenode", ["hdfs", "dfs", "-test", "-e", BRONZE_FILE], check=False) == 0:
        print(f"-> Bronze đã có trên HDFS: {BRONZE_FILE}")
    else:
        local_file = f"{APPS_DIR}/data/filtered_2021_2022/news_2021_2022.parquet"
        if not os.path.exists(local_file):
            raise FileNotFoundError(f"Chưa có Bronze trên HDFS và không tìm thấy {local_file}")
        hdfs_put(local_file, BRONZE_FILE)
    docker_exec("namenode", ["hdfs", "dfs", "-setrep", "3", BRONZE_FILE])


def task_upload_labels():
    """Đưa các tập nhãn có sẵn trong data/labels lên HDFS labels/."""
    for name in ["sentiment_train.csv", "manual_200.csv"]:
        local_file = f"{APPS_DIR}/data/labels/{name}"
        if os.path.exists(local_file):
            hdfs_put(local_file, f"{HDFS_ROOT}/labels/{name}")
            print(f"-> Đã đưa {name} lên HDFS")
        else:
            print(f"-> Bỏ qua {name} (chưa có trong data/labels)")


def task_upload_feedback():
    """Đưa nhãn sửa tay từ Dashboard (write-back) lên HDFS để MERGE vào Delta và huấn luyện lại."""
    local_file = f"{APPS_DIR}/data/writeback/feedback.csv"
    if os.path.exists(local_file):
        hdfs_put(local_file, f"{HDFS_ROOT}/labels/feedback.csv")
        print("-> Đã đưa feedback.csv lên HDFS")
    else:
        print("-> Chưa có nhãn sửa tay từ Dashboard")


def task_sync_gold():
    """Đồng bộ toàn bộ thư mục Gold trên HDFS về data/lakehouse/gold cho API & Dashboard."""
    docker_exec("namenode", ["rm", "-rf", "/tmp/gold"])
    docker_exec("namenode", ["hdfs", "dfs", "-get", f"{HDFS_ROOT}/gold", "/tmp/gold"])
    stream, _ = _docker().containers.get("namenode").get_archive("/tmp/gold")
    data = io.BytesIO(b"".join(stream))

    tmp_dir = f"{LOCAL_GOLD_DIR}.tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)
    with tarfile.open(fileobj=data) as tar:
        tar.extractall(tmp_dir)
    shutil.rmtree(LOCAL_GOLD_DIR, ignore_errors=True)
    shutil.move(os.path.join(tmp_dir, "gold"), LOCAL_GOLD_DIR)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    print(f"-> Đã đồng bộ Gold về {LOCAL_GOLD_DIR}")
