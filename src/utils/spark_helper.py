from pyspark.sql import SparkSession


def get_spark_session(app_name="Lakehouse_App", master="spark://spark-master:7077"):
    """
    Tạo hoặc lấy SparkSession chuẩn hóa cho toàn bộ hệ thống Lakehouse HDFS.
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

    if master:
        builder = builder.master(master)

    return builder.getOrCreate()


def path_exists(spark, path):
    """
    Kiểm tra một đường dẫn (HDFS hoặc local) có tồn tại không, thông qua Hadoop FileSystem API.
    """
    jvm = spark.sparkContext._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    return fs.exists(hadoop_path)
