import os, time
from pyspark.sql import SparkSession, types as T

BASE = os.environ.get("HDFS_BASE", "hdfs://namenode:8020/uglc")

# Same columns as table landslides in PostgreSQL (geom split into lon/lat) + start_year.
SCHEMA = T.StructType([
    T.StructField("id", T.LongType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("source_dataset", T.StringType()),
    T.StructField("source_id", T.StringType()),
    T.StructField("source_version", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("accuracy_m", T.IntegerType()),
    T.StructField("start_date", T.DateType()),
    T.StructField("end_date", T.DateType()),
    T.StructField("type", T.StringType()),
    T.StructField("physical_factors", T.StringType()),
    T.StructField("reliability", T.ShortType()),
    T.StructField("record_type", T.StringType()),
    T.StructField("fatalities", T.IntegerType()),
    T.StructField("injuries", T.IntegerType()),
    T.StructField("notes", T.StringType()),
    T.StructField("link", T.StringType()),
    T.StructField("submitted_by", T.IntegerType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("updated_at", T.TimestampType()),
])

def session(name):
    t0 = time.time()
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.session.timeZone", "UTC")
             # 1M rows: the default of 200 shuffle partitions would only add overhead
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark.range(1).count()   # first job: JVM + scheduler warm-up
    print(f"INFO|session_startup_s|{time.time() - t0:.1f}", flush=True)
    return spark
