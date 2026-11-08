# Raw UGLC_point.csv -> cleaned -> Parquet on HDFS. The cleaning rules are the same as the SQL lab's
# (setup/02_pg_clean_load.sql), but done here in Spark: the baseline does not use PostgreSQL.
from pyspark.sql import functions as F, types as T
from common import BASE, session

spark = session("uglc-load")

# Column ORDER of the CSV (separator "|"): the file's own header has spaces in the names
RAW_COLS = ["wkt_geom", "new_dataset", "id", "old_dataset", "old_id", "version", "country", "accuracy",
            "start_date", "end_date", "type", "physical_factors", "reliability", "record_type",
            "fatality", "injuries", "notes", "link"]
raw = (spark.read.schema(T.StructType([T.StructField(c, T.StringType()) for c in RAW_COLS]))
       .option("header", True).option("sep", "|")
       .option("multiLine", True).option("quote", '"').option("escape", '"')
       .csv("file:///data/UGLC_point.csv"))

def nullif(c, *bad):                     # 'ND', -99999, ... mean "unknown" -> real NULL
    return F.when(F.col(c).isin(*bad), F.lit(None)).otherwise(F.col(c))
def num(c, t):                           # text -> decimal -> rounded integer type, like ::numeric::integer
    return F.round(F.col(c).cast("decimal(38,10)") if isinstance(c, str) else c.cast("decimal(38,10)")).cast(t)

NUM = r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"
pt = rf"POINT\s*\(\s*({NUM})\s+({NUM})\s*\)"
inf = F.col("wkt_geom").contains("Infinity")           # 9 rows are POINT (-Infinity -Infinity)
d1 = F.to_date("start_date", "yyyy/MM/dd")
d2 = F.to_date("end_date", "yyyy/MM/dd")

# Data-quality facts the SQL lab documents; the setup script checks them
s = raw.agg(F.count("*").alias("n"),
            F.sum(F.when(d1 > d2, 1).otherwise(0)).alias("swapped"),
            F.sum(F.when(inf, 1).otherwise(0)).alias("inf"),
            F.sum(F.when(d1.isNull() | d2.isNull(), 1).otherwise(0)).alias("null_dates")).first()
for k, v in [("raw_rows", s["n"]), ("swapped_dates", s["swapped"]), ("infinity_points", s["inf"]),
             ("null_dates", s["null_dates"])]:
    print(f"INFO|{k}|{v}", flush=True)

clean = raw.select(
    num("id", "long").alias("id"),
    F.when(inf, F.lit(None)).otherwise(F.regexp_extract("wkt_geom", pt, 1).cast("double")).alias("lon"),
    F.when(inf, F.lit(None)).otherwise(F.regexp_extract("wkt_geom", pt, 2).cast("double")).alias("lat"),
    F.col("old_dataset").alias("source_dataset"),
    F.col("old_id").alias("source_id"),
    F.col("version").alias("source_version"),
    "country",
    num(nullif("accuracy", "-99999"), "int").alias("accuracy_m"),
    F.least(d1, d2).alias("start_date"),               # 109 rows have end < start: swap, keep
    F.greatest(d1, d2).alias("end_date"),
    nullif("type", "ND").alias("type"),
    nullif("physical_factors", "ND").alias("physical_factors"),
    num("reliability", "short").alias("reliability"),
    "record_type",
    F.when(F.col("fatality").rlike(r"^[0-9]+(\.[0-9]+)?$"), num("fatality", "int")).alias("fatalities"),
    num(nullif("injuries", "-99999"), "int").alias("injuries"),
    nullif("notes", "ND").alias("notes"),
    F.when(F.col("link").isin("Source: ND", "Source: 'ND'", "Source: Unknown"), F.lit(None))
     .otherwise(F.col("link")).alias("link"),
    F.lit(None).cast("int").alias("submitted_by"),     # NULL for every original UGLC row
    F.current_timestamp().alias("created_at"),
    F.current_timestamp().alias("updated_at"),
).withColumn("start_year", F.year("start_date"))

# 1. flat: no partitions, sorted by id, 32 MB row groups (so min/max stats can skip groups)
flat = f"{BASE}/landslides_flat"
(clean.orderBy("id").write.mode("overwrite").option("compression", "snappy")
      .option("parquet.block.size", 32 * 1024 * 1024).parquet(flat))

# 2. decade: partitioned by decade of start_date (only helps queries that filter on it)
dec = f"{BASE}/landslides_decade"
(spark.read.parquet(flat).withColumn("start_decade", (F.floor(F.col("start_year") / 10) * 10).cast("int"))
      .repartition("start_decade").sortWithinPartitions("id")
      .write.mode("overwrite").option("compression", "snappy")
      .partitionBy("start_decade").parquet(dec))

# 3. rw: a copy of flat for the contributor tests (C1/C2), so reads stay on untouched data
(spark.read.parquet(flat).write.mode("overwrite").option("compression", "snappy")
      .parquet(f"{BASE}/landslides_rw"))

r = spark.read.parquet(flat).agg(F.count("*"), F.sum("fatalities")).first()
print(f"INFO|parquet_rows|{r[0]}", flush=True)
print(f"INFO|parquet_fatalities|{r[1]}", flush=True)
