# C1 (submit) and C2 (edit own / someone else's report) on plain Parquet.
# Parquet files are immutable: C1 = append a new file, C2 = rewrite the whole dataset (copy-on-write).
import datetime as dt, time
from pyspark.sql import functions as F, types as T
from common import BASE, SCHEMA, session

spark = session("uglc-contrib")
rw = f"{BASE}/landslides_rw"
tmp, bak = f"{BASE}/landslides_rw_tmp", f"{BASE}/landslides_rw_bak"
A, B = 1, 2    # submitted_by of the two test contributors (ids they get in PostgreSQL)

def ms(t): return (time.perf_counter() - t) * 1000
def info(k, v): print(f"INFO|{k}|{v}", flush=True)

t = time.perf_counter()
new_id = spark.read.parquet(rw).agg(F.max("id")).first()[0] + 1
info("C1_next_id_lookup_ms", f"{ms(t):.0f}")          # no sequence in Parquet: scan for max(id)+1

now = dt.datetime.utcnow()
vals = dict(id=new_id, lon=106.816, lat=-6.595, source_dataset="Contributor portal", source_id=None,
            source_version=None, country="Indonesia", accuracy_m=None, start_date=now.date(),
            end_date=now.date(), type="debris flow", physical_factors="rainfall (T)", reliability=None,
            record_type="event", fatalities=3, injuries=5, notes=f"TEST {now:%Y%m%d-%H%M%S}: report submitted by test.sh",
            link=None, submitted_by=A, created_at=now, updated_at=now, start_year=now.year)
row = spark.createDataFrame([tuple(vals[f.name] for f in SCHEMA.fields) + (now.year,)],
                            T.StructType(SCHEMA.fields + [T.StructField("start_year", T.IntegerType())]))
t = time.perf_counter()
row.write.mode("append").parquet(rw)
info("C1_append_ms", f"{ms(t):.0f}")
info("C1_visible_rows", spark.read.parquet(rw).filter(F.col("id") == new_id).count())
info("C1_id", new_id)

jvm, conf = spark._jvm, spark._jsc.hadoopConfiguration()
P = jvm.org.apache.hadoop.fs.Path
fs = P(rw).getFileSystem(conf)

def update(owner):
    cond = (F.col("id") == new_id) & (F.col("submitted_by") == owner)
    t = time.perf_counter()
    df = spark.read.parquet(rw)
    n = df.filter(cond).count()
    if n:
        (df.withColumn("fatalities", F.when(cond, F.lit(4)).otherwise(F.col("fatalities")))
           .withColumn("updated_at", F.when(cond, F.current_timestamp()).otherwise(F.col("updated_at")))
           .write.mode("overwrite").parquet(tmp))
        fs.delete(P(bak), True)
        fs.rename(P(rw), P(bak))      # NOT atomic as a pair: between these two renames the table is missing
        fs.rename(P(tmp), P(rw))
        fs.delete(P(bak), True)
    return n, ms(t)

n, t_owner = update(A)
info("C2_owner_rows", n); info("C2_owner_ms", f"{t_owner:.0f}")
n, t_other = update(B)
info("C2_other_rows", n); info("C2_other_ms", f"{t_other:.0f}")
info("C2_fatalities_after", spark.read.parquet(rw).filter(F.col("id") == new_id).first()["fatalities"])
