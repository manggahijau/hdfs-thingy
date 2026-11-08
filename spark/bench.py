# V1-V4 and B1-B5 (+ B4 global/Indonesia) on Parquet, same questions as the PostgreSQL / ClickHouse tests.
# Each query runs twice in ONE Spark session; run 2 (warm) is the number to report.
import os, time
from common import BASE, session

LAYOUT = os.environ.get("LAYOUT", "flat")          # flat | decade
spark = session("uglc-bench")
path = f"{BASE}/landslides_{LAYOUT}"
spark.read.parquet(path).createOrReplaceTempView("landslides")

# (name, title, sql, sql_for_decade_layout or None)
HAV = ("2 * 6371000 * asin(sqrt(pow(sin(radians(lat - (-7.24917)) / 2), 2) + "
       "cos(radians(-7.24917)) * cos(radians(lat)) * pow(sin(radians(lon - 112.75083) / 2), 2)))")
Q = [
 ("V1", "Show one landslide event (by id)",
  "SELECT * FROM landslides WHERE id = 500000", None),
 ("V2", "Landslides between two dates (the year 2010)",
  "SELECT * FROM landslides WHERE start_date >= DATE '2010-01-01' AND start_date < DATE '2011-01-01'",
  "SELECT * FROM landslides WHERE start_decade = 2010 AND start_date >= DATE '2010-01-01' AND start_date < DATE '2011-01-01'"),
 ("V3", "Landslides in one country (Indonesia)",
  "SELECT * FROM landslides WHERE country = 'Indonesia'", None),
 ("V4", "Within 20 km of Surabaya (haversine over all rows)",
  f"SELECT * FROM landslides WHERE lat IS NOT NULL AND {HAV} <= 20000", None),
 ("V4bbox", "Within 20 km of Surabaya (bounding box first, then haversine)",
  "SELECT * FROM landslides WHERE lat BETWEEN -7.43 AND -7.07 AND lon BETWEEN 112.56 AND 112.94 "
  f"AND {HAV} <= 20000", None),
 ("B1", "Share of landslides per type",
  "SELECT type, count(*) AS cnt, round(100 * count(*) / sum(count(*)) OVER (), 2) AS pct "
  "FROM landslides GROUP BY type ORDER BY cnt DESC", None),
 ("B2", "Deadliest type in Indonesia",
  "SELECT type, count(*) AS type_count, count(fatalities) AS known_fatalities_count, "
  "round(avg(fatalities), 2) AS fatalities_per_landslide FROM landslides "
  "WHERE type IS NOT NULL AND country = 'Indonesia' GROUP BY type "
  "ORDER BY fatalities_per_landslide DESC LIMIT 20", None),
 ("B3.1", "Countries with the most landslides",
  "SELECT country, count(*) AS events FROM landslides WHERE country IS NOT NULL "
  "GROUP BY country ORDER BY events DESC LIMIT 20", None),
 ("B3.2", "Areas with the most landslides (1x1 degree cells)",
  "SELECT floor(lat) AS lat_cell, floor(lon) AS lon_cell, count(*) AS events, "
  "mode(country) AS main_country FROM landslides WHERE lat IS NOT NULL "
  "GROUP BY lat_cell, lon_cell ORDER BY events DESC LIMIT 20", None),
 ("B4", "Fatalities per decade by trigger (Indonesia)",
  "SELECT country, (start_year DIV 10) * 10 AS decade, physical_factors AS `trigger`, "
  "sum(fatalities) AS total_fatalities, count(*) AS events FROM landslides "
  "WHERE fatalities IS NOT NULL AND country = 'Indonesia' "
  "GROUP BY country, decade, `trigger` ORDER BY total_fatalities DESC LIMIT 20", None),
 ("B4_global", "B4 for all countries",
  "SELECT country, (start_year DIV 10) * 10 AS decade, physical_factors AS `trigger`, "
  "sum(fatalities) AS total_fatalities, count(*) AS events FROM landslides "
  "WHERE fatalities IS NOT NULL GROUP BY country, decade, `trigger` "
  "ORDER BY total_fatalities DESC LIMIT 20", None),
 ("B5", "Landslides and fatalities per year (Indonesia, after 2010)",
  "SELECT start_year, sum(fatalities) AS fatalities_total, count(*) AS event_count FROM landslides "
  "WHERE country = 'Indonesia' AND start_year > 2010 GROUP BY start_year "
  "ORDER BY start_year DESC LIMIT 20",
  "SELECT start_year, sum(fatalities) AS fatalities_total, count(*) AS event_count FROM landslides "
  "WHERE country = 'Indonesia' AND start_decade >= 2010 AND start_year > 2010 GROUP BY start_year "
  "ORDER BY start_year DESC LIMIT 20"),
]

def timed(sql):
    df = spark.sql(sql)
    t = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()   # runs the whole plan, no driver collect
    return (time.perf_counter() - t) * 1000

for name, title, sql, dsql in Q:
    q = dsql if (LAYOUT == "decade" and dsql) else sql
    r1, r2 = timed(q), timed(q)
    rows = spark.sql(q).count()
    print(f"RESULT|{name}|{title}|{r1:.0f}|{r2:.0f}|{rows}", flush=True)
