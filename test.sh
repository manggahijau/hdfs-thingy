#!/usr/bin/env bash
# Runs V1-V4, B1-B5 and C1/C2 on HDFS + Parquet (Spark SQL). Results: ../results/hdfs-<date-time>/
#
#   ./test.sh                 flat layout (the baseline)
#   LAYOUT=decade ./test.sh   decade-partitioned layout (V2/B5 get a partition predicate)
set -euo pipefail
cd "$(dirname "$0")"
source lib.sh

SPARK=uglc-lab-hdfs-spark
SPARK_MEM=${SPARK_MEM:-3g}
LAYOUT=${LAYOUT:-flat}
spark_submit() { docker exec -e LAYOUT="$LAYOUT" "$SPARK" /opt/spark/bin/spark-submit \
    --master 'local[*]' --driver-memory "$SPARK_MEM" "$@" </dev/null; }

require_docker
docker ps --format '{{.Names}}' | grep -x "$SPARK" >/dev/null || fail "The baseline isn't running. Run ./setup.sh first."

RUN=$(date +%Y%m%d-%H%M%S)
OUT=../results/hdfs-$RUN; mkdir -p "$OUT"
SUMMARY=$OUT/summary.md

fmt_ms() { awk -v v="$1" 'BEGIN { if (v == "") print "?"; else if (v < 1000) printf "%.0f ms", v; else printf "%.2f s", v / 1000 }'; }
verdict() { awk -v v="$1" -v lim="$2" 'BEGIN { print (v != "" && v + 0 < lim) ? "✅" : "❌" }'; }
yes_no() { [[ $1 == "$2" ]] && echo "✅" || echo "❌"; }

step "1/2 Read queries (layout: $LAYOUT), each twice in one Spark session"
spark_submit /opt/app/bench.py >"$OUT/1_queries.raw" 2>"$OUT/1_spark.log" || { tail -30 "$OUT/1_spark.log"; fail "bench.py failed"; }
startup=$(grep '^INFO|session_startup_s' "$OUT/1_queries.raw" | cut -d'|' -f3)

{
    echo "# UGLC HDFS + Parquet baseline run $RUN (layout: $LAYOUT)"
    echo
    echo "- Machine: $(uname -s) $(uname -m); Docker sees $(docker info --format '{{.NCPU}}') CPUs and $(docker info --format '{{.MemTotal}}' | awk '{ printf "%.1f GB", $1/1024/1024/1024 }')"
    echo "- Engine: Spark SQL 3.5 local mode, driver memory $SPARK_MEM, Parquet + snappy on HDFS (1 datanode, replication 1)"
    echo "- Every query runs twice in the same session; run 2 (warm) is reported, like the other lab."
    echo "- Spark session start-up (JVM + first job): **${startup} s**, not included in the times below."
    echo
    echo "## 1. Visitor queries V1–V4 (NFR-1: < 1 s) and dashboard queries B1–B5 (NFR-2: < 5 s)"
    echo
    echo "| Query | Run 1 | Run 2 (warm) | Rows | Limit |"
    echo "|---|---|---|---|---|"
} > "$SUMMARY"
while IFS='|' read -r _ name title r1 r2 rows; do
    case $name in V*) lim=1000;; *) lim=5000;; esac
    echo "| $name $title | $(fmt_ms "$r1") | $(fmt_ms "$r2") | $rows | $(verdict "$r2" $lim) |" >> "$SUMMARY"
    info "$name  $(fmt_ms "$r2")  rows=$rows"
done < <(grep '^RESULT|' "$OUT/1_queries.raw")
cat >> "$SUMMARY" <<'EOT'

V4 uses a spherical haversine; PostGIS `geography` measures on the WGS84 spheroid, so the row count can differ by a few from the SQL lab.
V4bbox adds a bounding box so Parquet min/max statistics can skip row groups; plain V4 has no spatial index to use.
Row counts can be compared with the SQL lab's results (same data, same cleaning rules).
EOT

step "2/2 Contributors: C1 (append) and C2 (rewrite), on a copy of the data"
spark_submit /opt/app/contrib.py >"$OUT/2_contrib.raw" 2>"$OUT/2_spark.log" || { tail -30 "$OUT/2_spark.log"; fail "contrib.py failed"; }
g() { grep "^INFO|$1|" "$OUT/2_contrib.raw" | cut -d'|' -f3; }
{
    echo
    echo "## 2. Contributors (C1, C2). Parquet is immutable, so there is no UPDATE"
    echo
    echo "| Check | Result | |"
    echo "|---|---|---|"
    echo "| C1: find next id (no sequence: scan for max(id)+1) | $(fmt_ms "$(g C1_next_id_lookup_ms)") | |"
    echo "| C1: append the new report as a new Parquet file | $(fmt_ms "$(g C1_append_ms)") | |"
    echo "| C1: visible to a fresh reader right away | $(g C1_visible_rows) row | $(yes_no "$(g C1_visible_rows)" 1) |"
    echo "| C2: owner edits own report (rewrites the whole dataset) | $(g C2_owner_rows) row, $(fmt_ms "$(g C2_owner_ms)") | $(yes_no "$(g C2_owner_rows)" 1) |"
    echo "| C2: other contributor edits it (matches nothing, still scans) | $(g C2_other_rows) row, $(fmt_ms "$(g C2_other_ms)") | $(yes_no "$(g C2_other_rows)" 0) |"
    echo "| C2: new value is stored | fatalities = $(g C2_fatalities_after) | $(yes_no "$(g C2_fatalities_after)" 4) |"
    echo
    echo "There is no sync and no lag (NFR-4 doesn't apply): a new Parquet file is visible to the next reader."
    echo "The price is C2: one edited row costs a rewrite of the whole dataset, and the swap (two renames) is not atomic,"
    echo "so a reader can hit the table while it is missing. Delta Lake or Iceberg would fix that; plain Parquet can't."
} >> "$SUMMARY"
echo; echo "Done. Results: $OUT/"; echo; cat "$SUMMARY"
