#!/usr/bin/env bash
# One-time setup of the HDFS + Parquet baseline. Standalone: it downloads the raw UGLC CSV, starts
# HDFS + Spark, cleans the data in Spark and writes Parquet to HDFS. No PostgreSQL / ClickHouse needed.
#
#   ./setup.sh           everything (safe to rerun)
#   ./setup.sh --reset   delete the HDFS containers and data, then build again (the CSV is kept)
set -euo pipefail
cd "$(dirname "$0")"
source lib.sh

CSV=../data/UGLC_point.csv        # same file the SQL lab uses; downloaded only if missing
CSV_URL="https://zenodo.org/records/18643456/files/UGLC_point.csv?download=1"
CSV_MD5=bb7cbd816944b6398a5e5bcd3a6b4bb5
NN=uglc-lab-hdfs-nn
SPARK=uglc-lab-hdfs-spark
SPARK_MEM=${SPARK_MEM:-3g}

require_docker
[[ ${1:-} == --reset ]] && docker compose down -v

step "1/4 Data: UGLC_point.csv (Zenodo record 18643456, licence CC BY-NC 4.0)"
mkdir -p ../data
if [[ ! -f $CSV ]]; then
    command -v curl >/dev/null 2>&1 || fail "curl is needed to download the data."
    info "Downloading 337 MB from Zenodo..."
    curl -fL --progress-bar -o "$CSV.part" "$CSV_URL" || fail "Download failed. Rerun ./setup.sh."
    mv "$CSV.part" "$CSV"
fi
if command -v md5sum >/dev/null 2>&1; then
    [[ $(md5sum "$CSV" | cut -d' ' -f1) == "$CSV_MD5" ]] || fail "Checksum mismatch. Delete $CSV and rerun."
    info "OK, checksum verified"
fi

step "2/4 Start HDFS (1 namenode + 1 datanode) and Spark"
docker compose up -d >/dev/null || fail "Containers didn't start. See: docker compose logs"
for i in $(seq 1 60); do
    # not "cmd | grep -q": with pipefail, grep exiting early can make the pipeline report failure
    report=$(docker exec "$NN" hdfs dfsadmin -report 2>/dev/null || true)
    grep -q 'Live datanodes (1)' <<<"$report" && break
    sleep 3
    [[ $i == 60 ]] && fail "The datanode didn't register. See: docker compose logs namenode datanode"
done
docker exec "$NN" hdfs dfsadmin -safemode wait >/dev/null
info "HDFS is up (web UI: http://localhost:9871)"

step "3/4 Clean in Spark and write Parquet to HDFS (flat, decade-partitioned, and a copy for C1/C2)"
SECONDS=0
out=$(docker exec "$SPARK" /opt/spark/bin/spark-submit --master 'local[*]' --driver-memory "$SPARK_MEM" \
      /opt/app/load.py </dev/null 2>/tmp/uglc-hdfs-load.log) || { tail -30 /tmp/uglc-hdfs-load.log; fail "load.py failed"; }
info "done in ${SECONDS}s"
docker exec "$NN" hdfs dfs -du -s -h /uglc/landslides_flat /uglc/landslides_decade | sed 's/^/    /'

step "4/4 Checking the cleaned data against what the dataset is known to contain"
g() { grep "^INFO|$1|" <<<"$out" | cut -d'|' -f3; }
ok=1
chk() { # name expected
    local got; got=$(g "$1")
    if [[ $got == "$2" ]]; then info "OK   $1 = $got"; else info "FAIL $1 = $got (expected $2)"; ok=0; fi
}
chk raw_rows 1060503
chk swapped_dates 109
chk infinity_points 9
chk null_dates 0
chk parquet_rows 1060503
info "total fatalities in Parquet: $(g parquet_fatalities)"
[[ $ok == 1 ]] || fail "The cleaned data doesn't match the dataset facts above."
echo; echo "Setup complete. Next: ./test.sh"
