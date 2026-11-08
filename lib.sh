# Small helpers for this folder (the baseline does not depend on the PostgreSQL / ClickHouse lab).
export MSYS_NO_PATHCONV=1
step() { printf '\n==> %s\n' "$*"; }
info() { printf '    %s\n' "$*"; }
fail() { printf '\nERROR: %s\n' "$*" >&2; exit 1; }
require_docker() {
    command -v docker >/dev/null 2>&1 || fail "Docker is not installed (https://docs.docker.com/get-docker/)."
    docker info >/dev/null 2>&1 || fail "Docker is installed but not running."
    docker compose version >/dev/null 2>&1 || fail "'docker compose' (Compose v2) is missing."
}
