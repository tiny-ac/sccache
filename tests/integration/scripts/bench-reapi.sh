#!/bin/bash
# Times builds of ripgrep and rust-analyzer, at pinned commits, BENCH_RUNS times (3 by default)
# in each configuration:
# - plain cargo;
# - local sccache, cold (empty cache) and warm (the same checkout again, with the cache it wrote);
# - REAPI cold, against a server with an empty action cache, and REAPI warm, from a second
#   checkout at another path with empty local caches, so that every hit comes from the server.
# Writes the wall times, sccache's stats and the cache sizes to results.csv and results.md in
# BENCH_RESULTS.
#
# Without REAPI_ENDPOINT, each run starts a NativeLink with empty stores, from
# tests/integration/reapi/nativelink.json5, so the image needs `nativelink` on its PATH. With
# REAPI_ENDPOINT, the runs use that server and a new salt each, so the action cache starts empty
# but the CAS keeps what it holds. REAPI_CONFIG names a file whose lines go after the
# `[dist.reapi]` keys the script writes, such as `auth_token_file` or a `[dist.reapi.platform]`
# table.
#
# Usage, from tests/integration: make bench-reapi
# Or on a Linux machine: SCCACHE_PATH=<sccache built with reapi> bash bench-reapi.sh
set -euo pipefail

SCCACHE="${SCCACHE_PATH:-/sccache/target/release/sccache}"
RUNS="${BENCH_RUNS:-3}"
PROJECTS="${BENCH_PROJECTS:-ripgrep rust-analyzer}"
WORK="${BENCH_WORK:-${TMPDIR:-/tmp}/sccache-bench}"
RESULTS="${BENCH_RESULTS:-$WORK/results}"
BUILD_TIMEOUT="${BENCH_BUILD_TIMEOUT:-3600}"
ENDPOINT="${REAPI_ENDPOINT:-}"
NATIVELINK_CONFIG="$(dirname "$0")/../reapi/nativelink.json5"
# Without `SCCACHE_ALLOW_INCREMENTAL=cache` sccache doesn't cache incremental compiles, so no
# configuration uses them.
export CARGO_INCREMENTAL=0
unset RUSTC_WRAPPER SCCACHE_LOG

# Prints the git URL, the tag and the commit the tag must point to, for project $1.
project_source() {
    case "$1" in
        ripgrep) echo https://github.com/BurntSushi/ripgrep 15.2.0 e89fff89ac9af12e8d4ce9d5fd07beb408ca730f ;;
        rust-analyzer) echo https://github.com/rust-lang/rust-analyzer 2026-10-26 0c4d74686b21f2b3bd8872b6d3cf36ad2e740d49 ;;
        *) return 1 ;;
    esac
}

fail() {
    echo "FAIL (${project:-} run ${run:-}, ${config:-}): $*" >&2
    exit 1
}

cleanup() {
    "$SCCACHE" --stop-server >/dev/null 2>&1 || true
    stop_nativelink
    [ -z "${HOST_UID:-}" ] || chown -R "$HOST_UID:${HOST_GID:-$HOST_UID}" "$RESULTS" || true
}
trap cleanup EXIT

# Clones project $1 into the sources dir once, and checks that its tag is still the pinned commit.
fetch_source() {
    local url tag commit dir=$WORK/sources/$1
    read -r url tag commit < <(project_source "$1") || fail "unknown project $1"
    if [ ! -d "$dir" ]; then
        git -c advice.detachedHead=false clone -q --depth 1 --branch "$tag" "$url" "$dir.tmp" &&
            mv "$dir.tmp" "$dir"
    fi
    [ "$(git -C "$dir" rev-parse HEAD)" = "$commit" ] || fail "$1 $tag isn't commit $commit"
    (cd "$dir" && cargo fetch -q --locked) || fail "cargo fetch failed for $1"
}

start_nativelink() {
    local data=$WORK/nativelink
    rm -rf "$data"
    mkdir -p "$data"
    sed "s|/data/|$data/|g" "$NATIVELINK_CONFIG" >"$WORK/nativelink.json5"
    nativelink "$WORK/nativelink.json5" >"$RESULTS/logs/nativelink-$project-$run.log" 2>&1 &
    nativelink_pid=$!
    for _ in $(seq 60); do
        (exec 3<>/dev/tcp/127.0.0.1/50051) 2>/dev/null && return
        sleep 1
    done
    fail "NativeLink didn't listen within 60 seconds"
}

stop_nativelink() {
    [ -n "${nativelink_pid:-}" ] || return 0
    kill "$nativelink_pid" 2>/dev/null || true
    wait "$nativelink_pid" 2>/dev/null || true
    nativelink_pid=
}

# Prints the size in bytes of the dirs in $@, or nothing if none exists.
size() {
    local dirs=() dir
    for dir in "$@"; do [ ! -d "$dir" ] || dirs+=("$dir"); done
    [ ${#dirs[@]} -eq 0 ] || du -scb "${dirs[@]}" | tail -n1 | cut -f1
}

# Builds the checkout at $1 as config $2 with RUSTC_WRAPPER $3 (empty for plain cargo), with
# the sccache config $4, and appends its row to the CSV. With a wrapper, an sccache daemon starts
# with the cache dirs from use_cache_dirs, and stops after the build.
build() {
    local checkout=$1 wrapper=$3 start elapsed stats=""
    config=$2
    if [ -n "$wrapper" ]; then
        SCCACHE_CONF=$4 SCCACHE_ERROR_LOG="$RESULTS/logs/sccache-$project-$run-$config.log" \
            "$SCCACHE" --start-server >/dev/null || fail "the sccache daemon didn't start"
    fi
    start=$(date +%s.%N)
    (cd "$checkout" && RUSTC_WRAPPER=$wrapper timeout "$BUILD_TIMEOUT" cargo build -q --locked) \
        >"$RESULTS/logs/cargo-$project-$run-$config.log" 2>&1 ||
        { tail -n 20 "$RESULTS/logs/cargo-$project-$run-$config.log" >&2; fail "cargo build failed"; }
    elapsed=$(awk -v s="$start" -v e="$(date +%s.%N)" 'BEGIN { printf "%.1f", e - s }')
    if [ -n "$wrapper" ]; then
        stats=$("$SCCACHE" --show-stats --stats-format=json) || fail "no stats"
        "$SCCACHE" --stop-server >/dev/null || fail "the sccache daemon didn't stop"
    fi
    python3 - "$RESULTS/results.csv" "$project" "$run" "$config" "$elapsed" "$stats" \
        "$(size "$SCCACHE_DIR")" "$(size "$SCCACHE_DIST_CLIENT_CACHE_DIR")" \
        "$(size "$WORK/nativelink/cas")" "$(size "$WORK/nativelink/ac")" <<'PY'
import csv, json, os, sys
path, project, run, config, wall, stats, local, dist, cas, ac = sys.argv[1:]
row = dict(project=project, run=run, config=config, wall_secs=wall)
if stats:
    s = json.loads(stats)["stats"]
    reapi = s["reapi"] or {}
    local_compiles = reapi.get("local_compilations", {})
    row.update(
        compile_requests=s["compile_requests"],
        cache_hits=sum(s["cache_hits"]["counts"].values()),
        cache_misses=sum(s["cache_misses"]["counts"].values()),
        reapi_hits=reapi.get("action_cache_hits", ""),
        reapi_executions=reapi.get("executions", ""),
        reapi_local_compiles=sum(local_compiles.values()) if s["reapi"] else "",
        reapi_local_reasons="; ".join(f"{k}: {v}" for k, v in sorted(local_compiles.items())),
        local_cache_bytes=local,
        dist_cache_bytes=dist,
    )
    if config.startswith("reapi"):
        row.update(server_cas_bytes=cas, server_ac_bytes=ac)
fields = [
    "project", "run", "config", "wall_secs", "compile_requests", "cache_hits", "cache_misses",
    "reapi_hits", "reapi_executions", "reapi_local_compiles", "reapi_local_reasons",
    "local_cache_bytes", "dist_cache_bytes", "server_cas_bytes", "server_ac_bytes",
]
new = not os.path.exists(path)
with open(path, "a", newline="") as f:
    writer = csv.DictWriter(f, fields)
    if new:
        writer.writeheader()
    writer.writerow(row)
PY
    echo "$project run $run: $config ${elapsed}s"
}

# Points the sccache daemons that start next at new, empty cache dirs named after $1.
use_cache_dirs() {
    export SCCACHE_DIR=$WORK/runs/$project/cache-$1 SCCACHE_DIST_CLIENT_CACHE_DIR=$WORK/runs/$project/dist-cache-$1
    rm -rf "$SCCACHE_DIR" "$SCCACHE_DIST_CLIENT_CACHE_DIR"
}

# Runs every configuration once for project $1, as run $2.
bench_project() {
    project=$1 run=$2
    local dir=$WORK/runs/$1 salt
    rm -rf "$dir"
    mkdir -p "$dir/elsewhere"
    cp -R "$WORK/sources/$1" "$dir/checkout-a"
    cp -R "$WORK/sources/$1" "$dir/elsewhere/checkout-b"
    : >"$dir/local.toml"

    use_cache_dirs plain
    build "$dir/checkout-a" plain "" ""
    rm -rf "$dir/checkout-a/target"

    use_cache_dirs local
    build "$dir/checkout-a" local-cold "$SCCACHE" "$dir/local.toml"
    rm -rf "$dir/checkout-a/target"
    build "$dir/checkout-a" local-warm "$SCCACHE" "$dir/local.toml"
    rm -rf "$dir/checkout-a/target" "$SCCACHE_DIR"

    [ -n "$ENDPOINT" ] || start_nativelink
    salt="bench-$1-$2-$(date +%s%N)"
    cat >"$dir/reapi.toml" <<TOML
[dist.reapi]
endpoint = "${ENDPOINT:-grpc://127.0.0.1:50051}"
salt = "$salt"
# Compiles wait for the connection and the toolchain, so none compiles locally while either is
# getting ready.
ready_wait_secs = 300
TOML
    [ -z "${REAPI_CONFIG:-}" ] || cat "$REAPI_CONFIG" >>"$dir/reapi.toml"
    use_cache_dirs reapi-a
    build "$dir/checkout-a" reapi-cold "$SCCACHE" "$dir/reapi.toml"
    rm -rf "$dir/checkout-a"
    use_cache_dirs reapi-b
    build "$dir/elsewhere/checkout-b" reapi-warm "$SCCACHE" "$dir/reapi.toml"
    stop_nativelink
    rm -rf "$dir"
}

# Writes results.md from results.csv: per project and configuration, the median wall time with
# its range, the median of each count and size, and the reasons for local compiles.
summarize() {
    python3 - "$RESULTS/results.csv" "$RESULTS/results.md" "$1" <<'PY'
import csv, statistics, sys
from collections import defaultdict
src, dst, header = sys.argv[1:]
rows = list(csv.DictReader(open(src)))
order = ["plain", "local-cold", "local-warm", "reapi-cold", "reapi-warm"]
def med(values):
    values = [float(v) for v in values if v != ""]
    return statistics.median(values) if values else None
def num(v, scale=1):
    return "" if v is None else f"{v / scale:.1f}" if scale > 1 else f"{v:g}"
out = [header, ""]
for project in dict.fromkeys(r["project"] for r in rows):
    by_config = defaultdict(list)
    for r in rows:
        if r["project"] == project:
            by_config[r["config"]].append(r)
    plain = med(r["wall_secs"] for r in by_config["plain"])
    out += [f"### {project}", "",
            "| Config | Runs | Wall s, median (min–max) | vs plain | Requests | sccache hits | REAPI hits | Executions | Local compiles | Local cache MiB | Dist cache MiB | Server CAS MiB | Server AC MiB |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for config in order:
        rs = by_config.get(config)
        if not rs:
            continue
        walls = [float(r["wall_secs"]) for r in rs]
        m = statistics.median(walls)
        col = lambda k, scale=1: num(med(r[k] for r in rs), scale)
        out.append(f"| {config} | {len(rs)} | {m:.1f} ({min(walls):.1f}–{max(walls):.1f}) | "
                   f"{plain / m:.2f}x | {col('compile_requests')} | {col('cache_hits')} | "
                   f"{col('reapi_hits')} | {col('reapi_executions')} | {col('reapi_local_compiles')} | "
                   f"{col('local_cache_bytes', 2**20)} | {col('dist_cache_bytes', 2**20)} | "
                   f"{col('server_cas_bytes', 2**20)} | {col('server_ac_bytes', 2**20)} |")
    out += ["", "Local compiles by reason, per run:", ""]
    for config in ("reapi-cold", "reapi-warm"):
        for r in by_config.get(config, []):
            out.append(f"- {config} run {r['run']}: {r['reapi_local_reasons'] or 'none'}")
    out.append("")
open(dst, "w").write("\n".join(out))
PY
}

mkdir -p "$WORK/sources" "$RESULTS/logs"
rm -f "$RESULTS/results.csv"
for project in $PROJECTS; do
    fetch_source "$project"
done
for project in $PROJECTS; do
    for run in $(seq "$RUNS"); do
        bench_project "$project" "$run"
    done
done
summarize "$(
    echo "- Client: $(nproc) CPUs, $(awk '/MemTotal/ { printf "%.1f GiB", $2 / 2^20 }' /proc/meminfo), $(uname -srm)"
    echo "- Toolchain: $(rustc -V), $("$SCCACHE" --version)"
    echo "- REAPI server: ${ENDPOINT:+the one in REAPI_ENDPOINT}${ENDPOINT:-a NativeLink with empty stores at each run, on the client}"
    for project in $PROJECTS; do
        echo "- $project: $(project_source "$project" | cut -d' ' -f2-)"
    done
)"
echo "Results: $RESULTS/results.md"
