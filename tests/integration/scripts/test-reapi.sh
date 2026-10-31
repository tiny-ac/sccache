#!/bin/bash
# Builds tests/integration/reapi/fixture with sccache and REAPI, each build with empty local
# caches, and checks:
# - on_miss = "execute": checkout A executes each compile it can on the server's workers, and
#   checkout B, at another path, hits the action cache for each one and links the rlibs, in
#   server-side mode and again in client-side mode;
# - on_miss = "local": a build executes nothing and writes no action result, so a later build
#   with the same salt still executes every compile, and writes the same rlibs;
# - an unreachable server: the build compiles everything locally.
set -euo pipefail

SCCACHE="${SCCACHE_PATH:-/sccache/target/debug/sccache}"
ENDPOINT="${REAPI_ENDPOINT:-grpc://nativelink:50051}"
# REAPI_PLATFORM is a TOML line such as `OSFamily = "linux"`, added under [dist.reapi.platform].
PLATFORM="${REAPI_PLATFORM:+[dist.reapi.platform]
$REAPI_PLATFORM}"
export CARGO_INCREMENTAL=0

# Prints REAPI stat $1 from the JSON stats in $2, or 0 if `reapi` is null.
reapi_stat() {
    python3 -c 'import sys, json
r = json.loads(sys.argv[2])["stats"]["reapi"]
print(r[sys.argv[1]] if r else 0)' "$1" "$2"
}

# Prints how many compiles in the JSON stats in $2 compiled locally for reason $1.
local_compiles() {
    python3 -c 'import sys, json
r = json.loads(sys.argv[2])["stats"]["reapi"] or {}
print(r.get("local_compilations", {}).get(sys.argv[1], 0))' "$1" "$2"
}

# Builds the checkout at $1 with empty local caches and prints sccache's stats as JSON.
build() {
    local stats
    "$SCCACHE" --stop-server >/dev/null 2>&1 || true
    export SCCACHE_DIR="$work/cache-${1//\//-}" SCCACHE_DIST_CLIENT_CACHE_DIR="$work/dist-cache-${1//\//-}"
    "$SCCACHE" --start-server >/dev/null || return 1
    # SCCACHE_LOG turns client-side mode off, so cargo runs without it there. The daemon keeps
    # it, for the log line execute_on_miss looks for.
    (cd "$work/$1" && ${SCCACHE_CLIENT_SIDE:+env -u SCCACHE_LOG} cargo build -q --locked >&2) ||
        return 1
    stats=$("$SCCACHE" --show-stats --stats-format=json) || return 1
    echo "$stats"
    "$SCCACHE" --stop-server >/dev/null || { echo "the sccache daemon didn't stop" >&2; return 1; }
}

# Prints the failure and the stats of the calling mode's builds, and exits.
fail() {
    echo "FAIL ($mode): $*" >&2
    # The calling mode's locals.
    for stats_json in "${stats_a:-}" "${stats_b:-}"; do
        [ -z "$stats_json" ] || echo "$stats_json" >&2
    done
    exit 1
}

# Starts mode $1: sets `mode`, and `work` to a new directory holding two checkouts of the
# fixture and a config for endpoint $2 that waits up to $3 seconds for the server and the
# toolchain, with $4 added to `[dist.reapi]`.
setup() {
    mode=$1
    work=/build/reapi-${mode// /-}
    mkdir -p "$work/elsewhere"
    cp -R /sccache/tests/integration/reapi/fixture "$work/checkout-a"
    cp -R /sccache/tests/integration/reapi/fixture "$work/elsewhere/checkout-b"
    cat >"$work/sccache.toml" <<TOML
[dist.reapi]
endpoint = "$2"
# A salt per run and mode, so each mode starts with an empty action cache.
salt = "${mode// /-}-$(date +%s%N)"
ready_wait_secs = $3
${4:-}
$PLATFORM
TOML
    export SCCACHE_CONF="$work/sccache.toml"

    echo "=========================================="
    echo "Testing: REAPI, $mode"
    echo "=========================================="
}

# Prints how many rlibs the checkout at $1 built.
rlibs() {
    find "$work/$1/target/debug/deps" -name '*.rlib' | wc -l
}

# Checks that cargo test links the rlibs in checkout B's target directory without rebuilding them.
check_links() {
    local log rebuilt
    log=$(cd "$work/elsewhere/checkout-b" && RUSTC_WRAPPER='' cargo test --locked 2>&1) ||
        fail "the test binary didn't build or pass: $log"
    grep -q '^ *Compiling reapi-fixture ' <<<"$log" || fail "cargo test compiled no test binary: $log"
    rebuilt=$(grep '^ *Compiling ' <<<"$log" | grep -v '^ *Compiling reapi-fixture ' || true)
    [ -z "$rebuilt" ] || fail "cargo test rebuilt crates in checkout B's target directory: $rebuilt"
}

# Runs the on_miss = "execute" builds, in client-side mode if $1 is "client-side".
execute_on_miss() {
    # Compiles wait for the connection and the toolchain, so none compiles locally while either
    # is getting ready.
    setup "on_miss execute ${1:-server-side}" "$ENDPOINT" 300
    [ "${1:-}" != client-side ] || export SCCACHE_CLIENT_SIDE=1
    local stats_a stats_b
    stats_a=$(build checkout-a) || fail "checkout A didn't build"
    stats_b=$(build elsewhere/checkout-b) || fail "checkout B didn't build"
    echo "checkout A: $(reapi_stat executions "$stats_a") REAPI executions of $(rlibs checkout-a) rlibs"
    echo "checkout B: $(reapi_stat action_cache_hits "$stats_b") REAPI action cache hits"

    [ "$(reapi_stat executions "$stats_a")" -eq "$(rlibs checkout-a)" ] ||
        fail "checkout A executed $(reapi_stat executions "$stats_a") of its $(rlibs checkout-a) rlibs"
    [ "$(reapi_stat action_cache_hits "$stats_b")" -eq "$(reapi_stat executions "$stats_a")" ] ||
        fail "checkout B didn't hit every action checkout A executed"
    [ "$(reapi_stat executions "$stats_b")" -eq 0 ] || fail "checkout B executed actions"
    if [ -n "${SCCACHE_CLIENT_SIDE:-}" ]; then
        # A debug line of the daemon's, which the compose file's SCCACHE_LOG=sccache=debug logs.
        grep -q 'handle_client: reapi' "$SCCACHE_ERROR_LOG" ||
            fail "no CLI sent the daemon a REAPI request, so the builds didn't run in client-side mode"
        unset SCCACHE_CLIENT_SIDE
    fi
    check_links
    echo "✓ PASS: checkout B hit every action checkout A executed, and links them"
    rm -rf "$work"
}

local_on_miss() {
    setup "on_miss local" "$ENDPOINT" 300 'on_miss = "local"'
    local stats_a stats_b rlib
    stats_a=$(build checkout-a) || fail "checkout A didn't build"
    [ "$(reapi_stat executions "$stats_a")" -eq 0 ] || fail "checkout A executed actions"
    [ "$(local_compiles "not in the action cache" "$stats_a")" -eq "$(rlibs checkout-a)" ] ||
        fail "checkout A didn't miss the action cache for each of its $(rlibs checkout-a) rlibs"
    # The same salt with on_miss = "execute" finds nothing checkout A could have written.
    sed -i 's/^on_miss = "local"$/on_miss = "execute"/' "$work/sccache.toml"
    grep -qx 'on_miss = "execute"' "$work/sccache.toml" || fail "the config still has on_miss = \"local\""
    stats_b=$(build elsewhere/checkout-b) || fail "checkout B didn't build"
    [ "$(reapi_stat action_cache_hits "$stats_b")" -eq 0 ] ||
        fail "checkout B hit $(reapi_stat action_cache_hits "$stats_b") actions"
    [ "$(reapi_stat executions "$stats_b")" -eq "$(rlibs elsewhere/checkout-b)" ] ||
        fail "checkout B executed $(reapi_stat executions "$stats_b") of its rlibs"
    # Local compiles run with the worker's arguments, so they write what the worker writes.
    for rlib in "$work"/checkout-a/target/debug/deps/*.rlib; do
        cmp -s "$rlib" "$work/elsewhere/checkout-b/target/debug/deps/${rlib##*/}" ||
            fail "${rlib##*/} differs between the local compile and the execution"
    done
    echo "✓ PASS: on_miss = \"local\" wrote nothing, and compiled what the worker compiles"
    rm -rf "$work"
}

unreachable_server() {
    setup "unreachable server" "grpc://127.0.0.1:1" 5
    local stats_a not_connected
    stats_a=$(build checkout-a) || fail "checkout A didn't build"
    [ "$(reapi_stat executions "$stats_a")" -eq 0 ] || fail "checkout A executed actions"
    [ "$(reapi_stat action_cache_hits "$stats_a")" -eq 0 ] || fail "checkout A hit the action cache"
    not_connected=$(($(local_compiles "failed to connect to the REAPI server" "$stats_a") +
        $(local_compiles "connecting to the REAPI server" "$stats_a")))
    [ "$not_connected" -eq "$(rlibs checkout-a)" ] ||
        fail "$not_connected of $(rlibs checkout-a) rlibs compiled locally for not connecting"
    echo "✓ PASS: an unreachable server leaves every compile local"
    rm -rf "$work"
}

execute_on_miss
execute_on_miss client-side
local_on_miss
unreachable_server
echo "All REAPI tests completed successfully!"
