#!/usr/bin/env bash
#
# Reproduces the three benchmark scenarios in PRD.md Section 11.
#
#   ./bench/run.sh            # all three scenarios
#   ./bench/run.sh single     # baseline, no consensus
#   ./bench/run.sh cluster    # healthy 3-node cluster
#   ./bench/run.sh failure    # 3-node cluster, leader killed mid-run
#
# Requires vegeta (brew install vegeta) and python3 for the reports.
#
# Every number in the README came from this script. Results depend heavily on
# the machine, and the load generator runs alongside the servers, so treat the
# figures as a floor rather than a ceiling.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="${BENCH_WORK:-$(mktemp -d)}"
BIN="$WORK/quorumd"

DURATION="${DURATION:-30s}"
CLUSTER_RATE="${CLUSTER_RATE:-10000}"
COMPARE_RATE="${COMPARE_RATE:-6000}"
FAILURE_RATE="${FAILURE_RATE:-3000}"

# Raft timings. The defaults in quorumd are tuned for a visible demo failover;
# these are the calmer values a real deployment would use, and the difference
# matters under sustained load (see explainers/stage-8-benchmarks.md).
HEARTBEAT="${HEARTBEAT:-100ms}"
ELECTION_MIN="${ELECTION_MIN:-1s}"
ELECTION_MAX="${ELECTION_MAX:-2s}"
# Must exceed ELECTION_MAX, or in-flight requests age out during a failover
# instead of being retried against the new leader.
GRACE="${GRACE:-3s}"

# A limit high enough that nothing is ever refused, so the benchmark measures
# throughput rather than the rate of rejection.
LIMIT=100000000

cleanup() { pkill -f "$BIN" 2>/dev/null || true; }
trap cleanup EXIT

report() {
  python3 -c "
import sys, json
d = json.load(sys.stdin); lat = d['latencies']
ms = lambda k: lat[k] / 1e6
print(f\"  {sys.argv[1]:<26} success {d['success']*100:5.1f}%  throughput {d['throughput']:8.1f}/s  \"
      f\"p50 {ms('50th'):6.2f}ms  p95 {ms('95th'):7.2f}ms  p99 {ms('99th'):7.2f}ms\")
" "$1"
}

leader_port() {
  for p in 8081 8082 8083; do
    role=$(curl -s --max-time 2 "http://127.0.0.1:$p/status" 2>/dev/null \
      | python3 -c "import sys,json;print(json.load(sys.stdin)['role'])" 2>/dev/null || true)
    [ "$role" = "leader" ] && { echo "$p"; return; }
  done
}

target_file() {
  printf 'POST http://127.0.0.1:%s/check\nContent-Type: application/json\n@%s\n' "$1" "$WORK/body.json" > "$WORK/target.txt"
}

start_cluster() {
  local p1="node-1=127.0.0.1:9091=127.0.0.1:8081"
  local p2="node-2=127.0.0.1:9092=127.0.0.1:8082"
  local p3="node-3=127.0.0.1:9093=127.0.0.1:8083"
  for n in 1 2 3; do
    case $n in 1) peers="$p2,$p3";; 2) peers="$p1,$p3";; 3) peers="$p1,$p2";; esac
    "$BIN" -node-id "node-$n" -addr "127.0.0.1:808$n" -raft-addr "127.0.0.1:909$n" \
      -peers "$peers" -limit "$LIMIT" -window 1h \
      -heartbeat "$HEARTBEAT" -election-timeout-min "$ELECTION_MIN" \
      -election-timeout-max "$ELECTION_MAX" -failover-grace "$GRACE" \
      > "$WORK/node-$n.log" 2>&1 &
  done
  sleep 4
}

commit_index() {
  curl -s --max-time 2 "http://127.0.0.1:$1/metrics" 2>/dev/null \
    | awk -F' ' '/^quorum_raft_commit_index/ {print $2}'
}

echo "building..."
go -C "$ROOT" build -o "$BIN" ./cmd/quorumd
echo '{"caller_id":"bench-caller"}' > "$WORK/body.json"
echo "work dir: $WORK"
echo

scenario_single() {
  echo "== 1. baseline: single node, no consensus =="
  cleanup; sleep 1
  "$BIN" -node-id node-solo -addr 127.0.0.1:8081 -limit "$LIMIT" -window 1h > "$WORK/solo.log" 2>&1 &
  sleep 2
  target_file 8081
  vegeta attack -targets="$WORK/target.txt" -rate=0 -max-workers=200 -duration="$DURATION" 2>/dev/null \
    | vegeta report -type=json 2>/dev/null | report "max throughput"
  sleep 3
  vegeta attack -targets="$WORK/target.txt" -rate="$COMPARE_RATE" -max-workers=200 -duration="$DURATION" 2>/dev/null \
    | vegeta report -type=json 2>/dev/null | report "fixed ${COMPARE_RATE}/s"
  cleanup; sleep 2; echo
}

scenario_cluster() {
  echo "== 2. healthy 3-node cluster =="
  cleanup; sleep 1
  start_cluster
  target_file "$(leader_port)"
  vegeta attack -targets="$WORK/target.txt" -rate="$COMPARE_RATE" -max-workers=200 -duration="$DURATION" 2>/dev/null \
    | vegeta report -type=json 2>/dev/null | report "fixed ${COMPARE_RATE}/s"
  sleep 5
  target_file "$(leader_port)"
  vegeta attack -targets="$WORK/target.txt" -rate="$CLUSTER_RATE" -max-workers=200 -duration="$DURATION" 2>/dev/null \
    | vegeta report -type=json 2>/dev/null | report "fixed ${CLUSTER_RATE}/s"
  echo "  elections during the runs: $(grep -ch 'election won' "$WORK"/node-*.log | paste -sd+ - | bc)"
  cleanup; sleep 2; echo
}

scenario_failure() {
  echo "== 3. 3-node cluster, leader killed mid-run =="
  cleanup; sleep 1
  start_cluster

  local lp leader follower before after ok
  lp="$(leader_port)"
  leader=$(curl -s "http://127.0.0.1:$lp/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['node_id'])")
  # Drive load at a follower, so the client keeps a live endpoint when the
  # leader dies and the forwarding path is exercised.
  for p in 8081 8082 8083; do [ "$p" != "$lp" ] && follower="$p" && break; done
  target_file "$follower"
  echo "  leader $leader on :$lp, load at follower :$follower"

  before=$(commit_index "$follower")
  vegeta attack -targets="$WORK/target.txt" -rate="$FAILURE_RATE" -max-workers=200 \
    -duration="$DURATION" > "$WORK/failure.bin" 2>/dev/null &
  local vpid=$!
  sleep 15
  pkill -f "node-id $leader" || true
  echo "  killed $leader halfway through"
  wait $vpid

  vegeta report -type=json < "$WORK/failure.bin" 2>/dev/null | report "fixed ${FAILURE_RATE}/s + kill"
  ok=$(vegeta report -type=json < "$WORK/failure.bin" 2>/dev/null \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['status_codes'].get('200',0))")

  echo "  --- accounting: every acknowledged request must be exactly one committed entry ---"
  echo "     successful requests : $ok"
  for p in 8081 8082 8083; do
    after=$(commit_index "$p")
    [ -n "$after" ] && echo "     commit index :$p    : $after"
  done
  echo "     (survivors must agree, and the delta must equal the successful count)"
  cleanup; echo
}

case "${1:-all}" in
  single)  scenario_single ;;
  cluster) scenario_cluster ;;
  failure) scenario_failure ;;
  all)     scenario_single; scenario_cluster; scenario_failure ;;
  *) echo "usage: $0 [single|cluster|failure|all]" >&2; exit 2 ;;
esac
