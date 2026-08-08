#!/usr/bin/env bash
#
# Stage 6 validation: proves a docker compose cluster actually works.
#
#   docker compose up -d --build
#   ./scripts/validate-cluster.sh
#
# Checks the cluster forms, replicates, survives losing its leader through the
# demo controls, survives a real container crash, and recovers from both.
#
# Every check prints the value it compared. A check that silently compares two
# empty strings is worse than no check at all - an earlier version of this
# script grepped for a metric name that did not exist and reported that all
# three nodes agreed, because "" equals "".

set -uo pipefail

API_1=${API_1:-8081}
API_2=${API_2:-8082}
API_3=${API_3:-8083}

port_of() { case "$1" in node-1) echo "$API_1";; node-2) echo "$API_2";; node-3) echo "$API_3";; esac; }

field() {
  curl -s --max-time 3 "http://127.0.0.1:$(port_of "$1")/status" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['$2'])" 2>/dev/null || echo "unreachable"
}
role_of() { field "$1" role; }
term_of() { field "$1" term; }

# commit_index is the length of the replicated log this node has applied. It is
# the independent audit of replication: it cannot agree by accident.
#
# The metric carries a node_id label, so the name is followed by '{' rather than
# a space. Anchoring on a trailing space silently matched nothing, and comparing
# the resulting empty strings reported that all three nodes agreed.
commit_index() {
  curl -s --max-time 3 "http://127.0.0.1:$(port_of "$1")/metrics" \
    | grep -E '^quorum_raft_commit_index[ {]' | awk '{print $2}'
}

find_leader() {
  for _ in $(seq 1 60); do
    for n in node-1 node-2 node-3; do
      [ "$(role_of "$n")" = "leader" ] && { echo "$n"; return 0; }
    done
    sleep 0.25
  done
  echo ""
  return 1
}

PASS=0
FAIL=0
check() {
  if [ "$2" = "ok" ]; then
    echo "  PASS  $1"
    PASS=$((PASS + 1))
  else
    echo "  FAIL  $1"
    FAIL=$((FAIL + 1))
  fi
}

echo "=== 1. the cluster forms and the nodes find each other ==="
L1=$(find_leader)
if [ -z "$L1" ]; then
  echo "  FAIL  no leader was elected; is the cluster up?"
  exit 1
fi
for n in node-1 node-2 node-3; do echo "     $n -> $(role_of "$n")"; done

NLEADERS=0
for n in node-1 node-2 node-3; do [ "$(role_of "$n")" = "leader" ] && NLEADERS=$((NLEADERS + 1)); done
[ "$NLEADERS" = "1" ] && check "exactly one leader ($L1, term $(term_of "$L1"))" ok \
                      || check "exactly one leader (saw $NLEADERS)" no

PEERS=$(curl -s "http://127.0.0.1:$(port_of "$L1")/status" | python3 -c 'import sys,json; print(len(json.load(sys.stdin)["peers"]))')
[ "$PEERS" = "2" ] && check "the leader sees both peers" ok || check "the leader sees both peers (saw $PEERS)" no

echo ""
echo "=== 2. replication: every accepted request becomes one log entry on every node ==="
BEFORE=$(commit_index "$L1")
WRITES=25
for _ in $(seq 1 "$WRITES"); do
  curl -s -o /dev/null -X POST "http://127.0.0.1:$(port_of "$L1")/check" -d '{"caller_id":"commit-audit"}'
done
sleep 2

echo "     commit index before: $BEFORE"
AGREED=ok
for n in node-1 node-2 node-3; do
  C=$(commit_index "$n")
  echo "     $n after $WRITES writes: $C"
  [ -z "$C" ] && AGREED=no
  [ "$C" != "$(commit_index "$L1")" ] && AGREED=no
done
check "all three nodes report the same commit index" "$AGREED"

AFTER=$(commit_index "$L1")
if [ -n "$BEFORE" ] && [ -n "$AFTER" ]; then
  GREW=$((AFTER - BEFORE))
  [ "$GREW" = "$WRITES" ] && check "the log grew by exactly $WRITES entries for $WRITES requests" ok \
                          || check "the log grew by $GREW entries for $WRITES requests" no
else
  check "commit index is readable" no
fi

echo ""
echo "=== 3. a write sent to a follower is forwarded to the leader ==="
F1=$(for n in node-1 node-2 node-3; do [ "$n" != "$L1" ] && echo "$n" && break; done)
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X POST "http://127.0.0.1:$(port_of "$F1")/check" -d '{"caller_id":"via-follower"}')
[ "$CODE" = "200" ] && check "write to follower $F1 succeeded (HTTP $CODE)" ok \
                    || check "write to follower $F1 returned HTTP $CODE" no

echo ""
echo "=== 4. the built-in load generator, and per-caller fairness ==="
curl -s -X POST "http://127.0.0.1:$(port_of "$L1")/swarm" \
  -d '{"rate":400,"duration_ms":3000,"caller_mix":"one_abusive"}' >/dev/null
sleep 4

SWARM=$(curl -s "http://127.0.0.1:$(port_of "$L1")/swarm" | python3 -c '
import sys, json
s = json.load(sys.stdin)
ab = [c for c in s["callers"] if c["abusive"]][0]
fair = [c for c in s["callers"] if not c["abusive"]]
print("sent=%d allowed=%d blocked=%d failed=%d dropped=%d abusive_blocked=%d fair_allowed=%d fair_blocked=%d" % (
    s["sent"], s["allowed"], s["blocked"], s["failed"], s["dropped"],
    ab["blocked"], sum(c["allowed"] for c in fair), sum(c["blocked"] for c in fair)))')
echo "     $SWARM"
eval "$SWARM"

[ "${abusive_blocked:-0}" -gt 0 ] && check "the abusive caller is blocked" ok || check "the abusive caller is blocked" no
[ "${fair_allowed:-0}" -gt 0 ]    && check "well-behaved callers still get through" ok || check "well-behaved callers still get through" no
[ "${fair_blocked:-1}" = "0" ]    && check "well-behaved callers are never blocked by the abuser" ok \
                                  || check "well-behaved callers were blocked ${fair_blocked} times" no

echo ""
echo "=== 5. killing the leader from the API ==="
TERM_BEFORE=$(term_of "$L1")
curl -s -X POST "http://127.0.0.1:$(port_of "$L1")/admin/kill" >/dev/null
[ "$(role_of "$L1")" = "down" ] && check "the killed node reports role=down" ok \
                                || check "the killed node reports role=$(role_of "$L1")" no

L2=$(find_leader)
{ [ -n "$L2" ] && [ "$L2" != "$L1" ]; } && check "a new leader took over ($L2, term $(term_of "$L2"))" ok \
                                        || check "a new leader took over" no

echo ""
echo "=== 6. the cluster keeps serving with one node down ==="
OK=0
BAD=0
for _ in $(seq 1 30); do
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 \
    -X POST "http://127.0.0.1:$(port_of "$L2")/check" -d '{"caller_id":"during-outage"}')
  [ "$code" = "200" ] && OK=$((OK + 1)) || BAD=$((BAD + 1))
done
echo "     $OK/30 succeeded, $BAD failed"
[ "$OK" = "30" ] && check "every request was served during the outage" ok \
                 || check "$BAD of 30 requests failed during the outage" no

echo ""
echo "=== 7. the downed node stays frozen, then rejoins without disruption ==="
sleep 4
TERM_AFTER=$(term_of "$L1")
[ "$TERM_BEFORE" = "$TERM_AFTER" ] && check "the downed node's term stayed at $TERM_BEFORE (it is not campaigning)" ok \
                                   || check "the downed node's term drifted $TERM_BEFORE -> $TERM_AFTER" no

curl -s -X POST "http://127.0.0.1:$(port_of "$L1")/admin/revive" >/dev/null
sleep 4
echo "     revived $L1: role=$(role_of "$L1") term=$(term_of "$L1")"
echo "     leader  $L2: role=$(role_of "$L2") term=$(term_of "$L2")"
[ "$(role_of "$L1")" = "follower" ]        && check "the revived node rejoined as a follower" ok || check "the revived node rejoined as a follower" no
[ "$(role_of "$L2")" = "leader" ]          && check "reviving did not unseat the leader" ok      || check "reviving did not unseat the leader" no
[ "$(term_of "$L1")" = "$(term_of "$L2")" ] && check "the revived node caught up to the cluster's term" ok || check "the revived node caught up to the cluster's term" no

echo ""
echo "=== 8. a real container crash, not a simulated one ==="
docker compose stop "$L2" >/dev/null 2>&1
L3=$(find_leader)
{ [ -n "$L3" ] && [ "$L3" != "$L2" ]; } && check "the cluster elected $L3 after $L2's container was stopped" ok \
                                        || check "the cluster survived a real container stop" no

docker compose start "$L2" >/dev/null 2>&1
sleep 10
echo "     restarted $L2: role=$(role_of "$L2") term=$(term_of "$L2")"
[ "$(role_of "$L2")" = "follower" ] && check "the restarted container rejoined as a follower" ok \
                                    || check "the restarted container rejoined as a follower" no

C_RESTARTED=$(commit_index "$L2")
C_LEADER=$(commit_index "$L3")
echo "     commit index: $L2=$C_RESTARTED  $L3=$C_LEADER"
{ [ -n "$C_RESTARTED" ] && [ "$C_RESTARTED" = "$C_LEADER" ]; } \
  && check "the restarted node was backfilled to the leader's log" ok \
  || check "the restarted node was backfilled ($C_RESTARTED vs $C_LEADER)" no

echo ""
echo "==================================="
echo "  $PASS passed, $FAIL failed"
echo "==================================="
[ "$FAIL" = "0" ] || exit 1
