#!/usr/bin/env bash
# Run one load or chaos test end to end and save a loadcheck report.
#
#   loadtest/run.sh <test> [--events N] [--endpoints N] [--rate R] [--workers W] [--mode kafka|postgres]
#
# tests: steady | spike | chaos-worker | chaos-endpoints | chaos-db | compare
#
# Everything runs in Docker: k6 (grafana/k6 image) and loadcheck (from the
# hooklane image) join the Compose network. Reports go to docs/results/.
set -euo pipefail
cd "$(dirname "$0")/.."

TEST=${1:?usage: run.sh <steady|spike|chaos-worker|chaos-endpoints|chaos-db|compare> [options]}
shift
EVENTS=100000
ENDPOINTS=100
RATE=2000
WORKERS=${WORKERS:-4}
MODE=${QUEUE_MODE:-kafka}
SPIKE_SECS=40
while [[ $# -gt 0 ]]; do
  case "$1" in
    --events) EVENTS=$2; shift 2 ;;
    --endpoints) ENDPOINTS=$2; shift 2 ;;
    --rate) RATE=$2; shift 2 ;;
    --workers) WORKERS=$2; shift 2 ;;
    --mode) MODE=$2; shift 2 ;;
    --spike-secs) SPIKE_SECS=$2; shift 2 ;;
    *) echo "unknown option $1"; exit 1 ;;
  esac
done

K6_IMAGE=grafana/k6:1.8.3
NETWORK=hooklane_default
OUT_DIR=loadtest/out
RESULTS=docs/results
mkdir -p "$OUT_DIR" "$RESULTS"
chmod 777 "$OUT_DIR"

machine() {
  local cpu mem vm
  cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)
  mem=$(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1024 / 1024 / 1024 ))
  vm=$(docker info --format '{{.NCPU}} CPUs, {{.MemTotal}}' 2>/dev/null | awk '{printf "%s %s %.1f GB", $1, $2, $3/1024/1024/1024}')
  echo "${cpu}, ${mem} GB RAM host; Docker VM: ${vm}"
}

# Refuse to start with too little disk: Docker Desktop/Colima keep their VM disk
# as a file on the host, and a full host disk corrupts the VM's filesystem.
check_disk() {
  local free_gb
  free_gb=$(df -k "$PWD" | awk 'NR==2 {printf "%d", $4/1024/1024}')
  if (( free_gb < 4 )); then
    echo "Only ${free_gb} GB free on the host disk; free at least 4 GB before running load tests." >&2
    exit 1
  fi
}

start_stack() { # $1 = queue mode
  check_disk
  echo "==> stack: QUEUE_MODE=$1, $WORKERS workers (fresh volumes)"
  # Every test starts from empty Postgres/Redpanda volumes: results do not
  # depend on earlier runs, and disk use does not pile up across runs.
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  QUEUE_MODE=$1 docker compose up -d --build --wait --scale worker="$WORKERS" >/dev/null
  # Fresh sink so its in-memory id table only holds this run.
  docker compose restart sink >/dev/null
  docker compose up -d --wait sink >/dev/null
}

consumer_lag() {
  docker compose exec -T redpanda rpk group describe hooklane-workers 2>/dev/null \
    | awk '/^TOTAL-LAG/ {print $2; found=1} END {if (!found) print 0}'
}

# Run one test. Sets REPORT_FILE and appends a JSON line to $OUT_DIR/results.jsonl.
run_one() { # $1 = test name, $2 = queue mode
  local test=$1 mode=$2 profile=steady failing=0 notes="" settings run_id
  case "$test" in
    spike) profile=spike ;;
    chaos-endpoints) failing=30 ;;
  esac
  start_stack "$mode"

  run_id=$(date +%s)
  local creds tenant key
  creds=$(docker compose exec -T api hooklane admin create-tenant --name "load-$test-$run_id" --env-format)
  tenant=$(echo "$creds" | sed -n 's/^TENANT_ID=//p' | tr -d '\r')
  key=$(echo "$creds" | sed -n 's/^API_KEY=//p' | tr -d '\r')
  rm -f "$OUT_DIR/summary.env"

  echo "==> $test ($mode): $EVENTS events, $ENDPOINTS endpoints, rate $RATE/s, profile $profile"
  docker run --rm --network "$NETWORK" -v "$PWD/loadtest:/scripts:ro" -v "$PWD/$OUT_DIR:/out" \
    -e API_URL=http://api:8080 -e SINK_URL=http://sink:8080 -e API_KEY="$key" -e RUN_ID="$run_id" \
    -e ENDPOINTS="$ENDPOINTS" -e EVENTS="$EVENTS" -e RATE="$RATE" -e PROFILE="$profile" \
    -e FAILING_PCT="$failing" -e SPIKE_SECS="$SPIKE_SECS" \
    "$K6_IMAGE" run --quiet /scripts/load.js > "$OUT_DIR/k6-$test-$mode.log" 2>&1 &
  local k6_pid=$!

  # Chaos actions while the load runs.
  case "$test" in
    chaos-worker)
      local kills=0
      sleep 20
      while kill -0 "$k6_pid" 2>/dev/null; do
        victim=$(docker compose ps -q worker | sort -R | head -1)
        echo "    SIGKILL worker ${victim:0:12}"
        docker kill -s KILL "$victim" >/dev/null || true
        kills=$((kills + 1))
        sleep 25
      done
      notes="Killed a random worker container with SIGKILL every 25 s during the run ($kills kills). Docker's restart policy restarted each one (like a Kubernetes pod restart). Duplicates are expected: a delivery whose HTTP request was in flight when its worker died is sent again after the reaper (2 min) re-queues it."
      ;;
    chaos-db)
      sleep 30
      echo "    restarting postgres"
      local t0; t0=$(date +%s)
      docker compose restart postgres >/dev/null
      docker compose up -d --wait postgres >/dev/null
      notes="Restarted Postgres (docker compose restart) 30 s into the run; it was unavailable for about $(( $(date +%s) - t0 )) s. During the outage the API answered 503 and k6 counted those as rejected (not accepted) events; roles retried their connections and resumed."
      ;;
    spike)
      notes="Base rate $RATE/s with a ${SPIKE_SECS}-second spike at $((RATE * 10))/s (10x)."
      ;;
    chaos-endpoints)
      notes="30% of endpoints misbehave: a third return 500, a third time out (sink waits 20 s, Hooklane gives up at 15 s), a third return 429 with Retry-After: 5. Only the 70% healthy (/ok/) endpoints count towards lost/latency/SLO."
      ;;
  esac
  local peak_lag=0 lag
  if [[ "$test" == "spike" && "$mode" == "kafka" ]]; then
    while kill -0 "$k6_pid" 2>/dev/null; do
      lag=$(consumer_lag); [[ "$lag" =~ ^[0-9]+$ ]] && (( lag > peak_lag )) && peak_lag=$lag
      sleep 2
    done
  fi
  wait "$k6_pid" || echo "    (k6 exited non-zero; see $OUT_DIR/k6-$test-$mode.log)"
  local load_end; load_end=$(date +%s)
  if [[ "$test" == "spike" && "$mode" == "kafka" ]]; then
    echo "    waiting for consumer lag to drain (peak seen: $peak_lag)"
    while true; do
      lag=$(consumer_lag)
      [[ "$lag" == "0" ]] && break
      (( $(date +%s) - load_end > 900 )) && break
      sleep 2
    done
    notes="$notes Peak consumer lag sampled every 2 s: $peak_lag messages. Lag drained to 0 in $(( $(date +%s) - load_end )) s after the load stopped."
  fi

  # shellcheck disable=SC1090
  source "$OUT_DIR/summary.env"
  echo "    k6: accepted=$ACCEPTED rejected=$REJECTED ingest p95=$INGEST_P95"
  settings="k6 ramping-arrival-rate, target $RATE/s ($profile), $ENDPOINTS endpoints, ingest latency p50/p95/p99 $INGEST_P50/$INGEST_P95/$INGEST_P99, rejected $REJECTED"
  REPORT_FILE="$RESULTS/$(date +%Y-%m-%d)-$test-$mode-$EVENTS.md"
  docker compose run --rm --no-deps -T --entrypoint loadcheck api \
    --database-url postgres://hooklane:hooklane@postgres:5432/hooklane --sink-url http://sink:8080 \
    --tenant-id "$tenant" --accepted "$ACCEPTED" --test "$test" --queue-mode "$mode" --workers "$WORKERS" \
    --machine "$(machine)" --settings "$settings" --notes "$notes" --drain-timeout-secs 900 \
    > "$REPORT_FILE" 2> "$OUT_DIR/loadcheck-$test-$mode.err" || true
  grep LOADCHECK_JSON "$OUT_DIR/loadcheck-$test-$mode.err" | sed 's/^LOADCHECK_JSON //' >> "$OUT_DIR/results.jsonl" || true
  echo "==> report: $REPORT_FILE"
  sed -n '/## Result/,/## Deliveries/p' "$REPORT_FILE" | head -14
}

if [[ "$TEST" == "compare" ]]; then
  run_one steady postgres; PG_REPORT=$REPORT_FILE
  run_one steady kafka; KAFKA_REPORT=$REPORT_FILE
  echo "==> compare reports: $PG_REPORT and $KAFKA_REPORT (summarised in docs/RESULTS.md)"
else
  run_one "$TEST" "$MODE"
fi
