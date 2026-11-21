#!/usr/bin/env bash
# make demo: create a tenant and three endpoints on the sink (healthy, flaky,
# always failing), send 100 events, then print what happened to them.
# Needs only Docker and curl (both present on a Mac with Docker Desktop).
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8080
SINK_ADMIN=http://localhost:8090
RUN=$(date +%s)
EVENTS=${DEMO_EVENTS:-100}

psql() { docker compose exec -T postgres psql -U hooklane -d hooklane -v ON_ERROR_STOP=1 "$@"; }
json_field() { sed -n "s/.*\"$1\":\"\([^\"]*\)\".*/\1/p"; }

echo "==> creating tenant"
OUT=$(docker compose exec -T api hooklane admin create-tenant --name "demo-$RUN" --env-format)
API_KEY=$(echo "$OUT" | sed -n 's/^API_KEY=//p' | tr -d '\r')
TENANT_ID=$(echo "$OUT" | sed -n 's/^TENANT_ID=//p' | tr -d '\r')
echo "    tenant $TENANT_ID"

create_endpoint() { # $1 = sink path, $2 = description
  local resp id secret
  resp=$(curl -fsS -X POST "$API/v1/endpoints" -H "Authorization: Bearer $API_KEY" -H 'content-type: application/json' \
    -d "{\"url\":\"http://sink:8080$1\",\"description\":\"$2\",\"event_types\":[\"demo.order_placed\"]}")
  id=$(echo "$resp" | json_field id)
  secret=$(echo "$resp" | json_field secret)
  # Let the sink verify our signatures for this path.
  curl -fsS -X POST "$SINK_ADMIN/_admin/secrets" -H 'content-type: application/json' \
    -d "{\"path\":\"${1%%\?*}\",\"secret\":\"$secret\"}"
  echo "$id"
}

echo "==> creating endpoints on the sink"
HEALTHY=$(create_endpoint "/ok/demo-$RUN" "healthy")
FLAKY=$(create_endpoint "/flaky/demo-$RUN?pct=25" "flaky (25% 500s)")
FAILING=$(create_endpoint "/fail/demo-$RUN" "always failing")
echo "    healthy $HEALTHY"
echo "    flaky   $FLAKY"
echo "    failing $FAILING"

echo "==> sending $EVENTS events"
for i in $(seq 1 "$EVENTS"); do
  curl -fsS -o /dev/null -X POST "$API/v1/events" -H "Authorization: Bearer $API_KEY" \
    -H 'content-type: application/json' -H "Idempotency-Key: demo-$RUN-$i" \
    -d "{\"type\":\"demo.order_placed\",\"data\":{\"order\":$i,\"amount_cents\":$((i * 137 % 10000))}}" &
  if (( i % 20 == 0 )); then wait; fi
done
wait

echo "==> waiting for the healthy endpoint to receive everything"
for _ in $(seq 1 60); do
  DONE=$(psql -tA -c "SELECT count(*) FROM deliveries WHERE endpoint_id = '$HEALTHY' AND status = 'succeeded'")
  [[ "$DONE" -ge "$EVENTS" ]] && break
  sleep 1
done
echo "    letting the first retry round (5 s) happen..."
sleep 12

echo ""
echo "==> delivery summary"
psql -c "
  SELECT e.description AS endpoint,
         count(*) FILTER (WHERE d.status = 'succeeded')       AS succeeded,
         count(*) FILTER (WHERE d.status = 'retry_scheduled') AS retrying,
         count(*) FILTER (WHERE d.status IN ('pending','delivering')) AS in_flight,
         count(*) FILTER (WHERE d.status = 'failed')          AS failed,
         count(*) FILTER (WHERE d.status = 'dead')            AS dead,
         round(100.0 * count(*) FILTER (WHERE d.status = 'succeeded') / count(*), 1) AS pct_delivered,
         max(d.attempt_count)                                 AS max_attempts,
         to_char(min(d.next_attempt_at) - now(), 'MI:SS')     AS next_retry_in
    FROM deliveries d JOIN endpoints e ON e.id = d.endpoint_id
   WHERE d.tenant_id = '$TENANT_ID'
   GROUP BY e.description ORDER BY e.description DESC;"

echo "==> what the sink (the fake customer) saw"
curl -fsS "$SINK_ADMIN/_admin/stats?prefix=/ok/demo-$RUN"; echo "   <- healthy"
curl -fsS "$SINK_ADMIN/_admin/stats?prefix=/flaky/demo-$RUN"; echo "   <- flaky"
curl -fsS "$SINK_ADMIN/_admin/stats?prefix=/fail/demo-$RUN"; echo "   <- failing"

cat <<EOF

The failing endpoint keeps retrying on the real schedule (5s, 5m, 30m, 2h, 5h,
10h, 10h) and becomes 'dead' after the 8th attempt, ~28 hours from now. Its
circuit breaker opened after 5 consecutive failures, so its deliveries are
being parked rather than hammering it.

Inspect one delivery and its attempts:
  curl -s -H "Authorization: Bearer $API_KEY" "$API/v1/deliveries?endpoint_id=$FAILING&limit=1"

API key for this demo tenant: $API_KEY
EOF
make --no-print-directory urls
