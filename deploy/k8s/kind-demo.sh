#!/usr/bin/env bash
# make k8s-demo: send a burst of events from inside the cluster (k6 Job) and
# watch KEDA scale the workers up as consumer lag grows and back down as it
# drains. Writes a timeline to docs/results/<date>-k8s-autoscaling.md.
set -euo pipefail
cd "$(dirname "$0")/../.."
NS=hooklane
EVENTS=${EVENTS:-100000}
RATE=${RATE:-3000}
ENDPOINTS=${ENDPOINTS:-64}
OUT=docs/results/$(date +%Y-%m-%d)-k8s-autoscaling.md
kubectl config use-context kind-hooklane >/dev/null

CREDS=$(kubectl -n $NS exec deploy/api -- hooklane admin create-tenant --name "k8s-demo-$(date +%s)" --env-format)
KEY=$(echo "$CREDS" | sed -n 's/^API_KEY=//p' | tr -d '\r')
TENANT=$(echo "$CREDS" | sed -n 's/^TENANT_ID=//p' | tr -d '\r')

kubectl -n $NS delete job k6-burst --ignore-not-found >/dev/null
kubectl -n $NS create configmap k6-script --from-file=load.js=loadtest/load.js --dry-run=client -o yaml | kubectl apply -f - >/dev/null
cat <<YAML | kubectl -n $NS apply -f - >/dev/null
apiVersion: batch/v1
kind: Job
metadata: {name: k6-burst}
spec:
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: k6
          image: grafana/k6:1.8.3
          imagePullPolicy: IfNotPresent
          args: ["run", "--quiet", "/scripts/load.js"]
          env:
            - {name: API_URL, value: "http://api:8080"}
            - {name: SINK_URL, value: "http://sink:8080"}
            - {name: API_KEY, value: "$KEY"}
            - {name: RUN_ID, value: "$(date +%s)"}
            - {name: ENDPOINTS, value: "$ENDPOINTS"}
            - {name: EVENTS, value: "$EVENTS"}
            - {name: RATE, value: "$RATE"}
          volumeMounts:
            - {name: scripts, mountPath: /scripts}
            - {name: out, mountPath: /out}
      volumes:
        - name: scripts
          configMap: {name: k6-script}
        - name: out
          emptyDir: {}
YAML

lag() {
  kubectl -n $NS exec deploy/redpanda -- rpk group describe hooklane-workers 2>/dev/null \
    | awk '/^TOTAL-LAG/ {print $2; f=1} END {if (!f) print 0}'
}

echo "==> burst of ~$EVENTS events at $RATE/s; watching workers (Ctrl-C to stop watching)"
{
  echo "# Kubernetes autoscaling demo (kind)"
  echo ""
  echo "> Measured on a local kind cluster inside a Colima VM ($(docker info --format '{{.NCPU}} CPUs') ), not production capacity."
  echo ""
  echo "Burst: ~$EVENTS events at $RATE/s from an in-cluster k6 Job, $ENDPOINTS endpoints. Workers: KEDA Kafka trigger, lagThreshold 500, min 1, max 16, CPU limit 300m each."
  echo ""
  echo "| t (s) | k6 | worker replicas (ready/desired) | consumer lag |"
  echo "|---:|---|---:|---:|"
} > "$OUT"

START=$(date +%s); PEAK=1; PEAK_LAG=0; DRAINED_AT=""
while true; do
  T=$(( $(date +%s) - START ))
  K6=$(kubectl -n $NS get job k6-burst -o jsonpath='{.status.succeeded}{.status.failed}' 2>/dev/null)
  [[ -z "$K6" ]] && K6=running || K6=done
  READY=$(kubectl -n $NS get deploy worker -o jsonpath='{.status.readyReplicas}'); READY=${READY:-0}
  DESIRED=$(kubectl -n $NS get deploy worker -o jsonpath='{.spec.replicas}')
  L=$(lag); [[ "$L" =~ ^[0-9]+$ ]] || L=0
  (( DESIRED > PEAK )) && PEAK=$DESIRED
  (( L > PEAK_LAG )) && PEAK_LAG=$L
  printf "  t=%4ss  k6=%-7s  workers=%s/%s  lag=%s\n" "$T" "$K6" "$READY" "$DESIRED" "$L"
  echo "| $T | $K6 | $READY/$DESIRED | $L |" >> "$OUT"
  if [[ "$K6" == done && "$L" == 0 && -z "$DRAINED_AT" ]]; then DRAINED_AT=$T; fi
  if [[ -n "$DRAINED_AT" && "$DESIRED" -le 1 ]]; then break; fi
  (( T > 1200 )) && break
  sleep 5
done

ACCEPTED=$(kubectl -n $NS logs job/k6-burst 2>/dev/null | sed -n 's/^ACCEPTED=//p' | tail -1)
DELIVERED=$(kubectl -n $NS exec deploy/postgres -- psql -U hooklane -d hooklane -tA -c \
  "SELECT count(*) FILTER (WHERE status='succeeded') || ' succeeded of ' || count(*) FROM deliveries WHERE tenant_id = '$TENANT'")
{
  echo ""
  echo "**Summary:** peak desired workers $PEAK, peak consumer lag $PEAK_LAG, lag drained at t=${DRAINED_AT:-n/a}s,"
  echo "back to 1 worker at t=$(( $(date +%s) - START ))s. k6 accepted ${ACCEPTED:-?} events; deliveries: $DELIVERED."
} >> "$OUT"
tail -3 "$OUT"
echo "timeline saved to $OUT"
