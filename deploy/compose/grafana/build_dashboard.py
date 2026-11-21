#!/usr/bin/env python3
"""Generates dashboards/hooklane.json. Edit the panel list here, then run:
    python3 deploy/compose/grafana/build_dashboard.py
"""
import json, os

DS = {"type": "prometheus", "uid": "prometheus"}
panels, y = [], 0
pid = 0

def nid():
    global pid
    pid += 1
    return pid

def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}})
    y += 1

def stat(title, expr, x, w=4, unit="short", desc="", decimals=None, thresholds=None):
    p = {"type": "stat", "title": title, "id": nid(), "datasource": DS, "description": desc,
         "gridPos": {"h": 4, "w": w, "x": x, "y": y},
         "targets": [{"refId": "A", "expr": expr, "datasource": DS}],
         "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"},
         "fieldConfig": {"defaults": {"unit": unit}, "overrides": []}}
    if decimals is not None:
        p["fieldConfig"]["defaults"]["decimals"] = decimals
    if thresholds:
        p["fieldConfig"]["defaults"]["thresholds"] = {"mode": "absolute", "steps": thresholds}
    panels.append(p)

def ts(title, targets, x, w=12, h=8, unit="short", desc="", stack=False):
    panels.append({"type": "timeseries", "title": title, "id": nid(), "datasource": DS, "description": desc,
                   "gridPos": {"h": h, "w": w, "x": x, "y": y},
                   "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": DS}
                               for i, (e, l) in enumerate(targets)],
                   "fieldConfig": {"defaults": {"unit": unit, "custom": {
                       "fillOpacity": 15, "lineWidth": 2, "showPoints": "never",
                       "stacking": {"mode": "normal" if stack else "none"}}}, "overrides": []},
                   "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                               "tooltip": {"mode": "multi"}}})

R = "$__rate_interval"
red_green = [{"color": "red", "value": None}, {"color": "orange", "value": 0.95}, {"color": "green", "value": 0.99}]

row("Health")
stat("Ingest rate", f"sum(rate(hooklane_events_ingested_total[{R}]))", 0, unit="reqps",
     desc="Events accepted by the API per second")
stat("Deliveries succeeded / s", f'sum(rate(hooklane_deliveries_total{{outcome="succeeded"}}[{R}]))', 4, unit="reqps")
stat("First-attempt success", f'sum(rate(hooklane_attempts_total{{first_attempt="true",result="success"}}[5m])) / sum(rate(hooklane_attempts_total{{first_attempt="true"}}[5m]))',
     8, unit="percentunit", decimals=1, thresholds=red_green)
stat("SLO: first attempt < 60 s", f'sum(rate(hooklane_delivery_latency_seconds_bucket{{first_attempt="true",le="60.0"}}[5m])) / sum(rate(hooklane_delivery_latency_seconds_count{{first_attempt="true"}}[5m]))',
     12, unit="percentunit", decimals=2, thresholds=red_green, desc="Target: 99% of first-attempt deliveries within 60 s")
stat("Open circuit breakers", "sum(hooklane_breakers_open)", 16, thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 1}])
stat("Dead deliveries", "max(hooklane_deliveries_dead)", 20, thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 1}])
y += 4

row("Latency and outcomes")
ts("End-to-end delivery latency (event created -> 2xx)", [
    (f'histogram_quantile(0.50, sum by (le) (rate(hooklane_delivery_latency_seconds_bucket{{first_attempt="true"}}[{R}])))', "p50 first attempt"),
    (f'histogram_quantile(0.95, sum by (le) (rate(hooklane_delivery_latency_seconds_bucket{{first_attempt="true"}}[{R}])))', "p95 first attempt"),
    (f'histogram_quantile(0.99, sum by (le) (rate(hooklane_delivery_latency_seconds_bucket{{first_attempt="true"}}[{R}])))', "p99 first attempt"),
    (f'histogram_quantile(0.50, sum by (le) (rate(hooklane_delivery_latency_seconds_bucket{{first_attempt="false"}}[{R}])))', "p50 retried"),
], 0, unit="s")
ts("Deliveries by outcome", [(f"sum by (outcome) (rate(hooklane_deliveries_total[{R}]))", "{{outcome}}")], 12, unit="reqps", stack=True)
y += 8

row("Pipeline")
ts("Kafka consumer lag", [("sum(hooklane_kafka_consumer_lag)", "total"),
                          ("sum by (service_instance_id) (hooklane_kafka_consumer_lag)", "{{service_instance_id}}")], 0, w=8,
   desc="Messages waiting in Kafka. KEDA scales workers on this.")
ts("Oldest unpublished outbox row", [("max(hooklane_outbox_oldest_unpublished_age_seconds)", "age")], 8, w=8, unit="s",
   desc="Relay health: should stay near zero")
ts("Retry backlog", [("max(hooklane_retry_backlog)", "retry_scheduled"), ("max(hooklane_deliveries_pending)", "pending"),
                     ("max(hooklane_retry_oldest_overdue_seconds)", "oldest overdue (s)")], 16, w=8)
y += 8

row("Traffic")
ts("API request latency p95 by route", [(f"histogram_quantile(0.95, sum by (le, route) (rate(hooklane_api_request_duration_seconds_bucket[{R}])))", "{{route}}")], 0, w=8, unit="s")
ts("Outbound attempt duration p95 by status", [(f"histogram_quantile(0.95, sum by (le, status_class) (rate(hooklane_attempt_duration_seconds_bucket[{R}])))", "{{status_class}}")], 8, w=8, unit="s")
ts("Outbound attempts by status class", [(f"sum by (status_class) (rate(hooklane_attempt_duration_seconds_count[{R}]))", "{{status_class}}")], 16, w=8, unit="reqps", stack=True)
y += 8

dash = {"title": "Hooklane", "uid": "hooklane", "schemaVersion": 39, "version": 1, "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"}, "tags": ["hooklane"], "panels": panels,
        "annotations": {"list": []}, "templating": {"list": []}}
out = os.path.join(os.path.dirname(__file__), "dashboards", "hooklane.json")
with open(out, "w") as f:
    json.dump(dash, f, indent=2)
print(f"wrote {out} ({len(panels)} panels)")
