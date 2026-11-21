// k6 load generator for Hooklane.
//
// setup(): creates N endpoints on the sink, each subscribed to its own event
// type (so each event goes to exactly one endpoint), and registers each
// endpoint's secret with the sink so it verifies signatures.
// default(): posts one event per iteration, round-robin over the endpoints,
// with an arrival-rate executor (requests per second, independent of latency).
//
// Environment (all set by loadtest/run.sh):
//   API_URL, SINK_URL, API_KEY, RUN_ID
//   ENDPOINTS        number of endpoints
//   EVENTS           target total events
//   RATE             target events/second (steady), or the base rate (spike)
//   PROFILE          steady | spike
//   FAILING_PCT      % of endpoints that misbehave (5xx / timeout / 429), default 0
//   SPIKE_SECS       length of the 10x spike (spike profile), default 40

import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import exec from 'k6/execution';

const API = __ENV.API_URL || 'http://api:8080';
const SINK = __ENV.SINK_URL || 'http://sink:8080';
const KEY = __ENV.API_KEY;
const RUN = __ENV.RUN_ID || `${Date.now()}`;
const N = parseInt(__ENV.ENDPOINTS || '100', 10);
const EVENTS = parseInt(__ENV.EVENTS || '10000', 10);
const RATE = parseInt(__ENV.RATE || '1000', 10);
const PROFILE = __ENV.PROFILE || 'steady';
const FAILING_PCT = parseInt(__ENV.FAILING_PCT || '0', 10);
const SPIKE_SECS = parseInt(__ENV.SPIKE_SECS || '40', 10);

const accepted = new Counter('events_accepted');
const rejected = new Counter('events_rejected');

function stages() {
  if (PROFILE === 'spike') {
    // base rate, then 10x the base rate for SPIKE_SECS, then base again.
    // The base periods are sized so the whole run is about EVENTS events.
    const spikeEvents = RATE * 10 * SPIKE_SECS;
    const baseSecs = Math.max(30, Math.round((EVENTS - spikeEvents) / RATE / 2));
    return [
      { target: RATE, duration: '5s' },
      { target: RATE, duration: `${baseSecs}s` },
      { target: RATE * 10, duration: '3s' },
      { target: RATE * 10, duration: `${SPIKE_SECS}s` },
      { target: RATE, duration: '3s' },
      { target: RATE, duration: `${baseSecs}s` },
    ];
  }
  // steady: ramp up over ~15% of the run, then hold. Total ~= EVENTS.
  const holdSecs = Math.max(10, Math.round(EVENTS / RATE));
  const rampSecs = Math.max(5, Math.round(holdSecs * 0.15));
  return [
    { target: RATE, duration: `${rampSecs}s` },
    { target: RATE, duration: `${Math.max(5, holdSecs - Math.round(rampSecs / 2))}s` },
  ];
}

export const options = {
  setupTimeout: '10m',
  summaryTrendStats: ['avg', 'p(50)', 'p(95)', 'p(99)', 'max'],
  scenarios: {
    ingest: {
      executor: 'ramping-arrival-rate',
      startRate: Math.max(1, Math.round(RATE / 10)),
      timeUnit: '1s',
      preAllocatedVUs: 200,
      maxVUs: 3000,
      stages: stages(),
    },
  },
};

const headers = { 'Content-Type': 'application/json', Authorization: `Bearer ${KEY}` };

// Which sink behaviour endpoint i gets. Failing endpoints are spread evenly.
function behaviour(i) {
  const bucket = (i * 7919) % 100; // pseudo-random but deterministic spread
  if (bucket < FAILING_PCT) {
    const kind = i % 3;
    if (kind === 0) return 'fail';
    if (kind === 1) return 'timeout';
    return 'ratelimit';
  }
  return 'ok';
}

export function setup() {
  const types = [];
  for (let i = 0; i < N; i++) {
    const kind = behaviour(i);
    const path = `/${kind}/ep-${RUN}-${i}`;
    const query = kind === 'ratelimit' ? '?retry_after=5' : kind === 'timeout' ? '?ms=20000' : '';
    const type = `load.r${RUN}.e${i}`;
    const res = http.post(
      `${API}/v1/endpoints`,
      JSON.stringify({ url: `${SINK}${path}${query}`, event_types: [type], description: `load ${kind}` }),
      { headers },
    );
    if (res.status !== 201) {
      throw new Error(`endpoint create failed: ${res.status} ${res.body}`);
    }
    const secret = res.json('secret');
    http.post(`${SINK}/_admin/secrets`, JSON.stringify({ path, secret }), {
      headers: { 'Content-Type': 'application/json' },
    });
    types.push(type);
  }
  return { types };
}

export default function (data) {
  const n = exec.scenario.iterationInTest;
  const type = data.types[n % data.types.length];
  const body = JSON.stringify({
    type,
    data: { n, run: RUN, order: { id: `ord_${n}`, amount_cents: (n * 137) % 100000, currency: 'NZD' } },
  });
  const res = http.post(`${API}/v1/events`, body, {
    headers: { ...headers, 'Idempotency-Key': `${RUN}-${n}` },
    timeout: '30s',
    tags: { name: 'POST /v1/events' },
  });
  if (res.status === 202) {
    accepted.add(1);
  } else {
    rejected.add(1);
  }
  check(res, { '202': (r) => r.status === 202 });
}

export function handleSummary(data) {
  const acc = data.metrics.events_accepted ? data.metrics.events_accepted.values.count : 0;
  const rej = data.metrics.events_rejected ? data.metrics.events_rejected.values.count : 0;
  const dur = data.metrics.http_req_duration ? data.metrics.http_req_duration.values : {};
  const fmt = (v) => (v === undefined ? 'n/a' : `${v.toFixed(1)}ms`);
  const text =
    `ACCEPTED=${acc}\nREJECTED=${rej}\n` +
    `INGEST_P50=${fmt(dur['p(50)'])}\nINGEST_P95=${fmt(dur['p(95)'])}\nINGEST_P99=${fmt(dur['p(99)'])}\n`;
  return {
    '/out/summary.env': text,
    '/out/summary.json': JSON.stringify(data, null, 2),
    stdout: `\n${text}`,
  };
}
