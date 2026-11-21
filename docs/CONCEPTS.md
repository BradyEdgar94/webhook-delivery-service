# Concepts

A plain-English glossary of every idea this system relies on, with where it
lives in the code. Written for an engineer who knows TypeScript well and is
new to Rust, Kafka and Kubernetes.

---

### Transactional outbox
Instead of "save to the database, then publish to the queue", you save the
data *and* a row saying "this needs publishing" in the same database
transaction. A separate process (the relay) reads those rows and publishes
them, retrying until it succeeds. The message is guaranteed to go out if and
only if the data was saved.
**Code:** `crates/hooklane/src/db/events.rs` (`ingest`, `fan_out`), `src/db/outbox.rs`, `src/relay.rs`.

### Dual-write problem
Writing the same fact to two systems (Postgres and Kafka) in two steps. If the
process dies between the steps, the systems disagree forever: an event that
exists but is never delivered, or one delivered but never saved. There is no
transaction that spans both systems; the outbox avoids needing one.
**Code:** explained at the top of `src/db/events.rs`.

### At-least-once delivery
The promise that every message is delivered one or more times: never zero,
occasionally twice. It is what you get whenever a sender retries after an
uncertain failure ("did it arrive before the crash?"). Exactly-once over a
network to a third party is impossible in general; at-least-once plus
receiver-side dedupe is the standard answer.
**Code:** the window is described in `src/worker/process.rs`; recovery in `src/db/deliveries.rs` (`reap_stuck`).

### Idempotency
An operation is idempotent if doing it twice has the same effect as doing it
once. Hooklane makes ingest idempotent with the `Idempotency-Key` header
(enforced by a unique index), and receivers make delivery idempotent by
deduping on the stable `webhook-id`.
**Code:** `ON CONFLICT ... DO NOTHING` in `src/db/events.rs`; unique index in `migrations/`.

### Competing consumers
Several workers read from the same queue and each message goes to only one of
them, so adding workers adds throughput. In Postgres mode this is done with
`FOR UPDATE SKIP LOCKED`; in Kafka mode with a consumer group.
**Code:** `src/db/deliveries.rs` (`claim_batch`), `src/queue/kafka.rs` (consumer group config).

### Partition
A Kafka topic is split into partitions: independent, ordered logs. A message's
*key* decides its partition (hash of the key), so all messages with the same
key (here, the endpoint id) land in the same partition, in order. The topic
has 64 partitions, fixed up front because adding partitions later re-maps keys.
**Code:** `src/relay.rs` (`.key(&key)`), `src/queue/kafka.rs` (`ensure_topic`).

### Consumer group
A named set of consumers that share a topic. Kafka assigns each partition to
exactly one member of the group, and rebalances when members join or leave.
That is why there can never be more useful workers than partitions (64).
**Code:** `KAFKA_CONSUMER_GROUP` in `src/config.rs`; rebalance handling in `src/worker/kafka_loop.rs`.

### Consumer lag
How many messages have been written to a partition but not yet committed by
the consumer group: the backlog. Growing lag means workers are falling behind.
KEDA scales workers on it, and it is on the dashboard.
**Code:** `stats()` in `src/worker/kafka_loop.rs`; KEDA trigger in `deploy/k8s/base/worker.yaml`.

### Offset commit
Kafka tracks a consumer group's progress as one number per partition: "every
message before offset N is handled". Committing too early loses messages on a
crash; committing late only causes harmless re-reads. Hooklane commits only the
highest offset below which *everything* is finished.
**Code:** `src/worker/offsets.rs`; commits in `src/worker/kafka_loop.rs`.

### Head-of-line blocking
When the item at the front of a queue is slow, everything behind it waits,
even items that would be quick. Here: one endpoint that times out delaying
every other endpoint in the same partition. Fixed with one lane per endpoint.
**Code:** `src/worker/lanes.rs`.

### Exponential backoff with jitter
Waiting longer after each failure (5 s, 5 m, 30 m, ...) so a struggling server
gets time to recover, plus a random ±20% so thousands of retries that failed at
the same moment do not all come back at the same moment (a "thundering herd").
**Code:** `src/delivery/retry.rs` (`next_delay`, `jitter`).

### Dead-letter queue
Where messages go after they have failed every allowed attempt, so they stop
consuming resources but are not silently dropped. Here it is a state, `dead`,
plus a `hooklane.delivery.dead` system event; `resend`/`recover` can revive them.
**Code:** `AttemptResult::Dead` in `src/db/deliveries.rs`.

### Circuit breaker
Stop calling something that keeps failing, and check occasionally whether it
has recovered. *Closed* = normal, *open* = not calling, *half-open* = letting
one probe through. Saves workers from spending 15 s timeouts on a server that
is known to be down.
**Code:** `src/delivery/breaker.rs`; used in `src/worker/endpoints.rs` and `src/worker/process.rs`.

### Token bucket
A rate limiter: a bucket refills at a fixed rate up to a maximum, and each
request takes a token. It allows short bursts but caps the long-run rate.
Here: 50 requests/second per endpoint by default.
**Code:** `src/delivery/ratelimit.rs`.

### Noisy neighbour
In a shared system, one customer's heavy or broken traffic degrading service
for everyone else. Lanes, timeouts, the breaker, the rate limit and
auto-disable are the layers that contain it.
**Code:** spec section 13; `src/worker/`.

### HMAC
Hash-based message authentication code: a hash of the message mixed with a
secret key. Only someone holding the key can produce the right HMAC, so the
receiver knows the message came from us and was not altered.
**Code:** `src/delivery/signing.rs`.

### Replay attack
An attacker captures a valid signed request and sends it again later. Because
the signature covers a timestamp, receivers reject requests older than a few
minutes (5 minutes in the Standard Webhooks libraries), and they dedupe on
`webhook-id`.
**Code:** timestamp signed in `src/delivery/signing.rs`; receiver side in `examples/typescript/verify.ts`.

### SSRF (server-side request forgery)
Tricking a server into making a request on the attacker's behalf, from inside
the server's network: for example to the cloud metadata service at
`169.254.169.254` to steal credentials. A webhook service is an SSRF machine by
design (customers choose the URL), so it must refuse internal destinations.
**Code:** `src/delivery/ssrf.rs`.

### DNS rebinding
A domain resolves to a harmless public IP when you check it, and to a private
IP when you actually connect. Defeated by checking the IP at connection time,
which is what Hooklane's custom resolver does.
**Code:** `SafeResolver` in `src/delivery/ssrf.rs`.

### Stateless service
A process that keeps nothing important in memory between requests: everything
durable is in a database. Any replica can serve any request, so you scale by
adding replicas and can kill one at any time. The API is stateless; workers
keep only disposable caches and protection state.
**Code:** `src/api/mod.rs`.

### Load balancer vs autoscaler
A load balancer spreads requests across the replicas that exist (a Kubernetes
Service does this). An autoscaler changes how many replicas exist, based on a
signal: CPU for the API (HPA), Kafka lag for workers (KEDA).
**Code:** `deploy/k8s/base/api.yaml` (Service + HPA), `deploy/k8s/base/worker.yaml` (ScaledObject).

### Image vs container vs deployment
An *image* is the packaged program (built by the `Dockerfile`). A *container*
is one running instance of an image. A Kubernetes *Deployment* says "keep N
containers of this image running with these settings" and replaces any that
die. Hooklane has one image and four Deployments (one per role).
**Code:** `Dockerfile`, `deploy/k8s/base/*.yaml`.

### SLO (service level objective)
A measurable target for how well the service should work, chosen so you can
tell when to act. Hooklane's: 99% of events to healthy endpoints delivered
within 60 seconds (first attempts). The alert `HooklaneSLOBurn` fires when it
is missed.
**Code:** `deploy/compose/alerts.yml`; computed by `crates/loadcheck` for every load test.
