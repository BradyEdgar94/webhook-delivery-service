# Integration guide

How an application such as **Formsuite** sends events through Hooklane, and
how its customers verify the webhooks they receive. Working code is in
[`examples/typescript/`](../examples/typescript).

## 1. Get an API key

An operator creates a tenant once:

```bash
docker compose exec api hooklane admin create-tenant --name formsuite
# tenant id: 01a1...
# api key:   hl_...   (shown once: store it in your secret manager)
```

Every API call sends `Authorization: Bearer hl_...`.

## 2. Register your customers' endpoints

When a Formsuite customer adds a webhook URL in your settings UI, create a
Hooklane endpoint for it and **show them the returned `secret` once**:

```bash
curl -X POST http://localhost:8080/v1/endpoints \
  -H "Authorization: Bearer $HOOKLANE_API_KEY" -H 'content-type: application/json' \
  -d '{"url":"https://customer.example.com/hooks/formsuite",
       "description":"Acme Corp",
       "event_types":["form.submitted"],
       "rate_limit_per_sec":50}'
# -> 201 {"id":"...","secret":"whsec_...", ...}
```

- `event_types`: omit or `[]` for every (non-system) type.
- URLs must be `https://` and must not point at private networks.
- Store the endpoint `id` against your customer record.
- To also tell the customer (or yourself) when deliveries die or an endpoint
  is disabled, subscribe an endpoint to `hooklane.delivery.dead` and/or
  `hooklane.endpoint.disabled`. These are only sent to endpoints that list
  them explicitly.

Other endpoint operations: `GET/PATCH/DELETE /v1/endpoints/{id}`,
`GET /v1/endpoints/{id}/secret`, `POST /v1/endpoints/{id}/rotate-secret`
(old secret keeps working for 24 h), `POST /v1/endpoints/{id}/recover`.

## 3. Send events

```ts
// examples/typescript/send-event.ts (trimmed)
await fetch(`${HOOKLANE_URL}/v1/events`, {
  method: "POST",
  headers: {
    Authorization: `Bearer ${HOOKLANE_API_KEY}`,
    "Content-Type": "application/json",
    "Idempotency-Key": `form.submitted:${submission.id}`,
  },
  body: JSON.stringify({ type: "form.submitted", data: submission }),
});
// -> 202 {"id": "<event id>", "duplicate": false}
```

- **`202` means durable.** The event is committed in Postgres and will be
  delivered or end in a visible `failed`/`dead` state. You can stop worrying
  about it.
- **Idempotency-Key**: derive it from the business event (submission id), and
  send the *same* key when you retry. A repeat returns the original event id
  with `"duplicate": true` and creates nothing. Retry on network errors, `429`
  and `5xx` with backoff; do not retry other `4xx` (fix the request).
- Limits: `type` matches `^[a-zA-Z0-9_.-]{1,128}$` and may not start with
  `hooklane.`; `data` is any JSON up to 256 KB.
- Hooklane signs and fans the event out to every subscribed endpoint of your
  tenant. Each endpoint gets one delivery.

**Next.js note.** Call Hooklane from server code (a Route Handler or Server
Action), after your own database transaction has committed. If you need
Formsuite's own write and the Hooklane call to be atomic, use the same outbox
pattern Hooklane uses internally: write an outbox row in your transaction and
have a background job call Hooklane with the idempotency key.

## 4. Track deliveries

```bash
GET /v1/events/{id}                     # the event and its deliveries
GET /v1/deliveries?endpoint_id=&status= # the delivery log (cursor-paginated)
GET /v1/deliveries/{id}                 # every attempt: status, duration, first 1 KB of body, error
POST /v1/deliveries/{id}/resend         # fresh retry cycle
```

List responses are `{"data": [...], "next_cursor": "..."}`; pass
`?cursor=<next_cursor>` for the next page (newest first).

## 5. What your customers receive

```http
POST /hooks/formsuite HTTP/1.1
content-type: application/json
user-agent: Hooklane-Webhooks/0.1.0
webhook-id: 01a1fbed-2a73-70d3-8943-03a6c924392d       # event id, same on every retry
webhook-timestamp: 1795231264                           # unix seconds, at send time
webhook-signature: v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=
webhook-sequence: 4812                                  # increases per endpoint (not gapless)
webhook-attempt: 1

{"type":"form.submitted","timestamp":"2026-11-20T10:15:00.123Z","data":{...}}
```

Retries: 8 attempts over ~28 hours (immediately, then 5 s, 5 m, 30 m, 2 h,
5 h, 10 h, 10 h, each ±20%). A `429` with `Retry-After` is honoured. `410 Gone`
disables the endpoint. Redirects are not followed. Each attempt times out at 15 s.

## 6. Verifying signatures (receiver side)

Hooklane implements the [Standard Webhooks](https://www.standardwebhooks.com/)
spec, so receivers can use the official library in any language. In
TypeScript, with the `standardwebhooks` npm package:

```ts
// examples/typescript/verify.ts (trimmed)
import { Webhook } from "standardwebhooks";

const wh = new Webhook(process.env.WEBHOOK_SECRET!); // "whsec_..."

// rawBody: the exact bytes received. Do NOT JSON.parse and re-stringify first.
const event = wh.verify(rawBody, {
  "webhook-id": req.headers["webhook-id"],
  "webhook-timestamp": req.headers["webhook-timestamp"],
  "webhook-signature": req.headers["webhook-signature"],
}); // throws if the signature is wrong or the timestamp is > 5 minutes off

if (await alreadyProcessed(req.headers["webhook-id"])) return res.status(200).end();
await markProcessed(req.headers["webhook-id"]); // unique index on webhook_id
```

Checklist for receivers:

1. **Verify on the raw body.** Next.js App Router: `await request.text()`
   before any parsing.
2. **5-minute timestamp tolerance** (built into the library) defeats replay of
   captured requests. Keep server clocks in sync (NTP).
3. **Dedupe on `webhook-id`.** Delivery is at-least-once; a duplicate is rare
   but possible after a crash or a lost `200`.
4. **Return 2xx fast** and do heavy work asynchronously.
5. **Ordering is best effort**: compare the body's `timestamp` or
   `webhook-sequence` if out-of-order updates matter.
6. **Secret rotation**: during the 24 h overlap the header carries two
   signatures; the library accepts either secret, so switch whenever convenient.

`npm run selftest` in `examples/typescript` checks all of the above against
the official library without a running Hooklane.
