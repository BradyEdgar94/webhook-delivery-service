/**
 * A webhook receiver that verifies Hooklane's signatures with the official
 * `standardwebhooks` package, rejects stale/replayed requests, and dedupes on
 * `webhook-id`.
 *
 *   WEBHOOK_SECRET=whsec_... npm run receive      # listens on :3005
 *
 * Rules for receivers:
 *  1. Verify against the RAW body bytes, exactly as received. Parsing and
 *     re-serialising JSON changes whitespace/key order and breaks the HMAC.
 *  2. The library rejects timestamps more than 5 minutes from now: a captured
 *     request cannot be replayed later.
 *  3. Hooklane delivers at least once. The same event can arrive twice (after
 *     a crash, or a timeout where your 200 was lost). `webhook-id` is identical
 *     on every retry: store it and skip ids you have already processed.
 *  4. Answer 2xx quickly (well under 15 s) and do slow work in the background.
 *     Any non-2xx is retried (5 s, 5 m, 30 m, 2 h, 5 h, 10 h, 10 h), except
 *     410 Gone, which tells Hooklane to disable the endpoint.
 *  5. Ordering is best effort. Use the body's `timestamp` (event creation
 *     time) or the `webhook-sequence` header to ignore stale updates.
 */
import { createServer } from "node:http";
import { Webhook, WebhookVerificationError } from "standardwebhooks";

const secret = process.env.WEBHOOK_SECRET;
if (!secret) throw new Error("set WEBHOOK_SECRET (from POST /v1/endpoints or GET /v1/endpoints/{id}/secret)");

// During a secret rotation Hooklane sends two signatures for 24 h; the
// library accepts the request if any of them matches this secret.
const wh = new Webhook(secret);

// In production this is a database table with a unique index on webhook_id.
const processed = new Set<string>();

type HooklaneEvent = { type: string; timestamp: string; data: unknown };

const server = createServer((req, res) => {
  const chunks: Buffer[] = [];
  req.on("data", (c: Buffer) => chunks.push(c));
  req.on("end", () => {
    const rawBody = Buffer.concat(chunks).toString("utf8");
    const headers = {
      "webhook-id": String(req.headers["webhook-id"] ?? ""),
      "webhook-timestamp": String(req.headers["webhook-timestamp"] ?? ""),
      "webhook-signature": String(req.headers["webhook-signature"] ?? ""),
    };

    let event: HooklaneEvent;
    try {
      event = wh.verify(rawBody, headers) as HooklaneEvent;
    } catch (err) {
      const reason = err instanceof WebhookVerificationError ? err.message : "invalid";
      console.warn("rejected webhook:", reason);
      res.writeHead(401).end();
      return;
    }

    const id = headers["webhook-id"];
    if (processed.has(id)) {
      res.writeHead(200).end(); // duplicate: already handled, acknowledge again
      return;
    }
    processed.add(id);

    console.log(
      `event ${id} type=${event.type} created=${event.timestamp}`,
      `attempt=${req.headers["webhook-attempt"]} sequence=${req.headers["webhook-sequence"]}`,
    );
    res.writeHead(200).end();
    // ...enqueue the real work here.
  });
});

server.listen(3005, () => console.log("listening on http://localhost:3005"));
