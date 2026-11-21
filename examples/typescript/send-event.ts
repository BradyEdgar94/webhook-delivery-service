/**
 * Sending an event to Hooklane from an app (e.g. Formsuite's Next.js backend).
 *
 *   HOOKLANE_URL=http://localhost:8080 HOOKLANE_API_KEY=hl_... npm run send
 *
 * The important part is the Idempotency-Key: derive it from something stable
 * about the business event (here, the submission id), and reuse the SAME key
 * when you retry. If the first request actually succeeded but the response was
 * lost (timeout, network blip), the retry returns the original event instead
 * of creating a duplicate.
 */

const HOOKLANE_URL = process.env.HOOKLANE_URL ?? "http://localhost:8080";
const API_KEY = process.env.HOOKLANE_API_KEY ?? "";

type SendResult = { id: string; duplicate: boolean };

export async function sendEvent(
  type: string,
  data: unknown,
  idempotencyKey: string,
): Promise<SendResult> {
  const body = JSON.stringify({ type, data });
  let lastError: unknown;

  // Retry transient failures (network errors, 429, 5xx) with backoff.
  // 4xx other than 429 means the request itself is wrong: do not retry.
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      const res = await fetch(`${HOOKLANE_URL}/v1/events`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${API_KEY}`,
          "Content-Type": "application/json",
          "Idempotency-Key": idempotencyKey, // same key on every retry
        },
        body,
        signal: AbortSignal.timeout(10_000),
      });
      if (res.status === 202) {
        return (await res.json()) as SendResult;
      }
      const text = await res.text();
      if (res.status !== 429 && res.status < 500) {
        throw new Error(`Hooklane rejected the event (${res.status}): ${text}`);
      }
      lastError = new Error(`Hooklane ${res.status}: ${text}`);
    } catch (err) {
      if (err instanceof Error && err.message.startsWith("Hooklane rejected")) throw err;
      lastError = err;
    }
    await new Promise((r) => setTimeout(r, 200 * 2 ** attempt));
  }
  throw lastError;
}

// Example: a form submission.
if (import.meta.url === `file://${process.argv[1]}`) {
  const submissionId = `sub_${Date.now()}`;
  const result = await sendEvent(
    "form.submitted",
    {
      submission_id: submissionId,
      form_id: "contact-us",
      submitted_at: new Date().toISOString(),
      fields: { name: "Ada Lovelace", email: "ada@example.com", message: "Hello" },
    },
    `form.submitted:${submissionId}`,
  );
  console.log("accepted:", result);
}
