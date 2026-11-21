/**
 * Self-check without a running Hooklane: sign a request exactly as Hooklane
 * does (Standard Webhooks: HMAC-SHA256 over "id.timestamp.body", key = the
 * base64-decoded secret) and verify it with the official library, including
 * the rotation case (two signatures) and the failure cases.
 */
import { createHmac, randomBytes } from "node:crypto";
import { Webhook } from "standardwebhooks";

const sign = (secret: Buffer, id: string, ts: number, body: string) =>
  "v1," + createHmac("sha256", secret).update(`${id}.${ts}.${body}`).digest("base64");

const oldKey = randomBytes(32);
const newKey = randomBytes(32);
const whsec = (k: Buffer) => "whsec_" + k.toString("base64");

const id = "01a1fbed-2a73-70d3-8943-03a6c924392d";
const ts = Math.floor(Date.now() / 1000);
const body = JSON.stringify({ type: "form.submitted", timestamp: new Date().toISOString(), data: { a: 1 } });
const headers = (sig: string, t = ts) => ({
  "webhook-id": id,
  "webhook-timestamp": String(t),
  "webhook-signature": sig,
});

const rotated = `${sign(newKey, id, ts, body)} ${sign(oldKey, id, ts, body)}`;
const checks: [string, () => unknown, boolean][] = [
  ["valid signature verifies", () => new Webhook(whsec(newKey)).verify(body, headers(sign(newKey, id, ts, body))), true],
  ["rotation: old secret verifies", () => new Webhook(whsec(oldKey)).verify(body, headers(rotated)), true],
  ["rotation: new secret verifies", () => new Webhook(whsec(newKey)).verify(body, headers(rotated)), true],
  ["tampered body rejected", () => new Webhook(whsec(newKey)).verify(body + " ", headers(sign(newKey, id, ts, body))), false],
  ["wrong secret rejected", () => new Webhook(whsec(oldKey)).verify(body, headers(sign(newKey, id, ts, body))), false],
  ["10-minute-old timestamp rejected", () => new Webhook(whsec(newKey)).verify(body, headers(sign(newKey, id, ts - 600, body), ts - 600)), false],
];

let failed = 0;
for (const [name, fn, shouldPass] of checks) {
  let passed: boolean;
  try {
    fn();
    passed = true;
  } catch {
    passed = false;
  }
  const ok = passed === shouldPass;
  if (!ok) failed++;
  console.log(`${ok ? "ok  " : "FAIL"} ${name}`);
}
process.exit(failed ? 1 : 0);
