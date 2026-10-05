import { ValidationError } from "./errors.js";
import { identifier } from "./rules.js";
import { isPlainObject } from "./values.js";

const COMMAND_KEYS = new Set(["id", "payload", "expires_at"]);
const RFC3339_UTC = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;

/**
 * Parse an expires_at timestamp: a strict RFC3339 UTC timestamp ending in Z.
 * Sub-millisecond digits are truncated so every stored timestamp has
 * millisecond precision, and a canonical round trip rejects impossible
 * calendar values. Returns the canonical form and its epoch milliseconds.
 */
function parseExpiresAt(value) {
  const match = typeof value === "string" ? RFC3339_UTC.exec(value) : null;
  if (match === null) {
    throw new ValidationError("expires_at must be an RFC3339 UTC timestamp ending in Z");
  }
  const [, year, month, day, hour, minute, second, fractionRaw] = match;
  const fraction = (fractionRaw ?? "").padEnd(3, "0").slice(0, 3);
  const canonical = `${year}-${month}-${day}T${hour}:${minute}:${second}.${fraction}Z`;
  const parsed = new Date(canonical);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== canonical) {
    throw new ValidationError("expires_at must be an RFC3339 UTC timestamp ending in Z");
  }
  return { canonical, millis: parsed.getTime() };
}

/**
 * A command body is exactly `{id, payload, expires_at}`: every field is
 * required and no other field is allowed. `id` follows the usual identifier
 * rule, `payload` is any JSON value (including null), and `expires_at` must
 * be later than the receipt time `receivedMs` in epoch milliseconds.
 */
export function parseCommand(raw, receivedMs) {
  if (!isPlainObject(raw)) {
    throw new ValidationError("command must be a JSON object");
  }
  for (const key of Object.keys(raw)) {
    if (!COMMAND_KEYS.has(key)) {
      throw new ValidationError(`unknown field ${key} in command`);
    }
  }
  for (const key of COMMAND_KEYS) {
    if (!Object.hasOwn(raw, key)) {
      throw new ValidationError(`command field ${key} is required`);
    }
  }
  const expiresAt = parseExpiresAt(raw.expires_at);
  if (expiresAt.millis <= receivedMs) {
    throw new ValidationError("expires_at must be later than the time the command is received");
  }
  return { id: identifier(raw.id, "command id"), payload: structuredClone(raw.payload), expiresAt: expiresAt.canonical };
}
