import { ValidationError } from "./errors.js";
import { identifier } from "./rules.js";
import { isPlainObject } from "./values.js";

const COMMAND_KEYS = new Set(["id", "payload", "expires_at"]);
const RFC3339_UTC = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;

/**
 * Parse a strict RFC3339 UTC timestamp ending in Z, the only shape expires_at
 * accepts. V8's parser rolls impossible calendar values over, so a canonical
 * round trip is required, and sub-millisecond digits are only tolerated when
 * they are zeros (commands are stamped at millisecond precision). Returns the
 * canonical millisecond form and its epoch milliseconds.
 */
function parseExpiresAt(value) {
  const invalid = () => new ValidationError("expires_at must be an RFC3339 UTC timestamp ending in Z");
  if (typeof value !== "string") {
    throw invalid();
  }
  const match = RFC3339_UTC.exec(value);
  if (match === null) {
    throw invalid();
  }
  const [, year, month, day, hour, minute, second, fractionRaw] = match;
  const fraction = fractionRaw ?? "";
  if (fraction.length > 3 && fraction.slice(3) !== "0".repeat(fraction.length - 3)) {
    throw invalid();
  }
  const canonical = `${year}-${month}-${day}T${hour}:${minute}:${second}.${fraction.padEnd(3, "0").slice(0, 3)}Z`;
  const parsed = new Date(canonical);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== canonical) {
    throw invalid();
  }
  return { canonical, millis: parsed.getTime() };
}

/**
 * A command body is exactly `{"id": ..., "payload": ..., "expires_at": ...}`:
 * `id` follows the usual identifier rule and is unique within the device,
 * `payload` is any JSON value (including null), and `expires_at` is a strict
 * RFC3339 UTC timestamp that must still be in the future at reception time
 * (`nowMillis`). Unknown or missing fields are rejected.
 */
export function parseCommand(raw, nowMillis) {
  if (!isPlainObject(raw)) {
    throw new ValidationError("command must be a JSON object");
  }
  for (const key of Object.keys(raw)) {
    if (!COMMAND_KEYS.has(key)) {
      throw new ValidationError(`unknown field ${key} in command`);
    }
  }
  const command = { id: identifier(raw.id, "command id") };
  if (!Object.hasOwn(raw, "payload")) {
    throw new ValidationError("command must declare a payload");
  }
  command.payload = structuredClone(raw.payload);
  const expires = parseExpiresAt(raw.expires_at);
  if (expires.millis <= nowMillis) {
    throw new ValidationError("expires_at must be later than the time the command is received");
  }
  command.expires_at = expires.canonical;
  return command;
}
