import { ValidationError } from "./errors.js";
import { deepEqual, isPlainObject } from "./values.js";

/**
 * delta = desired minus reported.
 *
 * For every key of `desired`: when `reported` has no such key, or holds a
 * different scalar/array value, the desired value (the whole subtree) becomes
 * part of the delta. When both sides hold objects the delta recurses and the
 * key is omitted if the nested delta is empty. Keys that exist only in
 * `reported` never appear in the delta, and an up-to-date shadow has `{}`.
 */
export function computeDelta(desired, reported) {
  const delta = {};
  for (const key of Object.keys(desired)) {
    const want = desired[key];
    if (!Object.hasOwn(reported, key)) {
      delta[key] = want;
      continue;
    }
    const have = reported[key];
    if (isPlainObject(want) && isPlainObject(have)) {
      const nested = computeDelta(want, have);
      if (Object.keys(nested).length > 0) {
        delta[key] = nested;
      }
    } else if (!deepEqual(want, have)) {
      delta[key] = want;
    }
  }
  return delta;
}

/**
 * A shadow update body is exactly `{"state": {...}}` and every section must be
 * an object; `desired` and `reported` are both optional but at least one is
 * required.
 */
export function parseShadowPatch(raw) {
  if (!isPlainObject(raw)) {
    throw new ValidationError("body must be a JSON object");
  }
  const outer = Object.keys(raw);
  if (outer.length !== 1 || outer[0] !== "state") {
    throw new ValidationError("body must contain exactly a state object");
  }
  if (!isPlainObject(raw.state)) {
    throw new ValidationError("state must be an object");
  }
  const sections = Object.keys(raw.state);
  if (sections.length === 0) {
    throw new ValidationError("state must contain desired or reported");
  }
  for (const section of sections) {
    if (section !== "desired" && section !== "reported") {
      throw new ValidationError(`unknown field ${section} in state`);
    }
    if (!isPlainObject(raw.state[section])) {
      throw new ValidationError(`state.${section} must be an object`);
    }
  }
  return { desired: raw.state.desired ?? null, reported: raw.state.reported ?? null };
}

export function parseReportedPatch(raw) {
  const patch = parseShadowPatch(raw);
  if (patch.desired !== null) {
    throw new ValidationError("a reported update must not contain desired");
  }
  if (patch.reported === null) {
    throw new ValidationError("state must contain reported");
  }
  return patch;
}
