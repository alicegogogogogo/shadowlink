import { ValidationError } from "./errors.js";
import { validateTopicFilter } from "./mqtt.js";
import { deepEqual, isPlainObject, readPath, validatePath } from "./values.js";

export const OPERATORS = Object.freeze(["eq", "ne", "gt", "gte", "lt", "lte", "exists", "not_exists", "contains"]);
const UNARY_OPERATORS = new Set(["exists", "not_exists"]);
const RULE_KEYS = new Set(["id", "source", "device_id", "topic", "path", "operator", "value", "event"]);
const IDENTIFIER = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,99}$/;
const EVENT_NAME = /^[a-z][a-z0-9_]{0,63}$/;

export function identifier(value, label) {
  if (typeof value !== "string" || !IDENTIFIER.test(value)) {
    throw new ValidationError(`${label} must be 1 to 100 characters matching [A-Za-z0-9][A-Za-z0-9._:-]*`);
  }
  return value;
}

/** Non-throwing variant of {@link identifier}, for paths that ignore bad input. */
export function isIdentifier(value) {
  return typeof value === "string" && IDENTIFIER.test(value);
}

/**
 * A rule is either shadow-scoped (it watches one device's shadow document) or
 * mqtt-scoped (its topic filter and the decoded payload form the document).
 */
export function parseRule(raw) {
  if (!isPlainObject(raw)) {
    throw new ValidationError("rule must be a JSON object");
  }
  for (const key of Object.keys(raw)) {
    if (!RULE_KEYS.has(key)) {
      throw new ValidationError(`unknown field ${key} in rule`);
    }
  }
  const rule = { id: identifier(raw.id, "rule id"), source: raw.source };
  if (raw.source === "shadow") {
    if (raw.topic !== undefined) {
      throw new ValidationError("a shadow rule must not declare a topic");
    }
    rule.device_id = identifier(raw.device_id, "device id");
  } else if (raw.source === "mqtt") {
    if (raw.device_id !== undefined) {
      throw new ValidationError("an mqtt rule must not declare a device id");
    }
    rule.topic = validateTopicFilter(raw.topic);
  } else {
    throw new ValidationError("source must be shadow or mqtt");
  }
  rule.path = validatePath(raw.path);
  if (!OPERATORS.includes(raw.operator)) {
    throw new ValidationError(`operator must be one of ${OPERATORS.join(", ")}`);
  }
  rule.operator = raw.operator;
  if (UNARY_OPERATORS.has(rule.operator)) {
    if (raw.value !== undefined) {
      throw new ValidationError(`operator ${rule.operator} must not declare a value`);
    }
  } else {
    if (raw.value === undefined) {
      throw new ValidationError(`operator ${rule.operator} requires a value`);
    }
    rule.value = structuredClone(raw.value);
  }
  if (typeof raw.event !== "string" || !EVENT_NAME.test(raw.event)) {
    throw new ValidationError("event must match [a-z][a-z0-9_]* and be at most 64 characters");
  }
  rule.event = raw.event;
  return rule;
}

function ordered(actual, expected) {
  const bothNumbers = typeof actual === "number" && Number.isFinite(actual) && typeof expected === "number" && Number.isFinite(expected);
  const bothStrings = typeof actual === "string" && typeof expected === "string";
  return bothNumbers || bothStrings;
}

function contains(actual, expected) {
  if (Array.isArray(actual)) {
    return actual.some((item) => deepEqual(item, expected));
  }
  if (typeof actual === "string" && typeof expected === "string") {
    return actual.includes(expected);
  }
  return false;
}

export function evaluateCondition(rule, document) {
  const actual = readPath(document, rule.path);
  switch (rule.operator) {
    case "exists":
      return actual !== undefined;
    case "not_exists":
      return actual === undefined;
    case "eq":
      return actual !== undefined && deepEqual(actual, rule.value);
    case "ne":
      return actual !== undefined && !deepEqual(actual, rule.value);
    case "gt":
      return ordered(actual, rule.value) && actual > rule.value;
    case "gte":
      return ordered(actual, rule.value) && actual >= rule.value;
    case "lt":
      return ordered(actual, rule.value) && actual < rule.value;
    case "lte":
      return ordered(actual, rule.value) && actual <= rule.value;
    case "contains":
      return contains(actual, rule.value);
    default:
      throw new ValidationError(`unsupported operator ${rule.operator}`);
  }
}

/** The value recorded with an emitted event; `undefined` is stored as null. */
export function matchedValue(rule, document) {
  const actual = readPath(document, rule.path);
  return actual === undefined ? null : actual;
}
