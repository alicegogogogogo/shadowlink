import { ConflictError, NotFoundError, ValidationError } from "./errors.js";
import { topicMatches } from "./mqtt.js";
import { evaluateCondition, identifier, matchedValue, parseRule } from "./rules.js";
import { computeDelta, parseReportedPatch, parseShadowPatch } from "./shadow.js";
import { Store } from "./store.js";
import { mergeValues } from "./values.js";

const SHADOW_PREFIX = "$shadow/";
const RFC3339_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;
const UNSIGNED_INTEGER = /^\d+$/;

/**
 * Timestamps are normalized to the canonical `Date#toISOString` form, which is
 * how events are persisted, so the store can compare them lexicographically.
 */
function parseTimestamp(value, label) {
  if (typeof value !== "string" || !RFC3339_UTC.test(value)) {
    throw new ValidationError(`${label} must be an RFC3339 UTC timestamp ending in Z`);
  }
  const time = Date.parse(value);
  if (Number.isNaN(time)) {
    throw new ValidationError(`${label} must be a valid RFC3339 UTC timestamp`);
  }
  return new Date(time).toISOString();
}

function parseInteger(value, label) {
  if (typeof value !== "string" || !UNSIGNED_INTEGER.test(value)) {
    throw new ValidationError(`${label} must be a safe non-negative integer`);
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number)) {
    throw new ValidationError(`${label} must be a safe non-negative integer`);
  }
  return number;
}

function byId(left, right) {
  return left.id < right.id ? -1 : left.id > right.id ? 1 : 0;
}

function decodePayload(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export class Service {
  constructor(database, options = {}) {
    this.store = new Store(database);
    this.now = options.now ?? (() => new Date().toISOString());
    this.publisher = null;
  }

  close() {
    this.store.close();
  }

  /** The broker registers itself here so shadow writes can publish deltas. */
  setPublisher(publisher) {
    this.publisher = publisher;
  }

  health() {
    return { status: "ok" };
  }

  getShadow(rawDeviceId) {
    const deviceId = identifier(rawDeviceId, "device id");
    const document = this.store.getShadow(deviceId);
    if (document === null) {
      throw new NotFoundError(`device ${deviceId} has no shadow`);
    }
    return document;
  }

  updateShadow(rawDeviceId, raw, key) {
    const deviceId = identifier(rawDeviceId, "device id");
    const patch = parseShadowPatch(raw);
    return this.#idempotent(key, `update-shadow:${deviceId}`, () => this.applyShadowPatch(deviceId, patch));
  }

  reportState(rawDeviceId, raw, key) {
    const deviceId = identifier(rawDeviceId, "device id");
    const patch = parseReportedPatch(raw);
    return this.#idempotent(key, `report-state:${deviceId}`, () => this.applyShadowPatch(deviceId, patch));
  }

  /**
   * Merge a patch into the shadow of `deviceId`, bump the version, evaluate
   * shadow rules and publish the delta. Used by HTTP writes and by the
   * reserved `$shadow/<deviceId>/update` topic.
   */
  applyShadowPatch(deviceId, patch) {
    const existing = this.store.getShadow(deviceId);
    const state = existing === null
      ? { desired: {}, reported: {} }
      : { desired: structuredClone(existing.state.desired), reported: structuredClone(existing.state.reported) };
    if (patch.desired !== null) {
      mergeValues(state.desired, patch.desired);
    }
    if (patch.reported !== null) {
      mergeValues(state.reported, patch.reported);
    }
    const document = {
      device_id: deviceId,
      state,
      delta: computeDelta(state.desired, state.reported),
      version: (existing?.version ?? 0) + 1,
      updated_at: this.now(),
    };
    this.store.putShadow(deviceId, document);
    this.#evaluateShadowRules(document);
    this.#publishDelta(document);
    return document;
  }

  createRule(raw, key) {
    const rule = parseRule(raw);
    return this.#idempotent(key, `create-rule:${rule.id}`, () => {
      if (this.store.getRule(rule.id) !== null) {
        throw new ConflictError(`rule ${rule.id} already exists`);
      }
      this.store.insertRule(rule);
      return rule;
    });
  }

  events(query = {}) {
    const filters = {
      ruleId: null,
      deviceId: null,
      event: null,
      source: null,
      occurredAfter: null,
      occurredBefore: null,
      afterSequence: null,
      limit: null,
    };
    if (query.ruleId !== undefined && query.ruleId !== null) {
      filters.ruleId = identifier(query.ruleId, "rule id");
    }
    if (query.deviceId !== undefined && query.deviceId !== null) {
      filters.deviceId = identifier(query.deviceId, "device id");
    }
    if (query.event !== undefined && query.event !== null) {
      if (query.event === "") {
        throw new ValidationError("event must not be empty");
      }
      filters.event = query.event;
    }
    if (query.source !== undefined && query.source !== null) {
      if (query.source === "") {
        throw new ValidationError("source must not be empty");
      }
      filters.source = query.source;
    }
    if (query.occurredAfter !== undefined && query.occurredAfter !== null) {
      filters.occurredAfter = parseTimestamp(query.occurredAfter, "occurred_after");
    }
    if (query.occurredBefore !== undefined && query.occurredBefore !== null) {
      filters.occurredBefore = parseTimestamp(query.occurredBefore, "occurred_before");
    }
    if (filters.occurredAfter !== null && filters.occurredBefore !== null && filters.occurredAfter >= filters.occurredBefore) {
      throw new ValidationError("occurred_after must be earlier than occurred_before");
    }
    if (query.afterSequence !== undefined && query.afterSequence !== null) {
      filters.afterSequence = parseInteger(query.afterSequence, "after_sequence");
    }
    if (query.limit !== undefined && query.limit !== null) {
      const limit = parseInteger(query.limit, "limit");
      if (limit < 1 || limit > 1000) {
        throw new ValidationError("limit must be an integer between 1 and 1000");
      }
      filters.limit = limit;
    }
    return { events: this.store.listEvents(filters) };
  }

  /**
   * Called by the broker for every routed publication: the reserved shadow
   * topic is applied first, then mqtt-scoped rules are evaluated once for the
   * published topic.
   */
  handlePublish(topic, payload) {
    if (topic.startsWith(SHADOW_PREFIX)) {
      const parts = topic.split("/");
      if (parts.length === 3 && parts[2] === "update" && parts[1] !== "") {
        let patch = null;
        try {
          identifier(parts[1], "device id");
          patch = parseShadowPatch(JSON.parse(payload.toString("utf8")));
        } catch {
          // A malformed shadow update is still routed to subscribers but never
          // modifies the shadow, and it never raises a broker protocol error.
          patch = null;
        }
        if (patch !== null) {
          this.applyShadowPatch(parts[1], patch);
        }
      }
    }
    const observation = { topic, payload: decodePayload(payload.toString("utf8")), device_id: null };
    for (const rule of this.store.listRules().filter((candidate) => candidate.source === "mqtt")) {
      if (topicMatches(rule.topic, topic) && evaluateCondition(rule, observation)) {
        this.#emit(rule, { device_id: null, topic, value: matchedValue(rule, observation) });
      }
    }
  }

  #idempotent(key, operation, action) {
    if (!key) {
      throw new ValidationError("Idempotency-Key header is required");
    }
    const existing = this.store.getIdempotency(key);
    if (existing !== null) {
      if (existing.operation !== operation) {
        throw new ConflictError("idempotency key was already used for another operation");
      }
      return existing.response;
    }
    return this.store.transaction(() => {
      const response = action();
      this.store.putIdempotency(key, operation, response);
      return response;
    });
  }

  /**
   * Shadow rules are edge triggered: a rule fires when its condition turns
   * from false to true for that device, so repeating a write that leaves the
   * condition true does not emit another event.
   */
  #evaluateShadowRules(document) {
    const rules = this.store
      .listRules()
      .filter((rule) => rule.source === "shadow" && rule.device_id === document.device_id)
      .sort(byId);
    for (const rule of rules) {
      const matched = evaluateCondition(rule, document);
      const previous = this.store.getRuleState(rule.id, document.device_id);
      this.store.setRuleState(rule.id, document.device_id, matched);
      if (matched && !previous) {
        this.#emit(rule, { device_id: document.device_id, topic: null, value: matchedValue(rule, document) });
      }
    }
  }

  #publishDelta(document) {
    if (this.publisher === null || Object.keys(document.delta).length === 0) {
      return;
    }
    this.publisher(`${SHADOW_PREFIX}${document.device_id}/delta`, Buffer.from(JSON.stringify(document.delta), "utf8"), {
      qos: 0,
      retain: false,
    });
  }

  #emit(rule, { device_id: deviceId, topic, value }) {
    return this.store.appendEvent({
      rule_id: rule.id,
      event: rule.event,
      source: rule.source,
      device_id: deviceId,
      topic,
      value,
      occurred_at: this.now(),
    });
  }
}
