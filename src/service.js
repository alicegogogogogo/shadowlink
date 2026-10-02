import { ConflictError, NotFoundError, ValidationError } from "./errors.js";
import { topicMatches } from "./mqtt.js";
import { evaluateCondition, identifier, isIdentifier, matchedValue, parseRule } from "./rules.js";
import { computeDelta, parseReportedPatch, parseShadowPatch } from "./shadow.js";
import { Store } from "./store.js";
import { mergeValues } from "./values.js";

const SHADOW_PREFIX = "$shadow/";
const TELEMETRY_PREFIX = "$telemetry/";

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

  /**
   * Receipt time for a sample in epoch milliseconds. Derived from the
   * injectable `now` clock (which returns canonical ISO-8601 UTC) so telemetry
   * is stamped by the same clock as shadows and events.
   */
  clock() {
    return Date.parse(this.now());
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
    if (query.ruleId !== undefined && query.ruleId !== null) {
      identifier(query.ruleId, "rule id");
    }
    if (query.deviceId !== undefined && query.deviceId !== null) {
      identifier(query.deviceId, "device id");
    }
    return {
      events: this.store.listEvents({
        ruleId: query.ruleId ?? null,
        deviceId: query.deviceId ?? null,
        event: query.event ?? null,
        source: query.source ?? null,
        occurredAfter: query.occurredAfter ?? null,
        occurredBefore: query.occurredBefore ?? null,
        afterSequence: query.afterSequence ?? null,
        limit: query.limit ?? null,
      }),
    };
  }

  /**
   * Aggregate a device/metric's samples into epoch-aligned buckets over the
   * half-open window [from, to). A device that never published a sample is a
   * normal result with empty buckets; it never creates a shadow or an event.
   */
  telemetry(rawDeviceId, query) {
    const deviceId = identifier(rawDeviceId, "device id");
    const metric = identifier(query.metric, "metric");
    const rows = this.store.aggregateTelemetry({
      deviceId,
      metric,
      fromMs: query.from.millis,
      toMs: query.to.millis,
      bucketSeconds: query.bucketSeconds,
      aggregate: query.aggregate,
    });
    const widthMs = query.bucketSeconds * 1000;
    return {
      device_id: deviceId,
      metric,
      from: query.from.canonical,
      to: query.to.canonical,
      bucket_seconds: query.bucketSeconds,
      aggregate: query.aggregate,
      buckets: rows.map((row) => ({
        start: new Date(row.startMs).toISOString(),
        end: new Date(row.startMs + widthMs).toISOString(),
        value: row.value,
        count: row.count,
      })),
    };
  }

  /**
   * Called by the broker for every routed publication: the reserved shadow
   * topic is applied first, then mqtt-scoped rules are evaluated once for the
   * published topic. A `$telemetry/<deviceId>/<metric>` publication carrying a
   * single finite JSON number also stores one sample; that capture never
   * changes routing, rules or events, and an invalid publication is simply not
   * recorded without raising anything.
   */
  handlePublish(topic, payload) {
    this.#recordTelemetry(topic, payload);
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

  /**
   * Store a telemetry sample for a `$telemetry/<deviceId>/<metric>` publication
   * whose payload is a single finite JSON number. Anything else — wrong topic
   * shape, illegal device id or metric, a payload that is not a JSON number, or
   * NaN/Infinity — stores nothing but is still routed and rule evaluated by the
   * caller as a normal publication.
   */
  #recordTelemetry(topic, payload) {
    if (!topic.startsWith(TELEMETRY_PREFIX)) {
      return;
    }
    const parts = topic.split("/");
    if (parts.length !== 3 || parts[1] === "" || parts[2] === "") {
      return;
    }
    const [, deviceId, metric] = parts;
    if (!isIdentifier(deviceId) || !isIdentifier(metric)) {
      return;
    }
    let value;
    try {
      value = JSON.parse(payload.toString("utf8"));
    } catch {
      return;
    }
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return;
    }
    this.store.insertTelemetrySample(deviceId, metric, this.clock(), value);
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
