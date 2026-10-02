import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import test from "node:test";

import { ConflictError, ValidationError } from "../src/errors.js";
import { evaluateCondition, parseRule } from "../src/rules.js";
import { Service } from "../src/service.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";

const DOCUMENT = {
  state: { desired: {}, reported: { temperature: 31, tags: ["cold", "wet"], name: "attic", nested: { depth: 2 } } },
  samples: [{ value: 3 }, { value: 7 }],
};

function service() {
  return new Service(":memory:", { now: CLOCK });
}

function rule(overrides) {
  return {
    id: "hot",
    source: "shadow",
    device_id: "device-1",
    path: "state.reported.temperature",
    operator: "gt",
    value: 30,
    event: "temperature_high",
    ...overrides,
  };
}

test("rules are validated strictly", () => {
  assert.equal(parseRule(rule()).event, "temperature_high");
  assert.deepEqual(Object.keys(parseRule(rule())), ["id", "source", "device_id", "path", "operator", "value", "event"]);
  assert.throws(() => parseRule(rule({ extra: 1 })), ValidationError);
  assert.throws(() => parseRule(rule({ operator: "matches" })), ValidationError);
  assert.throws(() => parseRule(rule({ source: "http" })), ValidationError);
  assert.throws(() => parseRule(rule({ device_id: undefined })), ValidationError);
  assert.throws(() => parseRule(rule({ topic: "a/b" })), ValidationError);
  assert.throws(() => parseRule({ id: "r", source: "mqtt", path: "x", operator: "eq", value: 1, event: "e" }), ValidationError);
  assert.throws(() => parseRule(rule({ topic: undefined, source: "mqtt", path: "x", operator: "eq", value: 1, event: "e" })), ValidationError);
  assert.throws(() => parseRule({ id: "r", source: "mqtt", topic: "a/#/b", path: "x", operator: "eq", value: 1, event: "e" }), ValidationError);
  assert.throws(() => parseRule(rule({ operator: "exists" })), ValidationError);
  assert.throws(() => parseRule({ ...rule(), value: undefined }), ValidationError);
  assert.throws(() => parseRule(rule({ event: "TemperatureHigh" })), ValidationError);
  assert.throws(() => parseRule(rule({ path: "a..b" })), ValidationError);
  assert.throws(() => parseRule(rule({ id: "not an id" })), ValidationError);
  assert.equal(parseRule({ id: "r", source: "mqtt", topic: "sensors/+/temp", path: "payload.value", operator: "gte", value: 5, event: "too_warm" }).topic, "sensors/+/temp");
});

test("operators evaluate against resolved paths", () => {
  const conditions = [
    ["state.reported.temperature", "gt", 30, true],
    ["state.reported.temperature", "gt", 31, false],
    ["state.reported.temperature", "gte", 31, true],
    ["state.reported.temperature", "lt", 31, false],
    ["state.reported.temperature", "lte", 31, true],
    ["state.reported.temperature", "eq", 31, true],
    ["state.reported.temperature", "ne", 31, false],
    ["state.reported.temperature", "eq", "31", false],
    ["state.reported.temperature", "gt", "30", false],
    ["state.reported.missing", "exists", null, false],
    ["state.reported.name", "exists", null, true],
    ["state.reported.name", "not_exists", null, false],
    ["state.reported.tags", "contains", "wet", true],
    ["state.reported.tags", "contains", "dry", false],
    ["state.reported.name", "contains", "tti", true],
    ["state.reported.nested", "eq", { depth: 2 }, true],
    ["samples.1.value", "eq", 7, true],
    ["samples.5.value", "eq", 7, false],
    ["", "exists", null, true],
  ];
  for (const [path, operator, value, expected] of conditions) {
    const raw = { id: "r", source: "shadow", device_id: "device-1", path, operator, event: "e" };
    const parsed = parseRule(value === null ? raw : { ...raw, value });
    assert.equal(evaluateCondition(parsed, DOCUMENT), expected, `${path} ${operator} ${JSON.stringify(value)}`);
  }
});

test("rule creation is idempotent and duplicate ids conflict", () => {
  const shadowlink = service();
  const created = shadowlink.createRule(rule(), "k1");
  assert.equal(created.id, "hot");
  assert.deepEqual(shadowlink.createRule(rule(), "k1"), created);
  assert.throws(() => shadowlink.createRule(rule(), "k2"), ConflictError);
  assert.throws(() => shadowlink.createRule(rule({ value: 20 }), undefined), ValidationError);
});

test("shadow rules fire on a false to true transition only", () => {
  const shadowlink = service();
  shadowlink.createRule(rule(), "k1");
  assert.deepEqual(shadowlink.events().events, []);
  shadowlink.reportState("device-1", { state: { reported: { temperature: 20 } } }, "k2");
  assert.deepEqual(shadowlink.events().events, []);
  shadowlink.reportState("device-1", { state: { reported: { temperature: 31 } } }, "k3");
  const fired = shadowlink.events().events;
  assert.equal(fired.length, 1);
  assert.deepEqual(fired[0], {
    sequence: 1,
    rule_id: "hot",
    event: "temperature_high",
    source: "shadow",
    device_id: "device-1",
    topic: null,
    value: 31,
    occurred_at: "2024-01-01T00:00:00.000Z",
  });
  shadowlink.reportState("device-1", { state: { reported: { temperature: 40 } } }, "k4");
  assert.equal(shadowlink.events().events.length, 1);
  shadowlink.reportState("device-1", { state: { reported: { temperature: 10 } } }, "k5");
  shadowlink.reportState("device-1", { state: { reported: { temperature: 35 } } }, "k6");
  assert.equal(shadowlink.events().events.length, 2);
});

test("shadow rules only watch their own device", () => {
  const shadowlink = service();
  shadowlink.createRule(rule(), "k1");
  shadowlink.reportState("device-2", { state: { reported: { temperature: 99 } } }, "k2");
  assert.deepEqual(shadowlink.events().events, []);
  assert.deepEqual(shadowlink.events({ deviceId: "device-2" }).events, []);
  shadowlink.reportState("device-1", { state: { reported: { temperature: 99 } } }, "k3");
  assert.equal(shadowlink.events({ deviceId: "device-1" }).events.length, 1);
  assert.equal(shadowlink.events({ ruleId: "hot" }).events.length, 1);
  assert.throws(() => shadowlink.events({ ruleId: "no such rule" }), ValidationError);
});

test("mqtt rules are evaluated once per published message", () => {
  const shadowlink = service();
  shadowlink.createRule(
    { id: "warm", source: "mqtt", topic: "sensors/+/temp", path: "payload.value", operator: "gte", value: 30, event: "too_warm" },
    "k1",
  );
  shadowlink.handlePublish("sensors/kitchen/temp", Buffer.from(JSON.stringify({ value: 22 })));
  assert.deepEqual(shadowlink.events().events, []);
  shadowlink.handlePublish("sensors/kitchen/temp", Buffer.from(JSON.stringify({ value: 33 })));
  shadowlink.handlePublish("sensors/kitchen/temp", Buffer.from(JSON.stringify({ value: 34 })));
  shadowlink.handlePublish("other/topic", Buffer.from(JSON.stringify({ value: 90 })));
  const events = shadowlink.events().events;
  assert.equal(events.length, 2);
  assert.deepEqual(events.map((entry) => entry.sequence), [1, 2]);
  assert.equal(events[0].source, "mqtt");
  assert.equal(events[0].topic, "sensors/kitchen/temp");
  assert.equal(events[0].device_id, null);
  assert.equal(events[0].value, 33);
});

test("non-JSON payloads are compared as text", () => {
  const shadowlink = service();
  shadowlink.createRule({ id: "alarm", source: "mqtt", topic: "alerts/#", path: "payload", operator: "eq", value: "fire", event: "alarm" }, "k1");
  shadowlink.handlePublish("alerts/hall", Buffer.from("fire"));
  shadowlink.handlePublish("alerts/hall", Buffer.from("flood"));
  assert.deepEqual(shadowlink.events().events.map((entry) => entry.value), ["fire"]);
});

test("the reserved update topic merges into the shadow", () => {
  const shadowlink = service();
  shadowlink.handlePublish("$shadow/device-1/update", Buffer.from(JSON.stringify({ state: { desired: { power: "on" } } })));
  const document = shadowlink.getShadow("device-1");
  assert.equal(document.version, 1);
  assert.deepEqual(document.delta, { power: "on" });
  shadowlink.handlePublish("$shadow/device-1/update", Buffer.from("not json"));
  assert.equal(shadowlink.getShadow("device-1").version, 1);
  shadowlink.handlePublish("$shadow/bad id!/update", Buffer.from(JSON.stringify({ state: { desired: { a: 1 } } })));
  assert.equal(shadowlink.store.getShadow("bad id!"), null);
  shadowlink.handlePublish("$shadow/device-1/reported", Buffer.from(JSON.stringify({ state: { desired: { a: 1 } } })));
  assert.equal(shadowlink.getShadow("device-1").version, 1);
});
