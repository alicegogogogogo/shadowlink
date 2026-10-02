import assert from "node:assert/strict";
import test from "node:test";

import { ConflictError, NotFoundError, ValidationError } from "../src/errors.js";
import { Service } from "../src/service.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";

function service() {
  return new Service(":memory:", { now: CLOCK });
}

test("delta is the desired state minus the reported state", () => {
  const shadowlink = service();
  const document = shadowlink.updateShadow(
    "device-1",
    { state: { desired: { power: "on", brightness: 70 }, reported: { power: "on" } } },
    "k1",
  );
  assert.deepEqual(document.delta, { brightness: 70 });
  assert.deepEqual(document.state.reported, { power: "on" });
  assert.equal(document.version, 1);
  assert.equal(document.updated_at, "2024-01-01T00:00:00.000Z");
  assert.deepEqual(Object.keys(document), ["device_id", "state", "delta", "version", "updated_at"]);
});

test("delta is empty once reported catches up", () => {
  const shadowlink = service();
  shadowlink.updateShadow("device-1", { state: { desired: { power: "on" } } }, "k1");
  const document = shadowlink.reportState("device-1", { state: { reported: { power: "on" } } }, "k2");
  assert.deepEqual(document.delta, {});
  assert.deepEqual(document.state, { desired: { power: "on" }, reported: { power: "on" } });
});

test("nested deltas recurse and omit up-to-date subtrees", () => {
  const shadowlink = service();
  const document = shadowlink.updateShadow(
    "device-1",
    { state: { desired: { config: { interval: 5, mode: "fast", nested: { depth: 1 } } }, reported: { config: { mode: "fast" } } } },
    "k1",
  );
  assert.deepEqual(document.delta, { config: { interval: 5, nested: { depth: 1 } } });
  const applied = shadowlink.reportState("device-1", { state: { reported: { config: { interval: 5, nested: { depth: 1 } } } } }, "k2");
  assert.deepEqual(applied.delta, {});
});

test("delta compares scalars, arrays and null-free subtrees", () => {
  const shadowlink = service();
  const document = shadowlink.updateShadow(
    "device-1",
    {
      state: {
        desired: { tags: ["a", "b"], counters: [1, 2], enabled: true, limit: 10, config: { x: 1 } },
        reported: { tags: ["a", "b"], counters: [2, 1], enabled: false, limit: "10", config: 5 },
      },
    },
    "k1",
  );
  assert.deepEqual(document.delta, { counters: [1, 2], enabled: true, limit: 10, config: { x: 1 } });
});

test("keys that exist only in reported never appear in the delta", () => {
  const shadowlink = service();
  const document = shadowlink.updateShadow("device-1", { state: { reported: { local: 1 } } }, "k1");
  assert.deepEqual(document.delta, {});
  assert.deepEqual(document.state.reported, { local: 1 });
});

test("a null value deletes the key at any depth and arrays replace wholesale", () => {
  const shadowlink = service();
  shadowlink.updateShadow("device-1", { state: { desired: { keep: 1, drop: 2, nested: { a: 1, b: 2 }, list: [1, 2, 3] } } }, "k1");
  const document = shadowlink.updateShadow(
    "device-1",
    { state: { desired: { drop: null, nested: { b: null }, list: [4] } } },
    "k2",
  );
  assert.deepEqual(document.state.desired, { keep: 1, nested: { a: 1 }, list: [4] });
  assert.equal(document.version, 2);
});

test("every accepted write increments the version even when nothing changes", () => {
  const shadowlink = service();
  shadowlink.updateShadow("device-1", { state: { desired: { a: 1 } } }, "k1");
  const repeated = shadowlink.updateShadow("device-1", { state: { desired: { a: 1 } } }, "k2");
  assert.equal(repeated.version, 2);
});

test("an idempotency key replays the first response and a foreign reuse conflicts", () => {
  const shadowlink = service();
  const first = shadowlink.updateShadow("device-1", { state: { desired: { a: 1 } } }, "shared");
  const replay = shadowlink.updateShadow("device-1", { state: { desired: { a: 999 } } }, "shared");
  assert.deepEqual(replay, first);
  assert.equal(shadowlink.getShadow("device-1").version, 1);
  assert.throws(() => shadowlink.updateShadow("device-2", { state: { desired: { a: 1 } } }, "shared"), ConflictError);
  assert.throws(() => shadowlink.reportState("device-1", { state: { reported: { a: 1 } } }, "shared"), ConflictError);
  assert.throws(() => shadowlink.updateShadow("device-1", { state: { desired: { a: 1 } } }, undefined), ValidationError);
});

test("reported and shadow writes are separate idempotency operations", () => {
  const shadowlink = service();
  shadowlink.updateShadow("device-1", { state: { reported: { a: 1 } } }, "shadow-key");
  shadowlink.reportState("device-1", { state: { reported: { a: 2 } } }, "reported-key");
  assert.deepEqual(shadowlink.getShadow("device-1").state.reported, { a: 2 });
});

test("unknown devices are not found and unknown fields are rejected", () => {
  const shadowlink = service();
  assert.throws(() => shadowlink.getShadow("device-1"), NotFoundError);
  assert.throws(() => shadowlink.updateShadow("device-1", { desired: { a: 1 } }, "k1"), ValidationError);
  assert.throws(() => shadowlink.updateShadow("device-1", { state: { desired: { a: 1 }, extra: {} } }, "k2"), ValidationError);
  assert.throws(() => shadowlink.updateShadow("device-1", { state: {} }, "k3"), ValidationError);
  assert.throws(() => shadowlink.updateShadow("device-1", { state: { desired: 5 } }, "k4"), ValidationError);
  assert.throws(() => shadowlink.reportState("device-1", { state: { desired: { a: 1 } } }, "k5"), ValidationError);
  assert.throws(() => shadowlink.getShadow("bad id!"), ValidationError);
});
