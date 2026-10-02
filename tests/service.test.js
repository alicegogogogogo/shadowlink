import assert from "node:assert/strict";
import test from "node:test";

import * as mqtt from "../src/mqtt.js";
import { start } from "../src/server.js";
import { MqttClient, publishFields, subscribePacket } from "./helpers.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";

async function withServer(run) {
  const started = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database: ":memory:", now: CLOCK });
  try {
    await run(started);
  } finally {
    await started.close();
  }
}

async function post(url, body, key) {
  const response = await fetch(url, {
    method: "POST",
    headers: key === undefined ? { "Content-Type": "application/json" } : { "Content-Type": "application/json", "Idempotency-Key": key },
    body: JSON.stringify(body),
  });
  return { status: response.status, body: await response.json() };
}

test("GET /health reports the service status", async () => {
  await withServer(async (started) => {
    const response = await fetch(`${started.httpUrl}/health`);
    assert.equal(response.status, 200);
    assert.equal(response.headers.get("content-type"), "application/json; charset=utf-8");
    assert.deepEqual(await response.json(), { status: "ok" });
  });
});

test("shadows can be written and read over HTTP", async () => {
  await withServer(async (started) => {
    const created = await post(`${started.httpUrl}/devices/device-1/shadow`, { state: { desired: { power: "on", brightness: 70 } } }, "k1");
    assert.equal(created.status, 200);
    assert.deepEqual(created.body, {
      device_id: "device-1",
      state: { desired: { power: "on", brightness: 70 }, reported: {} },
      delta: { power: "on", brightness: 70 },
      version: 1,
      updated_at: "2024-01-01T00:00:00.000Z",
    });
    const reported = await post(`${started.httpUrl}/devices/device-1/reported`, { state: { reported: { power: "on" } } }, "k2");
    assert.deepEqual(reported.body.delta, { brightness: 70 });
    assert.equal(reported.body.version, 2);
    const fetched = await (await fetch(`${started.httpUrl}/devices/device-1/shadow`)).json();
    assert.deepEqual(fetched, reported.body);
  });
});

test("shadows reject unknown fields and require an idempotency key", async () => {
  await withServer(async (started) => {
    const missingKey = await post(`${started.httpUrl}/devices/device-1/shadow`, { state: { desired: {} } }, undefined);
    assert.equal(missingKey.status, 400);
    assert.deepEqual(missingKey.body, { error: { code: "validation_error", message: "Idempotency-Key header is required" } });
    const unknown = await post(`${started.httpUrl}/devices/device-1/shadow`, { state: { desired: {}, shadow: {} } }, "k1");
    assert.equal(unknown.status, 400);
    assert.equal(unknown.body.error.code, "validation_error");
    const wrongType = await fetch(`${started.httpUrl}/devices/device-1/shadow`, { method: "POST", body: "{}" });
    assert.equal(wrongType.status, 400);
    assert.equal((await wrongType.json()).error.code, "validation_error");
    const missing = await fetch(`${started.httpUrl}/devices/device-1/shadow`);
    assert.equal(missing.status, 404);
    assert.deepEqual(await missing.json(), { error: { code: "not_found", message: "device device-1 has no shadow" } });
    const unknownRoute = await fetch(`${started.httpUrl}/devices`);
    assert.equal(unknownRoute.status, 404);
    assert.equal((await unknownRoute.json()).error.code, "not_found");
    const badBody = await post(`${started.httpUrl}/devices/device-1/shadow`, { state: { desired: {} } }, "k2");
    assert.equal(badBody.status, 200);
    const replay = await post(`${started.httpUrl}/devices/device-1/shadow`, { state: { desired: { power: "off" } } }, "k2");
    assert.deepEqual(replay.body, badBody.body);
  });
});

test("rules turn shadow updates into events", async () => {
  await withServer(async (started) => {
    const rule = {
      id: "hot",
      source: "shadow",
      device_id: "device-1",
      path: "state.reported.temperature",
      operator: "gt",
      value: 30,
      event: "temperature_high",
    };
    const created = await post(`${started.httpUrl}/rules`, rule, "r1");
    assert.equal(created.status, 201);
    assert.deepEqual(created.body, rule);
    const duplicate = await post(`${started.httpUrl}/rules`, rule, "r2");
    assert.equal(duplicate.status, 409);
    assert.deepEqual(duplicate.body, { error: { code: "conflict", message: "rule hot already exists" } });
    await post(`${started.httpUrl}/devices/device-1/reported`, { state: { reported: { temperature: 31 } } }, "w1");
    await post(`${started.httpUrl}/devices/device-1/reported`, { state: { reported: { temperature: 41 } } }, "w2");
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.equal(events.events.length, 1);
    assert.deepEqual(events.events[0], {
      sequence: 1,
      rule_id: "hot",
      event: "temperature_high",
      source: "shadow",
      device_id: "device-1",
      topic: null,
      value: 31,
      occurred_at: "2024-01-01T00:00:00.000Z",
    });
    const filtered = await (await fetch(`${started.httpUrl}/events?device_id=device-1&rule_id=hot`)).json();
    assert.equal(filtered.events.length, 1);
    const unknownQuery = await fetch(`${started.httpUrl}/events?bogus=1`);
    assert.equal(unknownQuery.status, 400);
    assert.equal((await unknownQuery.json()).error.code, "validation_error");
  });
});

test("an mqtt publication reaches subscribers and the rule engine", async () => {
  await withServer(async (started) => {
    await post(
      `${started.httpUrl}/rules`,
      { id: "warm", source: "mqtt", topic: "sensors/+/temp", path: "payload.value", operator: "gte", value: 30, event: "too_warm" },
      "r1",
    );
    const subscriber = await MqttClient.connect(started.broker.port, { clientId: "subscriber" });
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 0 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);
    const device = await MqttClient.connect(started.broker.port, { clientId: "device-1" });
    device.write(
      mqtt.encodePublish({ topic: "sensors/kitchen/temp", payload: Buffer.from(JSON.stringify({ value: 33 })), qos: 1, packetId: 4 }),
    );
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 4);
    const received = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.equal(received.topic, "sensors/kitchen/temp");
    assert.equal(received.text, JSON.stringify({ value: 33 }));
    await post(`${started.httpUrl}/devices/device-1/reported`, { state: { reported: { seen: true } } }, "w1");
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.deepEqual(events.events.map((entry) => [entry.rule_id, entry.topic, entry.value]), [["warm", "sensors/kitchen/temp", 33]]);
    await subscriber.disconnect();
    await device.disconnect();
  });
});
