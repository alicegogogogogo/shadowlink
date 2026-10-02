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
    const unknownQuery = await fetch(`${started.httpUrl}/events?cursor=1`);
    assert.equal(unknownQuery.status, 400);
    assert.equal((await unknownQuery.json()).error.code, "validation_error");
  });
});

test("events can be filtered by event, source, time range and paged", async () => {
  await withServer(async (started) => {
    const append = (sequence) =>
      started.service.store.appendEvent({
        rule_id: `rule-${sequence}`,
        event: sequence % 2 === 0 ? "even_event" : "odd_event",
        source: sequence % 2 === 0 ? "mqtt" : "shadow",
        device_id: "device-1",
        topic: null,
        value: sequence,
        occurred_at: `2024-01-0${sequence}T00:00:00.000Z`,
      });
    for (let sequence = 1; sequence <= 5; sequence += 1) {
      append(sequence);
    }
    const get = async (query) => {
      const response = await fetch(`${started.httpUrl}/events${query}`);
      return { status: response.status, body: await response.json() };
    };
    const all = await get("");
    assert.deepEqual(all.body.events.map((entry) => entry.sequence), [1, 2, 3, 4, 5]);
    const byEvent = await get("?event=even_event");
    assert.deepEqual(byEvent.body.events.map((entry) => entry.sequence), [2, 4]);
    const bySource = await get("?source=shadow");
    assert.deepEqual(bySource.body.events.map((entry) => entry.sequence), [1, 3, 5]);
    const intersection = await get("?event=odd_event&source=shadow&device_id=device-1");
    assert.deepEqual(intersection.body.events.map((entry) => entry.sequence), [1, 3, 5]);
    // The range is left-closed, right-open on occurred_at.
    const ranged = await get("?occurred_after=2024-01-02T00:00:00Z&occurred_before=2024-01-04T00:00:00.000Z");
    assert.deepEqual(ranged.body.events.map((entry) => entry.sequence), [2, 3]);
    const firstPage = await get("?limit=2");
    assert.deepEqual(firstPage.body.events.map((entry) => entry.sequence), [1, 2]);
    const secondPage = await get(`?after_sequence=${firstPage.body.events.at(-1).sequence}&limit=2`);
    assert.deepEqual(secondPage.body.events.map((entry) => entry.sequence), [3, 4]);
    const lastPage = await get(`?after_sequence=${secondPage.body.events.at(-1).sequence}&limit=2`);
    assert.deepEqual(lastPage.body.events.map((entry) => entry.sequence), [5]);
    const combined = await get("?source=mqtt&occurred_after=2024-01-01T00:00:00.000Z&after_sequence=2&limit=1");
    assert.deepEqual(combined.body.events.map((entry) => entry.sequence), [4]);
    const repeat = await get("?limit=2");
    assert.deepEqual(repeat.body, firstPage.body);
  });
});

test("events reject malformed filter parameters", async () => {
  await withServer(async (started) => {
    const bad = [
      "event=",
      "source=",
      "occurred_after=2024-01-01T00:00:00",
      "occurred_after=2024-01-01",
      "occurred_before=not-a-time",
      "occurred_after=2024-01-02T00:00:00Z&occurred_before=2024-01-02T00:00:00Z",
      "occurred_after=2024-01-03T00:00:00Z&occurred_before=2024-01-02T00:00:00Z",
      "after_sequence=-1",
      "after_sequence=1.5",
      "after_sequence=9007199254740993",
      "limit=0",
      "limit=1001",
      "limit=two",
    ];
    for (const query of bad) {
      const response = await fetch(`${started.httpUrl}/events?${query}`);
      assert.equal(response.status, 400, query);
      const body = await response.json();
      assert.equal(body.error.code, "validation_error", query);
      const parameter = query.split(/[=&]/)[0];
      assert.ok(body.error.message.includes(parameter), `${query}: ${body.error.message}`);
    }
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
