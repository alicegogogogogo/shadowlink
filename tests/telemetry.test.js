import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import * as mqtt from "../src/mqtt.js";
import { start } from "../src/server.js";
import { MqttClient, publishFields, subscribePacket, waitFor } from "./helpers.js";

const T0 = Date.parse("2024-01-01T00:00:00.000Z");

async function withServer(run, options = {}) {
  let nowMs = T0;
  const started = await start({
    host: "127.0.0.1",
    port: 0,
    mqttPort: 0,
    database: ":memory:",
    now: () => new Date(nowMs).toISOString(),
    ...options,
  });
  try {
    await run(started, (millis) => {
      nowMs = millis;
    });
  } finally {
    await started.close();
  }
}

function publish(client, topic, text, { qos = 0, packetId = 1, dup = false } = {}) {
  client.write(mqtt.encodePublish({ topic, payload: Buffer.from(text, "utf8"), qos, packetId, dup }));
}

function telemetryPath({
  deviceId = "device-1",
  metric = "temperature",
  from = "2024-01-01T00:00:00.000Z",
  to = "2024-01-01T01:00:00.000Z",
  bucketSeconds = 60,
  aggregate = "avg",
} = {}) {
  return `/devices/${deviceId}/telemetry?metric=${metric}`
    + `&from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`
    + `&bucket_seconds=${bucketSeconds}&aggregate=${aggregate}`;
}

async function getTelemetry(started, path) {
  const response = await fetch(`${started.httpUrl}${path}`);
  return { status: response.status, body: await response.json() };
}

async function postRule(started, rule) {
  const response = await fetch(`${started.httpUrl}/rules`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "Idempotency-Key": `rule-${rule.id}` },
    body: JSON.stringify(rule),
  });
  assert.equal(response.status, 201);
}

test("QoS 0 and QoS 1 telemetry publishes are saved and acknowledged", async () => {
  await withServer(async (started, setClock) => {
    const device = await MqttClient.connect(started.broker.port, { clientId: "device-1" });
    // QoS 1 first: the PUBACK is only sent after routing finished, so its
    // receipt timestamp is fixed before the clock moves for the QoS 0 sample,
    // which has no acknowledgement to synchronize on.
    setClock(T0 + 65_000);
    publish(device, "$telemetry/device-1/temperature", "20", { qos: 1, packetId: 7 });
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 7);
    setClock(T0 + 5_000);
    publish(device, "$telemetry/device-1/temperature", "10", { qos: 0 });
    await waitFor(async () => {
      const { body } = await getTelemetry(started, telemetryPath({ bucketSeconds: 60, aggregate: "avg" }));
      return body.buckets.length === 2;
    });
    const { status, body } = await getTelemetry(
      started,
      telemetryPath({ bucketSeconds: 60, aggregate: "avg" }),
    );
    assert.equal(status, 200);
    assert.deepEqual(body, {
      device_id: "device-1",
      metric: "temperature",
      from: "2024-01-01T00:00:00.000Z",
      to: "2024-01-01T01:00:00.000Z",
      bucket_seconds: 60,
      aggregate: "avg",
      buckets: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:00.000Z", value: 10, count: 1 },
        { start: "2024-01-01T00:01:00.000Z", end: "2024-01-01T00:02:00.000Z", value: 20, count: 1 },
      ],
    });
    await device.disconnect();
  });
});

test("samples aggregate into epoch aligned, ascending, non-empty buckets over a half-open window", async () => {
  await withServer(async (started, setClock) => {
    const store = started.service.store;
    // 90 second buckets: boundaries at ...00:00, ...01:30, ...03:00 ...
    store.insertTelemetrySample("device-1", "temperature", T0 + 5_000, 10); // bucket 00:00
    store.insertTelemetrySample("device-1", "temperature", T0 + 10_000, 20); // bucket 00:00
    store.insertTelemetrySample("device-1", "temperature", T0 + 95_000, 30); // bucket 01:30
    store.insertTelemetrySample("device-1", "temperature", T0, 40); // exactly `from`, included
    store.insertTelemetrySample("device-1", "temperature", T0 + 180_000, 50); // exactly `to`, excluded
    const path = telemetryPath({ from: "2024-01-01T00:00:00Z", to: "2024-01-01T00:03:00Z", bucketSeconds: 90 });
    const expectations = {
      avg: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:30.000Z", value: 70 / 3, count: 3 },
        { start: "2024-01-01T00:01:30.000Z", end: "2024-01-01T00:03:00.000Z", value: 30, count: 1 },
      ],
      min: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:30.000Z", value: 10, count: 3 },
        { start: "2024-01-01T00:01:30.000Z", end: "2024-01-01T00:03:00.000Z", value: 30, count: 1 },
      ],
      max: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:30.000Z", value: 40, count: 3 },
        { start: "2024-01-01T00:01:30.000Z", end: "2024-01-01T00:03:00.000Z", value: 30, count: 1 },
      ],
      sum: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:30.000Z", value: 70, count: 3 },
        { start: "2024-01-01T00:01:30.000Z", end: "2024-01-01T00:03:00.000Z", value: 30, count: 1 },
      ],
      count: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:30.000Z", value: 3, count: 3 },
        { start: "2024-01-01T00:01:30.000Z", end: "2024-01-01T00:03:00.000Z", value: 1, count: 1 },
      ],
    };
    for (const [aggregate, buckets] of Object.entries(expectations)) {
      const { status, body } = await getTelemetry(started, path.replace("aggregate=avg", `aggregate=${aggregate}`));
      assert.equal(status, 200, aggregate);
      assert.deepEqual(body.buckets, buckets, aggregate);
    }
  });
});

test("buckets stay epoch aligned for pre-1970 (negative epoch) timestamps", async () => {
  await withServer(async (started) => {
    const store = started.service.store;
    const beforeEpoch = "1969-12-31T23:58:00.000Z";
    const afterEpoch = "1970-01-01T00:02:00.000Z";
    store.insertTelemetrySample("device-1", "temperature", Date.parse("1969-12-31T23:59:59.000Z"), 1);
    store.insertTelemetrySample("device-1", "temperature", Date.parse("1970-01-01T00:00:01.000Z"), 2);
    const path = telemetryPath({ from: beforeEpoch, to: afterEpoch, bucketSeconds: 60, aggregate: "count" });
    const { status, body } = await getTelemetry(started, path);
    assert.equal(status, 200);
    assert.deepEqual(body.buckets, [
      { start: "1969-12-31T23:59:00.000Z", end: "1970-01-01T00:00:00.000Z", value: 1, count: 1 },
      { start: "1970-01-01T00:00:00.000Z", end: "1970-01-01T00:01:00.000Z", value: 1, count: 1 },
    ]);
  });
});

test("a device or window with no samples returns 200 and empty buckets without a shadow or events", async () => {
  await withServer(async (started) => {
    const { status, body } = await getTelemetry(started, telemetryPath({ deviceId: "ghost" }));
    assert.equal(status, 200);
    assert.deepEqual(body, {
      device_id: "ghost",
      metric: "temperature",
      from: "2024-01-01T00:00:00.000Z",
      to: "2024-01-01T01:00:00.000Z",
      bucket_seconds: 60,
      aggregate: "avg",
      buckets: [],
    });
    const shadow = await fetch(`${started.httpUrl}/devices/ghost/shadow`);
    assert.equal(shadow.status, 404);
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.deepEqual(events.events, []);
  });
});

test("invalid telemetry publications are routed normally but never saved", async () => {
  await withServer(async (started) => {
    const subscriber = await MqttClient.connect(started.broker.port, { clientId: "subscriber" });
    subscriber.write(subscribePacket(1, [{ filter: "$telemetry/#", qos: 0 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);
    const device = await MqttClient.connect(started.broker.port, { clientId: "device-1" });
    const cases = [
      ["$telemetry/device-1/temperature/extra", "1"], // too many levels
      ["$telemetry/device-1/", "1"], // empty metric
      ["$telemetry/bad device/temperature", "1"], // illegal device id
      ["$telemetry/device-1/bad metric", "1"], // illegal metric
      ["$telemetry/device-1/temperature", '{"value":1}'], // JSON object
      ["$telemetry/device-1/temperature", '"1"'], // JSON string
      ["$telemetry/device-1/temperature", "hello"], // not JSON
      ["$telemetry/device-1/temperature", "true"], // JSON boolean
      ["$telemetry/device-1/temperature", "null"], // JSON null
      ["$telemetry/device-1/temperature", "1e999"], // parses to Infinity
      ["$telemetry/device-1/temperature", "-1e999"], // parses to -Infinity
    ];
    let packetId = 10;
    for (const [topic, payloadText] of cases) {
      packetId += 1;
      publish(device, topic, payloadText, { qos: 1, packetId });
      assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), packetId);
      const routed = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
      assert.equal(routed.topic, topic);
      assert.equal(routed.text, payloadText);
    }
    const { body } = await getTelemetry(started, telemetryPath());
    assert.deepEqual(body.buckets, []);
    await subscriber.disconnect();
    await device.disconnect();
  });
});

test("telemetry publishes still run mqtt rules but telemetry itself writes no events", async () => {
  await withServer(async (started, setClock) => {
    await postRule(started, {
      id: "hot",
      source: "mqtt",
      topic: "$telemetry/+/temperature",
      path: "payload",
      operator: "gt",
      value: 30,
      event: "too_hot",
    });
    const device = await MqttClient.connect(started.broker.port, { clientId: "device-1" });
    setClock(T0 + 1_000);
    publish(device, "$telemetry/device-1/temperature", "42", { qos: 1, packetId: 3 });
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 3);
    await waitFor(async () => (await (await fetch(`${started.httpUrl}/events`)).json()).events.length === 1);
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.deepEqual(events.events.map((event) => [event.rule_id, event.topic, event.value]), [
      ["hot", "$telemetry/device-1/temperature", 42],
    ]);
    // A value that fails the rule condition saves a sample but emits nothing.
    setClock(T0 + 2_000);
    publish(device, "$telemetry/device-1/temperature", "10", { qos: 0 });
    await waitFor(async () => {
      const { body } = await getTelemetry(started, telemetryPath({ aggregate: "count" }));
      return body.buckets.reduce((sum, bucket) => sum + bucket.count, 0) === 2;
    });
    const after = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.equal(after.events.length, 1);
    await device.disconnect();
  });
});

test("a retransmitted QoS 1 publish saves one sample and gets one PUBACK flow", async () => {
  await withServer(async (started) => {
    const device = await MqttClient.connect(started.broker.port, { clientId: "device-1" });
    publish(device, "$telemetry/device-1/temperature", "11", { qos: 1, packetId: 9 });
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 9);
    publish(device, "$telemetry/device-1/temperature", "11", { qos: 1, packetId: 9, dup: true });
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 9);
    const { body } = await getTelemetry(started, telemetryPath({ aggregate: "count" }));
    assert.deepEqual(body.buckets.map((bucket) => bucket.count), [1]);
    await device.disconnect();
  });
});

test("samples survive a restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shadowlink-telemetry-"));
  const database = join(directory, "shadowlink.db");
  try {
    const first = await start({
      host: "127.0.0.1",
      port: 0,
      mqttPort: 0,
      database,
      now: () => new Date(T0 + 1_000).toISOString(),
    });
    const device = await MqttClient.connect(first.broker.port, { clientId: "device-1" });
    publish(device, "$telemetry/device-1/temperature", "15", { qos: 1, packetId: 1 });
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 1);
    await device.disconnect();
    await first.close();
    const second = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database });
    const { status, body } = await getTelemetry(second, telemetryPath({ aggregate: "sum" }));
    assert.equal(status, 200);
    assert.deepEqual(body.buckets, [
      { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:00.000Z", value: 15, count: 1 },
    ]);
    await second.close();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

const BAD_REQUESTS = [
  ["missing aggregate", telemetryPath().replace("&aggregate=avg", "")],
  ["missing bucket_seconds", telemetryPath().replace("&bucket_seconds=60", "")],
  ["missing from", telemetryPath().replace(/&from=[^&]*/, "")],
  ["missing to", telemetryPath().replace(/&to=[^&]*/, "")],
  ["missing metric", telemetryPath().replace("metric=temperature&", "")],
  ["duplicate metric", `${telemetryPath()}&metric=temperature`],
  ["duplicate bucket_seconds", `${telemetryPath()}&bucket_seconds=120`],
  ["unknown parameter", `${telemetryPath()}&bogus=1`],
  ["empty metric", telemetryPath({ metric: "" })],
  ["empty aggregate", telemetryPath({ aggregate: "" })],
  ["illegal metric", telemetryPath({ metric: "bad metric" })],
  ["illegal device id", telemetryPath({ deviceId: "bad device" })],
  ["from without Z", telemetryPath({ from: "2024-01-01T00:00:00" })],
  ["from with offset", telemetryPath({ from: "2024-01-01T00:00:00+00:00" })],
  ["impossible from date", telemetryPath({ from: "2024-02-30T00:00:00Z" })],
  ["from equal to to", telemetryPath({ from: "2024-01-01T00:30:00Z", to: "2024-01-01T00:30:00Z" })],
  ["from later than to", telemetryPath({ from: "2024-01-01T00:31:00Z", to: "2024-01-01T00:30:00Z" })],
  ["bucket zero", telemetryPath({ bucketSeconds: 0 })],
  ["bucket too large", telemetryPath({ bucketSeconds: 86401 })],
  ["bucket negative", telemetryPath({ bucketSeconds: -60 })],
  ["bucket fractional", telemetryPath({ bucketSeconds: "60.5" })],
  ["bucket non-numeric", telemetryPath({ bucketSeconds: "sixty" })],
  ["unknown aggregate", telemetryPath({ aggregate: "mean" })],
];

for (const [label, path] of BAD_REQUESTS) {
  test(`rejects ${label}`, async () => {
    await withServer(async (started) => {
      const { status, body } = await getTelemetry(started, path);
      assert.equal(status, 400);
      assert.equal(body.error.code, "validation_error");
    });
  });
}
