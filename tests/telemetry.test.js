import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import * as mqtt from "../src/mqtt.js";
import { start } from "../src/server.js";
import { MqttClient, publishFields, subscribePacket } from "./helpers.js";

const EPOCH = Date.parse("2024-01-01T00:00:00Z");

/** A controllable clock in millisecond precision ISO-8601 UTC. */
function clock(startMillis = EPOCH) {
  let current = startMillis;
  const fn = () => new Date(current).toISOString();
  fn.advance = (millis) => {
    current += millis;
  };
  fn.set = (millis) => {
    current = millis;
  };
  return fn;
}

async function withServer(run, options = {}) {
  const started = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database: ":memory:", now: clock(), ...options });
  try {
    await run(started);
  } finally {
    await started.close();
  }
}

function telemetryPath(deviceId, params = {}) {
  const search = new URLSearchParams({
    metric: params.metric ?? "temp",
    from: params.from ?? "2024-01-01T00:00:00Z",
    to: params.to ?? "2024-01-01T01:00:00Z",
    bucket_seconds: String(params.bucketSeconds ?? 60),
    aggregate: params.aggregate ?? "avg",
  });
  return `/devices/${deviceId}/telemetry?${search}`;
}

async function getTelemetry(started, path) {
  const response = await fetch(`${started.httpUrl}${path}`);
  return { status: response.status, body: await response.json() };
}

/** Insert a sample straight into the store at an exact millisecond. */
function sampleAt(service, deviceId, metric, millis, value) {
  service.store.insertTelemetrySample(deviceId, metric, millis, value);
}

async function publishRaw(started, clientId, topic, payload, { qos = 0, packetId = 1, retain = false } = {}) {
  const client = await MqttClient.connect(started.broker.port, { clientId });
  client.write(mqtt.encodePublish({ topic, payload: Buffer.from(payload, "utf8"), qos, packetId, retain }));
  if (qos === 1) {
    await client.next(mqtt.PACKET.PUBACK);
  }
  await client.disconnect();
  return client;
}

test("valid numeric publications are stored as samples at receive time", async () => {
  const now = clock(EPOCH);
  await withServer(async (started) => {
    await publishRaw(started, "d1", "$telemetry/dev1/temp", "10");
    now.advance(1000);
    await publishRaw(started, "d2", "$telemetry/dev1/temp", "20.5");
    const { status, body } = await getTelemetry(started, telemetryPath("dev1"));
    assert.equal(status, 200);
    assert.deepEqual(body, {
      device_id: "dev1",
      metric: "temp",
      from: "2024-01-01T00:00:00.000Z",
      to: "2024-01-01T01:00:00.000Z",
      bucket_seconds: 60,
      aggregate: "avg",
      buckets: [
        { start: "2024-01-01T00:00:00.000Z", end: "2024-01-01T00:01:00.000Z", value: 15.25, count: 2 },
      ],
    });
  }, { now });
});

test("a retransmitted QoS 1 publication is delivered twice but sampled once", async () => {
  const now = clock(EPOCH);
  await withServer(async (started) => {
    const device = await MqttClient.connect(started.broker.port, { clientId: "dev" });
    const publish = () =>
      device.write(mqtt.encodePublish({ topic: "$telemetry/dev1/temp", payload: Buffer.from("7"), qos: 1, packetId: 9 }));
    publish();
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 9);
    publish();
    assert.equal((await device.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 9);
    await device.disconnect();
    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "sum" }));
    assert.deepEqual(body.buckets.map((bucket) => [bucket.value, bucket.count]), [[7, 1]]);
  }, { now });
});

test("non-numeric or malformed telemetry stores no sample but is still routed and runs mqtt rules", async () => {
  const now = clock(EPOCH);
  await withServer(async (started) => {
    const created = await fetch(`${started.httpUrl}/rules`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "Idempotency-Key": "r1" },
      body: JSON.stringify({ id: "anypub", source: "mqtt", topic: "$telemetry/#", path: "topic", operator: "exists", event: "saw_publish" }),
    });
    assert.equal(created.status, 201);

    const subscriber = await MqttClient.connect(started.broker.port, { clientId: "sub" });
    subscriber.write(subscribePacket(1, [{ filter: "$telemetry/#", qos: 0 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);

    const badPayloads = ["NaN", "Infinity", "-Infinity", '"30"', "abc", '{"v":1}', "true", "null", ""];
    let cid = 0;
    for (const payload of badPayloads) {
      cid += 1;
      await publishRaw(started, `p${cid}`, "$telemetry/dev1/temp", payload);
      const received = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
      assert.equal(received.topic, "$telemetry/dev1/temp");
      assert.equal(received.text, payload);
    }
    await subscriber.disconnect();

    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "count" }));
    assert.deepEqual(body.buckets, []);
    // Every publication, valid shape or not, still evaluates mqtt rules.
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.equal(events.events.length, badPayloads.length);
    assert.ok(events.events.every((event) => event.source === "mqtt"));
    // Telemetry itself never creates a shadow.
    assert.equal((await fetch(`${started.httpUrl}/devices/dev1/shadow`)).status, 404);
  }, { now });
});

test("telemetry on a four segment topic is neither sampled nor a protocol error", async () => {
  const now = clock(EPOCH);
  await withServer(async (started) => {
    await publishRaw(started, "p1", "$telemetry/dev1/temp/extra", "5");
    await publishRaw(started, "p2", "$telemetry/dev1", "5");
    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "count" }));
    assert.deepEqual(body.buckets, []);
  }, { now });
});

test("negative numbers, zero and decimals are accepted", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH + 1000, -3.5);
    sampleAt(started.service, "dev1", "temp", EPOCH + 2000, 0);
    sampleAt(started.service, "dev1", "temp", EPOCH + 3000, 100);
    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "sum" }));
    assert.equal(body.buckets[0].value, 96.5);
    assert.equal(body.buckets[0].count, 3);
  });
});

test("buckets are epoch aligned, ascending, and empty buckets are omitted", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH + 5000, 1); // bucket 0
    sampleAt(started.service, "dev1", "temp", EPOCH + 65000, 2); // bucket 60
    sampleAt(started.service, "dev1", "temp", EPOCH + 125000, 4); // bucket 120
    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "sum" }));
    assert.deepEqual(
      body.buckets.map((bucket) => [bucket.start, bucket.end, bucket.value, bucket.count]),
      [
        ["2024-01-01T00:00:00.000Z", "2024-01-01T00:01:00.000Z", 1, 1],
        ["2024-01-01T00:01:00.000Z", "2024-01-01T00:02:00.000Z", 2, 1],
        ["2024-01-01T00:02:00.000Z", "2024-01-01T00:03:00.000Z", 4, 1],
      ],
    );
  });
});

test("epoch alignment holds for buckets that do not divide a day and across the hour", async () => {
  await withServer(async (started) => {
    const millis = EPOCH + 100000; // 90s buckets must align to the epoch, not the window start
    sampleAt(started.service, "dev1", "temp", millis, 1);
    const startSeconds = Math.floor(millis / 1000 / 90) * 90;
    const { body } = await getTelemetry(started, telemetryPath("dev1", { bucketSeconds: 90, aggregate: "count" }));
    assert.equal(body.buckets[0].start, new Date(startSeconds * 1000).toISOString());
    assert.equal(body.buckets[0].end, new Date((startSeconds + 90) * 1000).toISOString());
  });
});

test("the window is half open: from inclusive, to exclusive", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH, 1);
    sampleAt(started.service, "dev1", "temp", EPOCH + 60000, 2);
    const path = telemetryPath("dev1", {
      from: "2024-01-01T00:00:00Z",
      to: "2024-01-01T00:01:00Z",
      aggregate: "sum",
    });
    const { body } = await getTelemetry(started, path);
    assert.equal(body.buckets.length, 1);
    assert.equal(body.buckets[0].value, 1);
  });
});

for (const [aggregate, expected] of [
  ["avg", 20],
  ["min", 10],
  ["max", 30],
  ["sum", 60],
  ["count", 3],
]) {
  test(`aggregate ${aggregate}`, async () => {
    await withServer(async (started) => {
      sampleAt(started.service, "dev1", "temp", EPOCH + 1000, 10);
      sampleAt(started.service, "dev1", "temp", EPOCH + 2000, 30);
      sampleAt(started.service, "dev1", "temp", EPOCH + 3000, 20);
      const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate }));
      assert.equal(body.buckets[0].value, expected);
      assert.equal(body.buckets[0].count, 3);
      assert.equal(body.aggregate, aggregate);
    });
  });
}

test("only the requested device and metric are aggregated", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH + 1000, 1);
    sampleAt(started.service, "dev1", "humidity", EPOCH + 1000, 9);
    sampleAt(started.service, "dev2", "temp", EPOCH + 1000, 5);
    const temp = await getTelemetry(started, telemetryPath("dev1", { metric: "temp", aggregate: "sum" }));
    assert.equal(temp.body.buckets[0].value, 1);
    const humidity = await getTelemetry(started, telemetryPath("dev1", { metric: "humidity", aggregate: "sum" }));
    assert.equal(humidity.body.buckets[0].value, 9);
  });
});

test("samples sharing a millisecond aggregate in a stable order", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH + 1000, 1);
    sampleAt(started.service, "dev1", "temp", EPOCH + 1000, 2);
    sampleAt(started.service, "dev1", "temp", EPOCH + 1000, 3);
    const first = (await getTelemetry(started, telemetryPath("dev1", { aggregate: "avg" }))).body.buckets;
    const second = (await getTelemetry(started, telemetryPath("dev1", { aggregate: "avg" }))).body.buckets;
    assert.deepEqual(first, second);
    assert.equal(first[0].count, 3);
    assert.equal(first[0].value, 2);
  });
});

test("a device with no samples returns 200 and an empty bucket list", async () => {
  await withServer(async (started) => {
    const { status, body } = await getTelemetry(started, telemetryPath("ghost"));
    assert.equal(status, 200);
    assert.deepEqual(body.buckets, []);
    assert.equal(body.device_id, "ghost");
    // No shadow is created by the query.
    assert.equal((await fetch(`${started.httpUrl}/devices/ghost/shadow`)).status, 404);
  });
});

test("samples survive a service restart on the same database", async () => {
  const dir = await promisify(mkdtemp)(join(tmpdir(), "shadowlink-"));
  const database = join(dir, "restart.db");
  try {
    const first = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database });
    first.service.store.insertTelemetrySample("dev1", "temp", EPOCH + 1000, 42);
    await first.close();
    const second = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database });
    try {
      const { status, body } = await getTelemetry(second, telemetryPath("dev1", { aggregate: "max" }));
      assert.equal(status, 200);
      assert.equal(body.buckets[0].value, 42);
    } finally {
      await second.close();
    }
  } finally {
    await promisify(rm)(dir, { recursive: true, force: true });
  }
});

test("a retained telemetry publication is both retained and sampled", async () => {
  const now = clock(EPOCH);
  await withServer(async (started) => {
    await publishRaw(started, "dev", "$telemetry/dev1/temp", "11", { retain: true });
    const late = await MqttClient.connect(started.broker.port, { clientId: "late" });
    late.write(subscribePacket(1, [{ filter: "$telemetry/dev1/temp", qos: 0 }]));
    const received = publishFields(await late.next(mqtt.PACKET.PUBLISH));
    assert.equal(received.text, "11");
    assert.equal(received.retain, true);
    await late.disconnect();
    const { body } = await getTelemetry(started, telemetryPath("dev1", { aggregate: "sum" }));
    assert.equal(body.buckets[0].value, 11);
  }, { now });
});

const VALID = "metric=temp&from=2024-01-01T00:00:00Z&to=2024-01-01T01:00:00Z&bucket_seconds=60&aggregate=avg";

const BAD_REQUESTS = [
  ["missing metric", VALID.replace("metric=temp&", "")],
  ["missing from", VALID.replace("from=2024-01-01T00:00:00Z&", "")],
  ["missing to", VALID.replace("&to=2024-01-01T01:00:00Z", "")],
  ["missing bucket_seconds", VALID.replace("&bucket_seconds=60", "")],
  ["missing aggregate", VALID.replace("&aggregate=avg", "")],
  ["duplicated metric", `${VALID}&metric=temp`],
  ["unknown parameter", `${VALID}&extra=1`],
  ["from without Z", "metric=temp&from=2024-01-01T00:00:00&to=2024-01-01T01:00:00Z&bucket_seconds=60&aggregate=avg"],
  ["from not a timestamp", "metric=temp&from=soon&to=2024-01-01T01:00:00Z&bucket_seconds=60&aggregate=avg"],
  ["from equal to to", "metric=temp&from=2024-01-01T00:00:00Z&to=2024-01-01T00:00:00Z&bucket_seconds=60&aggregate=avg"],
  ["from later than to", "metric=temp&from=2024-01-01T01:00:00Z&to=2024-01-01T00:00:00Z&bucket_seconds=60&aggregate=avg"],
  ["bucket zero", VALID.replace("bucket_seconds=60", "bucket_seconds=0")],
  ["bucket too large", VALID.replace("bucket_seconds=60", "bucket_seconds=86401")],
  ["bucket fractional", VALID.replace("bucket_seconds=60", "bucket_seconds=6.5")],
  ["bucket negative", VALID.replace("bucket_seconds=60", "bucket_seconds=-5")],
  ["bucket non-numeric", VALID.replace("bucket_seconds=60", "bucket_seconds=ten")],
  ["bad aggregate", VALID.replace("aggregate=avg", "aggregate=mean")],
  ["empty aggregate", VALID.replace("aggregate=avg", "aggregate=")],
];

for (const [label, query] of BAD_REQUESTS) {
  test(`rejects ${label}`, async () => {
    await withServer(async (started) => {
      const response = await fetch(`${started.httpUrl}/devices/dev1/telemetry?${query}`);
      assert.equal(response.status, 400);
      const body = await response.json();
      assert.equal(body.error.code, "validation_error");
    });
  });
}

test("rejects an illegal device id or metric", async () => {
  await withServer(async (started) => {
    const badDevice = await fetch(`${started.httpUrl}/devices/bad%2Fid/telemetry?${VALID}`);
    assert.equal(badDevice.status, 400);
    assert.equal((await badDevice.json()).error.code, "validation_error");
    const badMetric = await fetch(
      `${started.httpUrl}/devices/dev1/telemetry?${VALID.replace("metric=temp", "metric=bad/metric")}`,
    );
    assert.equal(badMetric.status, 400);
    assert.equal((await badMetric.json()).error.code, "validation_error");
  });
});

test("bucket_seconds accepts the full 1 to 86400 range", async () => {
  await withServer(async (started) => {
    sampleAt(started.service, "dev1", "temp", EPOCH, 1);
    for (const width of [1, 3600, 86400]) {
      const response = await fetch(
        `${started.httpUrl}/devices/dev1/telemetry?metric=temp&from=2024-01-01T00:00:00Z&to=2024-01-02T00:00:00Z&bucket_seconds=${width}&aggregate=count`,
      );
      assert.equal(response.status, 200);
      const body = await response.json();
      assert.equal(body.buckets[0].count, 1);
    }
  });
});
