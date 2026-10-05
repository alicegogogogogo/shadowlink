import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import * as mqtt from "../src/mqtt.js";
import { start } from "../src/server.js";
import { MqttClient, connack, publishFields, subscribePacket, waitFor } from "./helpers.js";

const CLOCK = "2024-01-01T00:00:00.000Z";
const FUTURE = "2024-01-01T01:00:00.000Z";

async function withServer(run, options = {}) {
  const started = await start({
    host: "127.0.0.1",
    port: 0,
    mqttPort: 0,
    database: ":memory:",
    now: options.now ?? (() => CLOCK),
  });
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

async function getCommand(started, deviceId, commandId) {
  const response = await fetch(`${started.httpUrl}/devices/${deviceId}/commands/${commandId}`);
  return { status: response.status, body: await response.json() };
}

function createCommand(started, deviceId, command, key) {
  return post(`${started.httpUrl}/devices/${deviceId}/commands`, command, key);
}

async function connectDevice(started, deviceId, options = {}) {
  const client = await MqttClient.connect(started.broker.port, { clientId: deviceId, cleanSession: false, ...options });
  const ack = connack(await client.next(mqtt.PACKET.CONNACK));
  return { client, ack };
}

function commandStatus(started, deviceId, commandId) {
  return started.service.store.getCommand(deviceId, commandId)?.status;
}

test("POST creates a queued command and GET returns it", async () => {
  await withServer(async (started) => {
    const created = await createCommand(started, "device-1", { id: "reboot", payload: { delay: 5 }, expires_at: FUTURE }, "c1");
    assert.equal(created.status, 201);
    assert.deepEqual(created.body, {
      device_id: "device-1",
      id: "reboot",
      payload: { delay: 5 },
      status: "queued",
      created_at: CLOCK,
      expires_at: FUTURE,
      delivered_at: null,
    });
    const fetched = await getCommand(started, "device-1", "reboot");
    assert.equal(fetched.status, 200);
    assert.deepEqual(fetched.body, created.body);
  });
});

test("command payloads may be any JSON value", async () => {
  await withServer(async (started) => {
    for (const [index, payload] of [null, 42, "text", [1, 2], { nested: { ok: true } }].entries()) {
      const created = await createCommand(started, "device-1", { id: `cmd-${index}`, payload, expires_at: FUTURE }, `k-${index}`);
      assert.equal(created.status, 201);
      assert.deepEqual(created.body.payload, payload);
    }
  });
});

test("command creation rejects malformed bodies and timestamps", async () => {
  await withServer(async (started) => {
    const expectBad = async (body) => {
      const response = await createCommand(started, "device-1", body, `k-${Math.random()}`);
      assert.equal(response.status, 400);
      assert.equal(response.body.error.code, "validation_error");
    };
    await expectBad({ id: "a", payload: 1, expires_at: FUTURE, extra: true });
    await expectBad({ payload: 1, expires_at: FUTURE });
    await expectBad({ id: "a", expires_at: FUTURE });
    await expectBad({ id: "a", payload: 1 });
    await expectBad({ id: "bad id!", payload: 1, expires_at: FUTURE });
    await expectBad({ id: "", payload: 1, expires_at: FUTURE });
    await expectBad({ id: "a", payload: 1, expires_at: "2024-01-01 01:00:00" });
    await expectBad({ id: "a", payload: 1, expires_at: "2024-01-01T01:00:00+01:00" });
    await expectBad({ id: "a", payload: 1, expires_at: "2024-02-30T01:00:00Z" });
    await expectBad({ id: "a", payload: 1, expires_at: "2024-01-01T01:00:00.0001Z" });
    // Not strictly in the future: the clock is exactly CLOCK.
    await expectBad({ id: "a", payload: 1, expires_at: CLOCK });
    await expectBad({ id: "a", payload: 1, expires_at: "2023-12-31T23:59:59.999Z" });
    await expectBad("not an object");
    const badDevice = await createCommand(started, "bad device", { id: "a", payload: 1, expires_at: FUTURE }, "kd");
    assert.equal(badDevice.status, 400);
    const missingKey = await createCommand(started, "device-1", { id: "a", payload: 1, expires_at: FUTURE }, undefined);
    assert.equal(missingKey.status, 400);
    assert.equal(missingKey.body.error.code, "validation_error");
    const wrongType = await fetch(`${started.httpUrl}/devices/device-1/commands`, { method: "POST", body: "{}" });
    assert.equal(wrongType.status, 400);
    assert.equal((await wrongType.json()).error.code, "validation_error");
  });
});

test("idempotent replay returns the first response and a duplicate id conflicts", async () => {
  await withServer(async (started) => {
    const command = { id: "reboot", payload: 1, expires_at: FUTURE };
    const first = await createCommand(started, "device-1", command, "c1");
    assert.equal(first.status, 201);
    const replay = await createCommand(started, "device-1", { ...command, payload: 999 }, "c1");
    assert.equal(replay.status, 201);
    assert.deepEqual(replay.body, first.body);
    const duplicate = await createCommand(started, "device-1", command, "c2");
    assert.equal(duplicate.status, 409);
    assert.equal(duplicate.body.error.code, "conflict");
    // The same id is free on another device, and another operation with the
    // same key conflicts.
    const otherDevice = await createCommand(started, "device-2", command, "c3");
    assert.equal(otherDevice.status, 201);
    const reused = await createCommand(started, "device-1", { ...command, id: "other" }, "c1");
    assert.equal(reused.status, 409);
  });
});

test("GET reports 404 for unknown commands and 400 for bad identifiers", async () => {
  await withServer(async (started) => {
    const missing = await getCommand(started, "device-1", "nope");
    assert.equal(missing.status, 404);
    assert.equal(missing.body.error.code, "not_found");
    const badCommand = await getCommand(started, "device-1", "bad id");
    assert.equal(badCommand.status, 400);
    const badDevice = await getCommand(started, "bad%20device", "x");
    assert.equal(badDevice.status, 400);
  });
});

test("an undelivered command expires and is never sent", async () => {
  let now = CLOCK;
  await withServer(
    async (started) => {
      const created = await createCommand(started, "device-1", { id: "a", payload: 1, expires_at: "2024-01-01T00:00:10.000Z" }, "c1");
      assert.equal(created.body.status, "queued");
      now = "2024-01-01T00:00:10.000Z";
      const fetched = await getCommand(started, "device-1", "a");
      assert.equal(fetched.body.status, "expired");
      assert.equal(fetched.body.delivered_at, null);
      // Connecting an eligible device now must not deliver anything.
      const device = await connectDevice(started, "device-1");
      device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
      await device.client.next(mqtt.PACKET.SUBACK);
      await assert.rejects(device.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
      await device.client.disconnect();
    },
    { now: () => now },
  );
});

test("commands are delivered one at a time in creation order and acknowledged", async () => {
  await withServer(async (started) => {
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);

    await createCommand(started, "device-1", { id: "first", payload: { n: 1 }, expires_at: FUTURE }, "c1");
    await createCommand(started, "device-1", { id: "second", payload: [2], expires_at: FUTURE }, "c2");

    const first = publishFields(await device.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([first.topic, first.qos, first.retain, first.dup], ["$commands/device-1", 1, false, false]);
    assert.deepEqual(JSON.parse(first.text), { id: "first", payload: { n: 1 } });
    // Only one command is in flight: the second waits for the PUBACK.
    await assert.rejects(device.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    let fetched = await getCommand(started, "device-1", "first");
    assert.equal(fetched.body.status, "queued");

    device.client.write(mqtt.encodePuback(first.packetId));
    const second = publishFields(await device.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(second.text), { id: "second", payload: [2] });
    assert.notEqual(second.packetId, first.packetId);
    fetched = await getCommand(started, "device-1", "first");
    assert.equal(fetched.body.status, "delivered");
    assert.equal(fetched.body.delivered_at, CLOCK);
    fetched = await getCommand(started, "device-1", "second");
    assert.equal(fetched.body.status, "queued");

    // Unrelated and duplicate PUBACKs change nothing.
    device.client.write(mqtt.encodePuback(first.packetId));
    device.client.write(mqtt.encodePuback(400));
    await assert.rejects(device.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    fetched = await getCommand(started, "device-1", "second");
    assert.equal(fetched.body.status, "queued");
    device.client.write(mqtt.encodePuback(second.packetId));
    await waitFor(() => commandStatus(started, "device-1", "second") === "delivered");
    await device.client.disconnect();
  });
});

test("only a persistent session subscribed at qos 1 to the exact topic receives commands", async () => {
  await withServer(async (started) => {
    // A clean-session connection with the device id is not eligible.
    const clean = await MqttClient.connect(started.broker.port, { clientId: "device-1", cleanSession: true });
    await clean.next(mqtt.PACKET.CONNACK);
    clean.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await clean.next(mqtt.PACKET.SUBACK);
    // A wildcard subscriber and a qos 0 exact subscriber are not eligible.
    const wildcard = await MqttClient.connect(started.broker.port, { clientId: "watcher", cleanSession: false });
    await wildcard.next(mqtt.PACKET.CONNACK);
    wildcard.write(subscribePacket(1, [{ filter: "$commands/#", qos: 1 }]));
    await wildcard.next(mqtt.PACKET.SUBACK);
    const qos0 = await connectDevice(started, "device-2");
    qos0.client.write(subscribePacket(1, [{ filter: "$commands/device-2", qos: 0 }]));
    await qos0.client.next(mqtt.PACKET.SUBACK);

    await createCommand(started, "device-1", { id: "a", payload: 1, expires_at: FUTURE }, "c1");
    await createCommand(started, "device-2", { id: "b", payload: 2, expires_at: FUTURE }, "c2");
    await assert.rejects(clean.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await assert.rejects(wildcard.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await assert.rejects(qos0.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    assert.equal((await getCommand(started, "device-1", "a")).body.status, "queued");
    assert.equal((await getCommand(started, "device-2", "b")).body.status, "queued");

    // The eligible persistent session receives both queued commands on
    // subscribe, one PUBACK at a time.
    await clean.disconnect();
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);
    const delivered = publishFields(await device.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(delivered.text), { id: "a", payload: 1 });
    device.client.write(mqtt.encodePuback(delivered.packetId));
    await waitFor(() => commandStatus(started, "device-1", "a") === "delivered");
    await device.client.disconnect();
    await wildcard.disconnect();
    await qos0.client.disconnect();
  });
});

test("commands created while offline are delivered on reconnect, in order", async () => {
  await withServer(async (started) => {
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);
    await device.client.disconnect();

    await createCommand(started, "device-1", { id: "one", payload: 1, expires_at: FUTURE }, "c1");
    await createCommand(started, "device-1", { id: "two", payload: 2, expires_at: FUTURE }, "c2");

    const restored = await connectDevice(started, "device-1");
    assert.equal(restored.ack.sessionPresent, true);
    const first = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([JSON.parse(first.text).id, first.dup], ["one", false]);
    await assert.rejects(restored.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    restored.client.write(mqtt.encodePuback(first.packetId));
    const second = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([JSON.parse(second.text).id, second.dup], ["two", false]);
    restored.client.write(mqtt.encodePuback(second.packetId));
    await waitFor(() => commandStatus(started, "device-1", "two") === "delivered");
    await restored.client.disconnect();
  });
});

test("an unacknowledged command is resent with its packet id and DUP set", async () => {
  await withServer(async (started) => {
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);
    await createCommand(started, "device-1", { id: "a", payload: 1, expires_at: FUTURE }, "c1");
    const sent = publishFields(await device.client.next(mqtt.PACKET.PUBLISH));
    device.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 0);

    const restored = await connectDevice(started, "device-1");
    assert.equal(restored.ack.sessionPresent, true);
    const retried = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([retried.packetId, retried.dup, JSON.parse(retried.text).id], [sent.packetId, true, "a"]);
    restored.client.write(mqtt.encodePuback(retried.packetId));
    await waitFor(() => commandStatus(started, "device-1", "a") === "delivered");
    await restored.client.disconnect();
  });
});

test("command delivery creates no retained message, event or rule evaluation", async () => {
  await withServer(async (started) => {
    const rule = await post(
      `${started.httpUrl}/rules`,
      { id: "cmds", source: "mqtt", topic: "$commands/#", path: "payload", operator: "exists", event: "command_seen" },
      "r1",
    );
    assert.equal(rule.status, 201);
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);
    await createCommand(started, "device-1", { id: "a", payload: 1, expires_at: FUTURE }, "c1");
    const delivered = publishFields(await device.client.next(mqtt.PACKET.PUBLISH));
    device.client.write(mqtt.encodePuback(delivered.packetId));
    await waitFor(() => commandStatus(started, "device-1", "a") === "delivered");
    assert.equal(started.service.store.listRetained().length, 0);
    assert.deepEqual(started.service.store.listEvents(), []);
    await device.client.disconnect();
  });
});

test("commands survive a broker restart and are delivered afterwards", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shadowlink-"));
  const database = join(directory, "shadowlink.db");
  let started = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database, now: () => CLOCK });
  try {
    const device = await connectDevice(started, "device-1");
    device.client.write(subscribePacket(1, [{ filter: "$commands/device-1", qos: 1 }]));
    await device.client.next(mqtt.PACKET.SUBACK);
    await device.client.disconnect();
    await createCommand(started, "device-1", { id: "a", payload: { reset: true }, expires_at: FUTURE }, "c1");
    const mqttPort = started.broker.port;
    await started.close();

    started = await start({ host: "127.0.0.1", port: 0, mqttPort, database, now: () => CLOCK });
    const fetched = await getCommand(started, "device-1", "a");
    assert.equal(fetched.body.status, "queued");
    const restored = await connectDevice(started, "device-1");
    assert.equal(restored.ack.sessionPresent, true);
    const delivered = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(delivered.text), { id: "a", payload: { reset: true } });
    restored.client.write(mqtt.encodePuback(delivered.packetId));
    await waitFor(() => commandStatus(started, "device-1", "a") === "delivered");
    await restored.client.disconnect();
  } finally {
    await started.close();
    rmSync(directory, { recursive: true, force: true });
  }
});
