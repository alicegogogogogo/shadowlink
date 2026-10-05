import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import * as mqtt from "../src/mqtt.js";
import { start } from "../src/server.js";
import { MqttClient, connack, publishFields, suback, subscribePacket, waitFor } from "./helpers.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";
const FUTURE = "2024-01-01T01:00:00Z";

async function withServer(run, now = CLOCK) {
  const started = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database: ":memory:", now });
  try {
    await run(started);
  } finally {
    await started.close();
  }
}

async function post(url, body, key) {
  const headers = { "Content-Type": "application/json" };
  if (key !== undefined) {
    headers["Idempotency-Key"] = key;
  }
  const response = await fetch(url, { method: "POST", headers, body: JSON.stringify(body) });
  return { status: response.status, body: await response.json() };
}

function commandBody(overrides = {}) {
  return { id: "cmd-1", payload: { mode: "eco" }, expires_at: FUTURE, ...overrides };
}

async function fetchCommand(started, deviceId, commandId) {
  const response = await fetch(`${started.httpUrl}/devices/${deviceId}/commands/${commandId}`);
  return response.json();
}

/** Poll a command until it reaches the expected status; returns the document. */
async function waitForStatus(started, deviceId, commandId, status) {
  const deadline = Date.now() + 4000;
  for (;;) {
    const document = await fetchCommand(started, deviceId, commandId);
    if (document.status === status) {
      return document;
    }
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for command ${commandId} to become ${status}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

/** Connect a persistent session for `deviceId` and subscribe to its command topic at QoS 1. */
async function commandClient(port, deviceId, qos = 1) {
  const client = await MqttClient.connect(port, { clientId: deviceId, cleanSession: false });
  await client.next(mqtt.PACKET.CONNACK);
  client.write(subscribePacket(1, [{ filter: `$commands/${deviceId}`, qos }]));
  await client.next(mqtt.PACKET.SUBACK);
  return client;
}

test("POST creates a queued command and GET reads it back", async () => {
  await withServer(async (started) => {
    const created = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    assert.equal(created.status, 201);
    assert.deepEqual(created.body, {
      device_id: "device-1",
      id: "cmd-1",
      payload: { mode: "eco" },
      status: "queued",
      created_at: "2024-01-01T00:00:00.000Z",
      expires_at: "2024-01-01T01:00:00.000Z",
      delivered_at: null,
    });
    const response = await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), created.body);
  });
});

test("a command payload may be any JSON value, including null", async () => {
  await withServer(async (started) => {
    const created = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ payload: null }), "k1");
    assert.equal(created.status, 201);
    assert.equal(created.body.payload, null);
    const number = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ id: "cmd-2", payload: 42 }), "k2");
    assert.equal(number.status, 201);
    assert.equal(number.body.payload, 42);
  });
});

test("command bodies reject unknown, missing and invalid fields", async () => {
  await withServer(async (started) => {
    const cases = [
      ["unknown field", commandBody({ status: "queued" })],
      ["missing id", { payload: 1, expires_at: FUTURE }],
      ["missing payload", { id: "cmd-1", expires_at: FUTURE }],
      ["missing expires_at", { id: "cmd-1", payload: 1 }],
      ["illegal command id", commandBody({ id: "bad id" })],
      ["expires_at without Z", commandBody({ expires_at: "2024-01-01T01:00:00" })],
      ["expires_at with offset", commandBody({ expires_at: "2024-01-01T01:00:00+00:00" })],
      ["impossible expires_at", commandBody({ expires_at: "2024-02-30T00:00:00Z" })],
      ["expires_at not a string", commandBody({ expires_at: 42 })],
      ["expires_at in the past", commandBody({ expires_at: "2023-12-31T23:59:59Z" })],
      ["expires_at at the receipt moment", commandBody({ expires_at: "2024-01-01T00:00:00Z" })],
      ["not an object", ["cmd-1"]],
    ];
    for (const [label, body] of cases) {
      const result = await post(`${started.httpUrl}/devices/device-1/commands`, body, `key-${label}`);
      assert.equal(result.status, 400, label);
      assert.equal(result.body.error.code, "validation_error", label);
    }
    const wrongType = await fetch(`${started.httpUrl}/devices/device-1/commands`, { method: "POST", body: "{}" });
    assert.equal(wrongType.status, 400);
    assert.equal((await wrongType.json()).error.code, "validation_error");
    const missingKey = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), undefined);
    assert.equal(missingKey.status, 400);
    assert.equal(missingKey.body.error.code, "validation_error");
  });
});

test("command creation is idempotent and duplicate ids conflict", async () => {
  await withServer(async (started) => {
    const created = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    assert.equal(created.status, 201);
    const replay = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ payload: { mode: "auto" } }), "k1");
    assert.equal(replay.status, 201);
    assert.deepEqual(replay.body, created.body);
    const duplicate = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k2");
    assert.equal(duplicate.status, 409);
    assert.equal(duplicate.body.error.code, "conflict");
    const sameIdElsewhere = await post(`${started.httpUrl}/devices/device-2/commands`, commandBody(), "k3");
    assert.equal(sameIdElsewhere.status, 201);
    const reusedKey = await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ id: "cmd-2" }), "k1");
    assert.equal(reusedKey.status, 409);
    assert.equal(reusedKey.body.error.code, "conflict");
  });
});

test("querying an unknown command is a 404, an illegal id a 400", async () => {
  await withServer(async (started) => {
    const missing = await fetch(`${started.httpUrl}/devices/device-1/commands/nope`);
    assert.equal(missing.status, 404);
    assert.equal((await missing.json()).error.code, "not_found");
    const illegal = await fetch(`${started.httpUrl}/devices/device-1/commands/bad%20id`);
    assert.equal(illegal.status, 400);
    assert.equal((await illegal.json()).error.code, "validation_error");
  });
});

test("commands survive a restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shadowlink-commands-"));
  const database = join(directory, "shadowlink.db");
  try {
    const first = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database, now: CLOCK });
    const created = await post(`${first.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    assert.equal(created.status, 201);
    await first.close();
    const second = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database, now: CLOCK });
    const response = await fetch(`${second.httpUrl}/devices/device-1/commands/cmd-1`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), created.body);
    await second.close();
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("queued commands are delivered in creation order, one PUBACK at a time", async () => {
  await withServer(async (started) => {
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ id: "cmd-2", payload: [1, 2] }), "k2");
    const client = await commandClient(started.broker.port, "device-1");
    const first = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.equal(first.topic, "$commands/device-1");
    assert.equal(first.qos, 1);
    assert.equal(first.retain, false);
    assert.equal(first.dup, false);
    assert.deepEqual(JSON.parse(first.text), { id: "cmd-1", payload: { mode: "eco" } });
    // The second command waits for the first one's PUBACK.
    await assert.rejects(client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    client.write(mqtt.encodePuback(first.packetId));
    const second = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(second.text), { id: "cmd-2", payload: [1, 2] });
    assert.notEqual(second.packetId, first.packetId);
    const delivered = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`)).json();
    assert.equal(delivered.status, "delivered");
    assert.equal(delivered.delivered_at, "2024-01-01T00:00:00.000Z");
    const queued = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-2`)).json();
    assert.equal(queued.status, "queued");
    assert.equal(queued.delivered_at, null);
    client.write(mqtt.encodePuback(second.packetId));
    await waitForStatus(started, "device-1", "cmd-2", "delivered");
    await client.disconnect();
  });
});

test("commands created while the device is subscribed are sent immediately", async () => {
  await withServer(async (started) => {
    const client = await commandClient(started.broker.port, "device-1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    const received = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(received.text), { id: "cmd-1", payload: { mode: "eco" } });
    client.write(mqtt.encodePuback(received.packetId));
    await client.disconnect();
  });
});

test("other clients and lesser subscriptions never receive commands", async () => {
  await withServer(async (started) => {
    // A persistent session that subscribed only at QoS 0.
    await post(`${started.httpUrl}/devices/dev-q0/commands`, commandBody(), "k1");
    const qos0 = await commandClient(started.broker.port, "dev-q0", 0);
    await assert.rejects(qos0.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    // A clean-session connection with the device's client id.
    await post(`${started.httpUrl}/devices/dev-clean/commands`, commandBody(), "k2");
    const clean = await MqttClient.connect(started.broker.port, { clientId: "dev-clean", cleanSession: true });
    await clean.next(mqtt.PACKET.CONNACK);
    clean.write(subscribePacket(1, [{ filter: "$commands/dev-clean", qos: 1 }]));
    await clean.next(mqtt.PACKET.SUBACK);
    await assert.rejects(clean.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    // A different persistent client subscribed to the device's exact topic.
    await post(`${started.httpUrl}/devices/dev-other/commands`, commandBody(), "k3");
    const other = await MqttClient.connect(started.broker.port, { clientId: "stranger", cleanSession: false });
    await other.next(mqtt.PACKET.CONNACK);
    other.write(subscribePacket(1, [{ filter: "$commands/dev-other", qos: 1 }, { filter: "$commands/#", qos: 1 }]));
    await other.next(mqtt.PACKET.SUBACK);
    await assert.rejects(other.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    // A wildcard-only subscription of the device itself does not qualify either.
    await post(`${started.httpUrl}/devices/dev-wild/commands`, commandBody(), "k4");
    const wild = await MqttClient.connect(started.broker.port, { clientId: "dev-wild", cleanSession: false });
    await wild.next(mqtt.PACKET.CONNACK);
    wild.write(subscribePacket(1, [{ filter: "$commands/#", qos: 1 }]));
    await wild.next(mqtt.PACKET.SUBACK);
    await assert.rejects(wild.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await qos0.disconnect();
    await clean.disconnect();
    await other.disconnect();
    await wild.disconnect();
  });
});

test("an unacknowledged command is resent with its packet id and DUP set", async () => {
  await withServer(async (started) => {
    const client = await commandClient(started.broker.port, "device-1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    const first = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    client.destroy();
    await waitFor(() => started.broker.clientIds.length === 0);
    const reconnected = await MqttClient.connect(started.broker.port, { clientId: "device-1", cleanSession: false });
    assert.deepEqual(connack(await reconnected.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    const resent = publishFields(await reconnected.next(mqtt.PACKET.PUBLISH));
    assert.equal(resent.topic, "$commands/device-1");
    assert.equal(resent.packetId, first.packetId);
    assert.equal(resent.dup, true);
    assert.equal(resent.retain, false);
    reconnected.write(mqtt.encodePuback(resent.packetId));
    await waitForStatus(started, "device-1", "cmd-1", "delivered");
    await reconnected.disconnect();
  });
});

test("unrelated and duplicate PUBACKs do not change command state", async () => {
  await withServer(async (started) => {
    const client = await commandClient(started.broker.port, "device-1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    const received = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    client.write(mqtt.encodePuback(60000));
    await assert.rejects(client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    const queued = await fetchCommand(started, "device-1", "cmd-1");
    assert.equal(queued.status, "queued");
    client.write(mqtt.encodePuback(received.packetId));
    client.write(mqtt.encodePuback(received.packetId));
    const document = await waitForStatus(started, "device-1", "cmd-1", "delivered");
    assert.equal(document.delivered_at, "2024-01-01T00:00:00.000Z");
    await client.disconnect();
  });
});

test("undelivered commands expire and are never sent", async () => {
  let clock = "2024-01-01T00:00:00.000Z";
  await withServer(async (started) => {
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ expires_at: "2024-01-01T00:00:01Z" }), "k1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ id: "cmd-2" }), "k2");
    clock = "2024-01-01T00:00:01.000Z";
    const expired = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`)).json();
    assert.equal(expired.status, "expired");
    assert.equal(expired.delivered_at, null);
    // Only the still-valid second command is delivered once the device is ready.
    const client = await commandClient(started.broker.port, "device-1");
    const received = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(JSON.parse(received.text), { id: "cmd-2", payload: { mode: "eco" } });
    await assert.rejects(client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    client.write(mqtt.encodePuback(received.packetId));
    const again = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`)).json();
    assert.equal(again.status, "expired");
    await client.disconnect();
  }, () => clock);
});

test("an in-flight command that expires frees the queue without being delivered", async () => {
  let clock = "2024-01-01T00:00:00.000Z";
  await withServer(async (started) => {
    const client = await commandClient(started.broker.port, "device-1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ expires_at: "2024-01-01T00:00:01Z" }), "k1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody({ id: "cmd-2" }), "k2");
    const first = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.equal(JSON.parse(first.text).id, "cmd-1");
    clock = "2024-01-01T00:00:02.000Z";
    // The query performs the expiry judgement and the next command is sent.
    const expired = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`)).json();
    assert.equal(expired.status, "expired");
    assert.equal(expired.delivered_at, null);
    const second = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    assert.equal(JSON.parse(second.text).id, "cmd-2");
    // The late PUBACK of the expired command changes nothing.
    client.write(mqtt.encodePuback(first.packetId));
    const still = await (await fetch(`${started.httpUrl}/devices/device-1/commands/cmd-1`)).json();
    assert.equal(still.status, "expired");
    client.write(mqtt.encodePuback(second.packetId));
    await client.disconnect();
  }, () => clock);
});

test("command delivery creates no events and no retained messages", async () => {
  await withServer(async (started) => {
    const rule = await post(
      `${started.httpUrl}/rules`,
      { id: "commands", source: "mqtt", topic: "$commands/#", path: "payload", operator: "exists", event: "command_seen" },
      "rule-1",
    );
    assert.equal(rule.status, 201);
    const client = await commandClient(started.broker.port, "device-1");
    await post(`${started.httpUrl}/devices/device-1/commands`, commandBody(), "k1");
    const received = publishFields(await client.next(mqtt.PACKET.PUBLISH));
    client.write(mqtt.encodePuback(received.packetId));
    const events = await (await fetch(`${started.httpUrl}/events`)).json();
    assert.deepEqual(events, { events: [] });
    assert.deepEqual(started.service.store.listRetained(), []);
    await client.disconnect();
  });
});
