import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import net from "node:net";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { Broker } from "../src/broker.js";
import * as mqtt from "../src/mqtt.js";
import { Service } from "../src/service.js";
import {
  MqttClient,
  connack,
  pingreqPacket,
  publishFields,
  publishPacket,
  suback,
  subscribePacket,
  waitFor,
} from "./helpers.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";

async function startBroker(database = ":memory:", port = 0) {
  const service = new Service(database, { now: CLOCK });
  const broker = new Broker(service, { host: "127.0.0.1", port });
  service.setPublisher((topic, payload, options) => broker.publish(topic, payload, options));
  const address = await broker.listen();
  return {
    service,
    broker,
    port: address.port,
    close: async () => {
      await broker.close();
      service.close();
    },
  };
}

test("a connection performs CONNECT, PINGREQ and DISCONNECT", async () => {
  const started = await startBroker();
  try {
    const client = await MqttClient.connect(started.port, { clientId: "device-1", keepAlive: 0 });
    assert.deepEqual(connack(await client.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 0 });
    assert.deepEqual(started.broker.clientIds, ["device-1"]);
    client.write(pingreqPacket());
    await client.next(mqtt.PACKET.PINGRESP);
    await client.disconnect();
    await waitFor(() => started.broker.clientIds.length === 0);
  } finally {
    await started.close();
  }
});

test("qos 0 publications reach matching wildcard subscriptions", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub" });
    subscriber.write(subscribePacket(1, [{ filter: "sensors/+/temp", qos: 0 }, { filter: "alerts/#", qos: 0 }]));
    assert.deepEqual(suback(await subscriber.next(mqtt.PACKET.SUBACK)), { packetId: 1, returnCodes: [0, 0] });
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/kitchen/temp", payload: "21" }));
    const received = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual(
      { topic: received.topic, text: received.text, qos: received.qos, retain: received.retain, dup: received.dup },
      { topic: "sensors/kitchen/temp", text: "21", qos: 0, retain: false, dup: false },
    );
    publisher.write(publishPacket({ topic: "sensors/kitchen/humidity", payload: "40" }));
    await assert.rejects(subscriber.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await subscriber.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("qos 1 publications are acknowledged once and duplicate packet ids are dropped", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub" });
    subscriber.write(subscribePacket(1, [{ filter: "readings/#", qos: 1 }]));
    assert.deepEqual(suback(await subscriber.next(mqtt.PACKET.SUBACK)), { packetId: 1, returnCodes: [1] });
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "readings/a", payload: "1", qos: 1, packetId: 5 }));
    assert.equal((await publisher.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 5);
    const first = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.equal(first.qos, 1);
    assert.equal(first.packetId, 1);
    subscriber.write(mqtt.encodePuback(first.packetId));
    publisher.write(publishPacket({ topic: "readings/a", payload: "1", qos: 1, packetId: 5, dup: true }));
    assert.equal((await publisher.next(mqtt.PACKET.PUBACK)).body.readUInt16BE(0), 5);
    await assert.rejects(subscriber.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await subscriber.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("retained publications are replayed to new subscribers with the retain flag", async () => {
  const started = await startBroker();
  try {
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "status/device-1", payload: "online", retain: true }));
    await waitFor(() => started.service.store.listRetained().length === 1);
    const late = await MqttClient.connect(started.port, { clientId: "late" });
    late.write(subscribePacket(1, [{ filter: "status/#", qos: 0 }]));
    await late.next(mqtt.PACKET.SUBACK);
    const retained = publishFields(await late.next(mqtt.PACKET.PUBLISH));
    assert.equal(retained.topic, "status/device-1");
    assert.equal(retained.text, "online");
    assert.equal(retained.retain, true);
    publisher.write(publishPacket({ topic: "status/device-1", payload: "", retain: true }));
    await waitFor(() => started.service.store.listRetained().length === 0);
    await publisher.disconnect();
    await late.disconnect();
  } finally {
    await started.close();
  }
});

test("the will is published when a client disappears without DISCONNECT", async () => {
  const started = await startBroker();
  try {
    const watcher = await MqttClient.connect(started.port, { clientId: "watcher" });
    watcher.write(subscribePacket(1, [{ filter: "clients/#", qos: 0 }]));
    await watcher.next(mqtt.PACKET.SUBACK);
    const dying = await MqttClient.connect(started.port, {
      clientId: "dying",
      will: { topic: "clients/dying", payload: "offline", qos: 0, retain: false },
    });
    await dying.next(mqtt.PACKET.CONNACK);
    dying.destroy();
    const will = publishFields(await watcher.next(mqtt.PACKET.PUBLISH));
    assert.equal(will.topic, "clients/dying");
    assert.equal(will.text, "offline");
    const polite = await MqttClient.connect(started.port, {
      clientId: "polite",
      will: { topic: "clients/polite", payload: "offline", qos: 0, retain: false },
    });
    await polite.next(mqtt.PACKET.CONNACK);
    await polite.disconnect();
    await assert.rejects(watcher.next(mqtt.PACKET.PUBLISH, 300), /timed out/);
    await watcher.disconnect();
  } finally {
    await started.close();
  }
});

test("a second connection with the same client identifier is refused", async () => {
  const started = await startBroker();
  try {
    const first = await MqttClient.connect(started.port, { clientId: "twin" });
    await first.next(mqtt.PACKET.CONNACK);
    const second = await MqttClient.connect(started.port, { clientId: "twin" });
    assert.deepEqual(connack(await second.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 2 });
    await second.waitClosed();
    assert.deepEqual(started.broker.clientIds, ["twin"]);
    await first.disconnect();
  } finally {
    await started.close();
  }
});

test("the reserved shadow topic updates the shadow and publishes the delta", async () => {
  const started = await startBroker();
  try {
    const watcher = await MqttClient.connect(started.port, { clientId: "watcher" });
    watcher.write(subscribePacket(1, [{ filter: "$shadow/+/delta", qos: 0 }]));
    await watcher.next(mqtt.PACKET.SUBACK);
    const device = await MqttClient.connect(started.port, { clientId: "device-1" });
    device.write(
      publishPacket({ topic: "$shadow/device-1/update", payload: JSON.stringify({ state: { desired: { power: "on" } } }) }),
    );
    const delta = publishFields(await watcher.next(mqtt.PACKET.PUBLISH));
    assert.equal(delta.topic, "$shadow/device-1/delta");
    assert.deepEqual(JSON.parse(delta.text), { power: "on" });
    assert.equal(started.service.getShadow("device-1").version, 1);
    device.write(publishPacket({ topic: "$shadow/device-1/update", payload: "not json" }));
    await assert.rejects(watcher.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await watcher.disconnect();
    await device.disconnect();
  } finally {
    await started.close();
  }
});

test("a packet that breaks the protocol closes the connection", async () => {
  const started = await startBroker();
  try {
    const socket = net.connect(started.port, "127.0.0.1");
    const client = new MqttClient(socket);
    await new Promise((resolve) => socket.once("connect", resolve));
    client.write(publishPacket({ topic: "a/b", payload: "x" }));
    await client.waitClosed();

    const unsupported = await MqttClient.connect(started.port, { clientId: "unsupported" });
    await unsupported.next(mqtt.PACKET.CONNACK);
    unsupported.write(mqtt.encodePacket(10, 2, Buffer.from([0x00, 0x01])));
    await unsupported.waitClosed();
  } finally {
    await started.close();
  }
});

test("a silent client is dropped after one and a half keep alive intervals", async () => {
  const started = await startBroker();
  try {
    const client = await MqttClient.connect(started.port, { clientId: "quiet", keepAlive: 1 });
    await client.next(mqtt.PACKET.CONNACK);
    await client.waitClosed(4000);
    await waitFor(() => started.broker.clientIds.length === 0);
  } finally {
    await started.close();
  }
});

async function publishQos1(client, options) {
  client.write(publishPacket(options));
  await client.next(mqtt.PACKET.PUBACK);
}

async function connectMqtt(port, options) {
  const client = await MqttClient.connect(port, options);
  return { client, connack: connack(await client.next(mqtt.PACKET.CONNACK)) };
}

test("a clean session 0 connect reports session present and restores subscriptions", async () => {  const started = await startBroker();
  try {
    const first = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    assert.deepEqual(first.connack, { sessionPresent: false, returnCode: 0 });
    first.client.write(subscribePacket(1, [{ filter: "sensors/+", qos: 1 }]));
    await first.client.next(mqtt.PACKET.SUBACK);
    await first.client.disconnect();

    const second = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    assert.deepEqual(second.connack, { sessionPresent: true, returnCode: 0 });
    // No SUBSCRIBE is sent again: the saved subscription still matches.
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    await publishQos1(publisher, { topic: "sensors/a", payload: "1", qos: 1, packetId: 1 });
    const message = publishFields(await second.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(message.topic, "sensors/a");
    assert.equal(message.qos, 1);
    assert.equal(message.dup, false);
    assert.equal(message.retain, false);
    assert.equal(message.packetId, 1);
    await assert.rejects(second.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await second.client.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("an unacknowledged retained replay is redelivered without the retain flag", async () => {
  const started = await startBroker();
  try {
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "status/x", payload: "on", qos: 1, packetId: 1, retain: true }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await waitFor(() => started.service.store.listRetained().length === 1);
    const subscriber = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    subscriber.client.write(subscribePacket(1, [{ filter: "status/#", qos: 1 }]));
    await subscriber.client.next(mqtt.PACKET.SUBACK);
    const replay = publishFields(await subscriber.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(replay.retain, true);
    assert.equal(replay.dup, false);
    assert.equal(replay.packetId, 1);
    subscriber.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 1);

    const restored = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const retried = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(retried.topic, "status/x");
    assert.equal(retried.retain, false);
    assert.equal(retried.dup, true);
    assert.equal(retried.packetId, 1);
    await restored.client.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("restoring a session does not replay retained messages", async () => {
  const started = await startBroker();
  try {
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "status/x", payload: "on", retain: true }));
    await waitFor(() => started.service.store.listRetained().length === 1);
    const first = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    first.client.write(subscribePacket(1, [{ filter: "status/#", qos: 1 }]));
    await first.client.next(mqtt.PACKET.SUBACK);
    const replay = publishFields(await first.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(replay.topic, "status/x");
    assert.equal(replay.retain, true);
    await first.client.disconnect();

    const second = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    assert.equal(second.connack.sessionPresent, true);
    await assert.rejects(second.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await second.client.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("offline sessions queue only effective qos 1 messages, merged across filters, in order", async () => {
  const started = await startBroker();
  try {
    const subscriber = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    subscriber.client.write(
      subscribePacket(1, [
        { filter: "q/#", qos: 1 },
        { filter: "sensors/+/temp", qos: 0 },
        { filter: "sensors/#", qos: 1 },
      ]),
    );
    await subscriber.client.next(mqtt.PACKET.SUBACK);
    await subscriber.client.disconnect();

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    // Effective qos 0 (published at qos 0) is never queued.
    publisher.write(publishPacket({ topic: "q/a", payload: "zero" }));
    await publishQos1(publisher, { topic: "q/b", payload: "one", qos: 1, packetId: 1 });
    // Matches a qos 0 and a qos 1 filter: one copy at the highest granted qos.
    await publishQos1(publisher, { topic: "sensors/x/temp", payload: "merged", qos: 1, packetId: 2 });
    // Published qos 0 is dropped even though the granted qos is 1.
    publisher.write(publishPacket({ topic: "sensors/x/temp", payload: "also-zero" }));
    await publisher.disconnect();

    const restored = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const first = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([first.topic, first.text, first.qos, first.dup, first.packetId], ["q/b", "one", 1, false, 1]);
    const second = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([second.topic, second.text, second.qos, second.dup, second.packetId], [
      "sensors/x/temp",
      "merged",
      1,
      false,
      2,
    ]);
    await assert.rejects(restored.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await restored.client.disconnect();
  } finally {
    await started.close();
  }
});

test("replay sends in-flight messages with DUP first, then queued messages with fresh packet ids", async () => {
  const started = await startBroker();
  try {
    const subscriber = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    subscriber.client.write(subscribePacket(1, [{ filter: "t/#", qos: 1 }]));
    await subscriber.client.next(mqtt.PACKET.SUBACK);
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    await publishQos1(publisher, { topic: "t/1", payload: "a", qos: 1, packetId: 10 });
    await publishQos1(publisher, { topic: "t/2", payload: "b", qos: 1, packetId: 11 });
    const first = publishFields(await subscriber.client.next(mqtt.PACKET.PUBLISH));
    const second = publishFields(await subscriber.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(first.packetId, 1);
    assert.equal(second.packetId, 2);
    // Only packet id 2 is acknowledged before the drop.
    subscriber.client.write(mqtt.encodePuback(second.packetId));
    await waitFor(() => started.service.store.listMqttOutbound("persist").length === 1);
    subscriber.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 1);

    await publishQos1(publisher, { topic: "t/3", payload: "c", qos: 1, packetId: 12 });
    await publishQos1(publisher, { topic: "t/4", payload: "d", qos: 1, packetId: 13 });
    await publisher.disconnect();

    const restored = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const inflight = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([inflight.topic, inflight.packetId, inflight.dup], ["t/1", 1, true]);
    const queued1 = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([queued1.topic, queued1.packetId, queued1.dup], ["t/3", 3, false]);
    const queued2 = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([queued2.topic, queued2.packetId, queued2.dup], ["t/4", 4, false]);
    await assert.rejects(restored.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);

    // Acknowledge two, drop again: only the unacknowledged one comes back.
    restored.client.write(mqtt.encodePuback(1));
    restored.client.write(mqtt.encodePuback(3));
    await waitFor(() => started.service.store.listMqttOutbound("persist").length === 1);
    restored.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 0);
    const again = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const retried = publishFields(await again.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([retried.topic, retried.packetId, retried.dup], ["t/4", 4, true]);
    again.client.write(mqtt.encodePuback(4));
    await waitFor(() => started.service.store.listMqttOutbound("persist").length === 0);
    again.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 0);
    const empty = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    await assert.rejects(empty.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await empty.client.disconnect();
  } finally {
    await started.close();
  }
});

test("queued messages keep their qos when a subscription is later downgraded", async () => {
  const started = await startBroker();
  try {
    const subscriber = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    subscriber.client.write(subscribePacket(1, [{ filter: "t/#", qos: 1 }]));
    await subscriber.client.next(mqtt.PACKET.SUBACK);
    await subscriber.client.disconnect();

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    await publishQos1(publisher, { topic: "t/old", payload: "kept", qos: 1, packetId: 1 });

    const restored = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const queued = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(queued.topic, "t/old");
    restored.client.write(subscribePacket(2, [{ filter: "t/#", qos: 0 }]));
    await restored.client.next(mqtt.PACKET.SUBACK);
    restored.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 1);
    // Published after the downgrade: effective qos 0, not queued.
    await publishQos1(publisher, { topic: "t/new", payload: "dropped", qos: 1, packetId: 2 });
    await publisher.disconnect();

    const again = await connectMqtt(started.port, { clientId: "persist", cleanSession: false });
    const retried = publishFields(await again.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([retried.topic, retried.text, retried.qos, retried.dup], ["t/old", "kept", 1, true]);
    await assert.rejects(again.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await again.client.disconnect();
  } finally {
    await started.close();
  }
});

test("a clean session 1 connect wipes a previous persistent session and starts with session present 0", async () => {
  const started = await startBroker();
  try {
    const persistent = await connectMqtt(started.port, { clientId: "mixed", cleanSession: false });
    persistent.client.write(subscribePacket(1, [{ filter: "t/#", qos: 1 }]));
    await persistent.client.next(mqtt.PACKET.SUBACK);
    await persistent.client.disconnect();
    assert.equal(started.service.store.hasMqttSession("mixed"), true);

    const clean = await connectMqtt(started.port, { clientId: "mixed", cleanSession: true });
    assert.deepEqual(clean.connack, { sessionPresent: false, returnCode: 0 });
    assert.equal(started.service.store.hasMqttSession("mixed"), false);
    await assert.rejects(clean.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await clean.client.disconnect();

    // Nothing persisted: the next clean session 0 connect creates a new session.
    const fresh = await connectMqtt(started.port, { clientId: "mixed", cleanSession: false });
    assert.deepEqual(fresh.connack, { sessionPresent: false, returnCode: 0 });
    await fresh.client.disconnect();
  } finally {
    await started.close();
  }
});

test("a still connected identifier is refused with return code 2 even with clean session 0", async () => {
  const started = await startBroker();
  try {
    const first = await connectMqtt(started.port, { clientId: "twin", cleanSession: false });
    assert.equal(first.connack.sessionPresent, false);
    const second = await connectMqtt(started.port, { clientId: "twin", cleanSession: false });
    assert.deepEqual(second.connack, { sessionPresent: false, returnCode: 2 });
    await second.client.waitClosed();
    assert.deepEqual(started.broker.clientIds, ["twin"]);
    await first.client.disconnect();

    // The refused connect neither created nor replaced the persistent session.
    const restored = await connectMqtt(started.port, { clientId: "twin", cleanSession: false });
    assert.equal(restored.connack.sessionPresent, true);
    await restored.client.disconnect();
  } finally {
    await started.close();
  }
});

test("a graceful DISCONNECT keeps the persistent session and an abnormal one publishes the will into it", async () => {
  const started = await startBroker();
  try {
    const watcher = await connectMqtt(started.port, { clientId: "watcher", cleanSession: false });
    watcher.client.write(subscribePacket(1, [{ filter: "clients/#", qos: 1 }]));
    await watcher.client.next(mqtt.PACKET.SUBACK);
    await watcher.client.disconnect();

    const dying = await MqttClient.connect(started.port, {
      clientId: "dying",
      will: { topic: "clients/dying", payload: "offline", qos: 1, retain: false },
    });
    await dying.next(mqtt.PACKET.CONNACK);
    dying.destroy();
    await waitFor(() => started.service.store.listMqttOutbound("watcher").length === 1);

    const restored = await connectMqtt(started.port, { clientId: "watcher", cleanSession: false });
    const will = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(will.topic, "clients/dying");
    assert.equal(will.text, "offline");
    assert.equal(will.qos, 1);
    await restored.client.disconnect();
  } finally {
    await started.close();
  }
});

test("broker-internal qos 0 publications follow the offline queue rules", async () => {
  const started = await startBroker();
  try {
    const online = await connectMqtt(started.port, { clientId: "online", cleanSession: false });
    online.client.write(subscribePacket(1, [{ filter: "$shadow/dev1/delta", qos: 1 }]));
    await online.client.next(mqtt.PACKET.SUBACK);
    started.service.applyShadowPatch("dev1", { desired: { power: "on" }, reported: null });
    const delivered = publishFields(await online.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(delivered.topic, "$shadow/dev1/delta");
    assert.equal(delivered.qos, 0);
    await online.client.disconnect();

    started.service.applyShadowPatch("dev1", { desired: { power: "off" }, reported: null });
    const restored = await connectMqtt(started.port, { clientId: "online", cleanSession: false });
    await assert.rejects(restored.client.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await restored.client.disconnect();
  } finally {
    await started.close();
  }
});

test("persistent sessions, subscriptions and in-flight messages survive a broker restart", async () => {
  const directory = mkdtempSync(join(tmpdir(), "shadowlink-"));
  const database = join(directory, "shadowlink.db");
  const port = await new Promise((resolve, reject) => {
    const probe = net.createServer();
    probe.once("error", reject);
    probe.listen(0, "127.0.0.1", () => {
      const chosen = probe.address().port;
      probe.close(() => resolve(chosen));
    });
  });
  let started = await startBroker(database, port);
  try {
    const subscriber = await connectMqtt(port, { clientId: "persist", cleanSession: false });
    subscriber.client.write(subscribePacket(1, [{ filter: "t/#", qos: 1 }]));
    await subscriber.client.next(mqtt.PACKET.SUBACK);
    const publisher = await MqttClient.connect(port, { clientId: "pub" });
    await publishQos1(publisher, { topic: "t/inflight", payload: "a", qos: 1, packetId: 1 });
    const inflight = publishFields(await subscriber.client.next(mqtt.PACKET.PUBLISH));
    assert.equal(inflight.packetId, 1);
    subscriber.client.destroy();
    await waitFor(() => started.broker.clientIds.length === 1);
    await publishQos1(publisher, { topic: "t/queued", payload: "b", qos: 1, packetId: 2 });
    await publisher.disconnect();
    await started.close();

    started = await startBroker(database, port);
    assert.equal(started.port, port);
    const restored = await connectMqtt(port, { clientId: "persist", cleanSession: false });
    assert.equal(restored.connack.sessionPresent, true);
    const retried = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([retried.topic, retried.text, retried.packetId, retried.dup], ["t/inflight", "a", 1, true]);
    const queued = publishFields(await restored.client.next(mqtt.PACKET.PUBLISH));
    assert.deepEqual([queued.topic, queued.text, queued.packetId, queued.dup], ["t/queued", "b", 2, false]);
    await restored.client.disconnect();
  } finally {
    await started.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

