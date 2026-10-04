import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { Broker } from "../src/broker.js";
import * as mqtt from "../src/mqtt.js";
import { Service } from "../src/service.js";
import { MqttClient, connack, publishFields, publishPacket, suback, subscribePacket, waitFor } from "./helpers.js";

const CLOCK = () => "2024-01-01T00:00:00.000Z";

async function startBroker(database = ":memory:") {
  const service = new Service(database, { now: CLOCK });
  const broker = new Broker(service, { host: "127.0.0.1", port: 0 });
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

async function waitOffline(started, clientId) {
  await waitFor(() => !started.broker.clientIds.includes(clientId));
}

test("a clean session 0 connect creates a session and the next one resumes it", async () => {
  const started = await startBroker();
  try {
    const first = await MqttClient.connect(started.port, { clientId: "device", cleanSession: false });
    assert.deepEqual(connack(await first.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 0 });
    await first.disconnect();
    await waitOffline(started, "device");

    const second = await MqttClient.connect(started.port, { clientId: "device", cleanSession: false });
    assert.deepEqual(connack(await second.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    await second.disconnect();
  } finally {
    await started.close();
  }
});

test("a clean session 1 connect discards the stored persistent session", async () => {
  const started = await startBroker();
  try {
    const persistent = await MqttClient.connect(started.port, { clientId: "device", cleanSession: false });
    await persistent.next(mqtt.PACKET.CONNACK);
    persistent.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await persistent.next(mqtt.PACKET.SUBACK);
    await persistent.disconnect();
    await waitOffline(started, "device");

    const clean = await MqttClient.connect(started.port, { clientId: "device", cleanSession: true });
    assert.deepEqual(connack(await clean.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 0 });
    await clean.disconnect();
    await waitOffline(started, "device");

    const again = await MqttClient.connect(started.port, { clientId: "device", cleanSession: false });
    assert.deepEqual(connack(await again.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 0 });
    // The old subscription is gone: nothing is delivered without subscribing.
    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "x", qos: 1, packetId: 3 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await assert.rejects(again.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await again.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("a live client identifier is still rejected for persistent connects", async () => {
  const started = await startBroker();
  try {
    const first = await MqttClient.connect(started.port, { clientId: "twin", cleanSession: false });
    await first.next(mqtt.PACKET.CONNACK);
    const second = await MqttClient.connect(started.port, { clientId: "twin", cleanSession: false });
    assert.deepEqual(connack(await second.next(mqtt.PACKET.CONNACK)), { sessionPresent: false, returnCode: 2 });
    await second.waitClosed();
    assert.deepEqual(started.broker.clientIds, ["twin"]);
    await first.disconnect();
  } finally {
    await started.close();
  }
});

test("subscriptions survive a graceful disconnect and retained messages are not replayed on resume", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "status/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "status/dev", payload: "online", qos: 1, retain: true, packetId: 4 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    const live = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.equal(live.text, "online");
    subscriber.write(mqtt.encodePuback(live.packetId));
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    // The retained message must not be replayed just because the session resumed.
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 300), /timed out/);
    // The saved subscription still receives new publications without resubscribing.
    publisher.write(publishPacket({ topic: "status/dev", payload: "later", qos: 1, packetId: 5 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    const fresh = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(fresh.text, "later");
    assert.equal(fresh.retain, false);
    resumed.write(mqtt.encodePuback(fresh.packetId));
    await resumed.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("only final qos 1 publications are queued, one copy per client, in publish order", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(
      subscribePacket(1, [
        { filter: "sensors/+", qos: 0 },
        { filter: "sensors/#", qos: 1 },
      ]),
    );
    assert.deepEqual(suback(await subscriber.next(mqtt.PACKET.SUBACK)), { packetId: 1, returnCodes: [0, 1] });
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "one", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    publisher.write(publishPacket({ topic: "sensors/b", payload: "two" }));
    publisher.write(publishPacket({ topic: "sensors/c/d", payload: "three", qos: 1, packetId: 8 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    publisher.write(publishPacket({ topic: "elsewhere", payload: "four", qos: 1, packetId: 9 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await publisher.disconnect();

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    // "one" matches both filters but is queued once, at the highest granted qos.
    const first = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(first.text, "one");
    assert.equal(first.qos, 1);
    assert.equal(first.dup, false);
    assert.ok(first.packetId > 0);
    // "two" was qos 0 end-to-end... it matched sensors/+ at qos 0 and sensors/#
    // at qos 1, but it was published at qos 0, so min(0, 1) = 0: dropped.
    const second = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(second.text, "three");
    assert.equal(second.dup, false);
    assert.ok(second.packetId > 0);
    assert.notEqual(first.packetId, second.packetId);
    resumed.write(mqtt.encodePuback(first.packetId));
    resumed.write(mqtt.encodePuback(second.packetId));
    // "four" never matched; nothing else is queued.
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await resumed.disconnect();
  } finally {
    await started.close();
  }
});

test("a subscription whose granted qos is 0 does not queue qos 1 publications", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 0 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "dropped", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await publisher.disconnect();

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await resumed.next(mqtt.PACKET.CONNACK);
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await resumed.disconnect();
  } finally {
    await started.close();
  }
});

test("re-subscribing a filter overwrites the stored granted qos", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);
    subscriber.write(subscribePacket(2, [{ filter: "sensors/#", qos: 0 }]));
    assert.deepEqual(suback(await subscriber.next(mqtt.PACKET.SUBACK)), { packetId: 2, returnCodes: [0] });
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "dropped", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await publisher.disconnect();

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await resumed.disconnect();
  } finally {
    await started.close();
  }
});

test("unacknowledged deliveries are retransmitted with DUP and the original packet id before the backlog", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "live", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    const unacked = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.equal(unacked.text, "live");
    // Never acknowledged; the client goes away gracefully.
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    publisher.write(publishPacket({ topic: "sensors/b", payload: "queued-1", qos: 1, packetId: 8 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    publisher.write(publishPacket({ topic: "sensors/c", payload: "queued-2", qos: 1, packetId: 9 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await publisher.disconnect();

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    const retransmitted = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(retransmitted.text, "live");
    assert.equal(retransmitted.dup, true);
    assert.equal(retransmitted.packetId, unacked.packetId);
    const first = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(first.text, "queued-1");
    assert.equal(first.dup, false);
    assert.notEqual(first.packetId, unacked.packetId);
    const second = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(second.text, "queued-2");
    assert.equal(second.dup, false);
    resumed.write(mqtt.encodePuback(retransmitted.packetId));
    resumed.write(mqtt.encodePuback(first.packetId));
    resumed.write(mqtt.encodePuback(second.packetId));
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await resumed.disconnect();
  } finally {
    await started.close();
  }
});

test("acknowledged messages are not retransmitted after a reconnect", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "acked", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    const delivered = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    subscriber.write(mqtt.encodePuback(delivered.packetId));
    await waitFor(() => started.broker.sessions.get("sub").queue.length === 0);
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    await assert.rejects(resumed.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await resumed.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("dropping mid-delivery resumes with the same retransmission rules", async () => {
  const started = await startBroker();
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/a", payload: "one", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    publisher.write(publishPacket({ topic: "sensors/b", payload: "two", qos: 1, packetId: 8 }));
    await publisher.next(mqtt.PACKET.PUBACK);

    const first = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await first.next(mqtt.PACKET.CONNACK);
    const one = publishFields(await first.next(mqtt.PACKET.PUBLISH));
    assert.equal(one.text, "one");
    assert.equal(one.dup, false);
    const two = publishFields(await first.next(mqtt.PACKET.PUBLISH));
    assert.equal(two.text, "two");
    assert.equal(two.dup, false);
    // Drop the connection without acknowledging either delivery.
    first.destroy();
    await waitOffline(started, "sub");

    const second = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await second.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    const reone = publishFields(await second.next(mqtt.PACKET.PUBLISH));
    assert.equal(reone.text, "one");
    assert.equal(reone.dup, true);
    assert.equal(reone.packetId, one.packetId);
    const retwo = publishFields(await second.next(mqtt.PACKET.PUBLISH));
    assert.equal(retwo.text, "two");
    assert.equal(retwo.dup, true);
    assert.equal(retwo.packetId, two.packetId);
    second.write(mqtt.encodePuback(reone.packetId));
    second.write(mqtt.encodePuback(retwo.packetId));
    await assert.rejects(second.next(mqtt.PACKET.PUBLISH, 200), /timed out/);
    await second.disconnect();
    await publisher.disconnect();
  } finally {
    await started.close();
  }
});

test("an abnormal disconnect still publishes the will and queues it for offline persistent sessions", async () => {
  const started = await startBroker();
  try {
    const watcher = await MqttClient.connect(started.port, { clientId: "watcher", cleanSession: false });
    await watcher.next(mqtt.PACKET.CONNACK);
    watcher.write(subscribePacket(1, [{ filter: "clients/#", qos: 1 }]));
    await watcher.next(mqtt.PACKET.SUBACK);
    await watcher.disconnect();
    await waitOffline(started, "watcher");

    const dying = await MqttClient.connect(started.port, {
      clientId: "dying",
      cleanSession: false,
      will: { topic: "clients/dying", payload: "offline", qos: 1, retain: false },
    });
    await dying.next(mqtt.PACKET.CONNACK);
    dying.destroy();
    await waitOffline(started, "dying");

    const resumed = await MqttClient.connect(started.port, { clientId: "watcher", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    const will = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(will.topic, "clients/dying");
    assert.equal(will.text, "offline");
    assert.equal(will.qos, 1);
    resumed.write(mqtt.encodePuback(will.packetId));
    await resumed.disconnect();
  } finally {
    await started.close();
  }
});

test("persistent sessions and their queues survive a broker restart", async () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "shadowlink-session-"));
  const database = path.join(directory, "shadowlink.db");
  let started = await startBroker(database);
  try {
    const subscriber = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    await subscriber.next(mqtt.PACKET.CONNACK);
    subscriber.write(subscribePacket(1, [{ filter: "sensors/#", qos: 1 }]));
    await subscriber.next(mqtt.PACKET.SUBACK);

    const publisher = await MqttClient.connect(started.port, { clientId: "pub" });
    publisher.write(publishPacket({ topic: "sensors/live", payload: "live-unacked", qos: 1, packetId: 6 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    const unacked = publishFields(await subscriber.next(mqtt.PACKET.PUBLISH));
    assert.equal(unacked.text, "live-unacked");
    // Never acknowledged; the subscriber goes offline before the restart.
    await subscriber.disconnect();
    await waitOffline(started, "sub");

    publisher.write(publishPacket({ topic: "sensors/a", payload: "before-restart", qos: 1, packetId: 7 }));
    await publisher.next(mqtt.PACKET.PUBACK);
    await publisher.disconnect();
    await started.close();

    started = await startBroker(database);
    const resumed = await MqttClient.connect(started.port, { clientId: "sub", cleanSession: false });
    assert.deepEqual(connack(await resumed.next(mqtt.PACKET.CONNACK)), { sessionPresent: true, returnCode: 0 });
    const retransmitted = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(retransmitted.text, "live-unacked");
    assert.equal(retransmitted.dup, true);
    assert.equal(retransmitted.packetId, unacked.packetId);
    const queued = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(queued.text, "before-restart");
    assert.equal(queued.dup, false);
    resumed.write(mqtt.encodePuback(retransmitted.packetId));
    resumed.write(mqtt.encodePuback(queued.packetId));
    // The subscription survived too: live fan-out works without resubscribing.
    const publisher2 = await MqttClient.connect(started.port, { clientId: "pub2" });
    publisher2.write(publishPacket({ topic: "sensors/b", payload: "after-restart", qos: 1, packetId: 3 }));
    await publisher2.next(mqtt.PACKET.PUBACK);
    const live = publishFields(await resumed.next(mqtt.PACKET.PUBLISH));
    assert.equal(live.text, "after-restart");
    resumed.write(mqtt.encodePuback(live.packetId));
    await resumed.disconnect();
    await publisher2.disconnect();
  } finally {
    await started.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
