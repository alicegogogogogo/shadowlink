import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import net from "node:net";
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

async function startBroker() {
  const service = new Service(":memory:", { now: CLOCK });
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
