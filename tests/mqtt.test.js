import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import test from "node:test";

import { ProtocolError, ValidationError } from "../src/errors.js";
import * as mqtt from "../src/mqtt.js";
import { connectPacket } from "./helpers.js";

test("remaining length round-trips through the base-128 varint", () => {
  const values = [0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455];
  for (const value of values) {
    const buffer = Buffer.concat([Buffer.from([0x30]), mqtt.encodeRemainingLength(value)]);
    assert.deepEqual(mqtt.decodeRemainingLength(buffer), { value, bytes: mqtt.encodeRemainingLength(value).length });
  }
});

test("remaining length uses the minimal encoding", () => {
  assert.deepEqual([...mqtt.encodeRemainingLength(0)], [0x00]);
  assert.deepEqual([...mqtt.encodeRemainingLength(127)], [0x7f]);
  assert.deepEqual([...mqtt.encodeRemainingLength(128)], [0x80, 0x01]);
  assert.deepEqual([...mqtt.encodeRemainingLength(16383)], [0xff, 0x7f]);
  assert.deepEqual([...mqtt.encodeRemainingLength(16384)], [0x80, 0x80, 0x01]);
  assert.deepEqual([...mqtt.encodeRemainingLength(2097152)], [0x80, 0x80, 0x80, 0x01]);
  assert.deepEqual([...mqtt.encodeRemainingLength(268435455)], [0xff, 0xff, 0xff, 0x7f]);
});

test("remaining length decoding reports incomplete and oversized input", () => {
  assert.equal(mqtt.decodeRemainingLength(Buffer.from([0x30])), null);
  assert.equal(mqtt.decodeRemainingLength(Buffer.from([0x30, 0x80])), null);
  assert.throws(() => mqtt.decodeRemainingLength(Buffer.from([0x30, 0x80, 0x80, 0x80, 0x80, 0x01])), ProtocolError);
  assert.throws(() => mqtt.encodeRemainingLength(268435456), ValidationError);
});

test("topic filters match per MQTT 3.1.1 section 4.7", () => {
  const cases = [
    ["sport/tennis/player1", "sport/tennis/player1", true],
    ["sport/tennis/+", "sport/tennis/player1", true],
    ["sport/tennis/+", "sport/tennis/player1/ranking", false],
    ["sport/+", "sport/", true],
    ["sport/+", "sport", false],
    ["sport/#", "sport", true],
    ["sport/#", "sport/tennis/player1", true],
    ["#", "sport", true],
    ["#", "sport/tennis", true],
    ["+/+", "/finance", true],
    ["/+", "/finance", true],
    ["+", "/finance", false],
    ["#", "$SYS/broker/uptime", false],
    ["+/monitor/Clients", "$SYS/monitor/Clients", false],
    ["$SYS/#", "$SYS/broker/uptime", true],
    ["a/b", "a/c", false],
    ["a/b", "a/b/c", false],
  ];
  for (const [filter, topic, expected] of cases) {
    assert.equal(mqtt.topicMatches(filter, topic), expected, `${filter} vs ${topic}`);
  }
});

test("topic filters are validated", () => {
  for (const filter of ["a/#/b", "a#", "a/b+", "a/+b", "#/a", "sport/tennis#", ""]) {
    assert.throws(() => mqtt.validateTopicFilter(filter), ValidationError, filter);
  }
  for (const filter of ["#", "+", "sport/+/player1", "sport/#", "$SYS/#", "/"]) {
    assert.equal(mqtt.validateTopicFilter(filter), filter);
  }
});

test("topic names reject wildcards and empty levels", () => {
  assert.throws(() => mqtt.validateTopicName("a/+/b"), ValidationError);
  assert.throws(() => mqtt.validateTopicName("a/#"), ValidationError);
  assert.throws(() => mqtt.validateTopicName(""), ValidationError);
  assert.equal(mqtt.validateTopicName("sport/tennis/player1"), "sport/tennis/player1");
});

test("connect packets are decoded including the will", () => {
  const connect = mqtt.decodeConnect(connectPacket({ clientId: "device-1", keepAlive: 30 }).subarray(2));
  assert.deepEqual(connect, { clientId: "device-1", cleanSession: true, keepAlive: 30, will: null });
  const withWill = mqtt.decodeConnect(
    connectPacket({ clientId: "device-2", keepAlive: 0, will: { topic: "status/device-2", payload: "offline", qos: 1, retain: true } }).subarray(2),
  );
  assert.equal(withWill.will.topic, "status/device-2");
  assert.equal(withWill.will.payload.toString("utf8"), "offline");
  assert.equal(withWill.will.qos, 1);
  assert.equal(withWill.will.retain, true);
});

test("connect packets reject unsupported protocol versions and empty client ids", () => {
  const body = connectPacket({ clientId: "device-1" }).subarray(2);
  const wrongLevel = Buffer.from(body);
  wrongLevel[6] = 5;
  assert.throws(() => mqtt.decodeConnect(wrongLevel), (error) => error instanceof ProtocolError && error.returnCode === 1);
  assert.throws(() => mqtt.decodeConnect(connectPacket({ clientId: "" }).subarray(2)), (error) => error.returnCode === 2);
  const persistent = mqtt.decodeConnect(connectPacket({ clientId: "device-1", cleanSession: false }).subarray(2));
  assert.equal(persistent.cleanSession, false);
});

test("publish packets round-trip with qos 0 and qos 1", () => {
  const zero = mqtt.decodePublish(0, mqtt.encodePublish({ topic: "a/b", payload: Buffer.from("hi") }).subarray(2));
  assert.deepEqual(
    { topic: zero.topic, payload: zero.payload.toString("utf8"), qos: zero.qos, retain: zero.retain, dup: zero.dup, packetId: zero.packetId },
    { topic: "a/b", payload: "hi", qos: 0, retain: false, dup: false, packetId: null },
  );
  const one = mqtt.decodePublish(0x02, mqtt.encodePublish({ topic: "a/b", payload: Buffer.from("hi"), qos: 1, packetId: 9 }).subarray(2));
  assert.equal(one.packetId, 9);
  assert.equal(one.qos, 1);
  assert.throws(() => mqtt.decodePublish(0x04, Buffer.alloc(0)), ProtocolError);
});

test("subscribe packets are decoded and malformed ones rejected", () => {
  const packet = mqtt.encodePacket(mqtt.PACKET.SUBSCRIBE, 2, Buffer.concat([Buffer.from([0x00, 0x07]), mqtt.encodeString("sensors/+/temp"), Buffer.from([1])]));
  assert.deepEqual(mqtt.decodeSubscribe(packet.subarray(2)), { packetId: 7, subscriptions: [{ filter: "sensors/+/temp", qos: 1 }] });
  assert.throws(() => mqtt.decodeSubscribe(Buffer.from([0x00, 0x00])), ProtocolError);
  assert.throws(() => mqtt.decodeSubscribe(Buffer.concat([Buffer.from([0x00, 0x01]), mqtt.encodeString("a/#/b"), Buffer.from([0])])), ValidationError);
});

test("server responses carry the documented fixed header flags", () => {
  assert.equal(mqtt.encodeConnack(false, 0)[0], 0x20);
  assert.equal(mqtt.encodeSuback(1, [0, 1])[0], 0x90);
  assert.equal(mqtt.encodePuback(4)[0], 0x40);
  assert.equal(mqtt.encodePingresp()[0], 0xd0);
  assert.deepEqual([...mqtt.encodeSuback(1, [0, 0x80]).subarray(2)], [0x00, 0x01, 0x00, 0x80]);
});
