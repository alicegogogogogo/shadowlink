import { Buffer } from "node:buffer";
import net from "node:net";

import { ProtocolError } from "./errors.js";
import * as mqtt from "./mqtt.js";

const DEFAULT_MAX_PACKET_BYTES = 1048576;
const KEEP_ALIVE_GRACE = 1.5;
const FIXED_FLAGS = new Map([
  [mqtt.PACKET.CONNECT, 0],
  [mqtt.PACKET.PUBACK, 0],
  [mqtt.PACKET.SUBSCRIBE, 2],
  [mqtt.PACKET.PINGREQ, 0],
  [mqtt.PACKET.DISCONNECT, 0],
]);

/**
 * Holds one client's subscription set and outbound QoS 1 state. A persistent
 * session (clean session = 0) is keyed by client id, survives the connection
 * that created it and mirrors every state change into the store, so it also
 * survives a broker restart. A clean session is purely in memory and is removed
 * as soon as its connection detaches.
 */
class Session {
  constructor(broker, id, persistent) {
    this.broker = broker;
    this.store = broker.store;
    this.id = id;
    this.persistent = persistent;
    this.connection = null;
    this.subscriptions = new Map();
    this.pendingPacketIds = new Set();
    this.nextPacketId = 1;
    if (persistent) {
      this.#load();
    }
  }

  #load() {
    this.subscriptions = new Map(this.store.listMqttSubscriptions(this.id).map((row) => [row.filter, row.qos]));
    this.pendingPacketIds = new Set();
    for (const row of this.store.listMqttOutbound(this.id)) {
      if (row.packetId !== null) {
        this.pendingPacketIds.add(row.packetId);
      }
    }
    this.nextPacketId = this.store.getMqttNextPacketId(this.id);
  }

  /** Bind a (re)connecting socket and refresh state that changed while offline. */
  attach(connection) {
    this.connection = connection;
    if (this.persistent) {
      this.#load();
    }
  }

  detach(connection) {
    if (this.connection === connection) {
      this.connection = null;
    }
    if (!this.persistent) {
      this.broker.sessions.delete(this.id);
    }
  }

  setSubscription(filter, qos) {
    this.subscriptions.set(filter, qos);
    if (this.persistent) {
      this.store.putMqttSubscription(this.id, filter, qos);
    }
  }

  /** Highest QoS granted across every matching filter, or null when none match. */
  #grantedQos(topic) {
    let granted = null;
    for (const [filter, filterQos] of this.subscriptions) {
      if (mqtt.topicMatches(filter, topic)) {
        granted = granted === null ? filterQos : Math.max(granted, filterQos);
      }
    }
    return granted;
  }

  /**
   * Route one normal publication at this session. At most one copy is sent, at
   * the highest QoS granted by the matching filters. While online the message
   * is delivered immediately; while offline only an effective QoS 1 message is
   * queued (effective QoS 0 is discarded).
   */
  deliver(topic, payload, qos, retain = false) {
    const granted = this.#grantedQos(topic);
    if (granted === null) {
      return;
    }
    const effectiveQos = Math.min(qos, granted);
    if (this.connection === null) {
      if (effectiveQos === 1) {
        this.#enqueue(topic, payload, 1);
      }
      return;
    }
    this.#sendTo(topic, payload, effectiveQos, retain);
  }

  /** Deliver a message whose matching was already decided (a retained replay). */
  sendTo(topic, payload, qos, retain) {
    this.#sendTo(topic, payload, qos, retain);
  }

  #enqueue(topic, payload, qos) {
    if (this.persistent) {
      // Queued messages are published normally on delivery, so the retain flag
      // is never carried into the session store (MQTT 3.1.1 section 3.3.1.3).
      this.store.insertMqttOutbound(this.id, { packetId: null, topic, payload, qos, retain: false });
    }
  }

  #sendTo(topic, payload, qos, retain) {
    if (qos === 0) {
      this.connection.send(mqtt.encodePublish({ topic, payload, qos: 0, retain }));
      return null;
    }
    const packetId = this.#takePacketId();
    if (this.persistent) {
      // A redelivery after a reconnect is never a retained replay, so the
      // stored in-flight copy always has retain clear even though the live
      // (possibly retained) delivery below keeps its real flag.
      this.store.insertMqttOutbound(this.id, { packetId, topic, payload, qos: 1, retain: false });
    }
    this.connection.send(mqtt.encodePublish({ topic, payload, qos: 1, packetId, retain }));
    return packetId;
  }

  /**
   * Send a broker-originated command to this session at QoS 1 with RETAIN
   * clear and return its packet id. The message joins the same in-flight
   * store as normal deliveries, so a reconnect replays it with its original
   * packet id and DUP set.
   */
  sendCommandMessage(topic, payload) {
    return this.#sendTo(topic, payload, 1, false);
  }

  #takePacketId() {
    for (let attempt = 0; attempt < 65535; attempt += 1) {
      const candidate = this.nextPacketId;
      this.nextPacketId = candidate === 65535 ? 1 : candidate + 1;
      if (!this.pendingPacketIds.has(candidate)) {
        this.pendingPacketIds.add(candidate);
        if (this.persistent) {
          this.store.setMqttNextPacketId(this.id, this.nextPacketId);
        }
        return candidate;
      }
    }
    throw new ProtocolError("no packet identifier is available for this session");
  }

  /** A PUBACK removes just the one in-flight message it identifies. */
  acknowledge(packetId) {
    this.pendingPacketIds.delete(packetId);
    if (this.persistent) {
      this.store.deleteMqttOutbound(this.id, packetId);
    }
  }

  /**
   * Resend after a reconnect: in-flight messages first, still carrying their
   * original packet ids with DUP set, then never-sent queued messages in
   * enqueue order with fresh packet ids and DUP clear.
   */
  replay() {
    if (!this.persistent || this.connection === null) {
      return;
    }
    const rows = this.store.listMqttOutbound(this.id);
    for (const row of rows.filter((entry) => entry.packetId !== null)) {
      this.connection.send(
        mqtt.encodePublish({ topic: row.topic, payload: row.payload, qos: 1, packetId: row.packetId, retain: row.retain, dup: true }),
      );
    }
    for (const row of rows.filter((entry) => entry.packetId === null)) {
      const packetId = this.#takePacketId();
      this.store.setMqttOutboundPacketId(row.seq, packetId);
      this.connection.send(
        mqtt.encodePublish({ topic: row.topic, payload: row.payload, qos: 1, packetId, retain: row.retain, dup: false }),
      );
    }
  }
}

class ClientConnection {
  constructor(broker, socket) {
    this.broker = broker;
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.id = null;
    this.session = null;
    this.inboundPacketIds = new Set();
    this.keepAlive = 0;
    this.timer = null;
    this.will = null;
    this.graceful = false;
    this.closed = false;
    socket.setNoDelay(true);
    socket.on("data", (chunk) => this.#onData(chunk));
    socket.on("error", () => this.destroy(false));
    socket.on("close", () => this.#onClose());
  }

  #onData(chunk) {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    try {
      while (this.buffer.length > 1) {
        const remaining = mqtt.decodeRemainingLength(this.buffer, 1);
        if (remaining === null) {
          return;
        }
        if (remaining.value > this.broker.maxPacketBytes) {
          throw new ProtocolError(`packet of ${remaining.value} bytes exceeds the ${this.broker.maxPacketBytes} byte limit`);
        }
        const total = 1 + remaining.bytes + remaining.value;
        if (this.buffer.length < total) {
          return;
        }
        const first = this.buffer[0];
        const body = Buffer.from(this.buffer.subarray(1 + remaining.bytes, total));
        this.buffer = this.buffer.subarray(total);
        this.#touch();
        this.#handle(first >> 4, first & 0x0f, body);
      }
    } catch (error) {
      this.fail(error);
    }
  }

  #handle(type, flags, body) {
    if (this.id === null && type !== mqtt.PACKET.CONNECT) {
      throw new ProtocolError("the first packet of a connection must be CONNECT");
    }
    if (type !== mqtt.PACKET.PUBLISH && FIXED_FLAGS.get(type) !== flags) {
      throw new ProtocolError(`packet type ${type} must use fixed header flags ${FIXED_FLAGS.get(type) ?? 0}`);
    }
    switch (type) {
      case mqtt.PACKET.CONNECT:
        return this.#connect(body);
      case mqtt.PACKET.PUBLISH:
        return this.#publish(flags, body);
      case mqtt.PACKET.PUBACK:
        return this.#puback(body);
      case mqtt.PACKET.SUBSCRIBE:
        return this.#subscribe(body);
      case mqtt.PACKET.PINGREQ:
        return this.send(mqtt.encodePingresp());
      case mqtt.PACKET.DISCONNECT:
        this.graceful = true;
        this.socket.end();
        return undefined;
      default:
        throw new ProtocolError(`packet type ${type} is not supported by this broker`);
    }
  }

  #connect(body) {
    const connect = mqtt.decodeConnect(body);
    if (this.broker.hasClient(connect.clientId)) {
      throw new ProtocolError(`client identifier ${connect.clientId} is already connected`, mqtt.RETURN_CODE.IDENTIFIER_REJECTED);
    }
    this.id = connect.clientId;
    this.keepAlive = connect.keepAlive;
    this.will = connect.will;
    let sessionPresent = false;
    if (connect.cleanSession) {
      // A clean start discards any stored session for this identifier.
      this.broker.discardSession(this.id);
      this.session = this.broker.createSession(this.id, false);
    } else {
      const existing = this.broker.sessions.get(this.id);
      if (existing !== undefined) {
        this.session = existing;
        sessionPresent = true;
      } else {
        this.session = this.broker.createSession(this.id, true);
      }
    }
    this.session.attach(this);
    this.send(mqtt.encodeConnack(sessionPresent, mqtt.RETURN_CODE.ACCEPTED));
    this.#touch();
    if (sessionPresent) {
      this.session.replay();
    }
    // A (re)connected device may now be eligible for its queued commands.
    this.broker.service.pumpCommands(this.id);
  }

  #publish(flags, body) {
    const publish = mqtt.decodePublish(flags, body);
    if (publish.qos === 0) {
      this.broker.publish(publish.topic, publish.payload, { qos: 0, retain: publish.retain });
      return;
    }
    const duplicate = this.inboundPacketIds.has(publish.packetId);
    this.inboundPacketIds.add(publish.packetId);
    if (!duplicate) {
      this.broker.publish(publish.topic, publish.payload, { qos: 1, retain: publish.retain });
    }
    this.send(mqtt.encodePuback(publish.packetId));
  }

  #puback(body) {
    if (body.length !== 2) {
      throw new ProtocolError("puback must contain exactly a packet identifier");
    }
    const packetId = body.readUInt16BE(0);
    this.session.acknowledge(packetId);
    this.broker.service.handleCommandPuback(this.id, packetId);
  }

  #subscribe(body) {
    const subscribe = mqtt.decodeSubscribe(body);
    const returnCodes = [];
    for (const subscription of subscribe.subscriptions) {
      const granted = Math.min(subscription.qos, 1);
      this.session.setSubscription(subscription.filter, granted);
      returnCodes.push(granted);
    }
    this.send(mqtt.encodeSuback(subscribe.packetId, returnCodes));
    const delivered = new Set();
    for (const retained of this.broker.store.listRetained()) {
      if (delivered.has(retained.topic)) {
        continue;
      }
      let granted = null;
      for (const subscription of subscribe.subscriptions) {
        if (mqtt.topicMatches(subscription.filter, retained.topic)) {
          granted = granted === null ? subscription.qos : Math.max(granted, subscription.qos);
        }
      }
      if (granted !== null) {
        delivered.add(retained.topic);
        this.session.sendTo(retained.topic, retained.payload, Math.min(retained.qos, granted), true);
      }
    }
    // A new or upgraded subscription may make this device eligible for its
    // queued commands.
    this.broker.service.pumpCommands(this.id);
  }

  /** Fan-out entry point for publications while this connection is online. */
  deliver(topic, payload, qos) {
    if (this.session === null) {
      return;
    }
    this.session.deliver(topic, payload, qos, false);
  }

  send(buffer) {
    if (!this.closed) {
      this.socket.write(buffer);
    }
  }

  #touch() {
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    if (this.keepAlive > 0) {
      this.timer = setTimeout(() => this.destroy(false), Math.ceil(this.keepAlive * KEEP_ALIVE_GRACE * 1000));
      this.timer.unref();
    }
  }

  fail(error) {
    if (this.closed) {
      return;
    }
    this.closed = true;
    if (this.id === null && error instanceof ProtocolError && error.returnCode !== null) {
      this.socket.end(mqtt.encodeConnack(false, error.returnCode));
    } else {
      this.socket.destroy();
    }
  }

  destroy(graceful) {
    this.graceful = graceful;
    this.socket.destroy();
  }

  #onClose() {
    this.closed = true;
    if (this.timer !== null) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.broker.unregister(this);
    if (this.session !== null) {
      // A persistent session goes offline and keeps its state; a clean session
      // is removed entirely.
      this.session.detach(this);
    }
    if (!this.graceful && this.will !== null) {
      this.broker.publish(this.will.topic, this.will.payload, { qos: this.will.qos, retain: this.will.retain });
    }
  }
}

export class Broker {
  constructor(service, options = {}) {
    this.service = service;
    this.store = service.store;
    this.host = options.host ?? "127.0.0.1";
    this.port = options.port ?? 1883;
    this.maxPacketBytes = options.maxPacketBytes ?? DEFAULT_MAX_PACKET_BYTES;
    this.connections = new Set();
    this.sessions = new Map();
    for (const clientId of this.store.listMqttSessionIds()) {
      this.sessions.set(clientId, new Session(this, clientId, true));
    }
    this.server = net.createServer((socket) => {
      this.connections.add(new ClientConnection(this, socket));
    });
  }

  listen() {
    return new Promise((resolve, reject) => {
      this.server.once("error", reject);
      this.server.listen(this.port, this.host, () => {
        this.port = this.server.address().port;
        resolve({ host: this.host, port: this.port });
      });
    });
  }

  close() {
    for (const connection of this.connections) {
      connection.destroy(true);
    }
    return new Promise((resolve) => this.server.close(() => resolve()));
  }

  hasClient(clientId) {
    for (const connection of this.connections) {
      if (connection.id === clientId) {
        return true;
      }
    }
    return false;
  }

  unregister(connection) {
    this.connections.delete(connection);
  }

  createSession(clientId, persistent) {
    if (persistent) {
      this.store.createMqttSession(clientId);
    }
    const session = new Session(this, clientId, persistent);
    this.sessions.set(clientId, session);
    return session;
  }

  /** Drop a persisted session (used when a clean session=1 connect succeeds). */
  discardSession(clientId) {
    if (this.sessions.delete(clientId)) {
      this.store.deleteMqttSession(clientId);
      this.service.resetCommandDelivery(clientId);
    }
  }

  /**
   * Deliver one queued command to its device. Only a persistent session whose
   * client id equals the device id, currently connected and subscribed at
   * QoS 1 to the exact `$commands/<deviceId>` topic, is eligible; anything
   * else (clean sessions, wildcard or QoS 0 subscriptions, other clients)
   * receives nothing and the command stays queued. Returns the packet id of
   * the QoS 1 PUBLISH, or null when no eligible connection is ready.
   */
  sendCommand(deviceId, command) {
    const session = this.sessions.get(deviceId);
    if (session === undefined || !session.persistent || session.connection === null) {
      return null;
    }
    const topic = `$commands/${deviceId}`;
    if (session.subscriptions.get(topic) !== 1) {
      return null;
    }
    const payload = Buffer.from(JSON.stringify({ id: command.id, payload: command.payload }), "utf8");
    return session.sendCommandMessage(topic, payload);
  }

  get clientIds() {
    return [...this.connections].filter((connection) => connection.id !== null).map((connection) => connection.id).sort();
  }

  /**
   * Route one publication: retain it, fan it out to matching subscriptions and
   * then let the service evaluate mqtt rules. An empty retained payload clears
   * the retained message for that topic. Online connections receive real-time
   * fan-out; offline persistent sessions queue effective QoS 1 messages.
   */
  publish(topic, payload, options = {}) {
    const { qos = 0, retain = false } = options;
    const body = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), "utf8");
    mqtt.validateTopicName(topic);
    if (retain) {
      if (body.length === 0) {
        this.store.deleteRetained(topic);
      } else {
        this.store.putRetained(topic, body, qos);
      }
    }
    for (const connection of this.connections) {
      connection.deliver(topic, body, qos);
    }
    for (const session of this.sessions.values()) {
      if (session.connection === null) {
        session.deliver(topic, body, qos, false);
      }
    }
    this.service.handlePublish(topic, body);
  }
}
