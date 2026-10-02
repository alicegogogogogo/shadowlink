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

class ClientConnection {
  constructor(broker, socket) {
    this.broker = broker;
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.id = null;
    this.subscriptions = new Map();
    this.inboundPacketIds = new Set();
    this.pendingPacketIds = new Set();
    this.nextPacketId = 1;
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
    this.send(mqtt.encodeConnack(false, mqtt.RETURN_CODE.ACCEPTED));
    this.#touch();
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
    this.pendingPacketIds.delete(body.readUInt16BE(0));
  }

  #subscribe(body) {
    const subscribe = mqtt.decodeSubscribe(body);
    const returnCodes = [];
    for (const subscription of subscribe.subscriptions) {
      this.subscriptions.set(subscription.filter, Math.min(subscription.qos, 1));
      returnCodes.push(Math.min(subscription.qos, 1));
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
        this.#deliverTo(retained.topic, retained.payload, Math.min(retained.qos, granted), true);
      }
    }
  }

  /** At most one copy per client, at the highest qos granted by its filters. */
  deliver(topic, payload, qos) {
    if (this.id === null) {
      return;
    }
    let granted = null;
    for (const [filter, filterQos] of this.subscriptions) {
      if (mqtt.topicMatches(filter, topic)) {
        granted = granted === null ? filterQos : Math.max(granted, filterQos);
      }
    }
    if (granted !== null) {
      this.#deliverTo(topic, payload, Math.min(qos, granted), false);
    }
  }

  #deliverTo(topic, payload, qos, retain) {
    if (qos === 0) {
      this.send(mqtt.encodePublish({ topic, payload, qos: 0, retain }));
      return;
    }
    const packetId = this.#takePacketId();
    this.pendingPacketIds.add(packetId);
    this.send(mqtt.encodePublish({ topic, payload, qos: 1, packetId, retain }));
  }

  #takePacketId() {
    for (let attempt = 0; attempt < 65535; attempt += 1) {
      const candidate = this.nextPacketId;
      this.nextPacketId = candidate === 65535 ? 1 : candidate + 1;
      if (!this.pendingPacketIds.has(candidate)) {
        return candidate;
      }
    }
    throw new ProtocolError("no packet identifier is available for this connection");
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

  get clientIds() {
    return [...this.connections].filter((connection) => connection.id !== null).map((connection) => connection.id).sort();
  }

  /**
   * Route one publication: retain it, fan it out to matching subscriptions and
   * then let the service evaluate mqtt rules. An empty retained payload clears
   * the retained message for that topic.
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
    this.service.handlePublish(topic, body);
  }
}
