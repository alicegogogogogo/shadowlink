import { Buffer } from "node:buffer";
import net from "node:net";

import * as mqtt from "../src/mqtt.js";

export function connectPacket({ clientId, keepAlive = 0, will = null, cleanSession = true }) {
  let flags = cleanSession ? 0x02 : 0;
  const payload = [mqtt.encodeString(clientId)];
  if (will !== null) {
    flags |= 0x04 | (will.qos << 3) | (will.retain ? 0x20 : 0);
    payload.push(mqtt.encodeString(will.topic));
    payload.push(mqtt.encodeString(will.payload ?? ""));
  }
  const header = Buffer.from([0x00, 0x04, 0x4d, 0x51, 0x54, 0x54, 0x04, flags, keepAlive >> 8, keepAlive & 0xff]);
  return mqtt.encodePacket(mqtt.PACKET.CONNECT, 0, Buffer.concat([header, ...payload]));
}

export function subscribePacket(packetId, filters) {
  const chunks = [Buffer.from([packetId >> 8, packetId & 0xff])];
  for (const entry of filters) {
    chunks.push(mqtt.encodeString(entry.filter), Buffer.from([entry.qos ?? 0]));
  }
  return mqtt.encodePacket(mqtt.PACKET.SUBSCRIBE, 2, Buffer.concat(chunks));
}

export function publishPacket({ topic, payload = "", qos = 0, retain = false, dup = false, packetId = null }) {
  return mqtt.encodePublish({
    topic,
    payload: Buffer.from(payload, "utf8"),
    qos,
    retain,
    dup,
    packetId: packetId ?? 1,
  });
}

export function disconnectPacket() {
  return mqtt.encodePacket(mqtt.PACKET.DISCONNECT, 0, Buffer.alloc(0));
}

export function pingreqPacket() {
  return mqtt.encodePacket(mqtt.PACKET.PINGREQ, 0, Buffer.alloc(0));
}

export class MqttClient {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.packets = [];
    this.waiters = [];
    this.closed = false;
    socket.on("data", (chunk) => this.#onData(chunk));
    socket.on("close", () => {
      this.closed = true;
    });
    socket.on("error", () => {
      this.closed = true;
    });
  }

  static async connect(port, options = {}) {
    const socket = net.connect(port, "127.0.0.1");
    await new Promise((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    const client = new MqttClient(socket);
    client.write(connectPacket({ clientId: options.clientId ?? "client", keepAlive: options.keepAlive ?? 0, will: options.will ?? null }));
    return client;
  }

  #onData(chunk) {
    this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    for (;;) {
      if (this.buffer.length < 2) {
        break;
      }
      const remaining = mqtt.decodeRemainingLength(this.buffer, 1);
      if (remaining === null) {
        break;
      }
      const total = 1 + remaining.bytes + remaining.value;
      if (this.buffer.length < total) {
        break;
      }
      const first = this.buffer[0];
      this.packets.push({ type: first >> 4, flags: first & 0x0f, body: Buffer.from(this.buffer.subarray(1 + remaining.bytes, total)) });
      this.buffer = this.buffer.subarray(total);
    }
    this.waiters = this.waiters.filter((waiter) => !waiter());
  }

  write(buffer) {
    this.socket.write(buffer);
  }

  next(type, timeoutMs = 4000) {
    return new Promise((resolve, reject) => {
      let waiter = null;
      const timer = setTimeout(() => {
        this.waiters = this.waiters.filter((candidate) => candidate !== waiter);
        reject(new Error(`timed out waiting for packet type ${type}`));
      }, timeoutMs);
      waiter = () => {
        const index = this.packets.findIndex((packet) => packet.type === type);
        if (index < 0) {
          return false;
        }
        clearTimeout(timer);
        resolve(this.packets.splice(index, 1)[0]);
        return true;
      };
      this.waiters.push(waiter);
      this.waiters = this.waiters.filter((candidate) => !candidate());
    });
  }

  async disconnect() {
    this.write(disconnectPacket());
    await this.waitClosed();
  }

  waitClosed(timeoutMs = 4000) {
    if (this.closed) {
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error("timed out waiting for the connection to close")), timeoutMs);
      this.socket.once("close", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  destroy() {
    this.socket.destroy();
  }
}

export function connack(packet) {
  return { sessionPresent: packet.body[0] === 1, returnCode: packet.body[1] };
}

export function suback(packet) {
  return { packetId: packet.body.readUInt16BE(0), returnCodes: [...packet.body.subarray(2)] };
}

export function pubackPacketId(packet) {
  return packet.body.readUInt16BE(0);
}

export function publishFields(packet) {
  const publish = mqtt.decodePublish(packet.flags, packet.body);
  return { ...publish, text: publish.payload.toString("utf8") };
}

export async function waitFor(predicate, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error("timed out waiting for condition");
    }
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
