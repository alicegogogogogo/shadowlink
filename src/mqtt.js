import { Buffer } from "node:buffer";

import { ProtocolError, ValidationError } from "./errors.js";

export const PACKET = Object.freeze({
  CONNECT: 1,
  CONNACK: 2,
  PUBLISH: 3,
  PUBACK: 4,
  SUBSCRIBE: 8,
  SUBACK: 9,
  PINGREQ: 12,
  PINGRESP: 13,
  DISCONNECT: 14,
});

export const RETURN_CODE = Object.freeze({
  ACCEPTED: 0,
  UNACCEPTABLE_PROTOCOL_VERSION: 1,
  IDENTIFIER_REJECTED: 2,
  SERVER_UNAVAILABLE: 3,
  BAD_USERNAME_OR_PASSWORD: 4,
  NOT_AUTHORIZED: 5,
});

export const SUBACK_FAILURE = 0x80;
export const MAX_REMAINING_LENGTH = 268435455;
const MAX_TOPIC_BYTES = 65535;

/** Encode a Remaining Length as a 1-4 byte base-128 varint. */
export function encodeRemainingLength(value) {
  if (!Number.isInteger(value) || value < 0 || value > MAX_REMAINING_LENGTH) {
    throw new ValidationError("remaining length must be an integer between 0 and 268435455");
  }
  const bytes = [];
  let remaining = value;
  let digit = 0;
  do {
    digit = remaining % 128;
    remaining = Math.floor(remaining / 128);
    if (remaining > 0) {
      digit |= 0x80;
    }
    bytes.push(digit);
  } while (remaining > 0);
  return Buffer.from(bytes);
}

/** Decode a Remaining Length; returns null when the buffer is incomplete. */
export function decodeRemainingLength(buffer, offset = 1) {
  let multiplier = 1;
  let value = 0;
  let index = 0;
  for (;;) {
    const position = offset + index;
    if (position >= buffer.length) {
      return null;
    }
    const digit = buffer[position];
    value += (digit & 0x7f) * multiplier;
    index += 1;
    if ((digit & 0x80) === 0) {
      break;
    }
    if (index >= 4) {
      throw new ProtocolError("remaining length must not span more than four bytes");
    }
    multiplier *= 128;
  }
  if (value > MAX_REMAINING_LENGTH) {
    throw new ProtocolError("remaining length exceeds 268435455");
  }
  return { value, bytes: index };
}

export function encodePacket(type, flags, body) {
  return Buffer.concat([Buffer.from([(type << 4) | flags]), encodeRemainingLength(body.length), body]);
}

export function encodeString(value) {
  const bytes = Buffer.from(value, "utf8");
  const header = Buffer.alloc(2);
  header.writeUInt16BE(bytes.length, 0);
  return Buffer.concat([header, bytes]);
}

export function readString(buffer, offset, label) {
  if (offset + 2 > buffer.length) {
    throw new ProtocolError(`${label} length is truncated`);
  }
  const length = buffer.readUInt16BE(offset);
  const start = offset + 2;
  if (start + length > buffer.length) {
    throw new ProtocolError(`${label} is truncated`);
  }
  return { value: buffer.toString("utf8", start, start + length), offset: start + length };
}

export function validateTopicName(topic) {
  if (typeof topic !== "string" || topic.length === 0) {
    throw new ValidationError("topic name must be a non-empty string");
  }
  if (Buffer.byteLength(topic, "utf8") > MAX_TOPIC_BYTES) {
    throw new ValidationError("topic name must be at most 65535 bytes");
  }
  if (topic.includes("\u0000")) {
    throw new ValidationError("topic name must not contain U+0000");
  }
  if (topic.includes("#") || topic.includes("+")) {
    throw new ValidationError("topic name must not contain the wildcards # or +");
  }
  return topic;
}

export function validateTopicFilter(filter) {
  if (typeof filter !== "string" || filter.length === 0) {
    throw new ValidationError("topic filter must be a non-empty string");
  }
  if (Buffer.byteLength(filter, "utf8") > MAX_TOPIC_BYTES) {
    throw new ValidationError("topic filter must be at most 65535 bytes");
  }
  if (filter.includes("\u0000")) {
    throw new ValidationError("topic filter must not contain U+0000");
  }
  const levels = filter.split("/");
  levels.forEach((level, index) => {
    if (level.includes("#") && (level !== "#" || index !== levels.length - 1)) {
      throw new ValidationError("'#' must occupy an entire topic level and be the last level of a filter");
    }
    if (level.includes("+") && level !== "+") {
      throw new ValidationError("'+' must occupy an entire topic level");
    }
  });
  return filter;
}

/**
 * MQTT 3.1.1 section 4.7 matching. `+` matches exactly one level (including an
 * empty level), `#` matches the parent level and every level below it, and a
 * filter whose first level is a wildcard never matches a `$`-prefixed topic.
 */
export function topicMatches(filter, topic) {
  const filterLevels = filter.split("/");
  const topicLevels = topic.split("/");
  if ((filterLevels[0] === "#" || filterLevels[0] === "+") && topicLevels[0].startsWith("$")) {
    return false;
  }
  let index = 0;
  for (; index < filterLevels.length; index += 1) {
    const level = filterLevels[index];
    if (level === "#") {
      return true;
    }
    if (index >= topicLevels.length) {
      return false;
    }
    if (level !== "+" && level !== topicLevels[index]) {
      return false;
    }
  }
  return index === topicLevels.length;
}

const MAX_CLIENT_ID_BYTES = 128;

export function decodeConnect(body) {
  const name = readString(body, 0, "protocol name");
  if (name.value !== "MQTT") {
    throw new ProtocolError("protocol name must be MQTT", RETURN_CODE.UNACCEPTABLE_PROTOCOL_VERSION);
  }
  let offset = name.offset;
  if (offset + 4 > body.length) {
    throw new ProtocolError("connect variable header is truncated", RETURN_CODE.SERVER_UNAVAILABLE);
  }
  const level = body[offset];
  const flags = body[offset + 1];
  const keepAlive = body.readUInt16BE(offset + 2);
  offset += 4;
  if (level !== 4) {
    throw new ProtocolError("protocol level must be 4 (MQTT 3.1.1)", RETURN_CODE.UNACCEPTABLE_PROTOCOL_VERSION);
  }
  if ((flags & 0x01) !== 0) {
    throw new ProtocolError("the reserved connect flag must be zero");
  }
  if ((flags & 0xc0) !== 0) {
    throw new ProtocolError("username and password authentication is not supported", RETURN_CODE.BAD_USERNAME_OR_PASSWORD);
  }
  const cleanSession = (flags & 0x02) !== 0;
  const willFlag = (flags & 0x04) !== 0;
  const willQos = (flags & 0x18) >> 3;
  const willRetain = (flags & 0x20) !== 0;
  if (!willFlag && (willQos !== 0 || willRetain)) {
    throw new ProtocolError("will qos and will retain must be zero when the will flag is unset");
  }
  if (willQos > 1) {
    throw new ProtocolError("will qos 2 is not supported");
  }
  if (!cleanSession) {
    throw new ProtocolError("clean session must be 1; persistent sessions are not supported");
  }
  const client = readString(body, offset, "client identifier");
  offset = client.offset;
  if (client.value.length === 0 || Buffer.byteLength(client.value, "utf8") > MAX_CLIENT_ID_BYTES) {
    throw new ProtocolError("client identifier must be 1 to 128 bytes", RETURN_CODE.IDENTIFIER_REJECTED);
  }
  let will = null;
  if (willFlag) {
    const topic = readString(body, offset, "will topic");
    const message = readString(body, topic.offset, "will message");
    offset = message.offset;
    try {
      validateTopicName(topic.value);
    } catch {
      throw new ProtocolError("will topic must be a valid topic name");
    }
    will = { topic: topic.value, payload: Buffer.from(message.value, "utf8"), qos: willQos, retain: willRetain };
  }
  if (offset !== body.length) {
    throw new ProtocolError("connect payload has trailing bytes");
  }
  return { clientId: client.value, cleanSession, keepAlive, will };
}

export function encodeConnack(sessionPresent, returnCode) {
  return encodePacket(PACKET.CONNACK, 0, Buffer.from([sessionPresent ? 1 : 0, returnCode]));
}

export function decodePublish(flags, body) {
  const dup = (flags & 0x08) !== 0;
  const qos = (flags & 0x06) >> 1;
  const retain = (flags & 0x01) !== 0;
  if (qos > 1) {
    throw new ProtocolError("qos 2 is not supported");
  }
  if (qos === 0 && dup) {
    throw new ProtocolError("dup must be zero for a qos 0 publish");
  }
  const topic = readString(body, 0, "topic name");
  validateTopicName(topic.value);
  let offset = topic.offset;
  let packetId = null;
  if (qos > 0) {
    if (offset + 2 > body.length) {
      throw new ProtocolError("publish packet identifier is truncated");
    }
    packetId = body.readUInt16BE(offset);
    offset += 2;
    if (packetId === 0) {
      throw new ProtocolError("packet identifier must be non-zero");
    }
  }
  return { dup, qos, retain, topic: topic.value, packetId, payload: Buffer.from(body.subarray(offset)) };
}

export function encodePublish({ topic, payload = Buffer.alloc(0), qos = 0, retain = false, dup = false, packetId = null }) {
  const chunks = [encodeString(topic)];
  if (qos > 0) {
    const identifier = Buffer.alloc(2);
    identifier.writeUInt16BE(packetId, 0);
    chunks.push(identifier);
  }
  chunks.push(Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload), "utf8"));
  return encodePacket(PACKET.PUBLISH, (dup ? 0x08 : 0) | (qos << 1) | (retain ? 0x01 : 0), Buffer.concat(chunks));
}

export function encodePuback(packetId) {
  const body = Buffer.alloc(2);
  body.writeUInt16BE(packetId, 0);
  return encodePacket(PACKET.PUBACK, 0, body);
}

export function decodeSubscribe(body) {
  if (body.length < 2) {
    throw new ProtocolError("subscribe packet is truncated");
  }
  const packetId = body.readUInt16BE(0);
  if (packetId === 0) {
    throw new ProtocolError("packet identifier must be non-zero");
  }
  const subscriptions = [];
  let offset = 2;
  while (offset < body.length) {
    const filter = readString(body, offset, "topic filter");
    offset = filter.offset;
    if (offset >= body.length) {
      throw new ProtocolError("subscribe payload is truncated");
    }
    const qos = body[offset];
    offset += 1;
    if (qos > 1) {
      throw new ProtocolError("requested qos 2 is not supported");
    }
    validateTopicFilter(filter.value);
    subscriptions.push({ filter: filter.value, qos });
  }
  if (subscriptions.length === 0) {
    throw new ProtocolError("subscribe must contain at least one topic filter");
  }
  return { packetId, subscriptions };
}

export function encodeSuback(packetId, returnCodes) {
  const body = Buffer.alloc(2);
  body.writeUInt16BE(packetId, 0);
  return encodePacket(PACKET.SUBACK, 0, Buffer.concat([body, Buffer.from(returnCodes)]));
}

export function encodePingresp() {
  return encodePacket(PACKET.PINGRESP, 0, Buffer.alloc(0));
}
