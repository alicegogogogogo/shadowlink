import { mkdirSync } from "node:fs";
import { dirname } from "node:path";
import { DatabaseSync } from "node:sqlite";

const SCHEMA = `
CREATE TABLE IF NOT EXISTS shadows (
  device_id TEXT PRIMARY KEY,
  document TEXT NOT NULL,
  version INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS rules (
  id TEXT PRIMARY KEY,
  document TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS rule_state (
  rule_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  matched INTEGER NOT NULL,
  PRIMARY KEY (rule_id, device_id)
);
CREATE TABLE IF NOT EXISTS events (
  sequence INTEGER PRIMARY KEY AUTOINCREMENT,
  rule_id TEXT NOT NULL,
  event TEXT NOT NULL,
  source TEXT NOT NULL,
  device_id TEXT,
  topic TEXT,
  value TEXT NOT NULL,
  occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS retained (
  topic TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  qos INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS idempotency (
  key TEXT PRIMARY KEY,
  operation TEXT NOT NULL,
  response TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS telemetry_samples (
  device_id TEXT NOT NULL,
  metric TEXT NOT NULL,
  ts INTEGER NOT NULL,
  value REAL NOT NULL,
  seq INTEGER PRIMARY KEY AUTOINCREMENT
);
CREATE INDEX IF NOT EXISTS telemetry_query
  ON telemetry_samples (device_id, metric, ts);
CREATE TABLE IF NOT EXISTS sessions (
  client_id TEXT PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS session_subscriptions (
  client_id TEXT NOT NULL,
  topic_filter TEXT NOT NULL,
  qos INTEGER NOT NULL,
  PRIMARY KEY (client_id, topic_filter)
);
CREATE TABLE IF NOT EXISTS session_queue (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id TEXT NOT NULL,
  topic TEXT NOT NULL,
  payload TEXT NOT NULL,
  retain INTEGER NOT NULL,
  packet_id INTEGER
);
CREATE INDEX IF NOT EXISTS session_queue_client
  ON session_queue (client_id, seq);
`;

export class Store {
  constructor(path) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true });
    }
    this.database = new DatabaseSync(path);
    this.database.exec("PRAGMA journal_mode = WAL");
    this.database.exec(SCHEMA);
  }

  close() {
    this.database.close();
  }

  transaction(action) {
    this.database.exec("BEGIN IMMEDIATE");
    try {
      const result = action();
      this.database.exec("COMMIT");
      return result;
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  /**
   * Documents are stored as JSON with their insertion order preserved, so a
   * document read back from the store is byte-identical to the document that
   * was written and an idempotent replay returns the original bytes.
   */
  encode(value) {
    return JSON.stringify(value);
  }

  decode(value) {
    return JSON.parse(value);
  }

  getShadow(deviceId) {
    const row = this.database.prepare("SELECT document FROM shadows WHERE device_id = ?").get(deviceId);
    return row ? this.decode(row.document) : null;
  }

  putShadow(deviceId, document) {
    this.database
      .prepare(
        `INSERT INTO shadows (device_id, document, version) VALUES (?, ?, ?)
         ON CONFLICT(device_id) DO UPDATE SET document = excluded.document, version = excluded.version`,
      )
      .run(deviceId, this.encode(document), document.version);
  }

  getRule(id) {
    const row = this.database.prepare("SELECT document FROM rules WHERE id = ?").get(id);
    return row ? this.decode(row.document) : null;
  }

  insertRule(rule) {
    this.database.prepare("INSERT INTO rules (id, document) VALUES (?, ?)").run(rule.id, this.encode(rule));
  }

  listRules() {
    return this.database
      .prepare("SELECT document FROM rules ORDER BY id")
      .all()
      .map((row) => this.decode(row.document));
  }

  getRuleState(ruleId, deviceId) {
    const row = this.database.prepare("SELECT matched FROM rule_state WHERE rule_id = ? AND device_id = ?").get(ruleId, deviceId);
    return row ? row.matched === 1 : false;
  }

  setRuleState(ruleId, deviceId, matched) {
    this.database
      .prepare(
        `INSERT INTO rule_state (rule_id, device_id, matched) VALUES (?, ?, ?)
         ON CONFLICT(rule_id, device_id) DO UPDATE SET matched = excluded.matched`,
      )
      .run(ruleId, deviceId, matched ? 1 : 0);
  }

  appendEvent(event) {
    const info = this.database
      .prepare("INSERT INTO events (rule_id, event, source, device_id, topic, value, occurred_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
      .run(event.rule_id, event.event, event.source, event.device_id ?? null, event.topic ?? null, this.encode(event.value), event.occurred_at);
    return { sequence: Number(info.lastInsertRowid), ...event };
  }

  listEvents({
    ruleId = null,
    deviceId = null,
    event = null,
    source = null,
    occurredAfter = null,
    occurredBefore = null,
    afterSequence = null,
    limit = null,
  } = {}) {
    // occurred_at is canonical RFC3339 UTC ending in Z; removing the Z yields
    // the form julianday() reads, so the half-open window is exact down to the
    // millisecond rather than a lexical comparison.
    const windowOf = () => "julianday(substr(occurred_at, 1, length(occurred_at) - 1))";
    const clauses = [];
    const parameters = [];
    if (ruleId !== null) {
      clauses.push("rule_id = ?");
      parameters.push(ruleId);
    }
    if (deviceId !== null) {
      clauses.push("device_id = ?");
      parameters.push(deviceId);
    }
    if (event !== null) {
      clauses.push("event = ?");
      parameters.push(event);
    }
    if (source !== null) {
      clauses.push("source = ?");
      parameters.push(source);
    }
    if (occurredAfter !== null) {
      clauses.push(`${windowOf()} >= julianday(?)`);
      parameters.push(occurredAfter);
    }
    if (occurredBefore !== null) {
      clauses.push(`${windowOf()} < julianday(?)`);
      parameters.push(occurredBefore);
    }
    if (afterSequence !== null) {
      clauses.push("sequence > ?");
      parameters.push(afterSequence);
    }
    const where = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
    if (limit !== null) {
      parameters.push(limit);
    }
    const cap = limit === null ? "" : " LIMIT ?";
    return this.database
      .prepare(`SELECT * FROM events${where} ORDER BY sequence${cap}`)
      .all(...parameters)
      .map((row) => ({
        sequence: row.sequence,
        rule_id: row.rule_id,
        event: row.event,
        source: row.source,
        device_id: row.device_id,
        topic: row.topic,
        value: this.decode(row.value),
        occurred_at: row.occurred_at,
      }));
  }

  putRetained(topic, payload, qos) {
    this.database
      .prepare(
        `INSERT INTO retained (topic, payload, qos) VALUES (?, ?, ?)
         ON CONFLICT(topic) DO UPDATE SET payload = excluded.payload, qos = excluded.qos`,
      )
      .run(topic, payload.toString("base64"), qos);
  }

  deleteRetained(topic) {
    this.database.prepare("DELETE FROM retained WHERE topic = ?").run(topic);
  }

  listRetained() {
    return this.database
      .prepare("SELECT topic, payload, qos FROM retained ORDER BY topic")
      .all()
      .map((row) => ({ topic: row.topic, payload: Buffer.from(row.payload, "base64"), qos: row.qos }));
  }

  getIdempotency(key) {
    const row = this.database.prepare("SELECT operation, response FROM idempotency WHERE key = ?").get(key);
    return row ? { operation: row.operation, response: this.decode(row.response) } : null;
  }

  putIdempotency(key, operation, response) {
    this.database
      .prepare("INSERT INTO idempotency (key, operation, response) VALUES (?, ?, ?)")
      .run(key, operation, this.encode(response));
  }

  /**
   * Load every persistent session with its subscriptions and its queued
   * messages. Queue rows come back in insertion (publication) order; a null
   * packetId marks a message that was never sent, a non-null one a message
   * that was sent but never acknowledged.
   */
  listSessions() {
    const subscriptions = this.database.prepare("SELECT client_id, topic_filter, qos FROM session_subscriptions").all();
    const queued = this.database.prepare("SELECT seq, client_id, topic, payload, retain, packet_id FROM session_queue ORDER BY seq").all();
    return this.database
      .prepare("SELECT client_id FROM sessions ORDER BY client_id")
      .all()
      .map((row) => ({
        clientId: row.client_id,
        subscriptions: subscriptions
          .filter((entry) => entry.client_id === row.client_id)
          .map((entry) => ({ filter: entry.topic_filter, qos: entry.qos })),
        queue: queued
          .filter((entry) => entry.client_id === row.client_id)
          .map((entry) => ({
            seq: entry.seq,
            topic: entry.topic,
            payload: Buffer.from(entry.payload, "base64"),
            retain: entry.retain === 1,
            packetId: entry.packet_id,
          })),
      }));
  }

  createSession(clientId) {
    this.database.prepare("INSERT INTO sessions (client_id) VALUES (?)").run(clientId);
  }

  deleteSession(clientId) {
    this.database.prepare("DELETE FROM sessions WHERE client_id = ?").run(clientId);
    this.database.prepare("DELETE FROM session_subscriptions WHERE client_id = ?").run(clientId);
    this.database.prepare("DELETE FROM session_queue WHERE client_id = ?").run(clientId);
  }

  putSessionSubscription(clientId, filter, qos) {
    this.database
      .prepare(
        `INSERT INTO session_subscriptions (client_id, topic_filter, qos) VALUES (?, ?, ?)
         ON CONFLICT(client_id, topic_filter) DO UPDATE SET qos = excluded.qos`,
      )
      .run(clientId, filter, qos);
  }

  /** Append one queued message; returns its sequence, which orders the queue. */
  appendSessionMessage(clientId, topic, payload, retain, packetId) {
    const info = this.database
      .prepare("INSERT INTO session_queue (client_id, topic, payload, retain, packet_id) VALUES (?, ?, ?, ?, ?)")
      .run(clientId, topic, payload.toString("base64"), retain ? 1 : 0, packetId);
    return Number(info.lastInsertRowid);
  }

  /** Record the packet identifier a queued message was sent with. */
  markSessionMessageSent(seq, packetId) {
    this.database.prepare("UPDATE session_queue SET packet_id = ? WHERE seq = ?").run(packetId, seq);
  }

  deleteSessionMessage(seq) {
    this.database.prepare("DELETE FROM session_queue WHERE seq = ?").run(seq);
  }

  /** Persist one telemetry sample stamped at the moment it was received. */
  insertTelemetrySample(deviceId, metric, timestampMs, value) {
    this.database
      .prepare("INSERT INTO telemetry_samples (device_id, metric, ts, value) VALUES (?, ?, ?, ?)")
      .run(deviceId, metric, timestampMs, value);
  }

  /**
   * Aggregate the samples of one device/metric inside the half-open window
   * [fromMs, toMs) into Unix-epoch aligned buckets of `bucketSeconds` width.
   * Empty buckets are omitted; rows come back in ascending bucket order. The
   * aggregation function is chosen by the caller from a fixed whitelist.
   */
  aggregateTelemetry({ deviceId, metric, fromMs, toMs, bucketSeconds, aggregate }) {
    const widthMs = bucketSeconds * 1000;
    let expression;
    switch (aggregate) {
      case "avg":
        expression = "AVG(value)";
        break;
      case "min":
        expression = "MIN(value)";
        break;
      case "max":
        expression = "MAX(value)";
        break;
      case "sum":
        expression = "SUM(value)";
        break;
      case "count":
        expression = "COUNT(*)";
        break;
      default:
        throw new Error(`unsupported telemetry aggregate ${aggregate}`);
    }
    return this.database
      .prepare(
        // Bucket start is floor(ts / width) * width, aligned to the Unix
        // epoch, including pre-1970 (negative) timestamps. Bound parameters
        // arrive as doubles, so `/` would be float division; `%` stays exact
        // at these magnitudes. The double-mod normalizes SQLite's truncated
        // remainder into a floored remainder in [0, width) for either sign.
        `SELECT ts - (((ts % ?) + ?) % ?) AS bucket, ${expression} AS value, COUNT(*) AS count
         FROM telemetry_samples
         WHERE device_id = ? AND metric = ? AND ts >= ? AND ts < ?
         GROUP BY bucket
         ORDER BY bucket ASC`,
      )
      .all(widthMs, widthMs, widthMs, deviceId, metric, fromMs, toMs)
      .map((row) => ({ startMs: row.bucket, value: row.value, count: row.count }));
  }
}
