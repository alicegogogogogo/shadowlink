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

  listEvents({ ruleId = null, deviceId = null, event = null, source = null, occurredAfter = null, occurredBefore = null, afterSequence = null, limit = null } = {}) {
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
      clauses.push("occurred_at >= ?");
      parameters.push(occurredAfter);
    }
    if (occurredBefore !== null) {
      clauses.push("occurred_at < ?");
      parameters.push(occurredBefore);
    }
    if (afterSequence !== null) {
      clauses.push("sequence > ?");
      parameters.push(afterSequence);
    }
    const where = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
    let sql = `SELECT * FROM events${where} ORDER BY sequence`;
    if (limit !== null) {
      sql += " LIMIT ?";
      parameters.push(limit);
    }
    return this.database
      .prepare(sql)
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
}
