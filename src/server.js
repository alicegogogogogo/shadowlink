import { Buffer } from "node:buffer";
import http from "node:http";
import { pathToFileURL } from "node:url";

import { Broker } from "./broker.js";
import { NotFoundError, ShadowlinkError, ValidationError } from "./errors.js";
import { Service } from "./service.js";

const MAX_BODY_BYTES = 1000000;
const QUERY_FIELDS = new Set([
  "rule_id",
  "device_id",
  "event",
  "source",
  "occurred_after",
  "occurred_before",
  "after_sequence",
  "limit",
]);
const RFC3339_UTC = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?Z$/;
const NON_NEGATIVE_INTEGER = /^(?:0|[1-9][0-9]*)$/;
const POSITIVE_INTEGER = /^[1-9][0-9]*$/;

function requireNonEmpty(name, value) {
  if (value === "") {
    throw new ValidationError(`${name} query parameter must not be empty`);
  }
}

/**
 * Parse a strict RFC3339 UTC timestamp (the only shape occurred_at is written
 * in). V8's parser rolls impossible calendar values over, so a canonical
 * round trip is required. Returns the canonical form without the trailing Z,
 * which is the shape julianday() compares against.
 */
function parseTimeParameter(name, value) {
  const match = RFC3339_UTC.exec(value);
  if (match === null) {
    throw new ValidationError(`${name} query parameter must be an RFC3339 UTC timestamp ending in Z`);
  }
  const [, year, month, day, hour, minute, second, fractionRaw] = match;
  const fraction = fractionRaw ?? "";
  // Events are stamped at millisecond precision, so a boundary with nonzero
  // sub-millisecond digits could never be honored exactly; reject it rather
  // than truncating it silently.
  if (fraction.length > 3 && fraction.slice(3) !== "0".repeat(fraction.length - 3)) {
    throw new ValidationError(`${name} query parameter must be an RFC3339 UTC timestamp ending in Z`);
  }
  const canonical = `${year}-${month}-${day}T${hour}:${minute}:${second}.${fraction.padEnd(3, "0").slice(0, 3)}Z`;
  const parsed = new Date(canonical);
  if (Number.isNaN(parsed.getTime()) || parsed.toISOString() !== canonical) {
    throw new ValidationError(`${name} query parameter must be an RFC3339 UTC timestamp ending in Z`);
  }
  return { withoutZ: canonical.slice(0, -1), millis: parsed.getTime() };
}

function parseSequenceParameter(value) {
  if (!NON_NEGATIVE_INTEGER.test(value)) {
    throw new ValidationError("after_sequence query parameter must be a safe non-negative integer");
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number)) {
    throw new ValidationError("after_sequence query parameter must be a safe non-negative integer");
  }
  return number;
}

function parseLimitParameter(value) {
  if (!POSITIVE_INTEGER.test(value)) {
    throw new ValidationError("limit query parameter must be an integer between 1 and 1000");
  }
  const number = Number(value);
  if (number > 1000) {
    throw new ValidationError("limit query parameter must be an integer between 1 and 1000");
  }
  return number;
}

function readQuery(url) {
  const query = {};
  for (const [name, value] of url.searchParams) {
    if (!QUERY_FIELDS.has(name)) {
      throw new ValidationError(`unknown query parameter ${name}`);
    }
    if (Object.hasOwn(query, name)) {
      throw new ValidationError(`${name} query parameter must appear at most once`);
    }
    switch (name) {
      case "event":
      case "source":
        requireNonEmpty(name, value);
        query[name] = value;
        break;
      case "occurred_after":
      case "occurred_before":
        query[name] = parseTimeParameter(name, value);
        break;
      case "after_sequence":
        query[name] = parseSequenceParameter(value);
        break;
      case "limit":
        query[name] = parseLimitParameter(value);
        break;
      default:
        query[name] = value;
    }
  }
  const after = query.occurred_after;
  const before = query.occurred_before;
  if (after !== undefined && before !== undefined && after.millis >= before.millis) {
    throw new ValidationError("occurred_after query parameter must be earlier than occurred_before");
  }
  return {
    ruleId: query.rule_id,
    deviceId: query.device_id,
    event: query.event,
    source: query.source,
    occurredAfter: after?.withoutZ,
    occurredBefore: before?.withoutZ,
    afterSequence: query.after_sequence,
    limit: query.limit,
  };
}

async function readJson(request) {
  const contentType = (request.headers["content-type"] ?? "").split(";")[0].trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new ValidationError("Content-Type must be application/json");
  }
  const chunks = [];
  let size = 0;
  for await (const chunk of request) {
    size += chunk.length;
    if (size > MAX_BODY_BYTES) {
      throw new ValidationError(`request body must be at most ${MAX_BODY_BYTES} bytes`);
    }
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8"));
  } catch {
    throw new ValidationError("request body must be valid JSON");
  }
}

function readSegments(url) {
  try {
    return url.pathname.split("/").filter((part) => part.length > 0).map((part) => decodeURIComponent(part));
  } catch {
    throw new ValidationError("request path must be percent-encoded correctly");
  }
}

async function dispatch(service, request) {
  const url = new URL(request.url, "http://localhost");
  const parts = readSegments(url);
  const method = request.method;
  if (method === "GET" && parts.length === 1 && parts[0] === "health") {
    return { status: 200, body: service.health() };
  }
  if (method === "GET" && parts.length === 3 && parts[0] === "devices" && parts[2] === "shadow") {
    return { status: 200, body: service.getShadow(parts[1]) };
  }
  if (method === "POST" && parts.length === 3 && parts[0] === "devices" && parts[2] === "shadow") {
    const body = await readJson(request);
    return { status: 200, body: service.updateShadow(parts[1], body, request.headers["idempotency-key"]) };
  }
  if (method === "POST" && parts.length === 3 && parts[0] === "devices" && parts[2] === "reported") {
    const body = await readJson(request);
    return { status: 200, body: service.reportState(parts[1], body, request.headers["idempotency-key"]) };
  }
  if (method === "POST" && parts.length === 1 && parts[0] === "rules") {
    const body = await readJson(request);
    return { status: 201, body: service.createRule(body, request.headers["idempotency-key"]) };
  }
  if (method === "GET" && parts.length === 1 && parts[0] === "events") {
    return { status: 200, body: service.events(readQuery(url)) };
  }
  throw new NotFoundError("route was not found");
}

function respond(response, status, body) {
  const payload = Buffer.from(JSON.stringify(body), "utf8");
  response.writeHead(status, { "Content-Type": "application/json; charset=utf-8", "Content-Length": payload.length });
  response.end(payload);
}

export async function start(options = {}) {
  const host = options.host ?? "127.0.0.1";
  const port = options.port ?? 8080;
  const service = new Service(options.database ?? "shadowlink.db", options.now === undefined ? {} : { now: options.now });
  const broker = new Broker(service, { host, port: options.mqttPort ?? port + 1 });
  service.setPublisher((topic, payload, publishOptions) => broker.publish(topic, payload, publishOptions));
  await broker.listen();
  const httpServer = http.createServer((request, response) => {
    dispatch(service, request)
      .then((result) => respond(response, result.status, result.body))
      .catch((error) => {
        if (error instanceof ShadowlinkError) {
          respond(response, error.status, { error: { code: error.code, message: error.message } });
          return;
        }
        respond(response, 500, { error: { code: "internal_error", message: "internal server error" } });
      });
  });
  await new Promise((resolve, reject) => {
    httpServer.once("error", reject);
    httpServer.listen(port, host, resolve);
  });
  const close = async () => {
    const closed = new Promise((resolve) => httpServer.close(() => resolve()));
    httpServer.closeIdleConnections();
    const forced = setTimeout(() => httpServer.closeAllConnections(), 1000);
    forced.unref();
    await closed;
    clearTimeout(forced);
    await broker.close();
    service.close();
  };
  return {
    service,
    broker,
    httpServer,
    close,
    httpUrl: `http://${host}:${httpServer.address().port}`,
    mqttUrl: `mqtt://${broker.host}:${broker.port}`,
  };
}

export function parseArguments(argv) {
  const options = { host: "127.0.0.1", port: 8080, mqttPort: null, database: "shadowlink.db" };
  const flags = new Map([
    ["--host", "host"],
    ["--port", "port"],
    ["--mqtt-port", "mqttPort"],
    ["--database", "database"],
  ]);
  for (let index = 0; index < argv.length; index += 2) {
    const flag = argv[index];
    const value = argv[index + 1];
    const key = flags.get(flag);
    if (key === undefined) {
      throw new ValidationError(`unknown option ${flag}`);
    }
    if (value === undefined || value === "") {
      throw new ValidationError(`option ${flag} requires a value`);
    }
    if (flag === "--port" || flag === "--mqtt-port") {
      const number = Number(value);
      if (!Number.isInteger(number) || number < 0 || number > 65535) {
        throw new ValidationError(`${flag} must be an integer between 0 and 65535`);
      }
      options[key] = number;
    } else {
      options[key] = value;
    }
  }
  return options;
}

export async function main(argv = process.argv.slice(2)) {
  const started = await start(parseArguments(argv));
  console.log(`Shadowlink listening on ${started.httpUrl}`);
  console.log(`Shadowlink MQTT broker listening on ${started.mqttUrl}`);
  const shutdown = () => {
    started.close().then(() => process.exit(0));
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
  return started;
}

if (process.argv[1] !== undefined && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exit(1);
  });
}
