import { Buffer } from "node:buffer";
import http from "node:http";
import { pathToFileURL } from "node:url";

import { Broker } from "./broker.js";
import { NotFoundError, ShadowlinkError, ValidationError } from "./errors.js";
import { Service } from "./service.js";

const MAX_BODY_BYTES = 1000000;
const QUERY_FIELDS = new Map([
  ["rule_id", "ruleId"],
  ["device_id", "deviceId"],
  ["event", "event"],
  ["source", "source"],
  ["occurred_after", "occurredAfter"],
  ["occurred_before", "occurredBefore"],
  ["after_sequence", "afterSequence"],
  ["limit", "limit"],
]);

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

function readQuery(url) {
  const query = {};
  for (const [name, value] of url.searchParams) {
    const field = QUERY_FIELDS.get(name);
    if (field === undefined) {
      throw new ValidationError(`unknown query parameter ${name}`);
    }
    query[field] = value;
  }
  return query;
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
