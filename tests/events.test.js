import assert from "node:assert/strict";
import test from "node:test";

import { start } from "../src/server.js";

async function withServer(run) {
  const started = await start({ host: "127.0.0.1", port: 0, mqttPort: 0, database: ":memory:" });
  try {
    await run(started);
  } finally {
    await started.close();
  }
}

function seedEvent(service, overrides = {}) {
  return service.store.appendEvent({
    rule_id: "r1",
    event: "temp_high",
    source: "shadow",
    device_id: "device-1",
    topic: null,
    value: 1,
    occurred_at: "2024-01-01T00:00:00.000Z",
    ...overrides,
  });
}

/** A fixed, varied event set used by the query tests. */
function seed(service) {
  seedEvent(service, { rule_id: "r1", event: "temp_high", source: "shadow", device_id: "device-1", occurred_at: "2024-01-01T00:00:00.000Z" });
  seedEvent(service, { rule_id: "r2", event: "offline", source: "mqtt", device_id: null, topic: "status/2", occurred_at: "2024-01-01T00:00:05.000Z" });
  seedEvent(service, { rule_id: "r1", event: "temp_high", source: "shadow", device_id: "device-2", occurred_at: "2024-01-01T00:00:10.000Z" });
  seedEvent(service, { rule_id: "r2", event: "offline", source: "mqtt", device_id: null, topic: "status/4", occurred_at: "2024-01-01T00:00:10.000Z" });
  seedEvent(service, { rule_id: "r3", event: "temp_high", source: "mqtt", device_id: null, topic: "status/5", occurred_at: "2024-01-01T00:00:20.000Z" });
  seedEvent(service, { rule_id: "r4", event: "restored", source: "shadow", device_id: "device-2", occurred_at: "2024-01-01T00:00:30.500Z" });
}

async function getJson(started, path) {
  const response = await fetch(`${started.httpUrl}/events${path}`);
  return { status: response.status, body: await response.json() };
}

const sequences = (body) => body.events.map((entry) => entry.sequence);

test("events without parameters are returned in ascending sequence", async () => {
  await withServer(async (started) => {
    seed(started.service);
    const { status, body } = await getJson(started, "");
    assert.equal(status, 200);
    assert.deepEqual(sequences(body), [1, 2, 3, 4, 5, 6]);
    assert.deepEqual(Object.keys(body), ["events"]);
    assert.equal(body.events[0].event, "temp_high");
  });
});

test("event and source match complete strings exactly and intersect", async () => {
  await withServer(async (started) => {
    seed(started.service);
    assert.deepEqual(sequences(await (await getJson(started, "?event=temp_high")).body), [1, 3, 5]);
    assert.deepEqual(sequences(await (await getJson(started, "?event=offline")).body), [2, 4]);
    assert.deepEqual(sequences(await (await getJson(started, "?event=temp_hi")).body), []);
    assert.deepEqual(sequences(await (await getJson(started, "?source=shadow")).body), [1, 3, 6]);
    assert.deepEqual(sequences(await (await getJson(started, "?source=mqtt")).body), [2, 4, 5]);
    assert.deepEqual(sequences(await (await getJson(started, "?event=temp_high&source=shadow")).body), [1, 3]);
    assert.deepEqual(sequences(await (await getJson(started, "?event=temp_high&source=mqtt")).body), [5]);
    assert.deepEqual(sequences(await (await getJson(started, "?event=restored&source=mqtt")).body), []);
  });
});

test("new filters intersect with rule_id and device_id", async () => {
  await withServer(async (started) => {
    seed(started.service);
    assert.deepEqual(sequences(await (await getJson(started, "?rule_id=r1")).body), [1, 3]);
    assert.deepEqual(sequences(await (await getJson(started, "?device_id=device-2")).body), [3, 6]);
    assert.deepEqual(sequences(await (await getJson(started, "?device_id=device-2&rule_id=r1")).body), [3]);
    assert.deepEqual(sequences(await (await getJson(started, "?rule_id=r1&event=temp_high")).body), [1, 3]);
    assert.deepEqual(sequences(await (await getJson(started, "?device_id=device-2&source=shadow&event=restored")).body), [6]);
  });
});

test("the time window is half open on occurred_at", async () => {
  await withServer(async (started) => {
    seed(started.service);
    const t = "2024-01-01T00:00:";
    assert.deepEqual(sequences(await (await getJson(started, `?occurred_after=${encodeURIComponent(`${t}10.000Z`)}`)).body), [3, 4, 5, 6]);
    assert.deepEqual(sequences(await (await getJson(started, `?occurred_before=${encodeURIComponent(`${t}10.000Z`)}`)).body), [1, 2]);
    assert.deepEqual(
      sequences(await (await getJson(started, `?occurred_after=${encodeURIComponent(`${t}05.000Z`)}&occurred_before=${encodeURIComponent(`${t}20.000Z`)}`)).body),
      [2, 3, 4],
    );
    // Millisecond boundaries are honored exactly.
    assert.deepEqual(sequences(await (await getJson(started, `?occurred_before=${encodeURIComponent(`${t}30.500Z`)}`)).body), [1, 2, 3, 4, 5]);
    assert.deepEqual(sequences(await (await getJson(started, `?occurred_after=${encodeURIComponent(`${t}30.500Z`)}`)).body), [6]);
    // A boundary without fractional seconds is still millisecond exact.
    assert.deepEqual(sequences(await (await getJson(started, `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00Z")}`)).body), [1, 2, 3, 4, 5, 6]);
  });
});

test("after_sequence is strict and limit pages without overlap or loss", async () => {
  await withServer(async (started) => {
    seed(started.service);
    assert.deepEqual(sequences(await (await getJson(started, "?after_sequence=3")).body), [4, 5, 6]);
    assert.deepEqual(sequences(await (await getJson(started, "?after_sequence=0")).body), [1, 2, 3, 4, 5, 6]);
    assert.deepEqual(sequences(await (await getJson(started, "?limit=2")).body), [1, 2]);
    const pageOne = (await getJson(started, "?limit=2")).body.events;
    const pageTwo = (await getJson(started, `?limit=2&after_sequence=${pageOne.at(-1).sequence}`)).body.events;
    const pageThree = (await getJson(started, `?limit=2&after_sequence=${pageTwo.at(-1).sequence}`)).body.events;
    assert.deepEqual(sequences({ events: [...pageOne, ...pageTwo, ...pageThree] }), [1, 2, 3, 4, 5, 6]);
    // A short last page is honored, and further pages are empty.
    assert.deepEqual(sequences(await (await getJson(started, "?limit=1000")).body), [1, 2, 3, 4, 5, 6]);
    assert.deepEqual((await getJson(started, "?after_sequence=6")).body.events, []);
    // Paging composes with the other filters.
    assert.deepEqual(sequences(await (await getJson(started, "?source=shadow&limit=1&after_sequence=1")).body), [3]);
  });
});

test("events sharing occurred_at stay ordered by sequence and queries are stable", async () => {
  await withServer(async (started) => {
    seed(started.service);
    // Sequences 3 and 4 share occurred_at 00:00:10.000Z.
    const first = (await getJson(started, "?limit=4")).body.events.map((entry) => entry.sequence);
    const second = (await getJson(started, "?limit=4")).body.events.map((entry) => entry.sequence);
    assert.deepEqual(first, [1, 2, 3, 4]);
    assert.deepEqual(second, first);
  });
});

const BAD_REQUESTS = [
  ["unknown parameter", "?bogus=1", "bogus"],
  ["empty event", "?event=", "event"],
  ["empty source", "?source=", "source"],
  ["time without Z", `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00.000")}`, "occurred_after"],
  ["time with offset", `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00+00:00")}`, "occurred_after"],
  ["impossible date", `?occurred_after=${encodeURIComponent("2024-02-30T00:00:00Z")}`, "occurred_after"],
  ["impossible time", `?occurred_before=${encodeURIComponent("2024-01-01T24:00:00Z")}`, "occurred_before"],
  ["sub-millisecond precision", `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00.000001Z")}`, "occurred_after"],
  ["after equal to before", `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00Z")}&occurred_before=${encodeURIComponent("2024-01-01T00:00:00Z")}`, "occurred_after"],
  ["after later than before", `?occurred_after=${encodeURIComponent("2024-01-01T00:00:01Z")}&occurred_before=${encodeURIComponent("2024-01-01T00:00:00Z")}`, "occurred_after"],
  ["negative sequence", "?after_sequence=-1", "after_sequence"],
  ["non-numeric sequence", "?after_sequence=abc", "after_sequence"],
  ["fractional sequence", "?after_sequence=1.5", "after_sequence"],
  ["unsafe sequence", "?after_sequence=9007199254740992", "after_sequence"],
  ["limit zero", "?limit=0", "limit"],
  ["limit too large", "?limit=1001", "limit"],
  ["limit negative", "?limit=-3", "limit"],
  ["limit fractional", "?limit=2.5", "limit"],
  ["limit non-numeric", "?limit=ten", "limit"],
];

for (const [label, path, parameter] of BAD_REQUESTS) {
  test(`rejects ${label}`, async () => {
    await withServer(async (started) => {
      seed(started.service);
      const { status, body } = await getJson(started, path);
      assert.equal(status, 400);
      assert.equal(body.error.code, "validation_error");
      assert.match(body.error.message, new RegExp(parameter));
    });
  });
}

test("sub-millisecond boundaries with only trailing zeros are accepted", async () => {
  await withServer(async (started) => {
    seed(started.service);
    const { status } = await getJson(started, `?occurred_after=${encodeURIComponent("2024-01-01T00:00:00.0000Z")}`);
    assert.equal(status, 200);
  });
});
