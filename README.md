# Shadowlink

Shadowlink is a small IoT backend that speaks a subset of MQTT 3.1.1 over TCP,
keeps a device shadow for every thing that reports state, and turns shadow
updates and telemetry into events with a rule engine.

The release intentionally supports a compact public contract:

- the broker implements `CONNECT`, `CONNACK`, `PUBLISH`, `PUBACK`, `SUBSCRIBE`,
  `SUBACK`, `PINGREQ`, `PINGRESP` and `DISCONNECT` for QoS 0 and QoS 1;
- a shadow stores `desired` and `reported` state and derives `delta` from them;
- a rule fires an event on a shadow transition or on a matching publication;
- every state change is written to SQLite, and `Idempotency-Key` makes
  repeating a POST return the first response instead of applying it twice.

## Requirements

- Node.js 22.5.0 or newer (the store uses the built-in `node:sqlite`, which
  prints an `ExperimentalWarning` on Node 22)
- no third-party runtime dependencies; only `node:net`, `node:http`,
  `node:sqlite`, `node:buffer`, `node:fs`, `node:path` and `node:url` are used

## Run the service

```bash
node src/server.js --host 127.0.0.1 --port 8080 --mqtt-port 8081 --database shadowlink.db
```

`--mqtt-port` defaults to `--port` plus one, `--host` to `127.0.0.1`,
`--port` to `8080` and `--database` to `shadowlink.db`. After both sockets are
bound the process prints:

```
Shadowlink listening on http://127.0.0.1:8080
Shadowlink MQTT broker listening on mqtt://127.0.0.1:8081
```

## MQTT broker

### Packet types

| Packet | Value | Direction | Fixed header flags | Variables |
| --- | --- | --- | --- | --- |
| `CONNECT` | 1 | client → broker | `0` | protocol name `MQTT`, level `4`, flags, keep alive, client id, optional will |
| `CONNACK` | 2 | broker → client | `0` | session present (`0` or `1`), return code |
| `PUBLISH` | 3 | both | `DUP<<3 \| QoS<<1 \| RETAIN` | topic name, packet id when QoS > 0, payload |
| `PUBACK` | 4 | both | `0` | packet id |
| `SUBSCRIBE` | 8 | client → broker | `2` | packet id, then (topic filter, requested QoS) pairs |
| `SUBACK` | 9 | broker → client | `0` | packet id, then one return code per filter (`0`, `1` or `0x80`) |
| `PINGREQ` | 12 | client → broker | `0` | none |
| `PINGRESP` | 13 | broker → client | `0` | none |
| `DISCONNECT` | 14 | client → broker | `0` | none |

Any other packet type, any wrong fixed header flag, or a first packet that is
not `CONNECT` is a protocol error: the broker closes the connection without a
reply. `UNSUBSCRIBE`, QoS 2, username/password (return code `4`) and `will`
QoS 2 are not supported.

### Remaining Length

Every packet is `fixed header byte`, then the Remaining Length of the variable
header plus payload, encoded as the MQTT base-128 varint: seven bits per byte,
little endian, high bit set on every byte that is followed by another. The
encoding always uses the fewest bytes, and at most four bytes are accepted;
`0x80 0x80 0x80 0x80` (a fifth byte) or anything above `268435455` is a
protocol error.

| Value | Bytes | Value | Bytes |
| --- | --- | --- | --- |
| 0 | `00` | 16384 | `80 80 01` |
| 127 | `7F` | 2097152 | `80 80 80 01` |
| 128 | `80 01` | 268435455 | `FF FF FF 7F` |
| 16383 | `FF 7F` | | |

Decoding never blocks on a partial packet: the broker keeps the bytes it has
and waits for the rest. A packet whose Remaining Length exceeds 1048576 bytes
is a protocol error.

### CONNECT and sessions

The connect flags byte carries, from bit 7 down: username, password, will
retain, will QoS (2 bits), will flag, clean session, reserved. In this subset
username and password must be absent, the reserved bit must be zero, and the
will QoS must be `0` or `1`. The client identifier must be 1 to 128 UTF-8
bytes and must not already be connected: a second connection that reuses a
live identifier is refused with return code `2` and the existing connection
keeps running. Return codes are `0` accepted, `1` unacceptable protocol
version, `2` identifier rejected, `3` server unavailable, `4` bad
username/password.

With clean session `1` the connection uses a throwaway session: any
persistent session stored for that client identifier is discarded, `CONNACK`
reports session present `0`, and subscriptions and in-flight deliveries vanish
when the connection ends. With clean session `0` the broker creates a
persistent session for the client identifier — or resumes the existing one —
and the session survives network drops, graceful `DISCONNECT` and broker
restarts. `CONNACK` reports session present `0` when the session is created
and `1` when an existing persistent session is resumed.

A persistent session stores its subscriptions when the `SUBSCRIBE` succeeds;
re-subscribing a filter overwrites the stored granted QoS. A resumed session
needs no new `SUBSCRIBE`, and resuming alone never replays retained messages.
While the client is offline, a publication that matches the saved
subscriptions is queued only when its final delivery QoS is `1`
(`min(published QoS, highest granted QoS)`); a final QoS of `0` is dropped.
Matching filters still merge to one queued copy per client, the queue keeps
publication order, and already queued messages are unaffected by later
subscription changes. After `CONNACK` the broker first retransmits the
messages it sent but never got a `PUBACK` for — in their original send order,
with the original packet id and `DUP` set — and then sends the never-sent
queued messages in queue order with freshly allocated non-zero packet ids and
`DUP` clear. Each `PUBACK` removes only the matching pending message, so an
acknowledged message never reappears on a later resume.

A `CONNECT` that fails for one of those reasons is answered with the matching
return code and then closed; every other malformed packet closes the connection
without a reply.

With a non-zero keep alive the broker drops a client that sends nothing for
1.5 times the interval. If the connection ends without a `DISCONNECT` — socket
error, keep alive expiry, protocol error — the will is published to its topic
at its QoS and retain flag. A graceful `DISCONNECT` never publishes the will.

Verified bytes for `CONNECT` with client id `sensor-1`, clean session and keep
alive 60, then the matching `CONNACK`, a `SUBSCRIBE` of `sensors/+/temp` with
packet id 1, its `SUBACK`, a QoS 0 and a QoS 1 `PUBLISH` of
`sensors/kitchen/temp` carrying `{"value":33}`, `PUBACK`, `PINGREQ` and
`DISCONNECT`:

```
CONNECT    10 14 00 04 4D 51 54 54 04 02 00 3C 00 08 73 65 6E 73 6F 72 2D 31
CONNACK    20 02 00 00
SUBSCRIBE  82 13 00 01 00 0E 73 65 6E 73 6F 72 73 2F 2B 2F 74 65 6D 70 00
SUBACK     90 03 00 01 00
PUBLISH    30 22 00 14 73 65 6E 73 6F 72 73 2F 6B 69 74 63 68 65 6E 2F 74 65 6D 70 7B 22 76 61 6C 75 65 22 3A 33 33 7D
PUBLISH q1 32 24 00 14 73 65 6E 73 6F 72 73 2F 6B 69 74 63 68 65 6E 2F 74 65 6D 70 00 07 7B 22 76 61 6C 75 65 22 3A 33 33 7D
PUBACK     40 02 00 07
PINGREQ    C0 00
DISCONNECT E0 00
```

### Topic names and filters

A topic name is 1 to 65535 UTF-8 bytes, must not contain `U+0000` and must not
contain `#` or `+`. A topic filter may contain wildcards: `+` matches exactly
one level (an empty level counts) and `#` matches the parent level and every
level below it. `#` must occupy the last level on its own and `+` must occupy a
whole level, so `sport/#`, `sport/+/player1`, `+` and `#` are valid while
`sport/tennis#`, `sport/#/ranking`, `sport/+player1` and `sport/tennis/+x` are
not. Levels are separated by `/`.

Matching follows MQTT 3.1.1 section 4.7:

| Filter | Topic | Match |
| --- | --- | --- |
| `sport/tennis/+` | `sport/tennis/player1` | yes |
| `sport/tennis/+` | `sport/tennis/player1/ranking` | no |
| `sport/+` | `sport/` | yes |
| `sport/+` | `sport` | no |
| `sport/#` | `sport` | yes |
| `sport/#` | `sport/tennis/player1` | yes |
| `+/+` | `/finance` | yes |
| `#` | `$SYS/broker/uptime` | no |
| `$SYS/#` | `$SYS/broker/uptime` | yes |

A filter whose first level is `#` or `+` never matches a topic that starts
with `$`, so broker bookkeeping and the reserved shadow topics stay isolated
unless they are subscribed to explicitly.

### Delivery, QoS and retained messages

A publication is stored if retained, fanned out to matching subscriptions, and
then handed to the rule engine. Fan-out sends **at most one copy per client**,
at the highest QoS granted by that client's matching filters, and includes the
publisher itself when it subscribes to its own topic. Granted QoS is
`min(published QoS, subscription QoS)`; `SUBACK` reports the granted value.

Inbound QoS 1 publications are acknowledged with `PUBACK` after routing. A
packet id that was already acknowledged on the same connection is acknowledged
again but **not routed twice**, whether or not `DUP` is set, which makes client
retransmissions idempotent. Outbound QoS 1 publications get a per-connection
packet id starting at 1 that skips ids still awaiting `PUBACK`. Clean-session
deliveries are never retransmitted; persistent sessions retransmit their
unacknowledged deliveries on resume as described above.

A `PUBLISH` with `RETAIN` and a non-empty payload replaces the retained message
for its topic; an empty payload with `RETAIN` clears it. Retained messages are
delivered only when a `SUBSCRIBE` arrives, at `min(subscription QoS, retained
QoS)`, with `RETAIN` set, at most one copy per topic per `SUBSCRIBE`. Normal
fan-out always clears `RETAIN`. Retained replays are not rule inputs.

### Reserved topics

The broker itself publishes on two topics under `$shadow/`:

- `$shadow/<deviceId>/update` — a client `PUBLISH` whose UTF-8 payload is a
  shadow update body merges into that device's shadow, exactly like
  `POST /devices/<deviceId>/shadow`. The publication is routed to subscribers
  like any other message; a payload that is not a valid update body, or a topic
  whose device id is not a valid identifier, is routed but ignored.
- `$shadow/<deviceId>/delta` — after an accepted write whose `delta` is not
  empty, the broker publishes the delta as JSON at QoS 0 with `RETAIN` clear.
- `$telemetry/<deviceId>/<metric>` — a client `PUBLISH` whose UTF-8 payload is a
  single JSON number stores one telemetry sample for that device and metric,
  stamped at receipt time. The publication is routed to subscribers and run
  through the `mqtt` rules exactly like any other publication, but telemetry
  itself never writes an event. A topic that is not exactly the three reserved
  segments, an illegal device id or metric, a payload that is not a JSON
  number, or a `NaN`/`Infinity` value stores nothing: the message is still a
  normal publication and raises no protocol error. QoS 0 and QoS 1 keep their
  usual delivery semantics, so a retransmitted QoS 1 packet (same packet id on
  one connection) is acknowledged again but records only the first sample.

## Telemetry

Samples are numeric observations grouped by a device and a metric. Both
identifiers follow the usual rule
(`[A-Za-z0-9][A-Za-z0-9._:-]*`, 1 to 100 characters). Samples persist in
SQLite and survive restarts; querying a device that never reported a sample
creates neither a shadow nor an event.

### `GET /devices/{deviceId}/telemetry`

Aggregates a device/metric's samples into fixed-width buckets. All five query
parameters are required, may appear at most once, and no other parameter is
accepted:

- `metric` — the metric identifier;
- `from` and `to` — RFC3339 UTC timestamps ending in `Z`, selecting the
  half-open range `ts >= from` and `ts < to`, with `from` strictly earlier than
  `to`;
- `bucket_seconds` — an integer between `1` and `86400`;
- `aggregate` — one of `avg`, `min`, `max`, `sum`, `count`.

Buckets are aligned to the Unix epoch (`start = floor(ts / width) * width`) and
returned in ascending order; only non-empty buckets are returned, so a window
without samples yields `"buckets": []`. Every timestamp in the response is
RFC3339 UTC ending in `Z`; `end` is the (exclusive) bucket boundary, `value` is
the aggregate and `count` is the number of samples in the bucket.

```bash
curl -s 'http://127.0.0.1:8080/devices/sensor-1/telemetry?metric=temperature&from=2024-05-01T10:00:00Z&to=2024-05-01T11:00:00Z&bucket_seconds=600&aggregate=avg'
```
```json
{"device_id":"sensor-1","metric":"temperature","from":"2024-05-01T10:00:00.000Z","to":"2024-05-01T11:00:00.000Z","bucket_seconds":600,"aggregate":"avg","buckets":[{"start":"2024-05-01T10:00:00.000Z","end":"2024-05-01T10:10:00.000Z","value":21.5,"count":4}]}
```

A missing, duplicated or unknown parameter, an illegal `device_id` or `metric`,
a malformed timestamp, `from` at or after `to`, or an out-of-range
`bucket_seconds`/`aggregate` is rejected with `400 validation_error`.

## Device shadows

A shadow document is exactly:

```json
{
  "device_id": "sensor-1",
  "state": {"desired": {}, "reported": {}},
  "delta": {},
  "version": 1,
  "updated_at": "2024-05-01T10:00:00.000Z"
}
```

An update body is exactly `{"state": {...}}` and `state` holds `desired`,
`reported`, or both; each must be an object. The patch is merged recursively:
nested objects merge key by key, `null` deletes the key at any depth, and every
other value — including a whole array — replaces the target. A device's first
accepted write is `version` 1, and **every** accepted write increments it, even
one that changes nothing. `updated_at` is the service clock in UTC ISO-8601
with milliseconds; the clock can be injected as `start({now})`.

`delta` is `desired` minus `reported`, computed after the merge for every key
of `desired`:

- `desired` has a key that `reported` lacks → the whole desired value is in the
  delta;
- both sides hold objects → the delta recurses and the key is omitted when the
  nested delta is empty;
- otherwise the values are compared structurally (arrays by position, numbers
  by value) and a difference puts the whole desired value in the delta;
- keys that exist only in `reported` never appear, and nothing to apply yields
  an empty object `{}`, never `null`.

Given `desired {"report_interval":30,"mode":"eco","config":{"gain":2}}` and
`reported {"mode":"eco","config":{"gain":3}}`, the delta is
`{"report_interval":30,"config":{"gain":2}}`.

## Rules

A rule is created once and never changes:

```json
{"id":"hot","source":"shadow","device_id":"sensor-1",
 "path":"state.reported.temperature","operator":"gt","value":30,
 "event":"temperature_high"}
```

A `shadow` rule needs `device_id` and must not carry `topic`; an `mqtt` rule
needs `topic` (a valid topic filter) and must not carry `device_id`. Every rule
needs `id` (1 to 100 characters matching `[A-Za-z0-9][A-Za-z0-9._:-]*`), `path`,
`operator` and `event` (matching `[a-z][a-z0-9_]*`). Unknown fields are
rejected. `exists` and `not_exists` must not carry `value`; the other operators
must.

`path` is dot separated and is resolved against the evaluated document: `""` is
the whole document, a numeric segment indexes an array (`samples.1.value`), and
a missing segment resolves to nothing. A key that contains a dot cannot be
addressed. The documents are the full shadow document for `shadow` rules and
`{"topic": "...", "payload": <value>, "device_id": null}` for `mqtt` rules,
where `payload` is the JSON value when the UTF-8 payload parses as JSON and the
text itself otherwise.

| Operator | True when |
| --- | --- |
| `eq` / `ne` | the path resolves and equals / differs from `value` structurally |
| `gt` `gte` `lt` `lte` | both sides are finite numbers, or both are strings, and the comparison holds |
| `exists` / `not_exists` | the path resolves / does not resolve |
| `contains` | the path is an array containing `value` structurally, or a string containing `value` as a substring |

Anything else — a missing path compared with `eq`, a type mismatch, an
incomparable value — is simply false, never an error.

**Shadow rules are edge triggered.** Each rule remembers, per device, whether
its condition held after the previous write; the remembered value starts as
false and is updated on every write to that device. An event is written only on
a false → true transition, so writing 31, 35 and 40 to a `> 30` rule produces
one event, and the rule can fire again only after the condition has been false.

**MQTT rules are evaluated once per publication** — a client `PUBLISH`, a will,
a retained-clear, or a broker delta publication — not once per subscription.
Every accepted publication that satisfies the condition produces one event.

Events are appended in a deterministic order: shadow rules in ascending rule id
order while the write is applied, then any `$shadow/<deviceId>/delta`
publication the write caused and its rules, then the rules of the publication
that carried the update. An event is:

```json
{"sequence":1,"rule_id":"hot","event":"temperature_high","source":"shadow",
 "device_id":"sensor-1","topic":null,"value":31,
 "occurred_at":"2024-05-01T10:00:00.000Z"}
```

`sequence` starts at 1 and increases by one per event; `value` is the resolved
path value, or `null` when the path was absent, which only `not_exists` can
match. Repeating a POST with the same `Idempotency-Key` writes no event,
because the first response is replayed instead.

## HTTP API

All bodies are JSON with `Content-Type: application/json`; unknown fields and
unknown query parameters are rejected. Every POST requires an `Idempotency-Key`
header, and reusing a key for a different operation or device is a conflict.

### `GET /health`

```http
GET /health
```
```json
{"status":"ok"}
```

### `POST /devices/{deviceId}/shadow`

Merges `desired` and/or `reported` into the shadow and returns the new
document.

```bash
curl -s -X POST http://127.0.0.1:8080/devices/sensor-1/shadow \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: shadow-1' \
  -d '{"state":{"desired":{"report_interval":30,"mode":"eco"}}}'
```
```json
{"device_id":"sensor-1","state":{"desired":{"report_interval":30,"mode":"eco"},"reported":{}},
 "delta":{"report_interval":30,"mode":"eco"},"version":1,"updated_at":"2024-05-01T10:00:00.000Z"}
```

### `POST /devices/{deviceId}/reported`

The same merge restricted to `reported`, for devices that report state.

```bash
curl -s -X POST http://127.0.0.1:8080/devices/sensor-1/reported \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: reported-1' \
  -d '{"state":{"reported":{"mode":"eco"}}}'
```
```json
{"device_id":"sensor-1","state":{"desired":{"report_interval":30,"mode":"eco"},"reported":{"mode":"eco"}},
 "delta":{"report_interval":30},"version":2,"updated_at":"2024-05-01T10:00:00.000Z"}
```

### `GET /devices/{deviceId}/shadow`

Returns the current document, or `404` when the device has never reported.

### `POST /rules`

Creates a rule and returns it with status `201`. A duplicate rule id is `409`.

```bash
curl -s -X POST http://127.0.0.1:8080/rules \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: rule-1' \
  -d '{"id":"hot","source":"shadow","device_id":"sensor-1","path":"state.reported.temperature","operator":"gt","value":30,"event":"temperature_high"}'
```

### `GET /events`

Returns events in ascending `sequence` order; with no parameters every event is
returned. All enabled filters are intersected:

- `rule_id` and `device_id` filter by exact value;
- `event` and `source` filter by exact, complete string and must not be empty;
- `occurred_after` and `occurred_before` are RFC3339 UTC timestamps ending in
  `Z` and select the half-open range `occurred_at >= occurred_after` and
  `occurred_at < occurred_before`; `occurred_after` must be strictly earlier
  than `occurred_before`;
- `after_sequence` returns only events whose `sequence` is greater;
- `limit` is an integer between 1 and 1000 bounding the number returned.

Any unknown parameter, an empty `event` or `source`, a timestamp not ending in
`Z`, `occurred_after` at or after `occurred_before`, a non-integer or unsafe
`after_sequence`, or a `limit` outside 1 to 1000 is rejected with
`400 validation_error` naming the parameter.

Paginate with the last event's `sequence`: fetch
`/events?limit=100&after_sequence=<last sequence>` for the next page. Results
are stable: events sharing an `occurred_at` are still ordered by `sequence`,
and repeating a query against an unchanged event set returns the same events.

```json
{"events":[{"sequence":1,"rule_id":"hot","event":"temperature_high","source":"shadow","device_id":"sensor-1","topic":null,"value":31,"occurred_at":"2024-05-01T10:00:00.000Z"}]}
```

## Errors

```json
{"error":{"code":"validation_error","message":"human readable detail"}}
```

| Code | Status | Raised by |
| --- | --- | --- |
| `validation_error` | 400 | malformed JSON, wrong content type, unknown or missing field, invalid identifier, topic filter, path, operator or event name |
| `not_found` | 404 | unknown route, unknown device shadow |
| `conflict` | 409 | duplicate rule id, `Idempotency-Key` reused for another operation |
| `protocol_error` | — | MQTT transport only: the broker closes the connection |

## Tests

```bash
node --test tests/
```

`node --test` and `node --test "tests/*.test.js"` run the same 112 tests;
`tests/index.js` exists so the directory form also works on Node 22, which does
not expand a directory argument on its own.
