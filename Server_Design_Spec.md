# Server Design Spec — nxIIoT Gateway Ingestion API (V2)

Audience: the team building the **Internal Server**, the downstream system that receives sensor data from nxIIoT Gateway devices. This document specifies what the server must implement to receive, acknowledge and de-duplicate data from a **V2** gateway. It covers only the wire contract between gateway and server, not the gateway's internals.

A reference implementation of the server side (both transports) is `cmd/server-sim/main.go` in the gateway repo. It is minimal but correct, and the gateway's own tests run against it.

> **V2 is not compatible with V1.** The payload, the ack and the de-duplication key all changed (see §11). A server must be updated before V2 gateways are switched on. V1 and V2 gateways can share a broker while they are being migrated; §11 describes how to tell their messages apart.

## 1. Architecture summary

- Each gateway polls its Modbus (RTU/TCP) sensors continuously and stores **every reading** in a local, durable queue **before** trying to send it. This does not depend on the server being reachable.
- Readings are stored in **chunks**: all readings from one ~2 second interval form one chunk. Each chunk gets a sequence number `seq`, which only increases.
- A separate loop on the gateway sends the oldest unacknowledged chunks to the server, **one message at a time**. One message contains one or more whole chunks. The transport is **MQTT** in production, or **HTTP** for dev/test only (see §4).
- Delivery is **at-least-once**. If the gateway is not sure a message was accepted, it sends the same chunks again, so the server can receive the same chunk more than once. **The server must de-duplicate** (see §6). This is required, not optional.
- If the server is unreachable, the gateway keeps collecting and resends everything once the server is back. The gateway's buffer holds months of data at typical rates (1 GiB by default). If the buffer does fill up, the gateway deletes its **oldest** unsent chunks. The server can detect this as a gap in `seq` (see §8).

## 2. Transports

| Transport | Status | Use |
|---|---|---|
| MQTT | Production | Recommended. Has an application-level ack, supports TLS, keeps a persistent connection. |
| HTTP | Dev/test only | Synchronous POST. **No authentication and no TLS in the current gateway.** Do not use in production. |

Each gateway uses exactly one transport at a time.

## 3. MQTT contract (production)

### 3.1 Topics

- **Data topic** (gateway → server): `gateway/{gateway_id}/data`
- **Ack topic** (server → gateway): `gateway/{gateway_id}/ack`

`{gateway_id}` is a free-form, non-empty string that is unique for each deployed unit (e.g. `GW002`). Both topic names can be overridden in each gateway's configuration. The defaults above are what you will normally see.

**Subscribe to a wildcard**, `gateway/+/data`, not to one gateway's topic. A real deployment has many gateways.

**Never hardcode a gateway_id when publishing an ack.** Derive the ack topic from the topic the message arrived on:

```
ack_topic = strip_suffix(received_topic, "/data") + "/ack"
```

Always derive it from the MQTT topic string, not from the `gateway_id` field in the body.

### 3.2 QoS

The data publish and the ack publish both use **QoS 1**. QoS 1 only confirms that the *broker* received the message, not that your server *stored* it. That is why the application-level ack in §3.4 exists on top of QoS 1.

### 3.3 Message (data topic)

```json
{
  "gateway_id": "GW002",
  "stream_id": "27d6d206-800e-4b76-ac46-32eb4bb327d6",
  "from_seq": 481,
  "to_seq": 482,
  "chunks": [
    {
      "seq": 481,
      "readings": [
        { "device_id": 3, "datapoint_id": 17, "ts": "2026-09-25T08:44:46.336Z", "value": 231.4, "quality": "GOOD" },
        { "device_id": 3, "datapoint_id": 18, "ts": "2026-09-25T08:44:46.336Z", "value": null,  "quality": "TIMEOUT" }
      ]
    },
    {
      "seq": 482,
      "readings": [ ... ]
    }
  ]
}
```

| Field | Type | Notes |
|---|---|---|
| `gateway_id` | string | The sending gateway. |
| `stream_id` | string (UUID) | Identifies the gateway's local queue file. It stays the same across gateway restarts. It changes only if the queue is recreated (for example, the file was deleted or the device was reflashed), and `seq` then starts again at 1. This is why `seq` alone is **not** a unique key. |
| `from_seq`, `to_seq` | integer (int64) | `seq` of the first and last chunk in `chunks`. `to_seq` is the value to echo in the ack. |
| `chunks` | array, at least 1 | Whole chunks in increasing `seq` order. `seq` inside one message is normally contiguous, but do not rely on it. |
| `chunks[].seq` | integer (int64) | Chunk sequence number. It only increases within a `stream_id` and is never reused. |
| `chunks[].readings` | array, at least 1 | The readings stored in that chunk; see §5. |

Message size: the gateway adds whole chunks until it reaches about 2,000 readings (configurable), so one message can be somewhat larger than that. Under normal load a message holds one chunk (~25 readings). While catching up after an outage, messages are close to the limit, at roughly 100–200 KB of JSON. Make sure your broker's maximum message size allows at least 1 MB.

### 3.4 Ack (ack topic)

On success:

```json
{ "stream_id": "27d6d206-800e-4b76-ac46-32eb4bb327d6", "to_seq": 482 }
```

On a genuine processing failure:

```json
{ "stream_id": "27d6d206-...", "to_seq": 482, "error": "human-readable reason" }
```

- Publish **exactly one ack per message**, **after** the message's chunks are stored durably, to the ack topic derived as in §3.1.
- `to_seq` must be the message's `to_seq`; the gateway matches the ack on it. `stream_id` is optional but recommended; if it is present, it must match the message.
- **A message whose chunks you already have is still a success.** De-duplication is not an error. Ack normally, without an `error` field.
- Set `error` only when you cannot store the message at all (for example, the payload is malformed or your database is down). The gateway treats an `error` ack the same as no ack: after a backoff it sends the same chunks again (§7). The ack has no per-reading structure, so there is no way to reject individual readings. Store what is usable and ack success.

### 3.5 Timing

After publishing, the gateway waits up to **10 seconds** (configurable) for the ack. If you are slower than that, the gateway resends. This is wasted work, not data loss, because of §6. Ack as soon as the data is stored durably, and do any heavy processing afterwards.

The gateway sends **one message at a time**: the next message goes out only after the previous one has been acked. A slow ack therefore also slows down the catch-up after an outage.

### 3.6 Connection / client ID

- The gateway's MQTT client ID defaults to its `gateway_id`. Most brokers enforce "last connection wins" per client ID, so **do not** connect your own tools with a live gateway's client ID, or you will disconnect the real device.
- The gateway reconnects on its own. Server-side, just keep your subscription alive.

### 3.7 Authentication / TLS

The gateway supports both, and each deployment configures them:

- **Username/password**: plain MQTT username and password. Optional.
- **TLS**: optional. It supports a custom CA file, a client certificate and key (mutual TLS), and an `insecure_skip_verify` option. That option is for lab setups only and must not be used in production.

Access control belongs on the broker: ACLs on `gateway/{id}/...`, plus TLS client certificates if you need mutual authentication.

## 4. HTTP contract (dev/test only)

`cmd/server-sim`'s `/ingest` endpoint is the reference. **Do not build production infrastructure on this transport.** It has no authentication and no TLS.

- The gateway sends a `POST` to one configured URL with `Content-Type: application/json`.
- **The body is exactly the same Message as §3.3.** In V1 the body was a bare array; that is no longer the case.
- Any 2xx status is the ack. The gateway does not read the response body.
- A non-2xx status, a connection error or a timeout counts as a failed send, and the gateway resends after a backoff (§7).
- De-duplication (§6) applies in the same way.

## 5. Reading field reference

| Field | Type | Notes |
|---|---|---|
| `device_id` | integer (int64) | The device (sensor) in that gateway's own configuration. It is only meaningful together with `gateway_id`. |
| `datapoint_id` | integer (int64) | The tag/register read from that device. Same scope as `device_id`. |
| `ts` | string, RFC 3339, UTC, millisecond precision (e.g. `2026-09-25T08:44:46.336Z`) | When the gateway **took** the reading. It is not when the message was sent or received, which can be later by as long as an outage lasted. Use `ts` for all time-series storage and ordering. |
| `value` | number or `null` | The decoded value. It is `null` whenever `quality` is not `GOOD`. |
| `quality` | string | One of `GOOD`, `TIMEOUT`, `CRC_ERROR`, `DEVICE_OFFLINE`, `INVALID`. Only `GOOD` carries a trustworthy value. The others are failed reads that are reported for observability. Accept and store unknown values rather than rejecting them. |

Readings no longer carry `gateway_id`, `sequence_id`, `event_timestamp` or `priority`; see §11. **Every** reading is sent: the gateway does not filter or deduplicate values that did not change.

## 6. Idempotency and de-duplication (required)

Under normal operation the same chunk **will** sometimes arrive more than once. Causes include a network blip after the publish but before the ack, an ack that arrived after the timeout, or a gateway restart in the middle of a send.

**The de-duplication key is `(gateway_id, stream_id, seq)`, per chunk.** A chunk with a key you have already stored is identical to what you have, so skip it and still ack success. Keep the dedup record durable (for example, a unique constraint on those three columns plus insert-or-ignore). An in-memory table, as in the reference server, forgets everything on a restart.

The chunk is the unit of de-duplication. If you store readings one row per reading, you can use `(gateway_id, stream_id, seq, index within the chunk)` as the reading's key. The gateway never changes a chunk's contents once it has been written, so the index is stable across resends.

## 7. Delivery and retry behaviour

The gateway treats a send as failed if:
- the publish fails,
- no ack arrives within the timeout (§3.5), or
- the ack contains `error`.

After a failure:
- It waits **1, 2, 4, 8, 16, 32 s, then 60 s** between further attempts and resends the same oldest chunks. The grouping into messages can differ between attempts.
- There is no maximum number of retries. Unacknowledged data is resent until it is acked, or until the gateway evicts it because its buffer is full (§8).
- Once the server is back, expect a catch-up burst: messages sent back to back, each close to the size limit in §3.3, until the backlog is gone.

## 8. Ordering and gaps

- The gateway sends chunks in **increasing `seq` order** within a stream, and a later message never starts below an earlier message's `to_seq`, except when the same chunks are resent after a failure.
- **A gap in `seq` within one `stream_id`** means the gateway's buffer filled up and it deleted those chunks before they could be sent. That data is lost. Consider logging or alerting on gaps; the reference server logs them.
- **Readings inside a chunk are only roughly in time order**, because devices on different connections are polled concurrently. Always order by `ts`, never by arrival order or by position.
- Across gateways there is no ordering relationship at all.

## 9. Multi-gateway checklist

- Subscribe to the wildcard `gateway/+/data`. New gateways appear over time.
- Derive each ack topic from the topic the message arrived on (§3.1).
- Scope `device_id` and `datapoint_id` to their `gateway_id`; they are not globally unique.
- Expect several gateways to catch up at the same time after a shared network or broker outage.

## 10. Implementation checklist

- [ ] Subscribe to `gateway/+/data` at QoS 1
- [ ] Parse the Message (§3.3) and every chunk's readings (§5)
- [ ] De-duplicate chunks on `(gateway_id, stream_id, seq)` against durable storage
- [ ] Store readings keyed and ordered by `ts`
- [ ] Publish `{"stream_id": ..., "to_seq": ...}` at QoS 1 to the derived ack topic, **after** storing, including when every chunk was a duplicate
- [ ] Set `error` only when the message cannot be stored at all
- [ ] Log or alert on `seq` gaps within a stream (data evicted on the gateway)
- [ ] Allow MQTT messages of at least 1 MB
- [ ] Support TLS (custom CA / mutual TLS) and username/password on the broker if the deployment needs them
- [ ] Load-test catch-up after an outage (back-to-back ~2,000-reading messages from several gateways)

## 11. Changes from V1

| | V1 | V2 |
|---|---|---|
| Data message | `{"batch_id", "entries": [...]}`, one entry per reading | `{"gateway_id", "stream_id", "from_seq", "to_seq", "chunks": [{"seq", "readings": [...]}]}` |
| HTTP body | bare JSON array of entries | the same Message as MQTT |
| Ack | `{"batch_id", "error"?}` | `{"stream_id"?, "to_seq", "error"?}` |
| De-duplication key | `(gateway_id, sequence_id)` per reading | `(gateway_id, stream_id, seq)` per chunk |
| Reading fields | `gateway_id`, `sequence_id`, `device_id`, `datapoint_id`, `value`, `quality`, `event_timestamp` (ns), `priority` | `device_id`, `datapoint_id`, `ts` (ms), `value`, `quality` |
| Priority | `CRITICAL`…`LOW`, affected send order | removed |
| Order | priority first, then oldest | oldest first (`seq`) |
| Data loss visible to server | not visible | gap in `seq` |

**Telling V1 and V2 apart on a shared broker:** a V2 message has a `chunks` array, and a V1 message has `entries` and `batch_id`. While both are deployed, a server can accept both formats by checking for these fields and answering each with the matching ack format.
