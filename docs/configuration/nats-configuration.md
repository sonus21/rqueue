---
layout: default
title: NATS Configuration
parent: Configuration
nav_order: 3
---

# NATS Configuration
{: .no_toc }

<details open markdown="block">
  <summary>Table of contents</summary>
  {: .text-delta }
1. TOC
{:toc}
</details>

---

Rqueue supports **NATS JetStream** as a drop-in replacement for Redis. All listener,
producer, and middleware APIs work identically — the only changes required are the
dependency and two properties.

{: .highlight }
**Scheduling support** — `enqueueIn`, `enqueueAt`, and `enqueuePeriodic` work on
**NATS ≥ 2.12** (released Dec 2024). On older servers they throw
`UnsupportedOperationException`. Rqueue detects support at startup via the server
version and advertises `supportsDelayedEnqueue` accordingly. Immediate `enqueue` works
on all NATS 2.2+ servers.

---

## Quick Setup

### 1. Add the dependency

**Gradle**
```groovy
implementation 'com.github.sonus21:rqueue-spring-boot-starter:4.0.0-RELEASE'
implementation 'com.github.sonus21:rqueue-nats:4.0.0-RELEASE'
```

**Maven**
```xml
<dependency>
    <groupId>com.github.sonus21</groupId>
    <artifactId>rqueue-spring-boot-starter</artifactId>
    <version>4.0.0-RELEASE</version>
</dependency>
<dependency>
    <groupId>com.github.sonus21</groupId>
    <artifactId>rqueue-nats</artifactId>
    <version>4.0.0-RELEASE</version>
</dependency>
```

### 2. Configure `application.properties`

```properties
rqueue.backend=nats
rqueue.nats.connection.url=nats://localhost:4222
```

No `RedisConnectionFactory` bean is needed. All Rqueue listener, producer, and
middleware annotations work without any code changes.

### 3. Start NATS with JetStream enabled

```sh
# native binary
nats-server -js

# Docker
docker run -p 4222:4222 nats:latest -js
```

At startup, Rqueue's `NatsStreamValidator` and `NatsKvBucketValidator` provision all
required streams and KV buckets automatically.

---

## How the NATS Backend Works

This section describes the internals so you can reason about behaviour, tune
configuration, and diagnose issues.

### Streams and subjects

Every registered queue maps to one **JetStream stream** and one **subject** derived
from the queue name. With the default `rqueue-js-` stream prefix and `rqueue.js.`
subject prefix, a queue named `orders` becomes:

| Resource | Name |
|---|---|
| Stream | `rqueue-js-orders` |
| Subject | `rqueue.js.orders` |

Priority sub-queues each get their own stream and subject:

| Purpose | Stream | Subject |
|---|---|---|
| Main | `rqueue-js-orders` | `rqueue.js.orders` |
| Priority: high | `rqueue-js-orders_high` | `rqueue.js.orders_high` |
| DLQ | `rqueue-js-orders-dlq` | `rqueue.js.orders.dlq` |

Streams and subjects are provisioned once at startup by `NatsStreamValidator` — there
is no per-publish `getStreamInfo` round-trip.

### Pull consumers and the poll loop

Rqueue uses **durable pull consumers**, not push consumers. Each
`(stream, consumerName)` pair has exactly one durable consumer created lazily on the
first `pop()` call and cached in-process. The poll loop calls `fetchMessages(batchSize,
fetchWait)` in a tight loop:

1. Server returns up to `batchSize` messages.
2. Each message is dispatched to the listener thread pool.
3. On success the listener calls `message.ack()` — the server removes it from the
   consumer's pending set.
4. On failure or exception the listener calls `message.nak()` — the server immediately
   redelivers (up to `maxDeliver` times), then routes to the DLQ stream.
5. If no messages arrive within `fetchWait`, the fetch returns empty and the loop
   starts the next fetch immediately.

`ackWait` is the server-side timer: if the consumer holds a message longer than
`ackWait` without acking, naking, or sending a keep-alive, the server redelivers it.
This is the single most important consumer knob — set it longer than your slowest
handler.

### Metadata storage (KV buckets)

State that the Redis backend stores in keys, hashes, and sorted sets is stored in
**JetStream KV buckets**:

| Bucket | What it holds |
|---|---|
| `rqueue-queue-config` | Per-queue configuration, DLQ wiring, paused flag |
| `rqueue-message-metadata` | Per-message delivery status, retry count, soft-delete flag |
| `rqueue-jobs` | Job execution history keyed by message ID |
| `rqueue-locks` | Distributed locks for scheduler leadership |
| `rqueue-workers` | Worker process info (host, PID, last-seen) |
| `rqueue-worker-heartbeats` | Per-(queue, worker) heartbeat timestamps |

KV entries are readable via `nats kv get <bucket> <key>` and are stored as JSON,
making them straightforward to inspect without any Rqueue tooling.

### Scheduled and delayed messages (NATS ≥ 2.12)

Rqueue uses the **ADR-51** `Nats-Next-Deliver-Time` header to schedule messages.
When `enqueueIn("orders", payload, 30_000)` is called, Rqueue publishes the message
with:

```
Nats-Next-Deliver-Time: 2026-05-09T12:30:00Z   ← RFC 3339 UTC, computed from now + delayMs
Nats-Msg-Id:            <id>-at-<processAtMs>   ← dedup key (see below)
Rqueue-Process-At:      1746789000000           ← epoch-ms, restored on delivery
```

The NATS server holds the message and delivers it only when wall-clock time reaches the
specified timestamp. No polling loop or Redis `ZRANGEBYSCORE` equivalent is involved —
delivery is server-side.

**Periodic messages** (`enqueuePeriodic`) work the same way: after the listener
processes a period, Rqueue republishes the message with a new
`Nats-Next-Deliver-Time` set to `now + period`, triggering the next delivery.

**Scheduling requires NATS ≥ 2.12.** Rqueue auto-detects support at startup.

### Deduplication key shape

Every scheduled or periodic publish includes a `Nats-Msg-Id` header with the form
`<messageId>-at-<processAtMs>`. This key shape has two important properties:

* **Periodic messages** — consecutive periods have different `processAt` values, so
  each period gets a unique key and is not dropped as a duplicate.
* **Retries** — a retry for the same delivery shares the same `processAt`, so if the
  handler crashes and the producer publishes the same message twice before the
  dedup window expires, JetStream silently drops the second publish.

Deduplication is managed server-side using JetStream's built-in dedup mechanism with
its server default window.

### Long-running jobs and keep-alive

If a handler runs longer than `ackWait`, the NATS server redelivers the message
assuming the consumer died. To prevent this, the handler can send a **WIP
(work-in-progress)** signal every `<ackWait / 2` using the `Job` handle:

```java
@RqueueListener(value = "reports", visibilityTimeout = "60000") // ackWait = 60 s
void generateReport(String payload, @Header(RqueueMessageHeaders.JOB) Job job) {
    for (int chunk = 0; chunk < totalChunks; chunk++) {
        processChunk(chunk);
        // reset the 60-second ackWait timer every 30 seconds
        job.updateVisibilityTimeout(Duration.ofSeconds(30));
    }
}
```

`job.updateVisibilityTimeout(duration)` issues a NATS `+WIP` ack that resets the
consumer's `ackWait` timer without acknowledging the message. The server will not
redeliver as long as keep-alive signals arrive before each `ackWait` expiry.

{: .warning }
Forgetting to call `updateVisibilityTimeout` on a long handler causes the message to be
redelivered to another consumer while the original handler is still running, resulting
in **duplicate processing**. Set `visibilityTimeout` to at least 2× your expected
handler duration and send keep-alives at half that interval.

### Dashboard operations

The NATS backend supports the full explore panel:

* **Browse messages** — `peek()` walks the JetStream stream by sequence number via
  `JetStreamManagement.getMessage()`.
* **Move messages** — `moveMessage()` reads each message from the source stream,
  republishes it to the destination stream (stripping `Nats-Msg-Id` to avoid dedup
  collisions), then hard-deletes the source sequence.
* **Re-enqueue** — `enqueueMessage()` looks up the `RqueueMessage` from the metadata
  store and republishes it immediately without a `Nats-Next-Deliver-Time` header so the
  poller picks it up on its next fetch.
* **Soft-delete** — marks the metadata record as deleted; the stream message persists
  (JetStream streams are append-only), but the dashboard and consumers honor the
  `deleted` flag.

{: .note }
**Purge queue** (the "make empty" dashboard action) is **not supported** — purging a
JetStream stream is a destructive admin operation. Use `nats stream purge <stream>` or
the NATS dashboard directly.

---

## Connection Properties

All connection properties are under the `rqueue.nats.connection` prefix.

| Property | Type | Default | Description |
|---|---|---|---|
| `url` | `String` | `nats://localhost:4222` | Single NATS server URL. |
| `username` | `String` | — | Username for username/password authentication. |
| `password` | `String` | — | Password for username/password authentication. |
| `token` | `String` | — | Token for token-based authentication. |
| `credentials-path` | `String` | — | Path to a `.creds` file for NKey/JWT authentication. |
| `tls` | `boolean` | `false` | Enable TLS for the connection. |
| `connection-name` | `String` | — | Logical name visible in NATS server monitoring. |
| `connect-timeout` | `Duration` | (client default) | Maximum time to wait for initial connection. |
| `reconnect-wait` | `Duration` | (client default) | Time to wait between reconnect attempts. |
| `max-reconnects` | `int` | `-1` (unlimited) | Maximum reconnect attempts. |
| `ping-interval` | `Duration` | (client default) | Interval between server pings. |

### Authentication examples

**Token authentication**
```properties
rqueue.nats.connection.token=s3cr3t
```

**Username / password**
```properties
rqueue.nats.connection.username=rqueue
rqueue.nats.connection.password=s3cr3t
```

**NKey / JWT credentials file**
```properties
rqueue.nats.connection.credentials-path=/etc/nats/rqueue.creds
```

### Connection resilience

```properties
# Retry for up to 10 minutes (120 attempts × 5 s wait)
rqueue.nats.connection.max-reconnects=120
rqueue.nats.connection.reconnect-wait=5s
```

Set `max-reconnects=-1` (the default) for unlimited retries in production — NATS
reconnects silently to any cluster node without dropping in-flight messages.

---

## Stream Properties

Each registered queue maps to one or more JetStream streams. The defaults below apply
to every stream Rqueue creates. All properties are under `rqueue.nats.stream`.

| Property | Type | Default | Description |
|---|---|---|---|
| `replicas` | `int` | `1` | Number of stream replicas. Must not exceed the number of JetStream-enabled servers in the cluster. |
| `storage` | `String` | `FILE` | Storage backend: `FILE` (durable) or `MEMORY` (faster, lost on restart). |
| `retention` | `String` | `LIMITS` | Retention policy: `LIMITS`, `INTEREST`, or `WORK_QUEUE`. |
| `max-age` | `Duration` | `14d` | Maximum age of messages before automatic removal. |
| `max-bytes` | `long` | `-1` (unlimited) | Maximum total stream size in bytes. |
| `max-messages` | `long` | `-1` (unlimited) | Maximum number of messages in the stream. |
| `discard-policy` | `String` | `OLD` | What to discard when limits are hit: `OLD` (oldest messages) or `NEW` (reject new publishes). |

### Retention policy guide

| Value | When to use |
|---|---|
| `LIMITS` (default) | General-purpose queues. Messages are kept until age/size limits are hit. |
| `INTEREST` | Fan-out / pub-sub patterns. Messages are removed once every active consumer has acked. |
| `WORK_QUEUE` | Lowest storage overhead. Message is removed as soon as **any** consumer acks it. Use only for non-fan-out queues. |

{: .warning }
`WORK_QUEUE` retention deletes the message on the first ack, regardless of which
consumer acked it. Do not use it if multiple independent consumers (different
`consumerName` values) need to process the same message.

### Three-replica production setup

```properties
rqueue.nats.stream.replicas=3
rqueue.nats.stream.storage=FILE
rqueue.nats.stream.max-age=7d
```

---

## Consumer Properties

Consumer properties control the durable pull consumers Rqueue creates for each
`(queue, consumerName)` pair. All properties are under `rqueue.nats.consumer`.

| Property | Type | Default | Description |
|---|---|---|---|
| `ack-wait` | `Duration` | `30s` | Time the server waits for an ack before redelivering. Must be longer than your slowest message handler. |
| `max-deliver` | `long` | `3` | Delivery attempts before a message is forwarded to the DLQ. |
| `max-ack-pending` | `long` | `1000` | Maximum unacked messages a consumer can hold before the server stops delivering. |
| `fetch-wait` | `Duration` | `2s` | How long `pop()` blocks waiting for messages before returning empty. |

{: .warning }
`ack-wait` is the most critical consumer setting. If a handler takes longer than
`ack-wait`, the server redelivers the message to another consumer **while the original
handler is still running**, causing duplicate processing. Set it to at least 2× your
99th-percentile handler latency. For long-running handlers, use
`job.updateVisibilityTimeout()` to send keep-alive signals (see
[Long-running jobs and keep-alive](#long-running-jobs-and-keep-alive)).

### Tuning for slow handlers

```properties
# Handlers can take up to 5 minutes; add 20% headroom
rqueue.nats.consumer.ack-wait=6m
# Give each message 5 delivery attempts before DLQ
rqueue.nats.consumer.max-deliver=5
```

### Tuning for high-throughput queues

```properties
# Allow more unacked messages in-flight
rqueue.nats.consumer.max-ack-pending=5000
# Reduce idle wait to pick up bursts faster
rqueue.nats.consumer.fetch-wait=500ms
```

---

## Naming Properties

Naming properties control how stream and subject names are derived from queue names.
All properties are under `rqueue.nats.naming`.

| Property | Type | Default | Description |
|---|---|---|---|
| `stream-prefix` | `String` | `rqueue-js-` | Prefix for every JetStream stream name. |
| `subject-prefix` | `String` | `rqueue.js.` | Prefix for every JetStream subject. |
| `dlq-suffix` | `String` | `-dlq` | Suffix appended to stream and subject names for DLQ streams. |

{: .warning }
Change the prefixes **before the first deployment**. Renaming them afterward requires
manually migrating or recreating all streams and updating all KV bucket entries.

---

## Auto-Provisioning

### Streams (`rqueue.nats.auto-create-streams`)

When `true` (default), `NatsStreamValidator` creates every required stream at startup,
immediately after all `@RqueueListener` methods are registered and before message
pollers start. This means the hot publish/pop path never pays a `getStreamInfo`
round-trip to confirm stream existence.

Set to `false` for accounts where credentials lack `add_stream` permission. The
validator will instead check that every required stream exists and abort boot with a
clear `IllegalStateException` listing all missing streams — a deterministic startup
failure rather than a `stream not found` error on first enqueue.

### DLQ streams (`rqueue.nats.auto-create-dlq-stream`)

When `true`, a dead-letter stream is automatically created for every queue whose
`@RqueueListener` declares a `deadLetterQueue`. Default is `false` — enable it when
you want the DLQ stream provisioned alongside the main stream without pre-creating it
manually.

### Consumers (`rqueue.nats.auto-create-consumers`)

When `true` (default), durable pull consumers are created lazily on the first `pop`
call for each `(stream, consumerName)` pair and the subscription is cached in-process.
There is no per-pop round-trip after warm-up.

Set to `false` to fail-fast on missing consumers instead of creating them.

### KV buckets (`rqueue.nats.auto-create-kv-buckets`)

When `true` (default), each store lazily creates its bucket on first use. When `false`,
`NatsKvBucketValidator` walks every bucket at startup and aborts boot listing any that
are missing.

---

## Locked-Down JetStream Accounts

For deployments where application credentials cannot call `add_stream` or `kv_create`
at runtime, disable all auto-provisioning and pre-create every resource before
starting the application:

```properties
rqueue.nats.auto-create-streams=false
rqueue.nats.auto-create-consumers=false
rqueue.nats.auto-create-dlq-stream=false
rqueue.nats.auto-create-kv-buckets=false
```

### Pre-creating streams

For a queue `orders` with a DLQ:

```sh
nats stream add rqueue-js-orders \
    --subjects "rqueue.js.orders" \
    --storage file --replicas 3 --retention limits

nats stream add rqueue-js-orders-dlq \
    --subjects "rqueue.js.orders.dlq" \
    --storage file --replicas 3 --retention limits
```

For priority sub-queues (`high`, `low`):

```sh
nats stream add rqueue-js-orders_high \
    --subjects "rqueue.js.orders_high" \
    --storage file --replicas 3 --retention limits

nats stream add rqueue-js-orders_low \
    --subjects "rqueue.js.orders_low" \
    --storage file --replicas 3 --retention limits
```

### Pre-creating KV buckets

Match TTL values to your `rqueue.worker.registry.*` settings:

```sh
# Persistent state — no TTL
nats kv add rqueue-queue-config      --replicas=3 --storage=file
nats kv add rqueue-message-metadata  --replicas=3 --storage=file

# Job history — match rqueue.message.durability (default 7 days)
nats kv add rqueue-jobs              --replicas=3 --storage=file --ttl=7d

# Locks — cover your longest expected lock hold
nats kv add rqueue-locks             --replicas=3 --storage=file --ttl=10m

# Worker registry — match rqueue.worker.registry.worker.ttl (default 300 s)
nats kv add rqueue-workers           --replicas=3 --storage=file --ttl=5m

# Queue heartbeats — match rqueue.worker.registry.queue.ttl (default 3600 s)
nats kv add rqueue-worker-heartbeats --replicas=3 --storage=file --ttl=1h
```

{: .warning }
KV bucket TTLs are immutable after creation. To change a TTL, delete the bucket and
recreate it. Do not delete `rqueue-queue-config` without backing it up first — it
stores all registered queue configurations.

---

## Inspecting Runtime State

Use the `nats` CLI to inspect what Rqueue has created:

```sh
# List all Rqueue streams
nats stream ls | grep rqueue-js-

# Show message counts and consumer lag per queue
nats stream info rqueue-js-orders

# List KV buckets
nats kv ls | grep rqueue-

# Inspect queue configuration
nats kv get rqueue-queue-config orders

# Inspect message metadata (delivery status, retry count)
nats kv get rqueue-message-metadata <messageId>

# Manually purge a queue (dashboard "make empty" is not supported)
nats stream purge rqueue-js-orders
```

---

## Pitfalls

### `ack-wait` shorter than handler duration → duplicate processing

The most common NATS pitfall. If your handler takes longer than `ack-wait`, the server
considers the consumer dead and redelivers the message to another poller instance. Both
instances run the handler concurrently. To avoid this:

* Set `ack-wait` to at least 2× your slowest handler's P99 latency.
* For unpredictably long handlers, call `job.updateVisibilityTimeout(duration)` at
  regular intervals to send keep-alive (`+WIP`) signals and reset the timer.

### Long-running jobs without keep-alive get redelivered

A handler sleeping or doing I/O for longer than `ack-wait` will be redelivered even if
it eventually succeeds. `visibilityTimeout` on `@RqueueListener` sets the initial
`ack-wait`, but you must also send periodic keep-alives:

```java
@RqueueListener(value = "etl-job", visibilityTimeout = "120000") // 2-minute ack-wait
void runEtl(String id, @Header(RqueueMessageHeaders.JOB) Job job) {
    // send a +WIP signal every 60 seconds to keep the server from redelivering
    scheduledExecutor.scheduleAtFixedRate(
        () -> job.updateVisibilityTimeout(Duration.ofSeconds(60)),
        60, 60, TimeUnit.SECONDS);
    doHeavyWork(id);
}
```

### Scheduling requires NATS ≥ 2.12

`enqueueIn`, `enqueueAt`, and `enqueuePeriodic` throw `UnsupportedOperationException`
at runtime on NATS < 2.12. Rqueue detects the server version at startup and sets the
`supportsDelayedEnqueue` capability flag accordingly. You can check it programmatically:

```java
@Autowired MessageBroker broker;

if (broker.capabilities().supportsDelayedEnqueue()) {
    enqueuer.enqueueIn("reports", payload, Duration.ofMinutes(5));
} else {
    enqueuer.enqueue("reports", payload); // fall back to immediate
}
```

### Periodic message silently dropped after retry (dedup window)

When a periodic message handler fails and Rqueue republishes the "next period" message,
the new publish uses the same `Nats-Msg-Id` (`id-at-<newProcessAt>`) as the original
scheduled publish. If the retry happens within the server's dedup window and the same
`id-at-processAt` key was already seen, NATS silently drops the second publish — the
message is lost. This can happen if a retry races with a scheduled re-publish within
the same period.

Mitigation: keep handler idempotent and `numRetries` low. With the default dedup window
the risk is low, but handlers that retry many times within a single period can
encounter this.

### `WORK_QUEUE` retention deletes on first ack

With `retention=WORK_QUEUE`, a message is removed from the stream as soon as the
first consumer acks it — even if other consumer groups have not processed it. Only use
this retention policy when a single consumer group processes each stream.

### Priority weighting is not enforced

`@RqueueListener(priority = "high=10,low=1")` registers correctly, but the NATS
backend does not honor the numeric weights — it polls each priority sub-queue with equal
frequency. If strict prioritization is required, use the Redis backend.

### Elastic concurrency collapses to `max`

`concurrency = "2-10"` (min–max) always runs at `max` (10 threads) on NATS because
JetStream pull consumers do not have a push-based back-pressure signal that Rqueue
can use to scale down. All threads poll continuously.

### `makeEmpty` (purge queue) is unsupported from the dashboard

The dashboard "empty queue" action returns "not supported" on NATS. Purge via CLI:

```sh
nats stream purge rqueue-js-<queueName> --force
```

### Cluster `replicas` must not exceed server count

Setting `rqueue.nats.stream.replicas=3` on a single-node NATS server causes stream
creation to fail at startup. Match `replicas` to the number of JetStream-enabled nodes
in your cluster (or leave it at `1` for single-node deployments).

---

## Feature Comparison

| Feature | Redis backend | NATS backend |
|---|---|---|
| Immediate enqueue | Supported | Supported |
| `enqueueIn` / `enqueueAt` (delayed) | Supported | Supported on NATS ≥ 2.12 |
| `enqueuePeriodic` (recurring) | Supported | Supported on NATS ≥ 2.12 |
| Long-running job keep-alive | Supported | Supported via `job.updateVisibilityTimeout()` → `+WIP` |
| Priority queues | Full support | Registered; weighting not honored |
| Elastic concurrency | Supported | Falls back to `max` |
| `@RqueueHandler(primary)` | Supported | Ignored with boot warning |
| Dashboard explore / browse | Full support | Full support |
| Dashboard move messages | Supported | Supported |
| Dashboard re-enqueue | Supported | Supported |
| Dashboard purge queue | Supported | Not supported — use `nats stream purge` |
| Charts and stats graphs | Supported | Queue sizes only |
| Reactive enqueue | Supported | Supported |
| Reactive listener container | Supported | Enqueue side only |
| Scheduled message introspection | Supported | Not supported (no scheduled ZSET) |
| Server-side cron jobs | Supported | Not supported |
