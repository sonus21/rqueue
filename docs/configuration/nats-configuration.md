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

{: .warning }
The NATS backend does not support delayed enqueue, scheduled messages, or periodic/cron
jobs. Calls to `enqueueIn`, `enqueueAt`, and `enqueuePeriodic` throw
`UnsupportedOperationException` at runtime. Use the Redis backend for workloads that
need scheduling.

---

## Quick Setup

### 1. Add the dependency

Add `rqueue-nats` alongside `rqueue-spring-boot-starter`:

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
| `duplicate-window` | `Duration` | `2m` | Server-side dedup window for the `Nats-Msg-Id` header. |

{: .note }
`duplicate-window` must be less than or equal to `max-age`. Set it to cover the
maximum time a publisher might retry the same message ID (e.g. after a crash recovery).

### Retention policy guide

| Value | When to use |
|---|---|
| `LIMITS` (default) | General-purpose queues. Messages are kept until age/size limits are hit. |
| `INTEREST` | Fan-out / pub-sub patterns. Messages are removed once every active consumer has acked. |
| `WORK_QUEUE` | Lowest storage overhead. Message is removed as soon as any consumer acks it. Use for non-fan-out queues where exactly-once delivery per message is the goal. |

### Three-replica production setup

```properties
rqueue.nats.stream.replicas=3
rqueue.nats.stream.storage=FILE
rqueue.nats.stream.max-age=7d
rqueue.nats.stream.duplicate-window=5m
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

{: .note }
`ack-wait` is the most important consumer setting. If a message handler takes longer
than `ack-wait`, the server redelivers the message to another consumer, causing
duplicate processing. Set it to at least 2× your 99th-percentile handler latency.

### Tuning for slow handlers

```properties
# Handlers can take up to 5 minutes
rqueue.nats.consumer.ack-wait=6m
# Give each message 5 attempts before DLQ
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

For a queue named `orders` with priority sub-queues `high` and `low` and a DLQ, the
default naming produces:

| Purpose | Stream name | Subject |
|---|---|---|
| Main queue | `rqueue-js-orders` | `rqueue.js.orders` |
| Priority: high | `rqueue-js-orders-high` | `rqueue.js.orders.high` |
| Priority: low | `rqueue-js-orders-low` | `rqueue.js.orders.low` |
| Dead-letter queue | `rqueue-js-orders-dlq` | `rqueue.js.orders.dlq` |

{: .note }
Change the prefixes before the first deployment. Renaming them afterward requires
manually migrating or recreating all streams.

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

Rqueue uses six shared KV buckets for state that Redis stores in keys, hashes, and
sorted sets:

| Bucket | Purpose | TTL |
|---|---|---|
| `rqueue-queue-config` | Per-queue configuration and DLQ wiring | None (persists) |
| `rqueue-jobs` | Job execution history per message ID | `rqueue.message.durability` (default 7 days) |
| `rqueue-locks` | Distributed locks for scheduler leadership | Set per lock acquisition |
| `rqueue-message-metadata` | Per-message delivery status and retry count | None |
| `rqueue-workers` | Worker process info (host, PID, last-seen) | `rqueue.worker.registry.worker.ttl` (default 300 s) |
| `rqueue-worker-heartbeats` | Per-(queue, worker) heartbeats | `rqueue.worker.registry.queue.ttl` (default 3600 s) |

When `auto-create-kv-buckets=true` (default), each store lazily creates its bucket on
first use. When set to `false`, `NatsKvBucketValidator` walks every bucket and aborts
boot listing any that are missing.

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

For a queue `orders` with priorities `high` / `low` and a DLQ:

```sh
nats stream add rqueue-js-orders \
    --subjects "rqueue.js.orders" \
    --storage file --replicas 3 --retention limits

nats stream add rqueue-js-orders-high \
    --subjects "rqueue.js.orders.high" \
    --storage file --replicas 3 --retention limits

nats stream add rqueue-js-orders-low \
    --subjects "rqueue.js.orders.low" \
    --storage file --replicas 3 --retention limits

nats stream add rqueue-js-orders-dlq \
    --subjects "rqueue.js.orders.dlq" \
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

# Show message counts per queue
nats stream info rqueue-js-orders

# List KV buckets
nats kv ls | grep rqueue-

# Inspect queue configuration
nats kv get rqueue-queue-config orders
```

---

## Limitations

| Feature | Redis backend | NATS backend |
|---|---|---|
| `enqueueIn` (delayed) | Supported | Not supported (throws `UnsupportedOperationException`) |
| `enqueueAt` (scheduled) | Supported | Not supported |
| `enqueuePeriodic` (cron) | Supported | Not supported |
| `priorityGroup` weighting | Full support | Boot warning; weighting not honored |
| Elastic `concurrency` (min < max) | Supported | Falls back to `max` |
| `@RqueueHandler(primary)` | Supported | Ignored with boot warning |
| Dashboard charts and message browse | Full support | Queue sizes only; charts and message browse unavailable |
| Reactive listener container | Supported | Enqueue side only |
