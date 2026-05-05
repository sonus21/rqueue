# NATS backend — v2 task tracker

## v1 status: COMPLETE

All v1 items are done and 360 unit tests pass. Branch `nats-backend` is ready to merge.

---

## v2 pending items

### 1. Web dashboard — NATS gaps  *(IN PROGRESS — pause/delete/explore landed)*

Controllers are no longer Redis-gated but several operations throw `BackendCapabilityException` (HTTP 501) on NATS. The front-end should hide unsupported panels proactively instead of relying on 501s.

- ✅ `GET /rqueue/api/capabilities` already returns the `Capabilities` record so the UI can conditionally hide panels.
- ✅ `RqueueQDetailServiceImpl.getRunningTasks()` / `getScheduledTasks()` now return header-only tables on NATS instead of zero rows / 501s. Pending queue browsing routes through `MessageBroker.peek()`.
- ✅ `NatsRqueueUtilityService` implements `pauseUnpauseQueue` (persists flag + notifies local `RqueueMessageListenerContainer`), soft `deleteMessage` (KV metadata flag), `getDataType` (returns `"STREAM"`), `aggregateDataCounter`. 20 unit tests cover the path.
- ⏳ Pause-event multi-instance fan-out: `RqueueInternalPubSubChannel` is Redis-only. NATS bridge follow-up: subscribe to `rqueue.internal.<broker>` via `MessageBroker.subscribe/publish` and rebroadcast pause requests across worker JVMs.
- ⏳ Extend `Capabilities` with dashboard-op flags: `supportsCharts`, `supportsMessageBrowse`, `supportsAdminMove` (not yet — current flags suffice for the panels we hide today).
- ⏳ Pebble templates: `hideScheduledPanel` / `hideCronJobs` already wired into `DataViewResponse`. Front-end hides those panels; chart and message-browse hides still TBD.

Affected services that throw on NATS today:
- `RqueueDashboardChartServiceImpl` — time-series charts (no equivalent in JetStream) — still pending
- `RqueueUtilityServiceImpl` — move/enqueue admin ops — `moveMessage`, `enqueueMessage`, `makeEmpty` deliberately remain `notSupported` (no JetStream primitive); `pauseUnpauseQueue` and `deleteMessage` now implemented
- `NatsMessageBrowsingRepository.viewData` — positional message browse (Redis-only by design)

### 2. Reactive listener container

Only the enqueue side is reactive in v1. The listener/pop side still uses blocking `BrokerMessagePoller` threads.

- Implement a `ReactiveMessagePoller` using `js.publishAsync` + Project Reactor for the NATS path.
- Gate behind `@Conditional(ReactiveEnabled.class)` + `NatsBackendCondition`.

### 3. Delayed / scheduled / cron messages on NATS

`enqueueWithDelay` throws `UnsupportedOperationException` in v1. Options:

- Use NATS JetStream `MaxAge` + a separate "delay bucket" stream per delay tier (coarse buckets: 1s, 5s, 30s, 5m, 1h).
- Or implement a lightweight delay-scheduler sidecar using KV TTL expiry events.

### 4. `priorityGroup` weighting on NATS

In v1, cross-queue `priorityGroup` weighting logs a boot WARN and is not honored. `BrokerMessagePoller` spawns one thread per `(queue, consumerName, priority)` triple at fixed weight.

- Implement weighted round-robin across pollers sharing the same `priorityGroup`.

### 5. Elastic concurrency (`@RqueueListener.concurrency min < max`)

Falls back to fixed `max` on NATS in v1. Implement auto-scaling poller count based on queue depth via `MessageBroker.size()`.

### 6. `@RqueueHandler(primary)` on NATS

Ignored in v1 with a single boot WARN. On NATS all handler methods are dispatched independently (one consumer per method). `primary` could select the default handler when message type is ambiguous.

### 7. Spring Boot configuration metadata (source annotation processor)

`rqueue.nats.auto-create-kv-buckets` and sibling properties appear in the built metadata JSON but only from the compiled artifact. Add `spring-boot-configuration-processor` to the starter's `annotationProcessor` deps so IDEs pick up descriptions and defaults without a pre-build step.

### 8. `RqueueStringDao` javadoc

Mark as Redis-internal in the interface javadoc so future contributors don't try to add a NATS impl.

---

## Local verification commands

```bash
./gradlew :rqueue-core:test :rqueue-redis:test :rqueue-web:test :rqueue-nats:test -DincludeTags=unit
./gradlew :rqueue-spring-boot-starter:test --tests "com.github.sonus21.rqueue.spring.boot.integration.NatsBackendEndToEndIT"
./gradlew :rqueue-nats:test -DincludeTags=nats
```

## Commit-rule reminder

`CLAUDE.md` forbids `Co-Authored-By:` for any AI tool. Use `Assisted-By: Claude Code` only.
