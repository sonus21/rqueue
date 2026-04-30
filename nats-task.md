# NATS backend port — task tracker

Snapshot of `nats-backend` branch progress and what's left to land. Kept here so a fresh session can resume cleanly.

## What's done

### Phase 1 — internal SPI in `rqueue-core`
- `com.github.sonus21.rqueue.core.spi` package with:
  - `MessageBroker` interface — `enqueue / enqueueWithDelay / pop / ack / nack / moveExpired / peek / size / subscribe / publish / capabilities` and the `default` reactive overloads (`enqueueReactive`, `enqueueWithDelayReactive`).
  - `Capabilities` record (`supportsDelayedEnqueue`, `supportsScheduledIntrospection`, `supportsCronJobs`, `usesPrimaryHandlerDispatch`).
  - `MessageBrokerFactory` + `MessageBrokerLoader` (ServiceLoader).
- `RedisMessageBroker` thin delegate over the existing `RqueueMessageTemplate`.
- Public-API additions only — no removals: `setMessageBroker` / `getMessageBroker` on `RqueueMessageTemplateImpl`, `SimpleRqueueListenerContainerFactory`, `RqueueMessageListenerContainer`. `RqueueMessageTemplate` interface frozen.
- Existing 461+ `:rqueue-core:test` cases pass unchanged; 14 new `RedisMessageBrokerDelegationTest` cases lock the delegation contract.

### Phase 2 — `rqueue-nats` module + `JetStreamMessageBroker`
- New module `rqueue-nats` (broker-impl only, no Spring/Boot deps; jnats 2.25.2 as `api`). Auto-config and `@Conditional` wiring live in `rqueue-spring-boot-starter` and `rqueue-spring`, gated by `@ConditionalOnClass(io.nats.client.JetStream.class)` + `@ConditionalOnProperty(rqueue.backend=nats)`.
- `JetStreamMessageBroker`:
  - Builder API (`builder().connection(...).jetStream(...).management(...).config(...).build()`).
  - `enqueue` → `js.publish(subject, headers, payload)` with `Nats-Msg-Id` header for dedup.
  - `enqueueWithDelay` → throws `UnsupportedOperationException` (NATS v1 doesn't support arbitrary delay).
  - `pop` → ensures stream + durable consumer, caches `JetStreamSubscription`, `sub.fetch(batch, wait)`, stashes raw `Message` in `inFlight` keyed by `RqueueMessage.id` for ack/nack lookup.
  - `ack` / `nack(delayMs)` → `Message.ack()` / `Message.nakWithDelay(...)`.
  - `peek` → ephemeral pull consumer with `AckPolicy.None`, fetch + unsubscribe (no perturbation of the durable's ack-pending state).
  - `size` → `jsm.getStreamInfo(stream).getStreamState().getMsgCount()`.
  - `subscribe` / `publish` → core NATS `Dispatcher`, returns `AutoCloseable` that calls `closeDispatcher`.
  - Reactive overrides via `js.publishAsync(...)` wrapped in `Mono.fromFuture`.
  - DLQ bridge: `installDeadLetterBridge(QueueDetail, consumerName)` subscribes to `$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.>` and republishes exhausted messages to the DLQ subject.
- `RqueueNatsConfig` POJO + nested `StreamDefaults` / `ConsumerDefaults` for stream replication/storage/retention/dedup-window and consumer ack-wait/max-deliver/max-ack-pending.
- `NatsProvisioner` (in `rqueue-nats/.../internal/`) — idempotent `ensureStream`, `ensureConsumer`, `ensureDlqStream`. Logs WARN if existing config drifts from desired (doesn't mutate).
- `JetStreamMessageBrokerFactory` + `META-INF/services/com.github.sonus21.rqueue.core.spi.MessageBrokerFactory` for ServiceLoader discovery (`name() == "nats"`).
- `RqueueNatsException` (RuntimeException) wraps `IOException` / `JetStreamApiException` with stream/subject/consumer context in the message.
- 9 Docker-gated ITs in `rqueue-nats/src/test/`: `EnqueueAck`, `Retry+DLQ`, `CompetingConsumers`, `IndependentConsumers`, `Dedup`, `Peek`, `PubSub`, `ReactiveEnqueue`, `DelayThrows`. All pass against `nats:2.10-alpine -js` via Testcontainers.

### Phase 3 — Spring/Boot wiring + listener-container branch
- `RqueueNatsAutoConfig` (Boot) registered via `META-INF/spring/...AutoConfiguration.imports`, gated `@ConditionalOnClass(JetStream.class) + @ConditionalOnProperty(rqueue.backend=nats)`. Provides `Connection`, `JetStream`, `JetStreamManagement`, `MessageBroker`, `RqueueQueueMetricsProvider` beans.
- `RqueueNatsListenerConfig` (non-Boot) activated by `NatsBackendCondition`. `@EnableRqueue(backend=NATS)` opts in via the new `Backend` enum.
- `RqueueListenerAutoConfig`'s default Redis broker uses `@ConditionalOnMissingBean(MessageBroker.class)`; the NATS bean wins when present.
- `@RqueueListener.consumerName()` attribute (additive). `ConsumerNameResolver` resolves: `consumerName` if set, else `"rqueue-" + queue + "-" + bean + "_" + method` with everything outside `[A-Za-z0-9_-]` collapsed to `_` (NATS durable-name constraint).
- `RqueueMessageHandler` skips primary-validation and logs one boot WARN listing `@RqueueHandler` annotated methods when capability says no primary dispatch.
- Cross-handler validation: `(queue, consumerName)` collisions fail boot fast.

### Phase 3.5 — runtime path
- `BrokerMessagePoller` in `rqueue-core/listener/`. One thread per `(queue, consumerName, priority)` triple per `@RqueueListener.concurrency.max`. Loop: `broker.pop` → deserialize via `MessageConverter` → reflection-invoke the bound `HandlerMethod` (calls `createWithResolvedBean()` so bean-name lookup works) → `broker.ack` / `broker.nack(delayMs)` with `TaskExecutionBackOff`.
- `RqueueMessageListenerContainer` branches on `messageBroker != null && !capabilities.usesPrimaryHandlerDispatch()`:
  - `startBrokerPollers()` enumerates active queues + handler methods, resolves consumer names, spawns pollers.
  - `MessageScheduler` not started; `RqueueMessageHandler` primary loop bypassed.
  - `doStop()` signals every poller; `doDestroy()` calls `broker.close()` if `AutoCloseable`.
- `BaseMessageSender.enqueue` routes through `MessageBroker.enqueue` when the active broker has `!usesPrimaryHandlerDispatch`. `storeMessageMetadata` short-circuits on the same flag.
- `RqueueListenerAutoConfig.rqueueMessageTemplate` propagates the autowired `MessageBroker` onto the template bean — without that, `BaseMessageSender#enqueue` would silently fall back to the Redis publish path.
- `SimpleRqueueListenerContainerFactory.createMessageListenerContainer()` skips the `redisConnectionFactory != null` assertion when a non-Redis broker is wired.

### Phase 4 — dashboard + `QueueDetail` NATS fields
- `QueueDetail` adds nullable NATS fields with `resolved*` helpers: `natsStream`, `natsSubject`, `natsDlqStream`, `natsDlqSubject`, `natsAckWaitOverride`, `natsMaxDeliverOverride`, `natsDedupWindow`. Defaults derived from `queueName` when null.
- `RqueueQDetailServiceImpl` routes `size` / `peek` through `MessageBroker` when set; falls back to existing Redis path otherwise.
- `DataViewResponse` adds `hideScheduledPanel` / `hideCronJobs` flags. Pebble template `base.html` hides the "Scheduled" sidebar entry when the flag is set.
- Dashboard chain (`RqueueRestController`, `RqueueDashboardChartServiceImpl`, etc.) gated `@Conditional(RedisBackendCondition)`; on NATS the dashboard reports broker-derived sizes only.

### Cross-phase summary
- Pluggable selection via `rqueue.backend=redis|nats` (default `redis`) and classpath presence.
- `RqueueConfig` carries the active `Backend` enum; downstream beans branch on that instead of probing the classpath.
- `Backend.AUTO` removed; `@EnableRqueue.backend()` defaults to `REDIS`.

### Backend wiring split
- New module `rqueue-redis` with the Redis-shaped impls (DAOs, lock manager, KV-shaped beans).
- `Backend` enum + `RedisBackendCondition` / `NatsBackendCondition` in `rqueue-core`.
- `RqueueConfig.backend` field (default `REDIS`) bound from the `rqueue.backend` property; `Backend.AUTO` removed.
- `RqueueListenerBaseConfig.rqueueConfig(...)` factory tolerates a missing `RedisConnectionFactory`.
- `RqueueRedisTemplate` and `RqueueMessageTemplateImpl` constructors tolerate null Redis connection factory (NATS path constructs them for type satisfaction but never invokes Redis ops on them).
- `SimpleRqueueListenerContainerFactory` skips its `redisConnectionFactory != null` assertion when a non-Redis broker is set.
- `BaseMessageSender` routes producer enqueue through `MessageBroker.enqueue` when broker has `!usesPrimaryHandlerDispatch`; `storeMessageMetadata` short-circuits on the same flag.
- `RqueueQueueMetricsProvider` is the new backend-agnostic interface for queue-depth gauges; `RedisRqueueQueueMetricsProvider` (rqueue-redis) and `NatsRqueueQueueMetricsProvider` (rqueue-nats) supply impls. `RqueueMetrics` now reads through this provider, decoupled from `RqueueStringDao`.

### NATS-native impls (KV-backed)
- `NatsRqueueLockManager` — KV bucket `rqueue-locks`, atomic create/release with revisioned delete, 6 ITs.
- `NatsRqueueSystemConfigDao` — KV bucket `rqueue-queue-config`, in-process cache, 6 ITs.
- `NatsRqueueJobDao` — KV bucket `rqueue-jobs`, scan-by-message-id, 7 ITs.
- `NatsRqueueMessageMetadataService` — KV bucket `rqueue-message-metadata`, 8 ITs.
- `NatsRqueueUtilityService` — admin-only stub returning "not supported" responses.
- `NatsRqueueStringDao` deleted — no consumer on the NATS path needs it.

### Bean-graph cleanup
- `RqueueStringDao` consumers (`RqueueLockManagerImpl`, `RqueueJobDaoImpl`, `RqueueMessageMetadataServiceImpl`, `RqueueSystemManagerServiceImpl`, `RqueueUtilityServiceImpl`, `RqueueMetrics`'s old size getter) all gated `@Conditional(RedisBackendCondition.class)` or refactored to use `RqueueQueueMetricsProvider`.
- `RqueueStringDao` is now strictly internal to the Redis backend; no NATS-path bean autowires it.
- `BaseMessageSender`, `RqueueMessageManagerImpl`, `RqueueEndpointManagerImpl`, `RqueueBeanProvider` reverted to plain `@Autowired` (no more `required=false` shotgun) — every required interface has either a Redis impl or a `NatsRqueueXxx` impl.

### CI
- `nats_integration_test` job in `.github/workflows/java-ci.yaml` installs nats-server v2.10.22 binary directly (no Docker), sets `NATS_RUNNING=true` + `NATS_URL`, mirrors the `redis_cluster_test` pattern.
- `AbstractNatsBootIT` and `AbstractJetStreamIT` honor `NATS_RUNNING` (CI path) and fall back to Testcontainers (local Docker path).
- Tests are tagged `@Tag("nats")` via `NatsIntegrationTest` / `NatsUnitTest` meta-annotations.

### Module moves to `rqueue-redis`
- DAO impls: `RqueueStringDaoImpl`, `RqueueJobDaoImpl`, `RqueueMessageMetadataDaoImpl`, `RqueueQStatsDaoImpl`, `RqueueSystemConfigDaoImpl` → `rqueue-redis/src/main/java/com/github/sonus21/rqueue/redis/dao/`.
- Metrics: `RedisRqueueQueueMetricsProvider` → `rqueue-redis/.../redis/metrics/`.
- 5 `@Bean` factories from `RqueueListenerBaseConfig` → `RqueueRedisListenerConfig`: `rqueueRedisLongTemplate`, `rqueueRedisListenerContainerFactory`, `stringRqueueRedisTemplate`, `rqueueInternalPubSubChannel`, `rqueueStringDao`.
- 5 more `@Bean` factories moved most recently: `scheduledMessageScheduler`, `processingMessageScheduler`, `rqueueWorkerRegistry`, `rqueueLockManager`, `rqueueQueueMetrics`.
- 6 service impls (in flight, see Pending): `RqueueDashboardChartServiceImpl`, `RqueueJobServiceImpl`, `RqueueMessageMetadataServiceImpl`, `RqueueQDetailServiceImpl`, `RqueueSystemManagerServiceImpl`, `RqueueUtilityServiceImpl` — moved to `rqueue-redis/.../redis/web/service/impl/`.

### CI & PR
- Branch `nats-backend` pushed to `origin`. ~50 commits, all carry `Assisted-By: Claude Code` only (no `Co-Authored-By:`). `CLAUDE.md` documents the rule.

## Pending items

### In flight — finish service impl move (slice B of `@Conditional` cleanup)

The 6 `*ServiceImpl` files have moved to `rqueue-redis`. Their unit tests have been moved alongside. **Compilation in `rqueue-redis:test` is failing** because the moved tests depend on `CoreUnitTest` (annotation) and `QueueStatisticsTest` (fixture data) which still live in `rqueue-core/src/test`. Two paths:

1. Promote `CoreUnitTest` + `QueueStatisticsTest` to `rqueue-test-util/src/main` so they're visible across modules. Smallest change.
2. Add Gradle `java-test-fixtures` to `rqueue-core` and pull from `rqueue-redis` via `testFixtures(project(":rqueue-core"))`. More canonical Gradle but bigger setup.

Pick **option 1** for simplicity. Move:
- `rqueue-core/src/test/java/com/github/sonus21/rqueue/CoreUnitTest.java` → `rqueue-test-util/src/main/java/com/github/sonus21/rqueue/CoreUnitTest.java`
- `rqueue-core/src/test/java/com/github/sonus21/rqueue/models/db/QueueStatisticsTest.java` → split into a fixture helper in `rqueue-test-util` and a thin test that re-exercises it in core (or just keep both copies during transition).

Then re-run `./gradlew :rqueue-core:test :rqueue-redis:test :rqueue-nats:test -DincludeTags=unit` and `./gradlew :rqueue-spring-boot-starter:test --tests NatsBackendEndToEndIT`.

### Other open follow-ups

- **`RqueueStringDao` interface** itself — analysis recommended keeping it Redis-only (don't split into smaller cross-backend interfaces). It already has zero NATS-path consumers; just document it as Redis-internal in javadoc.
- **`RqueueMessageMetadataDao`, `RqueueQStatsDao`** — no NATS impls needed; all consumers are Redis-only gated. Verified, no action.
- **Reactive listener container** — only enqueue side is reactive in v1. Phase 5 territory.
- **Delayed/scheduled/cron messages on NATS** — throws `UnsupportedOperationException`. Out of scope for v1.
- **Cross-queue `priorityGroup` weighting on NATS** — boot WARN, not honored. Acceptable for v1.
- **Elastic `@RqueueListener.concurrency` (min < max)** — falls back to fixed `max` on NATS. Acceptable.
- **`@RqueueHandler(primary)` on NATS** — ignored, single boot WARN.
- **PR open on `sonus21/rqueue:nats-backend`** — branch pushed; user opened the PR through the GitHub UI.

## Local verification commands

```
./gradlew :rqueue-core:test :rqueue-redis:test :rqueue-nats:test -DincludeTags=unit
./gradlew :rqueue-spring-boot-starter:test --tests "com.github.sonus21.rqueue.spring.boot.integration.NatsBackendEndToEndIT"
./gradlew :rqueue-nats:test --tests "com.github.sonus21.rqueue.nats.lock.NatsRqueueLockManagerIT"
./gradlew :rqueue-nats:test --tests "com.github.sonus21.rqueue.nats.dao.NatsRqueueSystemConfigDaoIT"
./gradlew :rqueue-nats:test --tests "com.github.sonus21.rqueue.nats.dao.NatsRqueueJobDaoIT"
./gradlew :rqueue-nats:test --tests "com.github.sonus21.rqueue.nats.service.NatsRqueueMessageMetadataServiceIT"
```

## Commit-rule reminder

`CLAUDE.md` at the repo root forbids `Co-Authored-By:` for any AI tool. Use `Assisted-By: Claude Code` as a single trailer per commit. The trailer rewrite has already been applied to historical commits; new commits just need the right form.
