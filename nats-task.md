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
- 5 more `@Bean` factories: `scheduledMessageScheduler`, `processingMessageScheduler`, `rqueueWorkerRegistry`, `rqueueLockManager`, `rqueueQueueMetrics`.
- 6 service impls: `RqueueDashboardChartServiceImpl`, `RqueueJobServiceImpl`, `RqueueMessageMetadataServiceImpl`, `RqueueQDetailServiceImpl`, `RqueueSystemManagerServiceImpl`, `RqueueUtilityServiceImpl` → `rqueue-redis/.../redis/web/service/impl/`.

### `rqueue-web` module extraction
- New module `rqueue-web` registered in `settings.gradle`. `rqueue-spring-boot-starter`, `rqueue-spring`, `rqueue-redis`, and `rqueue-spring-common-test` declare `api project(":rqueue-web")` so the dashboard ships by default; consumers `<exclude>` it for headless workers.
- Moved out of `rqueue-core/web/...` into `rqueue-web/web/...`: 5 controllers (`Base`, `BaseReactive`, `RqueueRest`, `RqueueView`, `ReactiveRqueueRest`, `ReactiveRqueueView`), `RqueueWebExceptionAdvice`, `RqueueViewControllerServiceImpl`, and the 6 web-only service interfaces (`RqueueDashboardChartService`, `RqueueJobMetricsAggregatorService`, `RqueueJobService`, `RqueueQDetailService`, `RqueueSystemManagerService`, `RqueueViewControllerService`). Stayed in core: `RqueueMessageMetadataService` and `RqueueUtilityService` interfaces (consumed by listener / endpoint manager).
- Moved out of `rqueue-core/utils/pebble/`: 7 Pebble extension classes → `rqueue-web/utils/pebble/` (same package, no import changes).
- Moved out of `rqueue-core/src/main/resources/`: `templates/rqueue/**`, `public/rqueue/**` (CSS, JS, vendor assets) → `rqueue-web/src/main/resources/`.
- Moved out of `rqueue-core/src/test/`: `web/**` and `utils/pebble/**` test files → `rqueue-web/src/test/`. **Currently unbuildable** — see Pending.
- Pebble view-resolver `@Bean`s extracted from `RqueueListenerBaseConfig` into a new `RqueueWebViewConfig` in `rqueue-web/web/config/`. Picked up via the existing `com.github.sonus21.rqueue.web` component scan.
- `rqueue-core/build.gradle` dropped: `spring-webmvc`, `spring-webflux`, `jakarta.servlet-api`, `pebble-spring7`, `seruco/base62`, `hibernate-validator`, `org.glassfish:jakarta.el`. Added `reactor-core` directly (no longer comes via `spring-webflux`). `jakarta.validation-api` retained for DTO annotations.

### `HttpUtils` JDK-client migration
- `HttpUtils.readUrl` rewritten to use `java.net.http.HttpClient` + Jackson; `org.springframework.web.client.RestTemplate` removed. `spring-web` dropped from `rqueue-core` deps.
- `joinPath` retained unchanged; `RqueueWebConfig.getUrlPrefix` still calls it.

### Backend-agnostic worker registry
- New SPI in `rqueue-core`: `WorkerRegistryStore` (7 narrow KV-shaped methods). `RqueueWorkerRegistryImpl` relocated from `rqueue-redis/redis/worker/` to `rqueue-core/worker/`; takes `(RqueueConfig, WorkerRegistryStore)` in the constructor. All heartbeat scheduling / view assembly logic backend-neutral.
- `RedisWorkerRegistryStore` (rqueue-redis) wraps `RqueueRedisTemplate` over `set`/`get`/`mget`/hash ops.
- `NatsWorkerRegistryStore` (rqueue-nats) wraps two JetStream KV buckets: `rqueue-workers` (TTL = `workerRegistry.workerTtl`), `rqueue-worker-heartbeats` (TTL = `workerRegistry.queueTtl`). Hash-of-strings emulated as flattened keys `<sanitizedQueueKey>__<sanitizedWorkerId>`. `refreshQueueTtl` is a no-op since NATS resets per-entry age on each write.
- Wired in `RqueueRedisListenerConfig` and `RqueueNatsAutoConfig` under `@ConditionalOnMissingBean`.

### NATS KV bucket lifecycle (`rqueue.nats.autoCreateKvBuckets`)
- New `com.github.sonus21.rqueue.nats.kv.NatsKvBuckets` constants class — single source of truth for the 6 bucket names (`QUEUE_CONFIG`, `JOBS`, `LOCKS`, `MESSAGE_METADATA`, `WORKERS`, `WORKER_HEARTBEATS`) + `ALL_BUCKETS` list. Every store / dao now references this constant instead of a private string.
- New `com.github.sonus21.rqueue.nats.kv.NatsKvBucketValidator` — config-source-agnostic class; constructor takes `(Connection, boolean autoCreate)`. Static `validate(Connection, boolean)` walks `ALL_BUCKETS` via `kvm.getStatus(name)` and aborts with `IllegalStateException` listing missing buckets. Implements `InitializingBean` so the bean form re-runs the same check.
- New `rqueue.nats.autoCreateKvBuckets` field on `RqueueNatsProperties` (default `true`). `rqueue-nats` itself never reads `rqueue.nats.*` keys directly — the property flows in only through the auto-config.
- Two enforcement layers in `RqueueNatsAutoConfig`:
  1. Inline call to `NatsKvBucketValidator.validate(connection, props.isAutoCreateKvBuckets())` inside the `natsConnection` `@Bean` factory, so validation completes during `Connection` bean creation (strictly before any other NATS bean can inject the connection).
  2. `@Bean public NatsKvBucketValidator natsKvBucketValidator(...)` declared from `RqueueNatsProperties`. Five NATS components (`NatsRqueueSystemConfigDao`, `NatsRqueueJobDao`, `NatsRqueueLockManager`, `NatsRqueueMessageMetadataService`, `NatsWorkerRegistryStore`) plus the `WorkerRegistryStore` `@Bean` factory carry `@DependsOn("natsKvBucketValidator")` so they wait on it even when the inline path is bypassed.

### Web-layer infrastructure for capability-aware errors
- `BackendCapabilityException` (in `rqueue-core/exception/`) — carries `{backend, operation, reason}`. Mapped to HTTP 501 with structured JSON body by `RqueueWebExceptionAdvice` (in `rqueue-web/web/controller/`, scoped `@RestControllerAdvice(basePackageClasses = ...)`). No callers yet — landed as scaffolding for the upcoming web-service repository-interface refactor.

### README — NATS backend section
- New "NATS backend" section in `README.md` covering: the 6 KV buckets (table with name, purpose, TTL behaviour, code link), how buckets are configured (lazy / immutable `ttl` / connection wiring), pre-create commands for restricted JetStream accounts, the `rqueue.nats.autoCreateKvBuckets=false` flag and its two-layer enforcement, and a recreate-with-new-TTL recipe.

### CI & PR
- Branch `nats-backend` pushed to `origin`. ~50 commits, all carry `Assisted-By: Claude Code` only (no `Co-Authored-By:`). `CLAUDE.md` documents the rule.

## Pending items

### Build green — three test-compile failures across the tree

Same root cause for two of three (cross-module visibility of test fixtures). Pick **option 1** below: promote the offenders to `rqueue-test-util/src/main`.

1. **`rqueue-redis:compileTestJava`** — moved tests reference `CoreUnitTest` (annotation) and `QueueStatisticsTest` (fixture data) still living in `rqueue-core/src/test`.
2. **`rqueue-web:compileTestJava`** — `DateTimeFunctionTest`, `RqueueTaskMetricsAggregatorServiceTest`, `RqueuePebbleExtensionTest` reference `CoreUnitTest` and `TestUtils.createQueueDetail`, both still in `rqueue-core/src/test`.
3. **`rqueue-nats:compileTestJava`** — `JetStreamMessageBrokerDelayThrowsTest:36` calls `new JetStreamMessageBroker(Connection, JetStream, JetStreamManagement, RqueueNatsConfig, ObjectMapper)` from outside the broker's package. The constructor is package-private (regression from a recent visibility tighten). Fix: widen the constructor to `public`, or use the existing `JetStreamMessageBroker.builder()` API in the test.

Plan for items 1 + 2:
- Move `rqueue-core/src/test/java/com/github/sonus21/rqueue/CoreUnitTest.java` → `rqueue-test-util/src/main/java/com/github/sonus21/rqueue/CoreUnitTest.java`.
- Move `rqueue-core/src/test/java/com/github/sonus21/rqueue/utils/TestUtils.java` → `rqueue-test-util/src/main/java/com/github/sonus21/rqueue/utils/TestUtils.java`.
- (Optional) `QueueStatisticsTest` fixture data — promote helpers if the moved Redis tests still need them.
- All consumer modules already pull `rqueue-test-util` as `testImplementation`, so no build wiring needed.

Then re-run:
```
./gradlew :rqueue-core:test :rqueue-redis:test :rqueue-web:test :rqueue-nats:test -DincludeTags=unit
./gradlew :rqueue-spring-boot-starter:test --tests "com.github.sonus21.rqueue.spring.boot.integration.NatsBackendEndToEndIT"
```

### Web-layer NATS dashboard gap (new follow-up)

All 4 controllers and the 5 web service impls (`RqueueDashboardChartService*`, `RqueueQDetailService*`, `RqueueJobService*`, `RqueueSystemManagerService*`, `RqueueUtilityService*`) are still gated `@Conditional(RedisBackendCondition)`. On NATS the dashboard reports broker-derived sizes only; no charts, no message browse, no admin ops. Plan to fix:

1. Introduce repository interfaces in `rqueue-core/repository/` for the few storage primitives the web services share (queue browsing, time-series counters, atomic move). Web service impls move into core / `rqueue-web` and depend only on the repos.
2. Redis impls of the repos stay in `rqueue-redis`; NATS impls go in `rqueue-nats` and throw `BackendCapabilityException("nats", "operation", "reason")` for primitives JetStream can't model (positional message moves, time-bucket charts).
3. Drop `@Conditional(RedisBackendCondition)` from controllers; the advice already maps the exception to HTTP 501 with a structured body.
4. Extend the existing `Capabilities` record (`MessageBroker.capabilities()`) with dashboard-op flags so the front-end can hide unsupported panels instead of relying on 501s. Expose `GET /rqueue/api/capabilities`.

Order of operations: easiest first — `RqueueSystemManagerService` (already mostly goes through `RqueueSystemConfigDao`), then `RqueueJobService`, `RqueueViewControllerService`, then `RqueueQDetailService` (needs new `MessageBrowsingRepository`), then `RqueueDashboardChartService` and `RqueueUtilityService.move/enqueue` last (these throw on NATS).

### Spring Boot configuration metadata

`spring-configuration-metadata.json` has no entry for `rqueue.nats.autoCreateKvBuckets`. IDE autocomplete won't show it. Easy follow-up: add `spring-boot-configuration-processor` to the starter's annotation processors if not already wired.

### Other open follow-ups

- **`RqueueStringDao` interface** — keep Redis-only; document as Redis-internal in javadoc.
- **`RqueueMessageMetadataDao`, `RqueueQStatsDao`** — no NATS impls needed; all consumers are Redis-only gated. Re-verify in light of the web-layer refactor above.
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
