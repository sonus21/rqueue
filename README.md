<div>
   <img  align="left" src="https://raw.githubusercontent.com/sonus21/rqueue/master/rqueue-core/src/main/resources/public/rqueue/img/android-chrome-192x192.png" alt="Rqueue Logo" width="90">
   <h1 style="float:left">Rqueue: Redis-Backed Job Queue and Scheduler for Spring and Spring Boot</h1>
</div>

[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.sonus21/rqueue-core)](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core)
[![Javadoc](https://javadoc.io/badge2/com.github.sonus21/rqueue-core/javadoc.svg)](https://javadoc.io/doc/com.github.sonus21/rqueue-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Rqueue** is a Redis-backed job queue and producer-consumer system for Spring and Spring Boot. It
supports both producers and consumers for background jobs, scheduled tasks, and event-driven
workflows, similar to Sidekiq or Celery, but fully integrated into the Spring programming model with
annotation-driven APIs and minimal setup.

<br/>

![Message Flow](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/rqueue-message-flow.svg?sanitize=true)

## Features

* **Job execution**
  * Run background jobs asynchronously
  * Schedule jobs for any future time
  * Run periodic jobs at fixed intervals
  * Guarantee at-least-once delivery
  * Retry failed jobs automatically with fixed or exponential backoff

* **Queues and routing**
  * Deduplicate messages using message IDs
  * Process priority workloads such as high, medium, and low
  * Prioritize workloads with group-level queue priority and weighted, strict, or hard strict ordering
  * Fan out the same message to multiple listeners
  * Poll messages in batches for higher throughput

* **Consumers and scale**
  * Use annotation-driven listeners with Spring beans
  * Get started with just the dependency in Spring Boot applications
  * Run multiple competing consumers in parallel
  * Configure listener concurrency per worker
  * Support long-running jobs with periodic check-ins
  * Serialize and deserialize message payloads automatically

* **Operations and extensibility**
  * Add middleware before listener execution
  * Use callbacks for dead-letter, discard, and related flows
  * Subscribe to bootstrap and task execution events
  * Monitor in-flight, queued, and scheduled messages with metrics
  * Use the built-in web dashboard for queue visibility and latency insights

* **Redis and platform support**
  * Use a separate Redis setup for Rqueue if needed
  * Support Redis standalone, Sentinel, and Cluster setups
  * Work with Lettuce for Redis Cluster
  * Support reactive Redis and Spring WebFlux

### Requirements

* Spring 5+, 6+, 7+
* Java 1.8+,17, 21
* Spring boot 2+,3+,4+
* Lettuce client for Redis cluster
* Read master preference for Redis cluster

## Getting Started

### Dependency

Release Version: [Maven central](https://search.maven.org/search?q=g:com.github.sonus21)

#### Spring Boot

**NOTE:**

* For Spring Boot 3.x use Rqueue 3.x
* For Spring Boot 4.x use Rqueue 4.x

Get the latest one
from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter)

* Add dependency
  * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring-boot-starter:4.0.0-RELEASE'
    ```
  * Maven
    ```xml
     <dependency>
        <groupId>com.github.sonus21</groupId>
        <artifactId>rqueue-spring-boot-starter</artifactId>
        <version>4.0.0-RELEASE</version>
    </dependency>
    ```

  No additional configurations are required, only dependency is required.

---

#### Spring Framework

**NOTE**

* For Spring Framework 6.x use Rqueue 3.x
* For Spring Framework 7.x use Rqueue 4.x

Get the latest one
from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring)

* Add Dependency
  * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring:4.0.0-RELEASE'
    ```
  * Maven
    ```xml
     <dependency>
       <groupId>com.github.sonus21</groupId>
       <artifactId>rqueue-spring</artifactId>
       <version>4.0.0-RELEASE</version>
     </dependency>
    ```
* Add annotation `EnableRqueue` on application config class
* Provide a RedisConnectionFactory bean

###### Configuration

```java

@EnableRqueue
public class Application {
  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    // return a redis connection factory
  }
}
```

---

### Message publishing/Task submission

All messages need to be sent using `RqueueMessageEnqueuer` bean's `enqueueXXX`, `enqueueInXXX`
and `enqueueAtXXX` methods. It has handful number of `enqueue`, `enqueueIn`, `enqueueAt` methods, we
can use any one of them based on the use case.

```java
public class MessageService {

  @AutoWired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void doSomething() {
    rqueueMessageEnqueuer.enqueue("simple-queue", "Rqueue is configured");
  }

  public void createJOB(Job job) {
    rqueueMessageEnqueuer.enqueue("job-queue", job);
  }

  // send notification in 30 seconds
  public void sendNotification(Notification notification) {
    rqueueMessageEnqueuer.enqueueIn("notification-queue", notification, 30 * 1000L);
  }

  // enqueue At example
  public void createInvoice(Invoice invoice, Instant instant) {
    rqueueMessageEnqueuer.enqueueAt("invoice-queue", invoice, instant);
  }

  // enqueue with priority, when sub queues are used as explained in the queue priority section.
  enum SmsPriority {
    CRITICAL("critical"),
    HIGH("high"),
    MEDIUM("medium"),
    LOW("low");
    private String value;
  }

  public void sendSms(Sms sms, SmsPriority priority) {
    rqueueMessageEnqueuer.enqueueWithPriority("sms-queue", priority.value(), sms);
  }

  // Index chat every 1 minute
  public void sendPeriodicEmail(Email email) {
    rqueueMessageEnqueuer.enqueuePeriodic("chat-indexer", chatIndexer, 60_000);
  }

}
```

---

### Worker/Consumer/Task Executor/Listener

Any method that's part of spring bean, can be marked as worker/message listener
using `RqueueListener` annotation

```java

@Component
@Slf4j
public class MessageListener {

  @RqueueListener(value = "simple-queue")
  public void simpleMessage(String message) {
    log.info("simple-queue: {}", message);
  }

  @RqueueListener(value = "job-queue", numRetries = "3",
      deadLetterQueue = "failed-job-queue", concurrency = "5-10")
  public void onMessage(Job job) {
    log.info("Job alert: {}", job);
  }

  @RqueueListener(value = "push-notification-queue", numRetries = "3",
      deadLetterQueue = "failed-notification-queue")
  public void onMessage(Notification notification) {
    log.info("Push notification: {}", notification);
  }

  @RqueueListener(value = "sms", priority = "critical=10,high=8,medium=4,low=1")
  public void onMessage(Sms sms) {
    log.info("Sms : {}", sms);
  }

  @RqueueListener(value = "chat-indexing", priority = "20", priorityGroup = "chat")
  public void onMessage(ChatIndexing chatIndexing) {
    log.info("ChatIndexing message: {}", chatIndexing);
  }

  @RqueueListener(value = "chat-indexing-daily", priority = "10", priorityGroup = "chat")
  public void onMessage(ChatIndexing chatIndexing) {
    log.info("ChatIndexing message: {}", chatIndexing);
  }

  // checkin job example
  @RqueueListener(value = "chat-indexing-weekly", priority = "5", priorityGroup = "chat")
  public void onMessage(ChatIndexing chatIndexing,
      @Header(RqueueMessageHeaders.JOB) com.github.sonus21.rqueue.core.Job job) {
    log.info("ChatIndexing message: {}", chatIndexing);
    job.checkIn("Chat indexing...");
  }
}
```

---

## Dashboard

Link: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

[![Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)

#### Queue Statistics

Micrometer based dashboard for queue

[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)

#### Message Waiting For Execution

[![Explore Queue](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)

#### Recent jobs details

[![Jobs](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/jobs.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/jobs.png)

---

## NATS backend

Rqueue can use NATS JetStream as the message broker instead of Redis by setting
`rqueue.backend=nats` and including the `rqueue-nats` module on the classpath. JetStream streams
(one per queue) are provisioned by `JetStreamMessageBrokerFactory` / `NatsProvisioner`. State
that Redis stores in keys, hashes, and sorted-sets is mapped onto JetStream **KV buckets** —
one bucket per concern. All buckets use the default replicas / storage settings of the JetStream
account unless noted; per-entry TTL relies on the bucket's `ttl` (NATS' name for `maxAge`),
which is set once at bucket creation.

| Bucket name                  | Purpose                                                                 | TTL behaviour                                                                                                  | Created in                                                                                                                                                            |
|------------------------------|-------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `rqueue-queue-config`        | Per-queue `QueueConfig` records (registered queues, DLQ wiring, flags). | No TTL. Entries persist until explicitly overwritten.                                                          | [`NatsRqueueSystemConfigDao`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/dao/NatsRqueueSystemConfigDao.java) (`@Conditional(NatsBackendCondition)`)      |
| `rqueue-jobs`                | `RqueueJob` execution history per message id.                           | TTL captured from the first `createJob`/`save` call's `expiry` argument; bucket-level so it applies uniformly. | [`NatsRqueueJobDao`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/dao/NatsRqueueJobDao.java)                                                              |
| `rqueue-locks`               | Distributed locks (scheduler leadership, message-level locks).          | TTL captured from the first `acquireLock` call's `duration` argument.                                          | [`NatsRqueueLockManager`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/lock/NatsRqueueLockManager.java)                                                   |
| `rqueue-message-metadata`    | Per-message metadata (delivery status, retry count, dead-letter flags). | No TTL at the bucket. Per-write `ttl` arguments are ignored on this v1 impl.                                   | [`NatsRqueueMessageMetadataService`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/service/NatsRqueueMessageMetadataService.java)                          |
| `rqueue-workers`             | Worker process info (host, pid, version, last-seen).                    | TTL = `rqueue.workerRegistry.workerTtl` (captured on first heartbeat).                                         | [`NatsWorkerRegistryStore`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/worker/NatsWorkerRegistryStore.java)                                             |
| `rqueue-worker-heartbeats`   | Per-(queue, worker) heartbeats. Keys flattened as `<queue>__<worker>`.  | TTL = `rqueue.workerRegistry.queueTtl` (captured on first refresh; falls back to 1 h if registry not enabled). | [`NatsWorkerRegistryStore`](rqueue-nats/src/main/java/com/github/sonus21/rqueue/nats/worker/NatsWorkerRegistryStore.java)                                             |

### How buckets are configured

- **Lazy, code-driven creation.** Each store / dao calls `kvm.create(KeyValueConfiguration...)`
  the first time it is touched after startup. There is no `application.yml` switch to disable
  this, and there is no provisioning step you need to run by hand — but the JetStream account
  used by your `Connection` bean must have permission to create KV buckets (i.e. JetStream must
  be enabled and account limits must allow it).
- **TTL is fixed at bucket creation.** All buckets that take a `ttl` snapshot the value at
  creation. Changing the corresponding rqueue property after the bucket exists has no effect
  until the bucket is deleted out-of-band and recreated. This matches NATS KV semantics — the
  bucket's `maxAge` is immutable.
- **No bucket per queue.** All queues share the same buckets above; per-queue scoping is done
  via the key prefix (`rqueue.workerRegistry.queueKey(queueName)`, etc.).
- **Connection wiring.** The `io.nats.client.Connection` bean comes from
  [`RqueueNatsAutoConfig`](rqueue-spring-boot-starter/src/main/java/com/github/sonus21/rqueue/spring/boot/RqueueNatsAutoConfig.java)
  (Spring Boot) when `rqueue.backend=nats` and `io.nats.client.JetStream` is on the classpath.
  All KV stores receive that same `Connection` and call `connection.keyValueManagement()` /
  `connection.keyValue(name)` against it.

### Pre-creating buckets (restricted JetStream accounts)

In managed or locked-down JetStream deployments the credentials your application uses may not
have permission to create KV buckets at runtime. In that case the lazy `kvm.create(...)` call
on first use will fail with `JetStreamApiException` ("permission violation" or "stream not
found"), and depending on the call site the failure may be logged and swallowed (registry,
metadata) or surface as a missing record.

To run against such a NATS, **pre-create every bucket below before starting the application**.
The commands assume the [`nats` CLI](https://docs.nats.io/using-nats/nats-tools/nats_cli) is
configured against the same account and creds your application uses. Substitute your own
values for replicas, storage, and TTL; the values shown match the defaults Rqueue would use if
it created the bucket itself.

```bash
# State that must persist (no TTL).
nats kv add rqueue-queue-config       --replicas=3 --storage=file
nats kv add rqueue-message-metadata   --replicas=3 --storage=file

# Job history. Use the same value as rqueue.job.durability (default 7 days).
nats kv add rqueue-jobs               --replicas=3 --storage=file --ttl=7d

# Distributed locks. Use a value at least as large as your longest expected lock hold.
nats kv add rqueue-locks              --replicas=3 --storage=file --ttl=10m

# Worker registry. Match rqueue.workerRegistry.workerTtl / queueTtl exactly.
nats kv add rqueue-workers            --replicas=3 --storage=file --ttl=5m
nats kv add rqueue-worker-heartbeats  --replicas=3 --storage=file --ttl=10m
```

Once the buckets exist, Rqueue's lazy initialiser short-circuits — `kvm.getStatus(name)` returns
non-null and the existing bucket is opened, no `create` call is made. The application
credentials only need read/write on the buckets, not management privileges.

> Today there is no `rqueue.nats.autoCreateKvBuckets` flag to fail fast at startup if a bucket
> is missing — the failure is observed lazily on first use. If you want stricter validation,
> file an issue / PR; the hook point is the `ensureBucket(...)` method in each store and dao.

### Re-creating a bucket with new settings

If you need to change a bucket's TTL or replication settings after deployment, delete the
bucket via the NATS CLI and either let Rqueue recreate it on the next startup (open accounts)
or recreate it yourself with the new flags (restricted accounts):

```bash
nats kv del rqueue-worker-heartbeats --force
nats kv add rqueue-worker-heartbeats --replicas=3 --storage=file --ttl=20m
```

Be aware that any data in the bucket is lost (which is acceptable for the worker registry and
locks, but **not** for `rqueue-queue-config` — back it up first if you have configured queues
through the dashboard).

---

## Status

Rqueue is stable and production ready, processing millions of messages daily in production
environments.
**Some of the Rqueue Users**

<a href="https://airtel.africa" target="_blank">
<img alt="Airtel" src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/airtel-africa.png" width="160" align="middle"/>
</a>
&nbsp;&nbsp;
<a href="https://vonage.com" target="_blank">
  <img alt="Vonage" src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/vonage.png" width="160" align="middle"/>
</a>
&nbsp;&nbsp;
<a href="https://www.t-mobile.com" target="_blank">
  <img alt="Vonage" src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/t-mobile.svg" align="middle"/>
</a>
&nbsp;&nbsp;
<a href="https://line.me" target="_blank">
  <img alt="Line Chat" src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/line.png" width="60" height="60" align="middle"/>
</a>

<a href="https://opentext.com/" target="_blank">
  <img alt="Opentext" src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/opentext.png" width="200" height="100" align="middle"/>
</a>

**We would love to add your organization name here, if you're one of the Rqueue users, please raise
a
PR/[issue](https://github.com/sonus21/rqueue/issues/new?template=i-m-using-rqueue.md&title=Add+my+organisation+in+Rqueue+Users)
.**


---

<!---- Signing Key
~/.gradle/gradle.properties file

sonatypeUsername=xyz
sonatypePassword=xyz
signing.keyId=371EDCC6
signing.password=xyz
signing.secretKeyRingFile=/Users/sonu/.gnupg/secring.gpg


For signing generate gpg key using gpg tool using `gpg --gen-key`

signing.password is gpg key password
signing.keyId is last 8 character of gpg key, find using `gpg -K`
signing.secretKeyRingFile=/Users/sonu/.gnupg/secring.gpg generate this as `gpg --keyring secring.gpg --export-secret-keys > ~/.gnupg/secring.gpg`

--->

## Support

* Please report bug,question,feature(s)
  to [issue](https://github.com/sonus21/rqueue/issues/new/choose) tracker.

## Contribution

You are most welcome for any pull requests for any feature/bug/enhancement. You would need Java8 and
gradle to start with. In root `build.gradle` file comment out spring related versions, or set
environment variables for Spring versions. You can use [module, class and other diagrams](https://sourcespy.com/github/sonus21rqueue/) 
to familiarise yourself with the project.

**Please format your code with Palantir Java Format using `./gradlew formatJava`.**

## Links

* Documentation: [https://sonus21.github.io/rqueue](https://sonus21.github.io/rqueue)
* Releases: [https://github.com/sonus21/rqueue/releases](https://github.com/sonus21/rqueue/releases)
* Issue tracker: [https://github.com/sonus21/rqueue/issues](https://github.com/sonus21/rqueue/issues)
* Maven Central:
  * [https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring)
  * [https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring-boot-starter](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring-boot-starter)

## License

© [Sonu Kumar](mailto:sonunitw12@gmail.com) 2019-Instant.now

The Rqueue is released under version 2.0 of the Apache License.
