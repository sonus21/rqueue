---
layout: default
title: Home
nav_order: 1
description: Rqueue Job Queue and Scheduler for Spring — Redis and NATS JetStream backends
permalink: /
---

# Rqueue | Job Queue and Scheduler For Spring Framework (Redis &amp; NATS)

{: .fs-4 }

Rqueue is a job queue and producer-consumer system for Spring and Spring Boot with pluggable
broker backends — **Redis** (default) and **NATS JetStream**. It supports producers and consumers
for background jobs, scheduled tasks, and event-driven workflows, similar to Sidekiq or Celery,
while staying fully integrated with the Spring programming model through annotation-driven APIs
and minimal setup.

{: .fs-6 .fw-300 }

[Get started now](#getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][Rqueue repo]{: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Features

* **Job execution**
  * Run background jobs asynchronously
  * Schedule jobs for any future time
  * Run periodic jobs at fixed intervals
  * Guarantee at-least-once delivery
  * Retry failed jobs automatically with fixed or exponential backoff
  * Disable retries for selected workloads when needed

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
  * Use the built-in web dashboard for queue visibility, worker activity, and message operations
  * Override message ID generation with a custom `RqueueMessageIdGenerator` bean

* **Backend and platform support**
  * Switch backends with a single property (`rqueue.backend=redis|nats`)
  * Support Redis standalone, Sentinel, and Cluster setups
  * Support reactive Redis and Spring WebFlux
  * Keep Redis configuration flexible for different deployment models
  * Use NATS JetStream as a drop-in Redis replacement (add `rqueue-nats` and set `rqueue.backend=nats`)

### Requirements

* Spring 6+, 7+
* Spring Boot 3+, 4+
* Java 21+
* Spring Reactive
* **Redis backend (default):** Lettuce client; read-master preference for Redis Cluster
* **NATS backend:** NATS Server 2.2+ with JetStream enabled (`nats-server -js`); `rqueue-nats` on the classpath

## Getting Started

{: .warning }
Queue names are dynamic. Manually creating queues with the `registerQueue` method may cause
inconsistencies. Queues should **only** be created when using Rqueue as a producer.

### Sample Applications

{: .highlight }
The Rqueue GitHub repository includes several sample applications for local testing and demonstration:

* [Rqueue Spring Boot Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example) — Redis backend
* [Rqueue Spring Boot NATS Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-nats-example) — NATS JetStream backend
* [Rqueue Spring Boot Reactive Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)
* [Rqueue Spring Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example)

### Project Integration

{: .warning }
When configuring the Redis connection factory, set `readFrom` to `MASTER_PREFERRED` for
Redis Cluster compatibility. Failure to do so may prevent the application from starting.

### Spring Boot

{: .warning }
Use Rqueue Spring Boot Starter 4.x for Spring Boot 4.x, and 3.x for Spring Boot 3.x.

Download the latest version from [Maven Central][Boot Maven Central] and add the 
dependency to your project:

#### Spring Boot 4.x Setup

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

---

### Spring Framework

{: .warning }
Use Rqueue Spring 4.x for Spring Framework 7.x, and 3.x for Spring Framework 6.x.

Download the latest version from [Maven Central][Maven Central] and add the 
dependency to your project:

#### Spring Framework 7.x Setup

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

{: .note }
When using the **Spring Framework**, ensure you:

* Add the `@EnableRqueue` annotation to your configuration class.
* Define a `RedisConnectionFactory` bean.

##### Example Spring Application Configuration

```java

@EnableRqueue
public class Application {
  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    // return a Redis connection factory
  }
}
```

---

### NATS JetStream Backend

To use NATS JetStream instead of Redis, add `rqueue-nats` alongside the starter, set
`rqueue.backend=nats`, and point `rqueue.nats.connection.url` at a JetStream-enabled server.
No `RedisConnectionFactory` bean is required. All listener, producer, and middleware APIs work
without any code changes.

```properties
rqueue.backend=nats
rqueue.nats.connection.url=nats://localhost:4222
```

{: .note }
See [NATS Configuration](configuration/nats-configuration) for the full reference — connection
options, stream defaults, consumer tuning, naming, auto-provisioning, locked-down account setup,
and feature limitations.

---

{: .highlight }

Once Rqueue is configured, you can use its methods and annotations consistently
across both Spring and Spring Boot environments.

### Message Publishing / Task Submission

Submit tasks using the `RqueueMessageEnqueuer` bean. Use the `enqueueXXX`, `enqueueInXXX`,
or `enqueueAtXXX` methods based on your requirements:

```java
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;

@Component
public class MessageService {
  @Autowired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void doSomething() {
    rqueueMessageEnqueuer.enqueue("simple-queue", "Rqueue is configured");
  }

  public void createJob(Job job) {
    rqueueMessageEnqueuer.enqueue("job-queue", job);
  }

  public void sendNotification(Notification notification) {
    rqueueMessageEnqueuer.enqueueIn("notification-queue", notification, 30 * 1000L);
  }

  public void createInvoice(Invoice invoice, Instant instant) {
    rqueueMessageEnqueuer.enqueueAt("invoice-queue", invoice, instant);
  }

  public void sendSms(Sms sms, SmsPriority priority) {
    rqueueMessageEnqueuer.enqueueWithPriority("sms-queue", priority.value(), sms);
  }

  public void sendPeriodicEmail(Email email) {
    rqueueMessageEnqueuer.enqueuePeriodic("email-queue", email, 30_000);
  }
}
```

### Workers and Task Listeners

Annotate any public method of a Spring bean with `@RqueueListener` to create a message consumer:

```java
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;

@Component
@Slf4j
public class MessageListener {

  @RqueueListener(value = "simple-queue")
  public void handleSimpleMessage(String message) {
    log.info("Received message from simple-queue: {}", message);
  }

  @RqueueListener(value = "job-queue", numRetries = "3", deadLetterQueue = "failed-job-queue", concurrency = "5-10")
  public void handleJob(Job job) {
    log.info("Received job: {}", job);
  }

  @RqueueListener(value = "push-notification-queue", numRetries = "3", deadLetterQueue = "failed-notification-queue")
  public void handleNotification(Notification notification) {
    log.info("Received notification: {}", notification);
  }

  @RqueueListener(value = "sms", priority = "critical=10,high=8,medium=4,low=1")
  public void handleSms(Sms sms) {
    log.info("Received SMS: {}", sms);
  }

  @RqueueListener(value = "chat-indexing", priority = "20", priorityGroup = "chat")
  public void handleChatIndexing(ChatIndexing chatIndexing) {
    log.info("Received chat indexing message: {}", chatIndexing);
  }

  @RqueueListener(value = "chat-indexing-daily", priority = "10", priorityGroup = "chat")
  public void handleDailyChatIndexing(ChatIndexing chatIndexing) {


    log.info("Received daily chat indexing message: {}", chatIndexing);
  }
}
```

#### Notes:

* **Retry Mechanism**: Configure retry behavior using `numRetries` and `deadLetterQueue` attributes.
* **Concurrency**: Adjust concurrency using the `concurrency` attribute.
* **Priority**: Set message priority using the `priority` attribute.

---

### Advanced Configuration

#### Rqueue Configuration

{: .note .fw-300 }
For advanced configurations such as message serialization, queue properties, message listener
details, and more, refer to the [official documentation][Rqueue Docs].

### Support

{: .fs-5 }

For any issues, questions, or feature requests, please create an issue on
the [GitHub repository][Rqueue repo] or contact the maintainers directly.

---

### License

{: .fs-5 }

Rqueue is licensed under the Apache License 2.0. See the [LICENSE](https://github.com/sonus21/rqueue/blob/master/LICENSE) file for more details.

[Rqueue Docs]: https://github.com/sonus21/rqueue/wiki
[Boot Maven Central]: https://search.maven.org/artifact/com.github.sonus21/rqueue-spring-boot-starter
[Maven Central]: https://search.maven.org/artifact/com.github.sonus21/rqueue-spring
[Rqueue repo]: https://github.com/sonus21/rqueue
