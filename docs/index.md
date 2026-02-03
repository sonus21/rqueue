---
layout: default
title: Home
nav_order: 1
description: Rqueue Redis Based Async Message Processor
permalink: /
---

# Rqueue | Redis Queue For Spring Framework

{: .fs-4 }

Rqueue is an asynchronous task executor (worker) built for the Spring Framework based on Spring's
messaging library, backed by Redis. It can serve as a message broker where all service code is
within Spring/Spring Boot applications. Rqueue fully supports both Spring and Spring Boot
frameworks.

{: .fs-6 .fw-300 }

[Get started now](#getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][Rqueue repo]{: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Features

* **Instant Delivery**: Immediate execution of messages.
* **Message Scheduling**: Schedule messages for any arbitrary period.
* **Unique Message Processing**: Ensures unique processing of messages based on message ID.
* **Periodic Message Processing**: Process the same message at defined intervals.
* **Priority Tasks**: Support for task prioritization (e.g., high, low, medium).
* **Message Delivery Guarantee**: Ensures each message is consumed at least once, and may be retried
  in case of worker failures or restarts.
* **Automatic Serialization and Deserialization of Messages**.
* **Message Multicasting**: Call multiple message listeners for each message.
* **Batch Message Polling**: Fetch multiple messages from Redis in one operation.
* **Metrics**: Provides insights into in-flight messages, waiting messages, and delayed messages.
* **Competing Consumers**: Multiple workers can consume messages in parallel.
* **Concurrency Control**: Configurable concurrency for message listeners.
* **Queue Priority**: Supports both group-level and sub-queue level priorities.
* **Long Execution Jobs**: Check-in mechanism for long-running jobs.
* **Execution Backoff**: Supports exponential and fixed backoff strategies.
* **Do not retry**: Supports do not retry strategy.
* **Middleware**: Allows integration of middleware to intercept messages before processing.
* **Callbacks**: Supports callbacks for handling dead letter queues and discarding messages.
* **Events**: Provides bootstrap and task execution events.
* **Redis Connection Options**: Supports different Redis configurations including Redis Cluster and
  Redis Sentinel.
* **Reactive Programming**: Integrates with reactive Redis and Spring WebFlux.
* **Web Dashboard**: Provides a web-based dashboard for managing queues and monitoring queue
  metrics.

### Requirements

* Spring 5+, 6+, 7+
* Spring Boot 2+, 3+, 4+
* Spring Reactive
* Lettuce client for Redis cluster
* Read master preference for Redis cluster

## Getting Started

{: .warning }
All queue names are dynamic. Manually creating queues using `registerQueue` method can lead to
inconsistencies. Queues should **only** be created when using Rqueue as a producer.

### Sample Apps

{: .highlight }
The Rqueue GitHub repository includes several sample apps for local testing and demonstration:

* [Rqueue Spring Boot Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example)
* [Rqueue Spring Boot Reactive Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)
* [Rqueue Spring Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example)

## Project Integration

{: .warning }
When configuring the Redis connection factory, ensure to set `readFrom` to `MASTER_PREFERRED` for
Redis cluster compatibility, otherwise the application may fail to start.

### Spring Boot

{: .warning }
Use Rqueue Spring Boot Starter 4.x for Spring Boot 4.x, Rqueue Spring Boot Starter 3.x for 
Spring Boot 3.x, and Rqueue Spring Boot Starter 2.x for Spring Boot 2.x.

Get the latest version of Rqueue Spring Boot Starter from [Maven Central][Boot Maven Central]. Add
the dependency to your project:

#### Spring Boot 2.x Setup

* Gradle
  ```groovy
  implementation 'com.github.sonus21:rqueue-spring-boot-starter:2.13.1-RELEASE'
  ```

* Maven
  ```xml
  <dependency>
      <groupId>com.github.sonus21</groupId>
      <artifactId>rqueue-spring-boot-starter</artifactId>
      <version>2.13.1-RELEASE</version>
  </dependency>
  ```

#### Spring Boot 3.x Setup

* Gradle
  ```groovy
  implementation 'com.github.sonus21:rqueue-spring-boot-starter:3.1.0-RELEASE'
  ```

* Maven
  ```xml
  <dependency>
      <groupId>com.github.sonus21</groupId>
      <artifactId>rqueue-spring-boot-starter</artifactId>
      <version>3.1.0-RELEASE</version>
  </dependency>
  ```

---

### Spring Framework

{: .warning }
Use Rqueue Spring 4.x for Spring Framework 7.x, Rqueue Spring 3.x for Spring Framework 6.x, 
and Rqueue Spring 2.x for Spring Framework 5.x.

Get the latest version of Rqueue Spring from [Maven Central][Maven Central]. Add the dependency to
your project:

#### Spring Framework 5.x Setup

* Gradle
  ```groovy
  implementation 'com.github.sonus21:rqueue-spring:2.13.1-RELEASE'
  ```

* Maven
  ```xml
  <dependency>
      <groupId>com.github.sonus21</groupId>
      <artifactId>rqueue-spring</artifactId>
      <version>2.13.1-RELEASE</version>
  </dependency>
  ```

#### Spring Framework 6.x Setup

* Gradle
  ```groovy
  implementation 'com.github.sonus21:rqueue-spring:3.1.0-RELEASE'
  ```

* Maven
  ```xml
  <dependency>
      <groupId>com.github.sonus21</groupId>
      <artifactId>rqueue-spring</artifactId>
      <version>3.1.0-RELEASE</version>
  </dependency>
  ```

{: .note }
For **Spring Framework**, ensure to:

* Add `EnableRqueue` annotation on the main method.
* Provide a `RedisConnectionFactory` bean.

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

{: .highlight }

Once Rqueue is configured in Spring or Spring Boot as described above, you can start using Rqueue
methods and annotations. The usage remains consistent whether using Spring Boot or the Spring
framework.

### Message Publishing / Task Submission

All messages should be sent using the `RqueueMessageEnqueuer` bean's `enqueueXXX`, `enqueueInXXX`,
and `enqueueAtXXX` methods. Use the appropriate method based on your use case:

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

### Worker / Consumer / Task Executor / Listener

Annotate any public method of a Spring bean with `RqueueListener` to make it a message consumer:

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
