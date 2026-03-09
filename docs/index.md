---
layout: default
title: Home
nav_order: 1
description: Rqueue Redis Based Async Message Processor
permalink: /
---

# Rqueue | Redis Queue For Spring Framework

{: .fs-4 }

Rqueue is an asynchronous task executor (worker) built for the Spring Framework. It leverages Spring's
messaging library and is backed by Redis. Rqueue can serve as a message broker where all service code
remains within Spring or Spring Boot applications. It fully supports both the Spring and Spring Boot
frameworks.

{: .fs-6 .fw-300 }

[Get started now](#getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][Rqueue repo]{: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Features

* **Instant Delivery**: Immediate execution of messages.
* **Message Scheduling**: Schedule messages for any future time.
* **Unique Message Processing**: Ensures unique message processing based on a message ID.
* **Periodic Message Processing**: Process messages at defined intervals.
* **Priority Tasks**: Support for task prioritization (e.g., high, medium, low).
* **Guaranteed Delivery**: Ensures each message is consumed at least once, with automatic retries
  in case of worker failures or restarts.
* **Automatic Serialization**: Seamless serialization and deserialization of message payloads.
* **Message Multicasting**: Support for multiple message listeners for a single message.
* **Batch Polling**: Efficiently fetch multiple messages from Redis in a single operation.
* **Metrics**: Real-time insights into in-flight, waiting, and delayed messages.
* **Competing Consumers**: Multiple workers can consume messages from the same queue in parallel.
* **Concurrency Control**: Configurable concurrency levels for message listeners.
* **Queue Priority**: Supports both group-level and sub-queue level priorities.
* **Long-Running Jobs**: Check-in mechanism to support jobs with extended execution times.
* **Execution Backoff**: Flexible exponential and fixed backoff strategies for retries.
* **No-Retry Strategy**: Support for explicitly disabling retries for specific tasks.
* **Middleware**: Intercept and process messages before they reach the listener.
* **Callbacks**: Custom handlers for messages moved to dead letter queues or discarded.
* **Events**: Comprehensive bootstrap and task execution lifecycle events.
* **Flexible Redis Options**: Support for standalone, Sentinel, and Cluster Redis configurations.
* **Reactive Support**: Full integration with Reactive Redis and Spring WebFlux.
* **Web Dashboard**: Integrated web-based interface for queue management and monitoring.

### Requirements

* Spring 5+, 6+, 7+
* Spring Boot 2+, 3+, 4+
* Spring Reactive
* Lettuce client for Redis cluster
* Read master preference for Redis cluster

## Getting Started

{: .warning }
Queue names are dynamic. Manually creating queues with the `registerQueue` method may cause
inconsistencies. Queues should **only** be created when using Rqueue as a producer.

### Sample Applications

{: .highlight }
The Rqueue GitHub repository includes several sample applications for local testing and demonstration:

* [Rqueue Spring Boot Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example)
* [Rqueue Spring Boot Reactive Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)
* [Rqueue Spring Example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example)

### Project Integration

{: .warning }
When configuring the Redis connection factory, set `readFrom` to `MASTER_PREFERRED` for
Redis Cluster compatibility. Failure to do so may prevent the application from starting.

### Spring Boot

{: .warning }
Use Rqueue Spring Boot Starter 4.x for Spring Boot 4.x, 3.x for Spring Boot 3.x, 
and 2.x for Spring Boot 2.x.

Download the latest version from [Maven Central][Boot Maven Central] and add the 
dependency to your project:

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
Use Rqueue Spring 4.x for Spring Framework 7.x, 3.x for Spring Framework 6.x, 
and 2.x for Spring Framework 5.x.

Download the latest version from [Maven Central][Maven Central] and add the 
dependency to your project:

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
