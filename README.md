<div>
   <img  align="left" src="https://raw.githubusercontent.com/sonus21/rqueue/master/rqueue-core/src/main/resources/public/rqueue/img/android-chrome-192x192.png" alt="Rqueue Logo" width="90">
   <h1 style="float:left">Rqueue: Redis Queue,Task Queue, Delayed Queue for Spring and Spring Boot</h1>
</div>

[![Build Status](https://circleci.com/gh/sonus21/rqueue/tree/master.svg?style=shield)](https://circleci.com/gh/sonus21/rqueue/tree/master)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.sonus21/rqueue-core)](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core)
[![Javadoc](https://javadoc.io/badge2/com.github.sonus21/rqueue-core/javadoc.svg)](https://javadoc.io/doc/com.github.sonus21/rqueue-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

**Rqueue** is an asynchronous task executor(worker) built for spring framework based on the spring
framework's messaging library backed by Redis. It can be used as message broker as well, where all
services code is in Spring.

## Features

* **Message Scheduling** : A message can be scheduled for any arbitrary period
* **Competing Consumers** multiple messages can be consumed in parallel by different workers.
* **Message delivery**: It's guaranteed that a message is consumed **at least once**.  (Message
  would be consumed by a worker more than once due to the failure in the underlying
  worker/restart-process etc, otherwise exactly one delivery)
* **Redis cluster** : Redis cluster can be used with driver.
* **Metrics** : In flight messages, waiting for consumption and delayed messages
* **Web Dashboard**:  a web dashboard to manage a queue and queue insights including latency
* **Automatic message serialization and deserialization**
* **Concurrency**: Concurrency of any queue can be configured
* **Queue Priority** :
  * Group level queue priority(weighted and strict)
  * Sub queue priority(weighted and strict)
* **Execution Backoff** : Exponential and fixed back off (default fixed back off)
* **Callbacks** : Callbacks for dead letter queue, discard etc
* **Events** 1. Bootstrap event 2. Task execution event.
* **Unique message** : Unique message processing for a queue based on the message id
* **Periodic message** : Process same message at certain interval
* **Redis connection**: A different redis setup can be used for Rqueue
* **Long execution job**: Long running jobs can check in periodically.

## Getting Started

### Dependency

#### Spring-boot

* Get the latest one
  from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter)
* Add dependency
  * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring-boot-starter:2.2.0-RELEASE'
    ```
  * Maven
    ```xml
     <dependency>
        <groupId>com.github.sonus21</groupId>
        <artifactId>rqueue-spring-boot-starter</artifactId>
        <version>2.2.0-RELEASE</version>
    </dependency>
    ```

#### Spring framework

* Get the latest one
  from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring)
* Add Dependency
  * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring:2.2.0-RELEASE'
    ```
  * Maven
    ```xml
     <dependency>
       <groupId>com.github.sonus21</groupId>
       <artifactId>rqueue-spring</artifactId>
       <version>2.2.0-RELEASE</version>
     </dependency>
    ```

2. Add annotation `EnableRqueue` on application config class
3. Provide a RedisConnectionFactory bean

### A Simple Spring Configuration

```java

@EnableRqueue
public class Application {

  @Bean
  public RedisConnectionFactory redisConnectionFactory() {
    // return a redis connection factory
  }
}
```

### Message publishing/Task submission

All messages need to be sent using `RqueueMessageEnqueuer` bean's `enqueueXXX`, `enqueueInXXX`
and `enqueueAtXXX` methods. It has handful number of `enqueue`, `enqueueIn`, `enqueueAt` methods, we
can use one of them based on the use case.

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

  // enqueue periodic job, email should be sent every 30 seconds
  public void sendPeriodicEmail(Email email) {
    rqueueMessageEnqueuer.enqueuePeriodic("email-queue", invoice, 30_000);
  }

}
```

### Worker/Consumer/Task executor/Listener

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
  public void onMessage(ChatIndexing chatIndexing, @Header(RqueueMessageHeaders.JOB) com.github.sonus21.rqueue.core.Job job) {
    log.info("ChatIndexing message: {}", chatIndexing);
    job.checkIn("Chat indexing...");
  }
}
```

## Queue Statistics

[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)

## Dashboard

Link: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

[![Execution Page](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)
<br/>
<br/>
[![Explore Queue](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)


<br/>
[![Jobs](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/jobs.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/jobs.png)

## In Production

**We would love to add your organization name here, if you're one of the Rqueue users, please raise
a
PR/[issue](https://github.com/sonus21/rqueue/issues/new?template=i-m-using-rqueue.md&title=Add+my+organisation+in+Rqueue+Users)
.**

<a href="https://tuneyou.com/"><img src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/tuneyou.png" width="150" align="middle"/></a>
&nbsp;&nbsp;
<a href="https://www.pokerstarssports.eu"><img src="https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/users/pokerstars.png" width="250" align="middle"/></a>
&nbsp;&nbsp;

## Support

Please report bug,question,feature(s)
to [issue](https://github.com/sonus21/rqueue/issues/new/choose) tracker.

You are most welcome for any pull requests for any feature/bug/enhancement. You would need Java8 and
gradle to start with. In root `build.gradle` file comment out spring related versions, or set
environment variables for Spring versions.

**Please format your code with Google Java formatter.**

```bash
//    springBootVersion = '2.1.0.RELEASE'
//    springVersion = '5.1.2.RELEASE'
//    springDataVersion = '2.1.2.RELEASE'
//    microMeterVersion = '1.1.0'
```

## Links

* Documentation:
  [https://github.com/sonus21/rqueue/wiki](https://github.com/sonus21/rqueue/wiki)
* Releases:
  [https://github.com/sonus21/rqueue/releases](https://github.com/sonus21/rqueue/releases)
* Issue tracker:
  [https://github.com/sonus21/rqueue/issues](https://github.com/sonus21/rqueue/issues)
* Maven Central:
  * [https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring)
  * [https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring-boot-starter](https://repo1.maven.org/maven2/com/github/sonus21/rqueue-spring-boot-starter)

## License

© [Sonu Kumar](mailto:sonunitw12@gmail.com) 2019-Instant.now

The Rqueue is released under version 2.0 of the Apache License.
