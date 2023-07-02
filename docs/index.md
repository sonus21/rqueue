---
layout: default
title: Home
nav_order: 1
description: Rqueue Redis Based Async Message Processor
permalink: /
---

# Rqueue | Redis Queue For Spring Framework

{: .fs-4 }

Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring
framework's messaging library backed by Redis. It can be used as message broker as well, where all
services code is in Spring/Spring Boot. It supports Spring and Spring Boot framework.

{: .fs-6 .fw-300 }

[Get started now](#getting-started){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View it on GitHub][Rqueue repo]{: .btn .fs-5 .mb-4 .mb-md-0 }

---

## Features

* **Instant delivery** : Instant execute this message
* **Message scheduling** : A message can be scheduled for any arbitrary period
* **Unique message** : Unique message processing for a queue based on the message id
* **Periodic message** : Process same message at certain interval
* **Priority tasks** : task having some special priority like high, low, medium
* **Message delivery** : It's guaranteed that a message is consumed **at least once**.  (Message
  would be consumed by a worker more than once due to the failure in the underlying
  worker/restart-process etc, otherwise exactly one delivery)
* **Message retry** : Message would be retried automatically on application crash/failure/restart
  etc.
* **Automatic message serialization and deserialization**
* **Message Multicasting** : Call multiple message listeners on every message
* **Batch Message Polling** : Fetch multiple messages from Redis at once
* **Metrics** : In flight messages, waiting for consumption and delayed messages
* **Competing Consumers** : multiple messages can be consumed in parallel by different
  workers/listeners.
* **Concurrency** : Concurrency of any listener can be configured
* **Queue Priority** :
  * Group level queue priority(weighted and strict)
  * Sub queue priority(weighted and strict)
* **Long execution job** : Long running jobs can check in periodically.
* **Execution Backoff** : Exponential and fixed back off (default fixed back off)
* **Middleware** :  Add one or more middleware, middlewares are called before listener method.
* **Callbacks** : Callbacks for dead letter queue, discard etc
* **Events** : 1. Bootstrap event 2. Task execution event.
* **Redis connection** : A different redis setup can be used for Rqueue
* **Redis cluster** : Redis cluster can be used with Lettuce client.
* **Redis Sentinel** : Redis sentinel can be used with Rqueue.
* **Reactive Programming** : Supports reactive Redis and spring webflux
* **Web Dashboard** :  Web dashboard to manage a queue and queue insights including latency

### Requirements

* Spring 5+, 6+
* Spring boot 2+, 3+
* Spring Reactive
* Lettuce client for Redis cluster
* Read master preference for Redis cluster

## Getting started

{: .warning }
All queue names are dynamic, we do not have to create any queue manually or programmatically using
`registerQueue` method. Creating queue manually could lead to inconsistencies, queue should be
**only** created when we're using Rqueue as producer.

### Sample Apps
{: .highlight }
The Rqueue Github repository has multiple sample test apps, try to run them in local all of these apps
provides simple APIs for demo. In cloned repo we can run one of these apps.

* [Rqueue Sprint Boot](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example)
* [Rqueue Sprint Boot Reactive](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)
* [Rqueue Sprint](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example)

## Project Integration

{: .warning }

While creating redis connection factory you must use readFrom `MASTER_PREFERRED` otherwise application won't start.


### Spring Boot

{: .warning }
For Spring Boot 3.x you need to use Rqueue Spring Boot Starter 3.x and for Spring Boot 2.x you need
to use 2.x.

Get the latest version of Rqueue spring boot starter from [Maven central][Boot Maven Central], add
the latest version in your dependency manager.

#### Spring Boot 2.x Setup

Add Rqueue Spring Boot Starter 2.13.1 and refresh your project. Once you've added the dependency,
you can start sending and consuming messages.

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

Add Rqueue Spring Boot Starter 3.1.0 and refresh your project. Once you've added the dependency,
you can start sending and consuming messages.

* Add dependency
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
For Spring Framework 6.x you need to use Rqueue Spring 3.x and for Spring Framework 5.x you need
to use 2.x

Get the latest version of Rqueue Spring from [Maven Central][Maven Central], add the latest version
in your dependency manager.

#### Spring Framework 5.x

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

#### Spring Framework 6.x

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
For **Spring framework**, just adding dependency won't work

* Add `EnableRqueue` annotation on main method. If you do not add this annotation than application
bootstrap will fail due to bean not found and message consumer will not work.
* Provide RedisConnectionFactory bean.

##### A Simple **Spring Application** Configuration

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

{: .highlight }

Once Rqueue is configured in Spring/Boot as mentioned above, we're ready to use the Rqueue method
and annotation. Either you're using Spring Boot or only Spring framework the usage is same. 

### Message publishing/Task submission

All messages need to be sent using `RqueueMessageEnqueuer` bean's `enqueueXXX`, `enqueueInXXX`
and `enqueueAtXXX` methods. It has handful number of `enqueue`, `enqueueIn`, `enqueueAt` methods, we
can use one of them based on the use case.

```java
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;

@Component
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

Annotate any public method of spring bean using `RqueueListener`, all annotated method will work as
consumer.

```java
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;

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

## Rqueue Users

Rqueue is stable and production ready, it's processing millions of on messages daily in production environment.

**We would love to add your organization name here, if you're one of the Rqueue users, please lets know either via [GitHub][I am Using Rqueue] or [Email](mailto:sonunitw12@gmail.com).**

<div markdown="1" style="background: white">
<div markdown="1" style="padding: 10px">

[![Airtel](static/users/airtel-africa.png){: width="160" height="60" alt="Airtel Africa" }](https://airtel.africa){: target="_blank" style="margin:10px"}
[![Line](static/users/line.png){: width="70" height="60" alt="Line Chat" }](https://line.me){:target="_blank" style="margin:10px"}
[![Aviva](static/users/aviva.jpeg){: width="70" height="60" alt="Aviva" }](https://www.aviva.com/){:target="_blank" style="margin:10px"}
[![Diamler Truck](static/users/mercedes.png){: width="80" height="60" alt="Daimler Truck (Mercedes)" }](https://www.daimlertruck.com/en){:target="_blank" style="margin:10px"}
[![T-Mobile](static/users/t-mobile.svg){: width="50" height="50" alt="T Mobile" }](https://www.t-mobile.com){:target="_blank" style="margin:10px"}
[![Bit bot](static/users/bitbot.png){: width="80" height="60" alt="BitBot" }](https://bitbot.plus){:target="_blank" style="margin:10px"}
[![Vonage](static/users/vonage.png){: width="250" height="60" alt="Vonage" }](http://vonage.com){:target="_blank" style="margin:10px"}
[![Poker Stars](static/users/pokerstars.png){: width="210" height="60" alt="PokerStars" }](https://www.pokerstarssports.eu){:target="_blank" style="margin:10px"}
[![Tune You](static/users/tuneyou.png){: width="140" height="60" alt="TuneYou" }](https://tuneyou.com){:target="_blank" style="margin:10px"}
[![CHAOTI INFO TECH(SHENZHEN)](static/users/chaoti-info.png){: width="140" height="60" alt="CHAOTI INFO TECH(SHENZHEN)" }](https://www.chaotiinfo.cn){:target="_blank" style="margin:10px"}

</div>
</div>

## About the project

Rqueue is &copy; 2019-{{ "now" | date: "%Y" }} by [Sonu Kumar](http://github.com/sonus21).

### License

Rqueue is distributed by an [Apache license](https://github.com/sonus21/rqueue/tree/master/LICENSE).

### Contributing

When contributing to this repository, please first discuss the change you wish to make via issue,
email, or any other method with the owners of this repository before making a change. Read more
about becoming a contributor in [our GitHub repo](https://github.com/sonus21/rqueue#contributing).

#### Thank you to the contributors of Rqueue!

<ul class="list-style-none">
{% for contributor in site.github.contributors %}
  <li class="d-inline-block mr-1">
     <a href="{{ contributor.html_url }}"><img src="{{ contributor.avatar_url }}" width="32" height="32" alt="{{ contributor.login }}"></a>
  </li>
{% endfor %}
</ul>

### Code of Conduct

Rqueue is committed to fostering a welcoming community.

[View our Code of Conduct](https://github.com/sonus21/rqueue/tree/master/CODE_OF_CONDUCT.md)
on our GitHub repository.

[Rqueue repo]: https://github.com/sonus21/rqueue

[Boot Maven Central]: https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter

[Maven Central]: https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring

[I am Using Rqueue]: https://github.com/sonus21/rqueue/issues/new?template=i-m-using-rqueue.md&title=Add+my+organisation+in+Rqueue+Users
