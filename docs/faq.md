---
layout: default
title: FAQ
nav_order: 8
description: Frequently Asked Questions about Rqueue
permalink: /faq
---

## How can we handle different types of messages with a single listener?

Sometimes, you may need a single message listener to handle various asynchronous tasks. To achieve
this, define a superclass and subclasses for different message types. In your listener, enqueue
instances of these subclasses using the superclass.

**Define Message Classes**

```java
class FancyMessage {

}

class SuperFancyMessage extends FancyMessage {
  private boolean fancy;
}

class OkOkFancyMessage extends FancyMessage {
  private boolean okOk;
}
```

Here, `FancyMessage` acts as a superclass for `SuperFancyMessage` and `OkOkFancyMessage`. You can
enqueue both `SuperFancyMessage` and `OkOkFancyMessage` instances in the same queue.

**Enqueuing Process**

```java

@Component
class MyMessageEnqueuer {

  @Autowired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void enqueueFancyMessage(FancyMessage fancyMessage) {
    rqueueMessageEnqueuer.enqueue("fancy-queue", fancyMessage);
    // handle errors
  }
}
```

**Message Listener**

```java

@Component
class FancyMessageListener {

  private void handleSuperFancyMessage(SuperFancyMessage superFancyMessage) {
    // handle SuperFancyMessage
  }

  private void handleOkOkFancyMessage(OkOkFancyMessage okOkFancyMessage) {
    // handle OkOkFancyMessage
  }

  @RqueueListener("fancy-queue")
  public void handleMessage(FancyMessage fancyMessage) {
    if (fancyMessage instanceof SuperFancyMessage) {
      handleSuperFancyMessage((SuperFancyMessage) fancyMessage);
    } else if (fancyMessage instanceof OkOkFancyMessage) {
      handleOkOkFancyMessage((OkOkFancyMessage) fancyMessage);
    } else {
      // handle other cases
    }
  }
}
```

## How do we apply rate limiting?

Rate limiting can be implemented using middleware. In the middleware, you can customize whether to
allow or reject messages based on specific criteria.

```java
class MyRateLimiter implements RateLimiterMiddleware {

  final RateLimiter rateLimiter;

  MyRateLimiter(RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  @Override
  public boolean isThrottled(Job job) {
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    if (rqueueMessage.getQueueName().equals("rate-limited-queue")) {
      return rateLimiter.tryAcquire();
    }
    Object message = job.getMessage();
    if (message instanceof RateLimitedMessage) {
      return rateLimiter.tryAcquire();
    }
    return true;
  }
}
```

Using rate limiting middleware:

```java

@Configuration
public class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    RateLimiterMiddleware limiterMiddleware = new MyRateLimiter();
    factory.useMiddleware(limiterMiddleware);
    // add other middlewares here
    return factory;
  }
}
```

## Does Rqueue support generic classes?

No, Rqueue does not support generic classes.

## Why are messages consumed late by a listener?

Messages should typically be consumed promptly by listeners. Delays may occur if there are more
messages in the queue than available listeners, or due to high processing times. To minimize delays,
consider increasing the concurrency of your queue.

For scheduled messages, you can monitor the queue details page and ensure the time left is always
greater than `-1000 milliseconds`.

## How can we retrieve a job's position in the queue?

Determining a job's exact position in the queue can be challenging due to parallel processing and
Redis data structures. You can estimate the queue size by checking pending and scheduled message
counts.

```java
class TestJobPosition {

  @Autowired
  private RqueueQueueMetrics rqueueQueueMetrics;

  public long getTestQueueSize() {
    return rqueueQueueMetrics.getPendingMessageCount("test-queue") +
        rqueueQueueMetrics.getScheduledMessageCount("test-queue");
  }
}
```

## How can we scale Rqueue to process millions of messages per hour?

To scale Rqueue for high throughput:

- Use a minimal number of queues and utilize them efficiently.
- Distribute queues across multiple machines.
- Group queues using priority groups.
- Increase batch sizes if threads are underutilized.
- Disable unnecessary features like job persistence and immediate message deletion.

For optimal performance, group queues based on message rates, business verticals, and message types.

## Rqueue is using a significant amount of Redis memory. How can this be managed?

Rqueue stores completed jobs and messages in Redis by default. To reduce Redis memory usage, disable
job persistence and immediate message deletion:

```properties
rqueue.job.enabled=false
rqueue.message.durability.in-terminal-state=0
```

## How can we consume events from the dead letter queue?

To consume messages from the dead letter queue:

```java

@Component
class ReservationRequestMessageConsumer {

  @RqueueListener(
      value = "reservation.request.queue",
      deadLetterQueue = "reservation.request.dead.letter.queue.name",
      deadLetterQueueListenerEnabled = "true",
      numRetries = "3")
  public void onMessageReservationRequest(ReservationRequest request) throws Exception {
    // Handle messages from main queue
  }

  @RqueueListener(value = "reservation.request.dead.letter.queue", numRetries = "1")
  public void onMessageReservationRequestDeadLetterQueue(
      ReservationRequest request,
      @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage) throws Exception {
    // Handle messages from dead letter queue
  }
}
```
