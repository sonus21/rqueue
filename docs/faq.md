---
layout: default
title: FAQ
nav_order: 8
description: Frequently Asked Questions about Rqueue
permalink: /faq
---

## How can I handle different message types with a single listener?

Sometimes you may want a single listener to handle various types of tasks. You can achieve this 
by using a common superclass for your message types. In your listener, you can then use 
`instanceof` to distinguish between them.

**1. Define Message Classes**

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

Here, `FancyMessage` is the superclass. You can enqueue both `SuperFancyMessage` and 
`OkOkFancyMessage` instances into the same queue.

**2. Enqueuing Process**

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

**3. Message Listener**

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

## How can I apply rate limiting?

Rate limiting can be implemented using middleware. Within your middleware, you can determine 
whether to allow or throttle messages based on your business criteria.

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

Registering the rate limiter middleware:

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

## Does Rqueue support generic classes as message types?

Currently, Rqueue does not support generic classes for message serialization/deserialization.

## Why are some messages consumed late?

Messages are usually consumed immediately. Delays can occur if:
- The volume of messages exceeds the available listener capacity.
- Task execution time is high, occupying worker threads for longer periods.

To improve performance, consider increasing the concurrency level for your queues. 
For scheduled messages, you can monitor the queue details on the dashboard and ensure 
that the "time left" does not stay negative for extended periods.

## How can I find a job's position in the queue?

Calculating an exact position is difficult due to parallel processing and the way Redis 
manages data. However, you can estimate the current queue length by checking the sum of 
pending and scheduled messages.

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

## How can I scale Rqueue to process millions of messages?

To achieve high throughput:
- Minimize the number of unique queues to reduce overhead.
- Distribute queues across multiple listener instances.
- Use priority groups to manage resource allocation.
- Increase batch sizes if processing threads are underutilized.
- Disable features like job history persistence and terminal state message durability 
  if they are not required.

## Rqueue is consuming too much Redis memory. How can I manage this?

By default, Rqueue stores job history in Redis. You can significantly reduce memory usage 
by disabling job persistence and setting terminal state durability to 0:

```properties
rqueue.job.enabled=false
rqueue.message.durability.in-terminal-state=0
```

## How can I consume messages from the dead letter queue?

You can define a separate listener for the dead letter queue by specifying its name:

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
