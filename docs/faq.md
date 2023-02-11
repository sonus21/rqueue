---
layout: default
title: FAQ
nav_order: 8
description: Rqueue FAQ
permalink: /faq
---

## How can we handle different type of messages by a single listener?

There are times when you want to use the same message listener to execute different types of
asynchronous tasks. In such cases you can create a superclass and multiple subclass for this class,
in the listener you should use the superclass and subclass objects in enqueueing.

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

`FancyMessage` is super class for `OkOkFancy` and `SuperFancyMessage`, now we can
enqueue  `OkOkFancyMessage` and `SuperFancyMessage` in the same queue.

**Enqueuing Process**

```java

@Component
class MyMessageEnqueuer {

  @Autowired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void enqueueFancyMessage(FancyMessage fancyMessage) {
    rqueueMessageEnqueuer.enqueue("fancy-queue", fancyMessage);
    // handle error
  }
}
```

**Message Listener**

```java

@Component
class FancyMessageListener {

  private void handleSuperFancyMessage(SuperFancyMessage superFancyMessage) {
    //TODO
  }

  private void handleOkOkFancyMessage(OkOkFancyMessage okOkFancyMessage) {
    //TODO
  }

  @RqueueListener("fancy-queue")
  public void handleMessage(FancyMessage fancyMessage) {
    if (fancyMessage instanceof SuperFancyMessage) {
      handleSuperFancyMessage((SuperFancyMessage) fancyMessage);
    } else if (fancyMessage instanceof OkOkFancyMessage) {
      handleOkOkFancyMessage((OkOkFancy) fancyMessage);
    } else {
      //TODO
    }
  }
}
```

## How do we apply rate limiting?

Rate limiting can be only implemented using Middleware, in the middleware you can do whatever you
want, so in this case we can check whether the given message should be allowed or rejected.

```java
 class MyRateLimiter implements RateLimiterMiddleware {

  // Guava rate limiter, you can use any other rate limiter
  final RateLimiter rateLimiter;

  TestRateLimiter(RateLimiter rateLimiter) {
    this.rateLimiter = rateLimiter;
  }

  @Override
  public boolean isThrottled(Job job) {
    // here you can check queue and any other details for rate limiting
    RqueueMessage rqueueMessage = job.getRqueueMessage();
    // check for rate-limited-queue
    if (rqueueMessage.getQueueName().equals("rate-limited-queue")) {
      return rateLimiter.tryAcquire();
    }
    // checking message object type, rate limiting is enabled for RateLimitedMessage
    Object message = job.getMessage();
    if (message instanceof RateLimitedMessage) {
      return rateLimiter.tryAcquire();
    }
    return true;
  }
}
```

Using rate limiting middleware

```java
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

## Does Rqueue support generic class?

Rqueue does not support generic class.

## Why message are consumer late by a listener?

Generally all scheduled/non-scheduled message should be consumed by a listener within 5 seconds (
polling interval/scheduled job polling interval). In some occasions there could be many messages in
the queue, and you don't have enough listeners to process those messages than delay could be large,
if you're observing high delay then you can increase concurrency of that queue. For scheduled
message you can also browse queue details web page, the time left should be always
greater > `-1000 (
1 second)`. Inspect Scheduled Queue Time left

* Head to http://localhost:8080/rqueue
* Click on queues
* Click on the required queue from list
* Click on Scheduled link, this will open a pop-up to display scheduled messages

If observe the value here is too high or so, in such cases you can set the value
of `rqueue.scheduler.delayed.message.thread.pool.size` to some higher value, by default it's
configured to use `3` threads.

## How to retrieve a job position in the queue?

A job can be either of three status

* Waiting for processing
* Waiting in scheduled state as scheduled time has not reached
* Being processed

Finding a job position is difficult since in some cases jobs are in Redis `LIST` and other case it's
in `ZSET`. We would have to do a sequential search to identify the job position, and some
calculation to arrive at the index, still it can be inaccurate since jobs are getting consumed in
parallel. We can do some approximation just like check size of pending messages queue etc.

```java
class TestJobPosition {

  @Autowired
  private RqueueQueueMetrics rqueueQueueMetrics;

  public long getTestQueueSize() {
    // not considering processing queue as they are currently being processed
    return rqueueQueueMetrics.getPendingMessageCount("test-queue") + rqueueQueueMetrics
        .getScheduledMessageCount("test-queue");
  }

}
```

## How can we scale Rqueue to process millions of message in an hour?

* Use minimum number of queues, utilise same **low throughput** queue for multiple purposes.
* Distribute queues among multiple machines
* Group queue using priority group
* Increase batch size if you find all threads are not utilized, batch size is configurable for each
  listener.
* Disable unwanted features like
    * Rqueue job feature `rqueue.job.enabled=false`
    * Delete message immediately `rqueue.message.durability.in-terminal-state=0`

For queues distribution put some set of queues in one cluster and another set in another cluster.
Each cluster should process different set of queues.

For example if you've 100 queues and 10 machines then you can create 4 clusters

**Sample cluster setups**

* Cluster1: [M1, M2]
* Cluster2: [M3, M4]
* Cluster3: [M5, M6, M7]
* Cluster4: [M8, M9, M10]

**Sample queue distribution**

* Cluster1 machines (M1, M2) should process only 20 queues Q1, Q2, Q3, ..., Q20
* Cluster2 machines (M3, M4) should process only 20 queues Q21, Q22, Q23, ..., Q40
* Cluster3 machines (M5, M6, M7) should process only 28 queues Q41, Q42, Q43, ..., Q68
* Cluster4 machines (M8, M9, M10) should process only 32 queues Q69, Q70, Q71,..., Q100

Multiple factors can be considered to group queues

* Listener/producer message rate
* Business vertical
* Message Criticality
* Message Type

General rule of thumb is, you should not run a single Rqueue instance with more than 40** queues

** **40** is not a Rqueue limitation, there would be higher number of thread context switching since
there are some long-running jobs in Rqueue that polls Redis for new messages. If you're using
priority group than you can have higher number of queues in a single machine as number of
long-running jobs is proportional to number of priority group, by default each queue has different
priority group.

## Rqueue is using significantly large amount of Redis Memory

Rqueue stores completed jobs and messages in Redis for 30 minutes, this feature can be turned off if
you don't need visibility about completed jobs and messages. To turn off we need to set following
properties as

`rqueue.job.enabled=false`
`rqueue.message.durability.in-terminal-state=0`

## How to consume events from dead letter queue?

By default, jobs/messages sent to dead letter queue are not consumable, but we can set additional
fields in `RqueueListener` to enable message consumable feature.

In the main listener set dead letter queue name using `deadLetterQueue` field and enable consumable
feature using `deadLetterQueueListenerEnabled` once these are set add another listener to consume
events from dead letter queue.

```java

@Component
@Sl4j
class ReservationRequestMessageConsumer {

  @RqueueListener(
      value = "reservation.request.queue",
      deadLetterQueue = "reservation.request.dead.letter.queue.name",
      deadLetterQueueListenerEnabled = "true",
      numRetries = "3")
  public void onMessageReservationRequest(ReservationRequest request) throws Exception {
    log.info("ReservationRequest {}", request);
    //TODO
  }

  @RqueueListener(value = "reservation.request.dead.letter.queue", numRetries = "1")
  public void onMessageReservationRequestDeadLetterQueue(
      ReservationRequest request, @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage)
      throws Exception {
    log.info("ReservationRequest Dead Letter Queue{}", request);
    //TODO
  }
}
```




