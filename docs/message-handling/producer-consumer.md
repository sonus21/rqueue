---
layout: default
title: Producer Consumer
nav_order: 3
has_children: true
description: Message Handling in Rqueue
permalink: /producer-consumer
---


The Rqueue provides an abstraction over Redis for Producer and Consumer use case. The library
provides two types of queues for producer/consumer.

* General queue (FIFO)
* Delay queue (Priority queue)

Rqueue has been designed to handle different use cases and scenarios.

Read More about Rqueue Design at [Introducing Rqueue: Redis Queue][Introducing Rqueue]

Rqueue Architecture
--------------------
Rqueue Broker runs scheduled job as well as it communicates with Redis. In general Rqueue works as
producers and consumers at the same time.

```text
+-----------------------+                         +------------------------+
|                       |                         |                        |
|     Application       |                         |        Application     | 
|                       |                         |                        |
|-----------------------|         +-----+         |------------------------|
|       Rqueue          |    +--->|Redis|<---+    |         Rqueue         |
| +----------------+    |    |    +-----+    |    |    +----------------+  |                    
| |Rqueue Scheduler|    |    |               |    |    |Rqueue Scheduler|  |
| |----------------|    |    |    +-----+    |    |    |----------------|  |
| |Rqueue Producer |<---+----+--->|Redis|<---+----+--->|Rqueue Producer |  |
| |----------------|    |    |    +-----+    |    |    |----------------|  |
| |Rqueue Consumer |    |    |               |    |    |Rqueue Consumer |  |
| +----------------+    |    |    +-----+    |    |    +----------------+  |                
+-----------------------+    +--->|Redis|<---+    +------------------------+
                                  +-----+                                 
```  

An application can operate in both producer and consumer modes. However, attempting to push a
message to an unknown queue will result in a `QueueDoesNotExist` error. This error can be mitigated
by using `registerQueue`, but there's a potential race condition where registered queues might get
deleted. To prevent encountering the `QueueDoesNotExist` error again, listen
for `RqueueBootStrapEvent` in your application and register queues only upon receiving this
bootstrap event.

```java

@Component
class AppMessageSender implements ApplicationListener<RqueueBootStrapEvent> {

  @Autowired
  private RqueueEndpointManager rqueueEndpointManager;

  @Override
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (!event.isStartup()) {
      return;
    }
    for (String queue : queues) {
      String[] priorities = getPriority(queue);
      rqueueEndpointManager.registerQueue(queue, priorities);
    }
  }

  private String[] getPriority(String queue) {
    return new String[]{};
  }
} 
```

Rqueue can facilitate running consumers and producers in separate clusters without requiring changes
to application code. This can be achieved effortlessly using the `active` flag of `RqueueListener`.
In the producer cluster, you set `active` to `false`, while in the consumer cluster, you set
the `active` flag to `true`. This approach allows for straightforward configuration and management
of distinct roles within different clusters using Rqueue.

```text
+-----------------------+                         +------------------------+
|                       |                         |                        |
| Producer Application  |                         | Consumer Application   | 
|                       |                         |                        |
|-----------------------|         +-----+         |------------------------|
|       Rqueue          |    +--->|Redis|<---+    |         Rqueue         |
| +----------------+    |    |    +-----+    |    |    +----------------+  |                    
| |Rqueue Scheduler|    |    |    +-----+    |    |    |Rqueue Scheduler|  |
| |----------------+----+----+--->|Redis|<---+----+--->|----------------|  |
| |Rqueue Producer |    |    |    +-----+    |    |    |Rqueue Consumer |  |
| +----------------+    |    |    +-----+    |    |    +----------------+  |
+-----------------------+    +--->|Redis|<---+    +------------------------+
                                  +-----+                                 
```  

If you have configured the producer and consumer on separate clusters, you can disable the Rqueue
scheduler on the producer machine by setting `rqueue.scheduler.enabled=false`. This configuration
ensures that the producer application operates solely as a producer without handling scheduling
tasks.

Additionally, it's crucial to set `rqueue.system.mode=PRODUCER` to prevent
potential `QueueDoesNotExist` errors. This setting explicitly defines the application mode as a
producer, ensuring that it doesn't attempt to perform consumer-specific operations that might lead
to queue-related issues in a split-cluster setup.

```text
+-----------------------+                         +------------------------+
|                       |                         |                        |
| Producer Application  |                         | Consumer Application   | 
|                       |         +-----+         |                        |
|-----------------------|    +--->|Redis|<---+    |------------------------|
|       Rqueue          |    |    +-----+    |    |         Rqueue         |
|                       |    |               |    |    +----------------+  |                    |
| +----------------+    |    |    +-----+    |    |    |Rqueue Scheduler|  |                    
| |Rqueue Producer |----+----+--->|Redis|<---+----+--->|----------------+  |
| +----------------+    |    |    +-----+    |    |    | Rqueue Consumer|  |
|                       |    |               |    |    +----------------+  |
|                       |    |    +-----+    |    |                        |
+-----------------------+    +--->|Redis|<---+    +------------------------+
                                  +-----+                                 
```  

### Rqueue Scheduler

The Rqueue scheduler offers extensive configurability across various parameters:

- `rqueue.scheduler.enabled=true`: By default, the scheduler runs on all machines. You can disable
  it globally using this flag if running on a single machine is sufficient.

- `rqueue.scheduler.listener.shared=true`: Controls whether Rqueue scheduler shares the
  RedisMessageListenerContainer for PUB/SUB communication with Redis consumers. Enabling this allows
  efficient use of the same Redis connection for both application and Rqueue code.

- `rqueue.scheduler.redis.enabled=true`: Disables event-based message movement, providing control
  over when messages are processed.

- `rqueue.scheduler.auto.start=true`: Manages thread pools for message handling. Setting this
  to `false` uses only event-based message movement.

- `rqueue.scheduler.scheduled.message.thread.pool.size=5`: Adjusts the thread pool size for handling
  messages moved from ZSET to LIST, balancing efficiency with the number of delayed queues.

- `rqueue.scheduler.processing.message.thread.pool.size=1`: Sets the thread pool size for handling
  messages in the processing queue, ensuring reliable at-least-once message delivery.

- `rqueue.scheduler.scheduled.message.time.interval=5000`: Specifies the interval for moving
  messages from the scheduled queue to the normal queue, providing control over delayed message
  consumption.

- `rqueue.scheduler.max.message.count=100`: Limits the number of messages moved per batch from
  scheduled/processing queues to the normal queue, optimizing processing efficiency during peak
  loads.

- `rqueue.scheduler.max.message.mover.delay=60000`: Specifies the maximum delay before retrying
  Redis calls in case of failure, preventing system overload through exponential backoff.

- `rqueue.scheduler.min.message.mover.delay=200`: Sets the minimum delay for periodic message
  fetching, ensuring efficient processing of scheduled messages.

These configurations allow fine-tuning of Rqueue scheduler behavior across different operational
scenarios.

### Dead Letter Queue Consumer/Listener

By default, an application cannot attach a listener to a dead letter queue. To enable dead letter
queue listener functionality for a specific queue, set the `deadLetterQueueListenerEnabled`
attribute of the `RqueueListener` annotation.

Example configuration

```java

@Component
@Slf4j
public class MessageListener {

  @RqueueListener(value = "job-queue",
      numRetries = "3",
      deadLetterQueueListenerEnabled = "true",
      deadLetterQueue = "failed-job-queue",
      concurrency = "5-10")
  public void onMessage(Job job) {
    log.info("Job alert: {}", job);
  }

  @RqueueListener(value = "failed-job-queue", concurrency = "1")
  public void onMessage(Job job) {
    log.info("Job alert: {}", job);
  }
}
```

[Introducing Rqueue]: https://sonus21.medium.com/introducing-rqueue-redis-queue-d344f5c36e1b
