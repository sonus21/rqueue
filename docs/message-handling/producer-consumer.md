---
layout: default
title: Producer Consumer
nav_order: 3
has_children: true
description: Message Handling in Rqueue
permalink: /producer-consumer
---


Rqueue provides an abstraction over Redis for Producer/Consumer patterns, 
supporting two main types of queues:

* **General Queue**: A standard First-In-First-Out (FIFO) queue.
* **Delayed Queue**: A priority-based queue for scheduled tasks.

Rqueue is designed to handle various messaging scenarios and high-throughput workloads.

For a deeper dive into the architectural decisions, see 
[Introducing Rqueue: Redis Queue][Introducing Rqueue].

## Rqueue Architecture

The Rqueue broker handles both scheduled task management and direct communication with 
Redis. In a standard setup, an application instance acts as both a producer and a 
consumer.

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

An application can operate as both a producer and a consumer. Attempting to push a 
message to an unknown queue will result in a `QueueDoesNotExist` error. You can 
avoid this by using the `registerQueue` method.

To prevent race conditions where queues might be deleted, it is recommended to 
listen for the `RqueueBootstrapEvent` and register your queues once the bootstrap 
process is complete.

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

### Split Producer/Consumer Clusters

Rqueue can run producers and consumers on entirely separate clusters without changing 
your code. This is managed using the `active` flag in `@RqueueListener`:
- In the **Producer Cluster**, set `active = false`.
- In the **Consumer Cluster**, set `active = true`.

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

When running a dedicated producer cluster, disable the Rqueue scheduler by setting 
`rqueue.scheduler.enabled=false`.

Crucially, set `rqueue.system.mode=PRODUCER` to prevent `QueueDoesNotExist` errors. 
This explicitly informs Rqueue that the application instance is only pushing 
messages and does not need to manage the lifecycle of consumer queues.

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

### Rqueue Scheduler Configuration

The Rqueue scheduler is highly configurable:

- **`rqueue.scheduler.enabled=true`**: Enable or disable the scheduler globally.
- **`rqueue.scheduler.listener.shared=true`**: Whether to share the 
  `RedisMessageListenerContainer` between the scheduler and message consumers to 
  save connections.
- **`rqueue.scheduler.redis.enabled=true`**: Enable or disable event-based message movement.
- **`rqueue.scheduler.auto.start=true`**: If `false`, the scheduler will only use 
  Redis events for message movement instead of dedicated polling threads.
- **`rqueue.scheduler.scheduled.message.thread.pool.size=5`**: Thread pool size for 
  moving tasks from the scheduled queue (`ZSET`) to the main queue (`LIST`).
- **`rqueue.scheduler.processing.message.thread.pool.size=1`**: Thread pool size for 
  handling recovery of tasks from the processing queue.
- **`rqueue.scheduler.scheduled.message.time.interval=5000`**: Polling interval 
  (in ms) for moving scheduled tasks.
- **`rqueue.scheduler.max.message.count=100`**: Max messages to move per batch.
- **`rqueue.scheduler.max.message.mover.delay=60000`**: Maximum retry delay 
  for Redis operations.
- **`rqueue.scheduler.min.message.mover.delay=200`**: Minimum interval for 
  periodic message movement.

These configurations allow fine-tuning of Rqueue scheduler behavior across different operational
scenarios.

### Dead Letter Queue Consumers

By default, Rqueue does not attach listeners to dead letter queues. To enable a 
listener for a specific dead letter queue, set `deadLetterQueueListenerEnabled = "true"` 
in the `@RqueueListener` annotation.

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
