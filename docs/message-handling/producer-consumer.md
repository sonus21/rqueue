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

An application can work in both modes(`producer` and `consumer`) but if application try to push message
to unknown queue then it would receive `QueueDoesNotExist` error. This error can be solved using 
`registerQueue` but there's a race condition in that case registered queues could be deleted.
So again you can receive `QueueDoesNotExist` error, you can avoid this error by listening to
`RqueueBootStrapEvent`, you should register queues **only** when your application receives bootstrap
event.

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

Rqueue can be used to run consumer and producer in two different clusters, one cluster would be 
acting as producer and another one would be acting as consumer. We do not have to change application
code, this can be done very easily using `active` flag of `RqueueListener`, in producer cluster
set `active` to `false` and in consumer cluster set `active` flag to `true`.

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

If we have configured producer and consumer on two different clusters then we can disable
Rqueue scheduler on producer machine using `rqueue.scheduler.enabled=false` once we turn off Rqueue
scheduler on producer application, it will just act as producer now. We should also set 
`rqueue.system.mode=PRODUCER`, otherwise we may observe QueueDoesNotExist error

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

Rqueue scheduler is very flexible, it can be customized on different parameters. Following are
supported configurations.

* `rqueue.scheduler.enabled=true` By default, the scheduler would be running on all machines, though
  it's not required, since running scheduler on one machine should be sufficient, disable Rqueue
  scheduler using this flag.
* `rqueue.scheduler.listener.shared=true` Rqueue scheduler communicated with Redis consumer Redis
  PUB/SUB. This communication allows Rqueue to handle the message where messages have not been moved
  from scheduled/processing queue to original queue. Rqueue uses RedisMessageListenerContainer of
  Spring Redis data to handle the Redis PUB/SUB communication. If you're using same Redis connection
  than the same RedisMessageListenerContainer container can be used shared for the Application and
  Rqueue code. The sharing of  `RedisMessageListenerContainer` container is controlled using this
  flag. If sharing is disabled than Rqueue would create a new container to communicate with Redis.
* `rqueue.scheduler.redis.enabled=true` Event based message movement can be turned off using this
  setting.
* `rqueue.scheduler.auto.start=true` Rqueue scheduler also have thread pools, that handles the
  message. If you would like to use only event based message movement than set auto start as false.
* `rqueue.scheduler.scheduled.message.thread.pool.size=5` There could be many delayed queues, in
  that case Rqueue has to move more messages from ZSET to LIST. In such cases, you can increase
  thread pool size, the number of threads used for message movement is minimum of queue count and
  pool size.
* `rqueue.scheduler.processing.message.thread.pool.size=1` there could be some dead message in
  processing queue as well, if you're seeing large number of dead messages in processing queue, then
  you should increase the thread pool size. Processing queue is used for at least once message
  delivery guarantee.
* `rqueue.scheduler.scheduled.message.time.interval=5000` At what interval message should be moved
  from scheduled queue to normal queue. The default value is 5 seconds, that means, you can observe
  minimum delay of 5 seconds in delayed message consumption.
* `rqueue.scheduler.max.message.count=100` Rqueue continuously move scheduled messages from
  processing/scheduled queue to normal queue so that we can process them asap. There are many
  instances when large number of messages are scheduled to be run in next 5 minutes. In such cases
  Rqueue can pull message from scheduled queue to normal queue at higher rate. By default, it copies
  100 messages from scheduled/processing queue to normal queue.
* `rqueue.scheduler.max.message.mover.delay=60000` Rqueue continuously move scheduled messages from
  processing/scheduled queue to normal queue so that we can process them asap. Due to failure, it
  can load the Redis system, in such cases it uses exponential backoff to limit the damage. This
  time indicates maximum time for which it should wait before making Redis calls.
* `rqueue.scheduler.min.message.mover.delay=200` Rqueue continuously move scheduled messages from
  processing/scheduled queue to normal queue so that we can process them asap. It periodically
  fetches the messages, the minium delay in such cases can be configured using this variable.

### Dead Letter Queue Consumer/Listener

By default, an application can't attach a listener to dead letter queue, if you want to attach a
dead letter queue listener for a queue than set `deadLetterQueueListenerEnabled` of `RqueueListener`
annotation.

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
