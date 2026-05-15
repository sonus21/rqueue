---
layout: default
title: Retry and Backoff
parent: Configuration
nav_order: 2
---

# Retry and Backoff

Rqueue is a polling-based library that interacts with Redis to retrieve messages. These 
interactions can fail due to Redis server stress, outages, connection timeouts, or 
network instability. To avoid overloading Redis during such failures, you can 
configure a **back-off interval**. 

When a command fails, the poller will wait for the duration specified by `backOffTime` 
in the `SimpleRqueueListenerContainerFactory` before trying again. The default 
back-off time is 5 seconds.

## Disabling Retries

If you want to prevent Rqueue from retrying specific task failures, you can use one of 
the following methods:

1. **Using the `@RqueueListener` Annotation**: Specify exceptions in the `doNotRetry` 
   attribute. Rqueue will not retry the task if it fails with any of these exceptions.

```java
public class MessageListener {
  @RqueueListener(value = "sms", doNoRetry = {Exception1.class, Exception2.class})
  public void onMessage(Sms sms) {
    log.info("Sms: {}", sms);
  }
}
```

2. **Returning `-1` from a Custom Backoff**: If you implement a custom 
   `TaskExecutionBackOff`, returning `-1` signals Rqueue to stop all further retry 
   attempts for that message.

Note: Messages handled this way are neither retried nor moved to the Dead Letter Queue.

## Task Execution Backoff

When a message handler fails, the message can be retried immediately, delayed for a 
future retry, moved to a dead letter queue, or dropped.

### Global Retry Limit

Set `rqueue.retry.max=N` to limit listeners that do not configure `numRetries` and
would otherwise retry forever. The default is `-1`, which leaves the retry-forever
default unchanged. Explicit `@RqueueListener(numRetries = "...")` values and the
dead-letter queue default retry count continue to take precedence.

For the NATS backend, this retry count is translated to JetStream `maxDeliver` as
`N + 1`, because JetStream counts the initial delivery plus retries. For example,
`rqueue.retry.max=3` creates consumers with at most four total deliveries.

### Immediate Retries
To retry a message immediately within the same polling cycle, set 
`rqueue.retry.per.poll` to a positive integer (e.g., `2`). This will cause the 
message to be retried twice in quick succession before being returned to the queue.

### Delayed Retries (Exponential/Linear Backoff)
By default, Rqueue uses a linear backoff with a 5-second delay. You can customize 
this by providing a `TaskExecutionBackOff` implementation. Rqueue includes built-in 
options like `FixedTaskExecutionBackOff` and `ExponentialTaskExecutionBackOff`.

```java
public class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    // ...
    factory.setTaskExecutionBackOff(getTaskExecutionBackoff());
    return factory;
  }

  private TaskExecutionBackOff getTaskExecutionBackoff() {
    // Example: 2 seconds delay, 2 retries
    // return new FixedTaskExecutionBackOff(2_000L, 2); 
    
    // Example: Exponential backoff starting at 2s, max 10m
    // return new ExponentialTaskExecutionBackOff(2_000L, 10 * 60_000L, 2, 200);
  }
}
```
 
