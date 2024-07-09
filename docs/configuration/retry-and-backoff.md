---
layout: default
title: Retry and Backoff
parent: Configuration
nav_order: 2
---

# Retry And Backoff

Rqueue is a poll-based library that sends various commands to Redis to retrieve messages. These
commands can fail for several reasons, such as Redis being under stress, outages, connection
timeouts, or poor internet connections. Continuous attempts to send commands can overload the Redis
server. To mitigate this, you can configure a back-off interval, causing the poller to sleep instead
of repeatedly sending commands. This is done by setting the `backOffTime` in
the `SimpleRqueueListenerContainerFactory`. The default back-off time is 5 seconds.

## Do not retry

In scenarios where you don't want Rqueue to retry failures, you can handle this in two ways:

- **Using `RqueueListener` Annotation**: Add exceptions to the `doNotRetry` list within
   the `RqueueListener` annotation. This instructs Rqueue not to retry for specific exceptions.

```java

public class MessageListener{
  @RqueueListener(value = "sms", priority = "critical=10,high=8,medium=4,low=1", doNoRetry={Exception1.class, Exception2.class})
  public void onMessage(Sms sms) {
    log.info("Sms : {}", sms);
  }
    
}
```

- **Returning `-1` from Execution Backoff Method**: Alternatively, you can return `-1` from the
   execution backoff method. This signals Rqueue to stop any further retry attempts for the failed
   message. 

These approaches offer flexibility in managing error handling and retry policies within your
application using Rqueue. Messages handled in this way also won't be sent to the Dead Letter Queue.

## Task Execution backoff

The method consuming the message can also fail for various reasons. In such cases, the message may
be retried, moved to a dead letter queue, or dropped. If a message needs to be retried, it can
either be retried immediately or after some time. To retry immediately, set `rqueue.retry.per.poll`
to a positive number like 2, which means the message will be retried twice in quick succession.

If you prefer not to retry the message immediately, you can configure Rqueue to retry the message
after a delay using an exponential or linear backoff approach, or any other strategy. By default,
Rqueue uses a linear backoff with a delay of 5 seconds, meaning the failed message will be retried
after 5 seconds. To customize this behavior, you can provide an implementation
of `TaskExecutionBackoff`, or use the default implementations such as `FixedTaskExecutionBackOff`
or `ExponentialTaskExecutionBackOff`.

```java
public class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory = new SimpleRqueueListenerContainerFactory();
    // ...
    simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(getTaskExecutionBackoff());
    return simpleRqueueListenerContainerFactory;
  }

  private TaskExecutionBackOff getTaskExecutionBackoff() {
    // return TaskExecutionBackOff implementation
    // return new FixedTaskExecutionBackOff(2_000L, 2); // 2 seconds delay and 2 retries
    // return new ExponentialTaskExecutionBackOff(2_000L, 10*60_000L,  2, 200) 
  }
}
```
 


