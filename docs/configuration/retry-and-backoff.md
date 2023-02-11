---
layout: default
title: Retry and Backoff
parent: Configuration
nav_order: 2
---

# Retry And Backoff

Rqueue is a poll based library, internally Rqueue sends various commands to Redis to retrieve the messages.
For various reasons the command could fail like Redis is in stress, outage, connection timeout, poor internet connection.
In such cases it will keep sending the commands to the Redis which can overload the Redis Server. 
Given command will always fail for some reason, we can configure the back off interval in which case poller
 will sleep instead of sending the Redis commands. We need to set `backOffTime` in 
 `SimpleRqueueListenerContainerFactory` . The default backoff time is 5 seconds. 
 

## Task Execution backoff

The method which is consuming the message can fail as well for any reason. In such case a message may
be retried or move to dead letter queue or dropped. Whenever a message needs to be retried it can be
either retried immediately or later. To retry immediately we need to set `rqueue.retry.per.poll` to 
some positive number like 2 which means this will be retried in the same time twice one after another. 
But if you do not want to retry the message right away, you can configure Rqueue to retry this message after 
some time. For this you can either use Exponential or linear backoff approach or any other.  
By default, Rqueue uses linear backoff with delay of 5 seconds which the failed message will be 
retried after 5 seconds. You need to provide an implementation of `TaskExecutionBackoff` or you can 
use the default implementation like `FixedTaskExecutionBackOff` or `ExponentialTaskExecutionBackOff`.

```java
public class RqueueConfiguration{
    
    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
        SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory = new SimpleRqueueListenerContainerFactory();
        // ...
        simpleRqueueListenerContainerFactory.setTaskExecutionBackOff(getTaskExecutionBackoff());
        return simpleRqueueListenerContainerFactory;
    }
    
    private TaskExecutionBackOff getTaskExecutionBackoff(){
        // return TaskExecutionBackOff implementation
        // return new FixedTaskExecutionBackOff(2_000L, 2); // 2 seconds delay and 2 retries
        // return new ExponentialTaskExecutionBackOff(2_000L, 10*60_000L,  2, 200) 
    }
}
```  
 


