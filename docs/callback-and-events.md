---
layout: default
title: Callback and Events
nav_order: 5
description: Callbacks and Events in Rqueue
permalink: /callback-and-events
---

Rqueue provides different types of callbacks and events.

Message Processor/Callback
---------------------------
Rqueue supports following message processors. Message processors can be used for different purpose,
some use case could be setting up tracer, creating transaction etc.

#### Pre Execution Processor

This message processor is called before calling the handler methods, if the method returns `false`
then message handler won't be called.

```java
class RqueueConfiguration {

  private MessageProcessor preExecutorMessageProcessor() {
    // return message processor object
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor preExecutorMessageProcessor = preExecutorMessageProcessor();
    factory.setPreExecutionMessageProcessor(preExecutorMessageProcessor);
    return factory;
  }
}
```

#### Discard Execution Processor

This message processor would be called whenever a message is discarded due to retry limit exceed.

```java
class RqueueConfiguration {

  private MessageProcessor discardMessageProcessor() {
    // return message processor object
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor discardMessageProcessor = discardMessageProcessor();
    factory.setDiscardMessageProcessor(discardMessageProcessor);
    return factory;
  }
}
```

#### Dead Letter Queue Processor

This message processor would be called whenever a message is moved to dead letter queue.

```java
class RqueueConfiguration {

  private MessageProcessor deadLetterQueueMessageProcessor() {
    // return message processor object
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor deadLetterQueueMessageProcessor = deadLetterQueueMessageProcessor();
    factory.setDeadLetterQueueMessageProcessor(deadLetterQueueMessageProcessor);
    return factory;
  }
}
```

#### Manual Deletion Processor

This message processor would be called whenever a message is deleted manually.

```java
class RqueueConfiguration {

  private MessageProcessor manualDeletionMessageProcessor() {
    // return message processor object
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor manualDeletionMessageProcessor = manualDeletionMessageProcessor();
    factory.setManualDeletionMessageProcessor(manualDeletionMessageProcessor);
    return factory;
  }
}
```

#### Post Execution Processor

This message processor is called on successful consumption of the message.

```java
class RqueueConfiguration {

  private MessageProcessor postExecutionMessageProcessor() {
    // return message processor object
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor postExecutionMessageProcessor = postExecutionMessageProcessor();
    factory.setPostExecutionMessageProcessor(postExecutionMessageProcessor);
    return factory;
  }
}
```

Events
------------

Rqueue generates two types of **application** events one for the Rqueue container start/shutdown and
one for the task execution status.

#### Job/Task Execution Event

On the completion of each task Rqueue generated `RqueueExecutionEvent`, the application can listen
to this event. This event has a job object that can provide all the information this job.

#### Application Bootstrap Event

Once RqueueListenerContainer is started it will emit one `RqueueBootstrapEvent`, this event is
generated post container shutdown. This event can be used for different purpose like queue
registration, cleaning up some local state etc.

