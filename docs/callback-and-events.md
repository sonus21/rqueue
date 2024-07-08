---
layout: default
title: Callbacks and Events
nav_order: 5
description: Callbacks and Events in Rqueue
permalink: /callback-and-events
---

Rqueue provides various types of callbacks and events for handling message processing and
application events.

## Message Processors/Callbacks

Rqueue supports the following message processors, which can be used for different purposes such as
setting up tracers or managing transactions.

### Pre Execution Processor

This message processor is invoked before calling the handler methods. If the processor
returns `false`, the message handler will not be called.

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

### Discard Execution Processor

This message processor is called whenever a message is discarded due to exceeding the retry limit.

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

### Dead Letter Queue Processor

This message processor is called whenever a message is moved to the dead letter queue.

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

### Manual Deletion Processor

This message processor is called whenever a message is deleted manually.

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

### Post Execution Processor

This message processor is called upon successful consumption of the message.

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

## Events

Rqueue generates two types of application events: one for Rqueue container start/shutdown and
another for task execution status.

### Job/Task Execution Event

Upon completion of each task, Rqueue generates `RqueueExecutionEvent`, which the application can
listen to. This event contains a job object providing all relevant information about the job.

### Application Bootstrap Event

Once the RqueueListenerContainer is started, it emits `RqueueBootstrapEvent`. This event is
generated post container shutdown and can be used for tasks such as queue registration or cleaning
up local states.
