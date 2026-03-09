---
layout: default
title: Callbacks and Events
nav_order: 5
description: Callbacks and Events in Rqueue
permalink: /callback-and-events
---

Rqueue provides various callbacks and events to hook into message processing and 
application lifecycles.

## Message Processors and Callbacks

Rqueue supports several message processors. These can be used for various purposes, such as 
setting up tracing contexts, managing transactions, or auditing.

### Pre-Execution Processor

The Pre-Execution Processor is invoked before a message handler is called. If the processor 
returns `false`, the handler execution is skipped.

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

### Discard Processor

The Discard Processor is called when a message is discarded after exceeding its retry limit.

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

This processor is invoked whenever a message is moved to a dead letter queue.

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

The Manual Deletion Processor is called whenever a message is manually deleted.

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

### Post-Execution Processor

This processor is called after a message has been successfully consumed.

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

Rqueue generates two types of Spring Application Events: one for container lifecycle 
(start/shutdown) and another for job execution status.

### Job/Task Execution Events

After a task completes, Rqueue publishes an `RqueueExecutionEvent`. Applications can listen 
for this event to get detailed information about job performance and outcomes.

### Container Lifecycle Events

When the `RqueueListenerContainer` starts or stops, it publishes an `RqueueBootstrapEvent`. 
This event can be used to perform setup or cleanup operations when the messaging system 
initializes or shuts down.
