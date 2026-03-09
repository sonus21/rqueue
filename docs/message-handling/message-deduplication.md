---
layout: default
title: Message Deduplication
nav_order: 2
parent: Producer Consumer
description: Message deduplication in Rqueue
permalink: /message-deduplication
---

You may sometimes need to ensure that only unique messages are processed in a queue. 
In such cases, when a new version of a message is scheduled, any older, pending versions 
should be discarded.

To implement this, first define what makes a message unique (e.g., an ID or a 
combination of fields). Then, use a **Pre-Execution Message Processor** to check 
the message's uniqueness and decide whether to process or discard it.

### Example: Discarding Older Messages

In this scenario, we keep track of the latest enqueue time. If a polled message is 
older than the recorded latest time, it is skipped.

#### 1. Enqueue Process

```java
interface MessageRepository {
  Long getLatestEnqueueAt(String messageId);

  void addEnqueueAt(String messageId, Long time);
}

class SimpleMessage {
  private String id;
}

class MessageSender {
  @Autowited
  private MessageRepository messageRepository;
  @Autowired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void sendMessage(SimpleMessage message) {
    String id = message.getId();
    //TODO handle error case
    messageRepository.addEnqueueAt(id, System.currentTimeMillis());
    rqueueMessageEnqueuer.enqueueIn("simple-queue", message, Duration.ofMinutes(10));
  }
}
```

#### 2. Unique Message Processor

Implement `MessageProcessor` and return `false` for older messages to skip them.

```java
class UniqueMessageProcessor implements MessageProcessor {

  @Autowired
  private MessageRepository messageRepository;

  @Override
  boolean process(Object message, RqueueMessage rqueueMessage) {
    if (message instanceof SimpleMessage) {
      // here you can get id using composite fields, add a method to find the unique id
      String messageId = ((SimpleMessage) message).getId();
      Long latestEnqueueTime = messageRepository.getLatestEnqueueAt(messageId);
      // no entry return true
      // no new message return true
      return latestEnqueueTime == null || latestEnqueueTime <= rqueueMessage.getQueuedTime();
    }
    return true;
  }
}
```

#### 3. Rqueue Configuration

Register the pre-execution message processor in your configuration.

```java
class RqueueConfiguration {

  @Bean
  public MessageProcessor preExecutionMessageProcessor() {
    return new UniqueMessageProcessor();
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    MessageProcessor preExecutionMessageProcessor = preExecutionMessageProcessor();
    factory.setPreExecutionMessageProcessor(preExecutionMessageProcessor);
    return factory;
  }
}
```

### Example: Ignoring New Messages if Old Ones Exist

If you prefer to execute the first message and ignore any subsequent versions 
scheduled while the first is still pending, use this approach:

```java
interface MessageRepository {

  Long getEnqueueAt(String messageId);

  void saveEnqueueAt(String messageId, Long time);
}

class MessageSender {

  @Autowired
  private RqueueMessageEnqueuer rqueueMessageEnqueuer;

  public void sendMessage(SimpleMessage message) {
    rqueueMessageEnqueuer.enqueueIn("simple-queue", message, Duration.ofMinutes(10));
  }
}
```

```java
class UniqueMessageProcessor implements MessageProcessor {

  @Autowired
  private MessageRepository messageRepository;

  @Override
  boolean process(Object message, RqueueMessage rqueueMessage) {
    if (message instanceof SimpleMessage) {
      String messageId = ((SimpleMessage) message).getId();
      Long enqueueAt = messageRepository.getEnqueueAt(messageId);
      if (enqueueAt == null) {
        messageRepository.saveEnqueueAt(messageId, System.currentTimeMillis());
        return true;
      }
      // allow running the same message multiple times
      return enqueueAt == rqueueMessage.getQueuedTime();
    }
    return true;
  }
}
```

### Limitations

These simple examples do not handle the following complex scenarios:

- **Simultaneous Enqueuing**: Multiple similar messages enqueued at the exact same time.
- **Concurrent Execution**: Multiple similar messages attempting to run at the same time.
- **In-flight Messages**: Enqueuing a new message while an existing one is currently being processed.
- **Deduplication Lag**: Enqueuing a new message shortly after an older message was discarded.
