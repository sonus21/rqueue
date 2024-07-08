---
layout: default
title: Message Deduplication
nav_order: 2
parent: Producer Consumer
description: Message deduplication in Rqueue
permalink: /message-deduplication
---

Sometimes, there's a need to schedule unique messages in a queue. In these instances, the older
message should be discarded to ensure only the newest one is consumed. Implementing this requires
careful consideration of what defines message uniqueness — whether it's a single `ID` field or a
combination of multiple fields. Once identified, you can use a pre-execution message processor to
manage the discarding of older messages before processing the latest ones.

##### Enqueue Process

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

UniqueMessageProcessor that implements MessageProcessor and returns false for the older messages.

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

Rqueue configuration, that uses preExecutionMessageProcessor to skip messages.

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

If your use case requires that older message should be executed while new one should be ignored than
you can also implement that using pre execution message processor.

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

##### This does not handle the following cases

* Multiple similar messages enqueue at the same time.
* Multiple similar messages are trying to run at the same time.
* Enqueuing new message when the existing one is running.
* Enqueuing new message when the older message was discarded.
