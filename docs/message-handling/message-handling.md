---
layout: default
title: Message Handling
nav_order: 1
parent: Producer Consumer
description: Message Handling in Rqueue
permalink: /message-handling
---


Rqueue supports two types of message handling

* Unicast: One message is handed over to one the listener/handler/consumer
* Multicast: One message is handed over to multiple listeners/handlers/consumers

[![Message Handling](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/rqueue-message-flow.jpg)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/qrqueue-message-flow.jpg)

## Message Multicasting

When multiple listeners are attached to the same queue, it's essential to designate one as the
primary listener. This primary listener handles retries and other operations related to message
processing. Secondary listeners are invoked alongside the primary listener whenever a message is
received. For example, if three listeners (`L1`, `L2`, and `L3`) are registered for
the `user-queue`, one of them must be designated as primary.

Designating a primary listener means that if the primary listener (`L2`, in this example) encounters
a processing failure, it retries the message. During retries, all listeners (`L1`, `L3`, and `L2`)
might be called again with the same message (`UserEvent1`), potentially leading to duplicate
processing for some listeners (`L1` and `L3`), even if they successfully processed the event
initially.

## Configuration

Add `RqueueListener` annotation to any class, and use any of the spring stereotype annotation, so
that a bean would be created. Annotate all listener methods in this class using `RqueueHandler`

```java

@RqueueListener(value = "${user.banned.queue.name}", active = "${user.banned.queue.active}")
@Slf4j
@Component
@RequiredArgsConstructor
public class UserBannedMessageListener {

  @NonNull
  private final ConsumedMessageStore consumedMessageStore;

  @Value("${user.banned.queue.name}")
  private String userBannedQueue;

  @RqueueHandler
  public void handleMessage1(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessage1", userBannedQueue);
    log.info("handleMessage1 {}", userBanned);
  }

  @RqueueHandler
  public void handleMessage2(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessage2", userBannedQueue);
    log.info("handleMessage2 {}", userBanned);
  }

  @RqueueHandler(primary = true)
  public void handleMessagePrimary(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessagePrimary", userBannedQueue);
    log.info("handleMessagePrimary {}", userBanned);
  }

  @RqueueHandler
  public void handleUserBanned(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleUserBanned", userBannedQueue);
    log.info("handleUserBanned {}", userBanned);
  }
}
```

## Limitation

- **Middleware**: Middlewares are invoked globally before any handler method is called. They are not
  called individually for each handler. It's crucial that message release is only handled by the
  primary handler to avoid inconsistent states.

- **Failure/Retry**: Failure and retry mechanisms apply exclusively to the primary handler. If the
  primary handler fails during execution, all handlers will be retried in the subsequent attempt.

- **Metrics/Job data**: Metrics and job data are recorded once per execution cycle. For instance,
  even if there are multiple handlers configured, such as four in this example, it will count as a
  single execution generating a single job record.