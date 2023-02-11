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

Multiple listeners can be attached with any queue, when more than one message consumers are working
for the same queue then one of them must be designed as primary. Retry and every other things works
on the primary listener, other listener would be called when a message is received. For example
three listeners `L1`, `L2` and `L3` are registered to queue `user-queue` then either `L1`, `L2`
or `L3` must be a primary listener. Designating primary listener means if execution of primary
listener fails then this will be retried and will call other listeners as well, so it can lead to
duplicate message for some listener due to failure in the primary listener. In this example let's
assume `L2` is primary listener and a message `UserEvent1` was dequeued from queue `user-queue` than
it will call `L1`, `L3` and  `L2` with the `UserEvent1` arguments. If execution of `L2` fails for
some reason than on next retry it will call `L1`, `L3` as well even though `L1` and `L3` might have
successfully consumed the event.

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

* **Middleware** : Middlewares are not called for each of the handler,global middlewares are called
  before calling any handler method. Also, message should be only released by a primary handler,
  releasing message from another handler can create inconsistent state.
* **Failure/Retry**: Failure/Retry handling is only applicable for the primary handler, if primary
  handler execution fails than all handlers would be called next time.
* **Metrics/Job data**: This is traced only once, for example in this example we've configured four
  handlers, but it will be counted as only one execution and each execution will create only one
  job.   
  