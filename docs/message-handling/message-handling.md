---
layout: default
title: Message Handling
nav_order: 1
parent: Producer Consumer
description: Message Handling in Rqueue
permalink: /message-handling
---


Rqueue supports two modes of message delivery:

* **Unicast**: A message is delivered to exactly one listener/handler.
* **Multicast**: A message is delivered to multiple listeners/handlers for the same queue.

[![Message Handling](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/rqueue-message-flow.jpg)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/rqueue-message-flow.jpg)

## Message Multicasting

When multiple handlers are attached to the same queue, one must be designated as the 
**primary listener**. The primary listener is responsible for managing retries and 
the overall message lifecycle. Secondary listeners are invoked alongside the primary 
listener whenever a message is consumed.

For example, if you have three handlers (`L1`, `L2`, and `L3`) for `user-queue`, and 
`L2` is the primary:
- If `L2` fails, the message will be retried.
- During the retry, **all** handlers (`L1`, `L2`, and `L3`) may be called again.
- This can lead to duplicate processing in `L1` and `L3` even if they succeeded 
  on the first attempt. Ensure your handlers are idempotent when using multicasting.

## Configuration

To use multicasting, annotate your class with `@RqueueListener` and ensure it is 
registered as a Spring bean (e.g., using `@Component`). Then, annotate each 
handler method with `@RqueueHandler`. Use the `primary = true` attribute on 
exactly one `@RqueueHandler` annotation.

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

## Limitations and Important Considerations

- **Middleware**: Middleware is invoked once before any handler methods are executed. 
  It is not called individually for each `@RqueueHandler`. Ensure that message 
  release or status handling is managed only by the primary handler to avoid 
  inconsistent states.

- **Failure and Retries**: Failure and retry logic applies exclusively based on the 
  result of the primary handler. If the primary handler fails, all handlers will 
  be retried in the next attempt.

- **Metrics and Job Data**: Metrics and job records are generated once per consumption 
  cycle. Even with multiple handlers, the execution counts as a single job.