---
layout: default
title: Configuration
nav_order: 2
has_children: true
permalink: configuration
---

# Configuration

{: .no_toc }

Rqueue offers numerous configuration settings that can be adjusted either through 
application properties or directly in code.

---

Beyond basic setup, Rqueue can be highly customized, for example, by adjusting the number 
of tasks executed concurrently. Further configurations are available via the 
`SimpleRqueueListenerContainerFactory` class. Refer to the 
[SimpleRqueueListenerContainerFactory Javadoc](https://javadoc.io/doc/com.github.sonus21/rqueue-core/latest/com/github/sonus21/rqueue/config/SimpleRqueueListenerContainerFactory.html) 
for more details.

```java

@Configuration
public class RqueueConfiguration {
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    // return SimpleRqueueListenerContainerFactory object
  }
}
```

## Task and Queue Concurrency

By default, the number of task executors is twice the number of queues. You can provide 
a custom or shared task executor using the factory's `setTaskExecutor` method. 

Queue-level concurrency can be configured using the `@RqueueListener` annotation's 
`concurrency` field. This can be a fixed number (e.g., `10`) or a range (e.g., `5-10`). 
When specified, that queue uses its own task executor; otherwise, the shared task 
executor is used.

You can also set a global limit on workers using `setMaxNumWorkers`. The `batchSize` 
field in `@RqueueListener` determines how many messages are fetched at once. By 
default, listeners with explicit concurrency fetch 10 messages per poll, while others 
fetch 1.

{: .note}
Increasing the batch size can lead to task rejection if the thread pool is too small and 
the `queueCapacity` is not sufficiently large.

```java
class RqueueConfiguration {
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    //...
    factory.setMaxNumWorkers(10);
    return factory;
  }
}
```

```java
class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    //...
    ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
    threadPoolTaskExecutor.setThreadNamePrefix("taskExecutor");
    threadPoolTaskExecutor.setCorePoolSize(10);
    threadPoolTaskExecutor.setMaxPoolSize(50);
    threadPoolTaskExecutor.setQueueCapacity(0);
    threadPoolTaskExecutor.afterPropertiesSet();
    factory.setTaskExecutor(threadPoolTaskExecutor);
    return factory;
  }
}
```

When providing a custom executor, it is essential to set `MaxNumWorkers` correctly to avoid 
over- or under-utilizing the thread pool. Over-utilization can cause task rejection and 
message consumption delays.

```java
ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
threadPoolTaskExecutor.setThreadNamePrefix("ListenerExecutor");
threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);
threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
threadPoolTaskExecutor.afterPropertiesSet();
factory.setTaskExecutor(threadPoolTaskExecutor);
```

Key configuration parameters for the executor include:
- `corePoolSize`: The minimum number of active threads.
- `maxPoolSize`: The maximum number of active threads.
- `queueCapacity`: The number of tasks that can wait in the internal queue before new 
  tasks are rejected.

With `N` queues, a common rule of thumb for setting the maximum number of workers is 
`(maxPoolSize + queueCapacity - N)`.

{: .warning}
In this case, `N` represents the threads allocated for polling. However, this count 
can vary significantly if **priorities** are used.

The total number of message pollers is determined by the sum of:
1. The number of unique priority groups.
2. The number of queues with explicit priority settings (e.g., `"critical=5,high=2"`).
3. The number of queues without specified priorities.

A safe baseline configuration without complex calculations:
- `queueCapacity >= 2 * number of queues`
- `maxPoolSize >= 2 * number of queues`
- `corePoolSize >= number of queues`

{: .note}
A non-zero `queueCapacity` can lead to duplicate message processing. If a message is 
polled and sits in the executor's queue longer than its `visibilityTimeout`, it may 
be re-polled by another listener. Ensure your `visibilityTimeout` is long enough to 
accommodate potential queuing delays.

## Manual Container Management

By default, the Rqueue container starts automatically. You can control this behavior 
using the `autoStartup` flag. If set to `false`, you must manually call the `start()` 
and `stop()` methods of the container. For a clean shutdown, also ensure that the 
`destroy()` method is called.

```java
class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    //...
    factory.setAutoStartup(false);
    return factory;
  }
}
```

```java
public class BootstrapController {

  @Autowired
  private RqueueMessageListenerContainer rqueueMessageListenerContainer;

  // ...
  public void start() {
    // ...
    rqueueMessageListenerContainer.start();
  }

  public void stop() {
    // ...
    rqueueMessageListenerContainer.stop();
  }

  public void destroy() {
    // ...
    rqueueMessageListenerContainer.destroy();
  }
  //...
}
```

## Message Converter Configuration

To customize message conversion, set the property
`rqueue.message.converter.provider.class` to the fully qualified name of your provider
class. This class must implement the `MessageConverterProvider` interface and return
a Spring `MessageConverter`.

{: .note}
Your custom provider must implement
`com.github.sonus21.rqueue.converter.MessageConverterProvider`.

```java
class MyMessageConverterProvider implements MessageConverterProvider {

  @Override
  public MessageConverter getConverter() {
    // here any message converter can be returned except null
    return new MyMessageConverter();
  }
}
```

The `DefaultRqueueMessageConverter` handles serialization for most use cases, but it
may fail if classes are not shared between producing and consuming applications. To
avoid shared dependencies, consider using JSON-based converters like
`com.github.sonus21.rqueue.converter.JsonMessageConverter` or Spring's
`JacksonJsonMessageConverter`. These serialize payloads into JSON, improving
interoperability.

Other serialization formats like MessagePack or Protocol Buffers (ProtoBuf) can also
be implemented based on your requirements.

### Generic Envelope Types

`GenericMessageConverter` (used by the default converter) supports **single-level
generic envelope types** such as `Event<T>`. The type parameter is resolved at
serialization time by inspecting the runtime class of the field value that corresponds
to `T`.

```java
// A generic envelope type
public class Event<T> {
  private String id;
  private T payload;
  // getters/setters ...
}

// Enqueue
Event<Order> event = new Event<>("evt-123", order);
rqueueMessageEnqueuer.enqueue("order-queue", event);

// Consume
@RqueueListener(value = "order-queue")
public void onEvent(Event<Order> event) { ... }
```

The serialized form encodes both the envelope class and the type parameter:

```
{"msg":"...","name":"com.example.Event#com.example.Order"}
```

**Constraints:**

- The type parameter `T` must be a **non-generic** concrete class (e.g. `Order`, not
  `List<Order>`).
- At least one field of type `T` on the envelope class must be **non-null** at
  serialization time, so the runtime type can be determined.
- For `List<T>`, items must also be non-generic concrete classes. Envelopes like
  `List<Event<Order>>` are not supported.
- Multi-level nesting (e.g. `Wrapper<Event<T>>`) is not supported.

## Message ID Generator

Rqueue now resolves message IDs through the `RqueueMessageIdGenerator` abstraction.
By default, Rqueue registers a UUIDv4-based implementation, but applications can
override it by defining their own bean.

This is useful when you need:

- time-ordered IDs such as UUIDv7
- custom prefixes or tenant-aware IDs
- IDs generated by an external system

```java
import com.github.sonus21.rqueue.core.RqueueMessageIdGenerator;

@Configuration
public class RqueueConfiguration {

  @Bean
  public RqueueMessageIdGenerator rqueueMessageIdGenerator() {
    return () -> java.util.UUID.randomUUID().toString();
  }
}
```

{: .note}
The default implementation is still UUIDv4. Custom generators are applied to the
normal enqueue APIs such as `enqueue`, `enqueueIn`, `enqueueAt`, and `enqueuePeriodic`
whenever Rqueue generates the message ID internally.

## Additional Configuration

- **`rqueue.retry.per.poll`**: Determines how many times a polled message is retried 
  immediately if processing fails, before it is moved back to the queue for a 
  subsequent poll. The default value is `1`. If increased to `N`, the message will 
  be retried `N` times consecutively within the same polling cycle.
- **`rqueue.worker.registry.enabled`**: Enables worker and queue-poller tracking for
  the dashboard. Default: `true`.
- **`rqueue.worker.registry.worker.ttl`**: TTL in seconds for worker metadata stored
  in Redis. Default: `300`.
- **`rqueue.worker.registry.worker.heartbeat.interval`**: Interval in seconds for
  refreshing worker metadata. Default: `60`.
- **`rqueue.worker.registry.queue.ttl`**: TTL in seconds for queue poller hashes.
  Default: `3600`.
- **`rqueue.worker.registry.queue.heartbeat.interval`**: Interval in seconds for
  queue poller heartbeats. Default: `15`.
