---
layout: default
title: Configuration
nav_order: 2
has_children: true
permalink: configuration
---

# Configuration

{: .no_toc }

Rqueue offers many configuration settings that can be adjusted either through the application
configuration or directly in the code.

{: .fs-6 .fw-300 }

## Table of contents

{: .no_toc .text-delta }

1. TOC
   {:toc}

---
Apart from the basic configuration, Rqueue can be heavily customized, such as adjusting the number
of tasks executed concurrently. Additional configurations can be provided using
the `SimpleRqueueListenerContainerFactory` class. See
SimpleRqueueListenerContainerFactory [doc](https://javadoc.io/doc/com.github.sonus21/rqueue-core/latest/com/github/sonus21/rqueue/config/SimpleRqueueListenerContainerFactory.html)
for more configs.

```java

@Configuration
public class RqueueConfiguration {
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    // return SimpleRqueueListenerContainerFactory object
  }
}
```

## Task or Queue Concurrency

By default, the number of task executors is twice the number of queues. You can configure a custom
or shared task executor using the factory's `setTaskExecutor` method. Additionally, queue
concurrency can be set using the `RqueueListener` annotation's `concurrency` field, which can be a
positive number like 10 or a range like 5-10. If queue concurrency is specified, each queue will use
its own task executor to handle consumed messages; otherwise, a shared task executor is used.

A global number of workers can be configured using the `setMaxNumWorkers` method.
The `RqueueListener` annotation also has a `batchSize` field. By default, listeners with a
concurrency
set will fetch 10 messages, while others will fetch 1.

{: .note}
Increasing the batch size has its consequences. If your thread pool size is too low, you may
encounter many processing jobs being rejected by the executor unless you have configured a
large `queueCapacity`.

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

When a custom executor is provided, it is essential to set `MaxNumWorkers` correctly. Otherwise, the
thread pool might be over- or under-utilized. Over-utilization of the thread pool is not possible,
as it will reject new tasks, leading to delays in message consumption. Under-utilization can be
managed by ensuring proper configuration of the executor and adjusting the `MaxNumWorkers` setting
appropriately.

```
ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
threadPoolTaskExecutor.setThreadNamePrefix( "ListenerExecutor" );
threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);
threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
threadPoolTaskExecutor.afterPropertiesSet();
factory.setTaskExecutor(threadPoolTaskExecutor);
```

In this configuration, there are three key variables: `corePoolSize`, `maxPoolSize`,
and `queueCapacity`.

- `corePoolSize` signifies the lower limit of active threads.
- `maxPoolSize` signifies the upper limit of active threads.
- `queueCapacity` signifies that even if you have `maxPoolSize` running threads, you can
  have `queueCapacity` tasks waiting in the queue, which can be dequeued and executed by the
  existing threads as soon as the running threads complete their execution.

If you have N queues, you can set the maximum number of workers
as `(maxPoolSize + queueCapacity - N)`.

{: .warning}
In this context, N threads are allocated for polling queues, but this is not a correct number when *
*priority** is used.

The number of message pollers is determined by the sum of the following:

1. Number of unique priority groups.
2. Number of queues with specified priorities (e.g., `"critical=5,high=2"`).
3. Number of queues without priority.

If you prefer not to delve into the calculations, you can set the following:

- `queueCapacity >= 2 * number of queues`
- `maxPoolSize >= 2 * number of queues`
- `corePoolSize >= number of queues`
-

{: .note}
Setting a non-zero `queueCapacity` can indeed lead to duplicate message problems. This occurs
because polled messages that are waiting to be executed might have their `visibilityTimeout` expire,
causing another message listener to pull the same message again. This scenario can result in
duplicate processing of messages, which can impact the correctness of your application's logic. To
mitigate this issue, it's crucial to carefully configure `queueCapacity` and `visibilityTimeout`
settings to ensure that messages are processed correctly without duplication.

## Manual start of the container

When using a container that starts automatically and offers graceful shutdown, you can control its
automatic startup behavior using the `autoStartup` flag. If `autoStartup` is set to `false`, then
your application needs to manually call the `start` and `stop` methods of the container to control
its lifecycle. Additionally, for a graceful shutdown, you should call the `destroy` method when
appropriate. This gives you finer control over when the container starts and stops within your
application's lifecycle.

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

## Message converters configuration

To configure the message converter, you can only use application configuration by specifying the
property `rqueue.message.converter.provider.class=com.example.MyMessageConverterProvider`. This
approach allows you to customize message conversion behavior using your own implementation
of `org.springframework.messaging.converter.MessageConverter`. Typically, this customization ensures
that messages can be converted to and from various formats smoothly within your application.

{: .note}
MyMessageConverterProvider class must
implement `com.github.sonus21.rqueue.converter.MessageConverterProvider` interface.

```java
class MyMessageConverterProvider implements MessageConverterProvider {

  @Override
  public MessageConverter getConverter() {
    // here any message converter can be returned except null 
    return new MyMessageConverter();
  }
}
```

The default implementation, `DefaultMessageConverterProvider`,
returns `DefaultRqueueMessageConverter`. While `DefaultRqueueMessageConverter` can handle encoding
and decoding for most messages, it may encounter issues when message classes are not shared across
applications. To avoid sharing classes as JAR files, you can opt for converters such
as `com.github.sonus21.rqueue.converter.JsonMessageConverter`
or `org.springframework.messaging.converter.MappingJackson2MessageConverter`. These converters
serialize messages into JSON format, facilitating interoperability without shared class
dependencies.

Additionally, alternatives like MessagePack or ProtoBuf can also be employed based on specific
requirements for message serialization and deserialization. Each of these options provides
flexibility in how messages are encoded and decoded across different systems and applications.

## Additional Configuration

- **rqueue.retry.per.poll**: This setting determines how many times a polled message should be
  retried before declaring it dead or moving it back into the queue for subsequent retries. The
  default value is `1`, meaning a message will be processed once initially, and if it fails, it will
  be retried on the next poll. If you increase this value to `N`, the polled message will be retried
  consecutively N times before it is considered failed and made available for other listeners to
  process.

This configuration allows you to control how many times Rqueue attempts to process a message before
handling it as a failed message, giving you flexibility in managing message retries and error
handling strategies.


