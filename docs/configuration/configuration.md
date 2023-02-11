---
layout: default
title: Configuration
nav_order: 2
has_children: true
permalink: configuration
---


# Configuration
{: .no_toc }

Rqueue has many configuration settings that can be configured either using application config or code.
{: .fs-6 .fw-300 }

## Table of contents
{: .no_toc .text-delta }

1. TOC
{:toc}

---
Apart from the basic configuration, it can be customized heavily, like number of tasks it would be
executing concurrently. More and more configurations can be provided using 
`SimpleRqueueListenerContainerFactory` class. See SimpleRqueueListenerContainerFactory [doc](https://javadoc.io/doc/com.github.sonus21/rqueue-core/latest/com/github/sonus21/rqueue/config/SimpleRqueueListenerContainerFactory.html) for more configs.

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
By default, the number of task executors are twice the number of queues. A custom or shared task 
executor can be configured using factory's `setTaskExecutor` method. It's also possible to provide 
queue concurrency using `RqueueListener` annotation's field `concurrency`. The concurrency could be
some positive number like 10, or range 5-10. If queue concurrency is provided then each queue will 
use their own task executor to execute consumed messages, otherwise a shared task executor is used 
to execute tasks. A global number of workers can be configured using  `setMaxNumWorkers` method.
`RqueueListener` annotation also has `batchSize` field, the default values are as, 
listener having concurrency set will fetch 10 messages while other 1.

{: .note}
Increasing batch size has its consequences, if your thread pool size is too low in that case
you would see many processing jobs since task would be rejected by executor unless you've configured
large queueCapacity.

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

When a custom executor is provided, then you must set MaxNumWorkers correctly, otherwise thread pool
might be over or under utilised. Over utilization of thread pool is not possible, it will reject new
tasks, this will lead to delay in message consumption, under utilization can be handled as

```
ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
threadPoolTaskExecutor.setThreadNamePrefix( "ListenerExecutor" );
threadPoolTaskExecutor.setCorePoolSize(corePoolSize);
threadPoolTaskExecutor.setMaxPoolSize(maxPoolSize);
threadPoolTaskExecutor.setQueueCapacity(queueCapacity);
threadPoolTaskExecutor.afterPropertiesSet();
factory.setTaskExecutor(threadPoolTaskExecutor);
```

In this configuration there are three variables `corePoolSize`, `maxPoolSize` and `queueCapacity`.

* `corePoolSize` signifies the lower limit of active threads.  
* `maxPoolSize` signifies the upper limit of active threads.
* `queueCapacity` signify even though we have `maxPoolSize` running threads we can have 
`queueCapacity` tasks waiting in the queue, that can be dequeue and executed by the existing thread 
as soon as the running threads complete the execution.

If you have N queues then you can set maximum number of workers as `(maxPoolSize + queueCapacity - N )`

{: .warning}
Here N threads are provided for polling queues, this is not a correct number when **priority** is
used.

The number of message pollers would be sum of the followings.

1. Number of unique priority groups.
2. Number of queues whose priority is provided as `"critical=5,high=2"`.
3. Number of queues without priority.

If you don't want to go into the maths, then you can set

* queueCapacity >= 2 * number of queues
* maxPoolSize >= 2 * number of queues
* corePoolSize >= number of queues

{: .note}
Whenever you set queue capacity to non-zero then it can create duplicate message problem,
since the polled messages are just waiting to be executed, if visibilityTimeout expires than other
message listener will pull the same message.

## Manual start of the container

Whenever container is refreshed or application is started then it is started automatically, it also
comes with a graceful shutdown. Automatic start of the container can be controlled
using `autoStartup` flag, when autoStartup is false then application must call start and stop
methods of container. For further graceful shutdown application should call destroy method as well.

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

Generally any message can be converted to and from without any problems, though it can be customized
by providing an implementation `org.springframework.messaging.converter.MessageConverter`, this
message converter must implement both the methods of `MessageConverter` interface. Implementation
must make sure the return type of method `toMessage` is `Message<String>` while as in the case
of `fromMessage` any object can be returned as well.

We can configure message converter only using application configuration using property
`rqueue.message.converter.provider.class=com.example.MyMessageConverterProvider`
{: .note}
MyMessageConverterProvider class must implement 
`com.github.sonus21.rqueue.converter.MessageConverterProvider` interface.

```java
class MyMessageConverterProvider implements MessageConverterProvider {

  @Override
  public MessageConverter getConverter() {
    // here any message converter can be returned except null 
    return new MyMessageConverter();
  }
}
```

The default implementation is `DefaultMessageConverterProvider`, ths converter
returns `DefaultRqueueMessageConverter`. DefaultRqueueMessageConverter can encode/decode most of the
messages, but it will have problem when message classes are not shared across application. If you do
not want to share class as jar files then you can
use `com.github.sonus21.rqueue.converter.JsonMessageConverter`
or `org.springframework.messaging.converter.MappingJackson2MessageConverter`  these converters
produce `JSON` data. Other implementation can be used as well MessagePack, ProtoBuf etc

## Additional Configuration

- **rqueue.retry.per.poll** : The number of times, a polled message should be tried before declaring it
  dead or putting it back in the simple queue. The default value is `1`, that means a message would
  be executed only once and next execution will happen on next poll. While if we increase this
  to `N` then the polled message would be tries consecutively N times before it will be made
  available for other listeners.
- 


