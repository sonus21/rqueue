---
layout: default
title: Redis Configuration
parent: Configuration
nav_order: 1
---

# Redis Configuration

You can configure your application to use separate Redis connections for Rqueue and other
application needs. This approach ensures complete isolation between the Redis connections and their
usage, providing distinct environments for storing Rqueue tasks and transient states, as well as
other application data.

By configuring separate Redis connections:

- Rqueue can manage its tasks and transient states in Redis without interference from other
  application components.
- Your main application can utilize a separate Redis connection for its own data storage and
  retrieval needs.

This isolation helps in managing resources efficiently and prevents potential conflicts or
performance issues that might arise from shared Redis usage across different parts of your
application.

## Redis Connection Configuration

As an application developer using Rqueue, you have the flexibility to specify which Redis cluster or
server should be used for storing Rqueue data. This is achieved by configuring the Redis connection
factory via the container factory's method `setRedisConnectionFactory`.

Here's how you can specify the Redis connection for Rqueue:

1. Implement or configure a `RedisConnectionFactory` that points to your desired Redis cluster or
   server.

2. Set this connection factory using the `setRedisConnectionFactory` method of your Rqueue container
   factory.

This approach allows you to control where Rqueue stores its data within Redis, ensuring it aligns
with your application's Redis configuration and deployment requirements.

{: .warning }

When creating a Redis connection factory for Rqueue, it's essential to set `readFrom`
to `MASTER_PREFERRED`. This ensures that the application starts correctly and operates as expected
with the Redis setup.

### Standalone Redis

 ```java

@Configuration
public class RqueueConfiguration {
  // this property must be set to true if you're using webflux or reactive redis
  @Value("${rqueue.reactive.enabled:false}")
  private boolean reactiveEnabled;

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    LettuceClientConfiguration lettuceClientConfiguration =
        LettuceClientConfiguration.builder().readFrom(ReadFrom.MASTER_PREFERRED).build();

    // Stand alone redis configuration, Set fields of redis configuration
    RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration();
    // set properties of redis configuration as you need. 

    // Create lettuce connection factory
    LettuceConnectionFactory redisConnectionFactory = new LettuceConnectionFactory(redisConfiguration, lettuceClientConfiguration);
    redisConnectionFactory.afterPropertiesSet();

    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.setRedisConnectionFactory(redisConnectionFactory);
    // if reactive redis is enabled set the correct connection factory
    if (reactiveEnabled) {
      factory.setReactiveRedisConnectionFactory(lettuceConnectionFactory);
    }
    // set other properties as you see
    return factory;
  }
}
 ``` 

### Redis Cluster

For Redis clusters, it's recommended to use the Lettuce client because Jedis does not
support `EVALSHA` requests, which are often used for efficient script execution in Redis
environments.

```java

@Configuration
public class RqueueConfiguration {
  // this property must be set to true if you're using webflux or reactive redis
  @Value("${rqueue.reactive.enabled:false}")
  private boolean reactiveEnabled;

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    // here always use MASTER_PREFERRED otherwise it will to start
    LettuceClientConfiguration lettuceClientConfiguration =
        LettuceClientConfiguration.builder().readFrom(ReadFrom.MASTER_PREFERRED).build();

    // add all nodes
    RedisClusterConfiguration redisClusterConfiguration = new RedisClusterConfiguration();
    List<RedisNode> redisNodes = new ArrayList<>();
    redisNodes.add(new RedisNode("127.0.0.1", 9000));
    redisNodes.add(new RedisNode("127.0.0.1", 9001));
    redisNodes.add(new RedisNode("127.0.0.1", 9002));
    redisNodes.add(new RedisNode("127.0.0.1", 9003));
    redisNodes.add(new RedisNode("127.0.0.1", 9004));
    redisNodes.add(new RedisNode("127.0.0.1", 9005));
    redisClusterConfiguration.setClusterNodes(redisNodes);

    // create lettuce connection factory
    LettuceConnectionFactory lettuceConnectionFactory =
        new LettuceConnectionFactory(redisClusterConfiguration, lettuceClientConfiguration);
    lettuceConnectionFactory.afterPropertiesSet();


    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(lettuceConnectionFactory);

    // set polling interval, by default its 5 seconds
    simpleRqueueListenerContainerFactory.setPollingInterval(Constants.ONE_MILLI);

    // if reactive redis is enabled set the correct connection factory
    if (reactiveEnabled) {
      simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(
          lettuceConnectionFactory);
    }

    // set any other property if you need

    // return connection factory
    return simpleRqueueListenerContainerFactory;
  }
}
```

### Redis Sentinel

Redis sentinel can be configured similar to Standalone redis.

```java

@Configuration
public class RedisClusterBaseApplication {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    LettuceClientConfiguration lettuceClientConfiguration =
        LettuceClientConfiguration.builder().readFrom(ReadFrom.MASTER_PREFERRED).build();

    // Sentinel redis configuration, Set fields of redis configuration
    RedisSentinelConfiguration redisConfiguration = new RedisSentinelConfiguration();

    // Create lettuce connection factory
    LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(redisConfiguration, lettuceClientConfiguration);
    lettuceConnectionFactory.afterPropertiesSet();

    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(lettuceConnectionFactory);
    simpleRqueueListenerContainerFactory.setPollingInterval(Constants.ONE_MILLI);
    if (reactiveEnabled) {
      simpleRqueueListenerContainerFactory.setReactiveRedisConnectionFactory(
          lettuceConnectionFactory);
    }
    return simpleRqueueListenerContainerFactory;
  }
}
```

## Redis connection failure and retry

To adjust the retry interval for failed Redis commands in Rqueue, you can configure the backoff time
to a different value. By default, Rqueue retries failed commands after 5 seconds. You can customize
this behavior by setting the backoff time to your desired interval. This ensures that if a Redis
command fails, Rqueue will retry the command after the specified backoff period before attempting
again.

 ```java
 class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    //...
    // set back off time to 100 milli second
    factory.setBackOffTime(100);
    return factory;
  }
}
 ```

## Redis Key configuration

Rqueue utilizes multiple Redis data types such as `SET`, `ZSET`, and `LIST`. It's crucial not to
delete or modify any Rqueue-related Redis keys, as a single mistake could result in the inadvertent
deletion of all tasks. To mitigate this risk, Rqueue prefixes all of its keys with specific
identifiers:

- **`rqueue.key.prefix`**: Prefix for every key used by Rqueue.
- **`rqueue.cluster.mode`**: Indicates whether your Redis database operates in cluster mode or not.
  By default, Rqueue assumes cluster mode. Switching from non-cluster to cluster mode can lead
  to `Cross Slot` errors due to the use of Lua scripts for atomic operations.
- **`rqueue.simple.queue.prefix:queue`**: Prefix used for the simple queue (`LIST`). By default, it
  uses `queue`, resulting in keys like `__rq::queue::XYZ`.
- **`rqueue.scheduled.queue.prefix`**: Prefix used for the delayed queue (`ZSET`). Default
  configuration uses `d-queue::`, resulting in keys like `__rq::d-queue::XYZ`.
- **`rqueue.scheduled.queue.channel.prefix`**: Prefix for Redis pub/sub channel used when moving
  messages from the scheduled queue to the simple queue for processing. Default value
  is `p-channel`.
- **`rqueue.processing.queue.name.prefix`**: Prefix used for the processing queue (`ZSET`). By
  default, it uses `p-queue::`, resulting in keys like `__rq::p-queue::XYZ`. This queue ensures
  at-least-once message delivery.
- **`rqueue.processing.queue.channel.prefix`**: Prefix for Redis pub/sub channel used when moving
  messages from the processing queue back to the origin queue, triggered if a listener stops
  unexpectedly. Default value is `p-channel`.
- **`rqueue.queues.key.suffix`**: Key suffix used for storing all active queues. Default value
  is `queues`.
- **`rqueue.lock.key.prefix`**: Prefix used for locking keys. Default value is `lock::`.
- **`rqueue.queue.stat.key.prefix`**: Prefix used for storing queue metrics. Default value
  is `q-stat`.
- **`rqueue.queue.config.key.prefix`**: Prefix used for storing listener configuration. Default
  value is `q-config`.

These prefixes help Rqueue manage its interactions with Redis effectively while minimizing the risk
of unintended data loss or corruption.