---
layout: default
title: Redis Configuration
parent: Configuration
nav_order: 1
---

# Redis Configuration

You can configure Rqueue to use a dedicated Redis connection, separate from your main 
application's Redis usage. This isolation ensures that Rqueue's task storage and 
transient states do not interfere with other application data, helping to manage 
resources efficiently and prevent performance bottlenecks.

## Customizing the Redis Connection

Rqueue provides the flexibility to specify exactly which Redis server or cluster should 
be used for task storage. This is done by providing a `RedisConnectionFactory` to the 
container factory using the `setRedisConnectionFactory` method.

To configure a custom connection:
1. Define a `RedisConnectionFactory` bean pointing to your target Redis instance.
2. Register this factory with your `SimpleRqueueListenerContainerFactory`.

{: .warning }
When creating a `LettuceConnectionFactory` for Rqueue, it is essential to set 
`readFrom` to `MASTER_PREFERRED`. This ensures correct operation, especially in 
clustered environments.

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

### Redis Cluster Configuration

For Redis Clusters, using the **Lettuce** client is recommended. Lettuce 
supports `EVALSHA` requests, which are essential for the efficient execution 
of the Lua scripts Rqueue uses.

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

### Redis Sentinel Configuration

Configuring Redis Sentinel is similar to the standalone configuration.

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

## Redis Connection Failures and Retries

You can configure the retry interval for failed Redis commands. By default, Rqueue 
retries failed operations after a 5-second backoff. This can be customized via the 
`setBackOffTime` method.

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

## Redis Key Naming and Prefixes

Rqueue uses various Redis data structures like `LIST` (for simple queues), `ZSET` (for 
scheduled and processing queues), and `SET`. It is critical not to manually delete or 
modify Rqueue-managed keys.

To prevent collisions and facilitate management, Rqueue uses several configurable 
prefixes. All keys are by default prefixed with `__rq::`.

### Key Configuration Properties

- **`rqueue.key.prefix`**: The root prefix for every key used by Rqueue.
- **`rqueue.cluster.mode`**: Specifies if Redis is in cluster mode (default: `true`). 
  Incorrect settings can lead to `Cross Slot` errors during Lua script execution.
- **`rqueue.simple.queue.prefix`**: Prefix for the `LIST` based queue. Default: `queue`.
- **`rqueue.scheduled.queue.prefix`**: Prefix for the `ZSET` based scheduled queue. 
  Default: `d-queue::`.
- **`rqueue.scheduled.queue.channel.prefix`**: Redis Pub/Sub channel for scheduled 
  messages. Default: `p-channel`.
- **`rqueue.processing.queue.name.prefix`**: Prefix for the `ZSET` processing queue, 
  ensuring at-least-once delivery. Default: `p-queue::`.
- **`rqueue.processing.queue.channel.prefix`**: Channel for moving messages from 
  processing back to the origin queue. Default: `p-channel`.
- **`rqueue.queues.key.suffix`**: Suffix for the key storing all active queues. 
  Default: `queues`.
- **`rqueue.lock.key.prefix`**: Prefix for distributed locks. Default: `lock::`.
- **`rqueue.queue.stat.key.prefix`**: Prefix for queue metrics. Default: `q-stat`.
- **`rqueue.queue.config.key.prefix`**: Prefix for listener configurations. 
  Default: `q-config`.