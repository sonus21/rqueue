---
layout: default
title: Redis Configuration
parent: Configuration
nav_order: 2
---

Rqueue stores tasks, and it's transient state in Redis. We can configure Application to use one
redis connection for Rqueue and another one for Application, this will allow complete isolation of
Redis connection and usage.

## Redis Connection Configuration

All tasks are stored in Redis database. As an application developer, you have the flexibility to
specify the redis connection to be used by Rqueue. You need to specify redis connection via
container factory's method `setRedisConnectionFactory`.

 ```java
 class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    // StandAlone redis can use Jedis connection factory but for clustered redis, it's required to use Lettuce
    // because EVALSHA will not work with Jedis.

    // Stand alone redis configuration, Set fields of redis configuration
    RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration();

    // Redis cluster, set required fields
    RedisClusterConfiguration redisConfiguration = new RedisClusterConfiguration();

    // Create lettuce connection factory
    LettuceConnectionFactory redisConnectionFactory = new LettuceConnectionFactory(
        redisConfiguration);
    redisConnectionFactory.afterPropertiesset();
    factory.setRedisConnectionFactory(redisConnectionFactory);
    return factory;
  }
}
 ``` 

## Redis connection failure and retry

Whenever a call to Redis failed then it would be retried in 5 seconds, to change that we can set
back off to some different value.

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

Rqueue uses multiple Redis data types like `SET`, `ZSET`, `LIST` etc. Generally, you should not
delete or change any Rqueue related redis keys. As developer mistake happens, one mistake can lead
to deletion of all tasks. Due to this Rqueue prefix all of it's key.

* `rqueue.key.prefix` : Prefix for every key used by Rqueue
* `rqueue.cluster.mode` : Whether your Redis database is cluster or not, by default it's assumed
  cluster mode. Rqueue uses `Lua` script for atomic operation, changing from non-cluster to cluster
  can lead to `Cross Slot` error.
* `rqueue.simple.queue.prefix:queue` : Prefix to be used for the simple queue (`LIST`), by default
  it used `queue`, so each key would look like `__rq::queue::XYZ`
* `rqueue.scheduled.queue.prefix` :  Prefix to be used for the delayed queue (`ZSET`), by default it's
  configured to use `d-queue::`, therefor each scheduled `ZSET` key would look
  like `__rq::d-queue::XYZ`
* `rqueue.scheduled.queue.channel.prefix`: Rqueue communicates with Redis using Redis pub/sub for some
  use cases, this channel is used when the message from scheduled queue must be moved to simple queue
  for processing. Default value is `p-channel`.
* `rqueue.processing.queue.name.prefix`:  Prefix to be used for the processing queue (`ZSET`), by
  default it's configured to use `p-queue::`, there for each processing `ZSET` key would look
  like `__rq::p-queue::XYZ`. This queue is used to provide at least once message delivery.
* `rqueue.processing.queue.channel.prefix`: Rqueue communicates with Redis using Redis pub/sub for
  some use cases, this channel is used when the message from processing queue must be moved to
  origin queue, this happens when the listener dies in between. Default value is `p-channel`.
* `rqueue.queues.key.suffix`: Key to be used fot storing all active queues, default value
  is `queues`.
* `rqueue.lock.key.prefix`: The key prefix to be used for locking, default value is `lock::`
* `rqueue.queue.stat.key.prefix` : The key prefix to be used for storing queue metrics, default
  value is `q-stat`.
* `rqueue.queue.config.key.prefix` : The key prefix to be used for storing listener configuration,
  default value is `q-config`.
