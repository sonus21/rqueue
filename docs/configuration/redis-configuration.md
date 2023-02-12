---
layout: default
title: Redis Configuration
parent: Configuration
nav_order: 1
---

# Redis Configuration

Rqueue stores tasks, and it's transient state in Redis. We can configure Application to use one
redis connection for Rqueue and another one for Application, this will allow complete isolation of
Redis connection and usage.

## Redis Connection Configuration

All Rqueue data is stored in Redis database. As an application developer, you have the flexibility to
specify which redis cluster/server should be used by Rqueue. You need to specify redis connection via
container factory's method `setRedisConnectionFactory`. 

{: .warning }

While creating redis connection factory you must use readFrom `MASTER_PREFERRED` otherwise application won't start.


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

For Redis cluster you should use Lettuce client as Jedis client does not support `EVALSHA` request.

```java
@Configuration
public  class RqueueConfiguration {
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
public class RedisClusterBaseApplication{

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

All Redis commands failure would be retried in 5 seconds, to change that we can set
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
