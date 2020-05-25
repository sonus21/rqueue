<div>
   <img  align="left" src="https://raw.githubusercontent.com/sonus21/rqueue/master/rqueue/src/main/resources/public/rqueue/img/android-chrome-192x192.png" alt="Rqueue Logo" width="90">
   <h1 style="float:left">Rqueue: Redis Queue,Task Queue, Delayed Queue for Spring and Spring Boot</h1>
</div>

[![Build Status](https://travis-ci.org/sonus21/rqueue.svg?branch=master)](https://travis-ci.org/sonus21/rqueue)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.github.sonus21/rqueue)](https://repo1.maven.org/maven2/com/github/sonus21/rqueue)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

### Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring framework's messaging library backed by Redis. It can be used as message broker as well, where all services code is in Spring.

## Features

* A message can be delayed for an arbitrary period or delivered immediately.
* Multiple messages can be consumed in parallel by different workers.
* Message delivery: It's guaranteed that a message is consumed **at least once**.  (Message would be consumed by a worker more than once due to the failure in the underlying worker/restart-process etc, otherwise exactly one delivery)
* Support Redis cluster
* Queue metrics
* Different Redis connection for application and worker
* Web interface for queue management and queue statistics
* Automatic message serialization and deserialization
* Queue concurrency
* Group level queue priority(weighted and strict)
* Sub queue priority(weighted and strict)
* Task execution back off, exponential and fixed back off (default fixed back off)
* Callbacks for different actions
* Events 1. Bootstrap event 2. Task execution event.

#### NOTE: For Redis cluster, it's required to provide MASTER [connection](https://github.com/sonus21/rqueue/issues/19)

## Configuration

### Spring-boot

* Get the latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter)
* Add dependency
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring-boot-starter:2.0.1-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
        <groupId>com.github.sonus21</groupId>
        <artifactId>rqueue-spring-boot-starter</artifactId>
        <version>2.0.1-RELEASE</version>
    </dependency>
    ```
    
### Spring framework

* Get the latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring)
* Add Dependency
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring:2.0.1-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
       <groupId>com.github.sonus21</groupId>
       <artifactId>rqueue-spring</artifactId>
       <version>2.0.1-RELEASE</version>
     </dependency>
    ```
    
2. Add annotation `EnableRqueue` on application config class
3. Provide a RedisConnectionFactory bean

```java
@EnableRqueue
public class Application{
  @Bean
  public RedisConnectionFactory redisConnectionFactory(){
    // return a redis connection factory
  }
}

```

## Message publishing/Task submission
Rqueue supports two types of tasks.
1. Execute tasks as soon as possible
2. Delayed tasks (task that would be scheduled at given time or run in 5 minutes)

All messages need to be sent using `RqueueMessageSender` bean's `enqueueXXX`, `enqueueInXXX` and `enqueueAtXXX` methods.
It has handful number of `enqueue`, `enqueueIn`, `enqueueAt` methods, we can use one of them based on the use case.

```java
public class MessageService {
  @AutoWired private RqueueMessageSender rqueueMessageSender;
  
  public void doSomething(){
    rqueueMessageSender.enqueue("simple-queue", "Rqueue is configured");
  }
  
  public void createJOB(Job job){
    rqueueMessageSender.enqueue("job-queue", job);
  }
  
  // send notification in 30 seconds
   public void sendNotification(Notification notification){
    rqueueMessageSender.enqueueIn("notification-queue", notification, 30*1000L);
  }
  
  // enqueue At example
  public void createInvoice(Invoice invoice, Instant instant){
    rqueueMessageSender.enqueueAt("invoice-queue", invoice, instant);
  }
  
  // enqueue with priority, when sub queues are used as explained in the queue priority section.
  enum SmsPriority{
      CRITICAL("critical"),
      HIGH("high"),
      MEDIUM("medium"),
      LOW("low");
      private String value;
  } 
  public void sendSms(Sms sms, SmsPriority priority){
    rqueueMessageSender.enqueueWithPriority("sms-queue", priority.value(), sms);
  }
}
```


## Worker/Consumer/Task executor/Listener
Any method that's part of spring bean, can be marked as worker/message listener using `RqueueListener` annotation

**Task execution policy:**

1. By default, all tasks would be retried for Integer.MAX_VALUE number of times, queue having dead letter queue set would be retried for 3 times, if retry count is not provided.
2. If we do not need any retry then we need to set retry count to zero
3. After retrying/executing a task N (>=1) times if we can't execute the given task then the task can be discarded or push to dead-letter-queue


```java
@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "simple-queue")
  public void simpleMessage(String message) {
    log.info("simple-queue: {}", message);
  }

  @RqueueListener(value = "job-queue", numRetries="3", 
    deadLetterQueue="failed-job-queue", concurrency="5-10")
  public void onMessage(Job job) {
    log.info("Job alert: {}", job);
  }
  
  @RqueueListener(value = "push-notification-queue",numRetries="3", 
    deadLetterQueue="failed-notification-queue")
  public void onMessage(Notification notification) {
    log.info("Push notification: {}", notification);
  }
  
  @RqueueListener(value = "sms", priority="critical=10,high=8,medium=4,low=1")
  public void onMessage(Sms sms) {
    log.info("Sms : {}", sms);
  }
  
  @RqueueListener(value = "chat-indexing", priority="20", priorityGroup="chat")
  public void onMessage(ChatIndexing chatIndexing) {
    log.info("ChatIndexing message: {}", chatIndexing);
  }
  
  @RqueueListener(value = "chat-indexing-daily", priority="10", priorityGroup="chat")
  public void onMessage(ChatIndexing chatIndexing) {
    log.info("ChatIndexing message: {}", chatIndexing);
  }
  
  @RqueueListener(value = "chat-indexing-weekly", priority="5", priorityGroup="chat")
  public void onMessage(ChatIndexing chatIndexing) {
    log.info("ChatIndexing message: {}", chatIndexing);
  }
}
```

** Key points: **
* A task would be retried without any further configuration
* Method arguments are handled automatically, as in above example even task of `Job` type can be executed by workers.

## Queue Priority
Rqueue supports two types of priority

1. Weighted 
2. Strict

You need to set `priorityMode` in container factory either `STRICT` or `WEIGHTED`.


Priority needs to be specified using annotation's field `priority`, priority can be grouped for multiple queues as well,
by default any queue having priority is added to the default priority group.
Queue can be classified into multiple priority groups as well for example sms queue has 4 types of sub queues
`critical`, `high`, `medium` and `low` their priority needs to be specified as `priority="critical=10, high=6, medium=4,low=1"`.

If you want to use simple priority mechanism, then you need to set priority to some number and their group, for example. 

`priority="60"`
`priorityGroup="critical-group"`

Let's say there're three queues 

|Queue|Priority|
|-----|--------|
|Q1   | 6      |
|Q2   | 4      |
|Q3   | 2      | 

#### Weighted Priority
Weighted priority works in round robin fashion, in this example Q1 would be polled 6 times, Q2 4 times and Q3 2 times and again this process repeats.  After polling if it's found that any queue does not have more jobs than their weight is reduced by Δ e.g Q1 does not have any item then it's weight is reduces by `Δ = currentWeight * (1-(6/(6+4+2))`. As soon as the weight becomes <= 0, queue is inactive and weight is reinitialised when all queues become inactive.

This algorithm is implemented in [WeightedPriorityPoller](https://github.com/sonus21/rqueue/blob/652ec46c52dc6f06604d500c15ac11a8633eb602/rqueue/src/main/java/com/github/sonus21/rqueue/listener/WeightedPriorityPoller.java)

#### Strict Priority
In Strict priority case, poller would always first poll from the highest priority queue, that's Q1 here. After polling if it encounters that queue does not have any element than that queue becomes inactive for **polling interval**, to avoid starvation a queue can be inactive for maximum of 1 minute.


This algorithm is implemented in [StrictPriorityPoller](https://github.com/sonus21/rqueue/blob/652ec46c52dc6f06604d500c15ac11a8633eb602/rqueue/src/main/java/com/github/sonus21/rqueue/listener/StrictPriorityPoller.java)


## Monitoring Queue Statistics
NOTE: Rqueue support micrometer library for monitoring.

**It provides 4 types of gauge metrics.**

* **queue.size** : number of tasks to be run
* **dead.letter.queue.size** : number of tasks in the dead letter queue
* **delayed.queue.size** : number of tasks scheduled for later time, it's an approximate number, since some tasks might not have moved to be processed despite best efforts
* **processing.queue.size** : number of tasks are being processed. It's also an approximate number due to retry and tasks acknowledgements.


**Execution and failure counters can be enabled (by default this is disabled).**

We need to set count.execution and count.failure fields of RqueueMetricsProperties
```
1. execution.count
2. failure.count 
```

All these metrics are tagged

**Spring Boot Application**
1.  Add `micrometer` and the exporter dependencies
2.  Set tags if any using `rqueue.metrics.tags.<name> = <value>`
3.  Enable counting features using `rqueue.metrics.count.execution=true`, `rqueue.metrics.count.failure=true`

**Spring Application**

1. Add `micrometer` and the exporter dependencies provide `MeterRegistry` as bean
2. Provide a bean of `RqueueMetricsProperties`, in this bean set all the required fields.


[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)


## Web Interface
* Queue stats (Scheduled, waiting to run, running, moved to dead letter queue)
* Latency: Min/Max and Average task execution time
* Queue Management: Move tasks from one queue to another
* Delete enqueued tasks
* Enqueued tasks insights

Link: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

[![Execution Page](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)
[![Queues Page](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)
[![Explore Queue](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)
[![Running tasks](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)



**Configuration**
* Add resource handler to handle the static resources.
```java
  public class MvcConfig implements WebMvcConfigurer {

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    //...
    if (!registry.hasMappingForPattern("/**")) {
      registry.addResourceHandler("/**").addResourceLocations("classpath:/public/");
    }
  }
}
```

All paths are under `/rqueue/**`. for authentication add interceptor(s) that would check for the session etc.


## Example Applications

1. [Rqueue Spring Boot-1](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example) 
2. [Rqueue Spring Boot-2](https://github.com/sonus21/rqueue-task-executor) 
3. [Rqueue Spring](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example) 

## Advance Configuration

Apart from the basic configuration, it can be customized heavily, like number of tasks it would be executing concurrently.
More and more configurations can be provided using `SimpleRqueueListenerContainerFactory` class.

```java
class RqueueConfiguration{
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
    // return SimpleRqueueListenerContainerFactory object
  } 
}
```

---

### Task store/Redis Connection

All tasks are stored in Redis database. As an application developer, you have the flexibility to specify the redis connection to be used by Rqueue. 
You need to specify redis connection via container factory's method `setRedisConnectionFactory`.
    
    
```java
class RqueueConfiguration {
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    // StandAlone redis can use Jedis connection factory but for clustered redis, it's required to use Lettuce
    // because EVALSHA will not work with Jedis.
    
     // Stand alone redis configuration, Set fields of redis configuration
     RedisStandaloneConfiguration redisConfiguration = new RedisStandaloneConfiguration();
     
     // Redis cluster, set required fields
     RedisClusterConfiguration redisConfiguration = new RedisClusterConfiguration();
   
     // Create lettuce connection factory
     LettuceConnectionFactory redisConnectionFactory = new LettuceConnectionFactory(redisConfiguration);
     redisConnectionFactory.afterPropertiesset();
     factory.setRedisConnectionFactory(redisConnectionFactory);
     return factory;
  }
}
``` 

---

### Redis connection failure and retry  
Whenever a call to Redis failed then it would be retried in 5 seconds,  to change that we can set back off to some different value. 

```java
class RqueueConfiguration {
    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
        SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
        //...
        // set back off time to 100 milli second
        factory.setBackOffTime( 100 );
        return factory;
    }
}
```

### Task executor/Queue Concurrency
The number of task executors are twice of the number of queues.
A custom or shared task executor can be configured using factory's `setTaskExecutor` method,
we need to provide an implementation of `AsyncTaskExecutor` class. 
It's possible to provide queue concurrency using `RqueueListener` annotation's field `concurrency`.
The concurrency could be some positive number like 10, it could be in range as well 5-10. 
If queue concurrency is provided then each queue will use their own task executor to execute consumed tasks, otherwise a shared task executor is used to execute tasks.   

A global number of workers can be configured using  `setMaxNumWorkers` method.
                                                                    

```java
class RqueueConfiguration {
    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
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
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
        SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
        //...
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix( "taskExecutor" );
        threadPoolTaskExecutor.setCorePoolSize(10);
        threadPoolTaskExecutor.setMaxPoolSize(50);
        threadPoolTaskExecutor.setQueueCapacity(0);
        threadPoolTaskExecutor.afterPropertiesSet();
        factory.setTaskExecutor(threadPoolTaskExecutor);
        return factory;
    }
}
```

---

### Manual/Auto start of the container
Whenever container is refreshed or application is started then it is started automatically, it also comes with a graceful shutdown.
Automatic start of the container can be controlled using `autoStartup` flag, when autoStartup is false then application must call start and stop methods of container.
For further graceful shutdown application should call destroy method as well.

```java
class RqueueConfiguration {
    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
        SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
        //...
        factory.setAutoStartup(false);
        return factory;
    }
}
```

```java
public class BootstrapController {
  @Autowired private RqueueMessageListenerContainer rqueueMessageListenerContainer;
  // ...
  public void start(){
    // ...
    rqueueMessageListenerContainer.start();
  }
   public void stop(){
    // ...
    rqueueMessageListenerContainer.stop();
  }
   public void destroy(){
    // ...
    rqueueMessageListenerContainer.destroy();
  }
  //...
}
```

---

### Message converters configuration
Generally any message can be converted to and from without any problems, though it can be customized by providing an implementation `org.springframework.messaging.converter.MessageConverter`, this message converter must implement both the methods of `MessageConverter` interface.
Implementation must make sure the return type of method `toMessage` is `Message<String>` while as in the case of `fromMessage` a object can be returned as well.


```java
class RqueueConfiguration { 
    @Bean
    public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
        SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
        //...
        MessageConverter messageConverter = new SomeFancyMessageConverter();
        List<MessageConverter> messageConverters = new ArrayList<>();
        messageConverters.add(messageConverter);
        factory.setMessageConverters(messageConverters);
        return factory;
    }
}
```
   
More than one message converter can be used as well, when more than one message converters are provided then they are used in the order, whichever returns **non null** value is used.

## Migration from 1.x to 2.x
Set redis key **__rq::version=1** or add `rqueue.db.version=1` in the properties file, this is required to handle data present in the old queues.
It's safe to use version 2.x if existing queues do not have any tasks to process. Look for the results of following redis commands.
```
1. LLEN <queueName> 
2. ZCARD rqueue-delay::<queueName>
3. ZCARD rqueue-processing::<queueName>
```
If all of these commands gives zero then all tasks have been consumed.

## Support
Please report bug,question,feature(s) to [issue](https://github.com/sonus21/rqueue/issues/new/choose) tracker. You are most welcome for any pull requests for any feature/bug/enhancement.

## License
© [Sonu Kumar](mailto:sonunitw12@gmail.com) 2019-Instant.now

The Rqueue is released under version 2.0 of the Apache License.
