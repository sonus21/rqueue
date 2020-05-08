<img align="left" src="https://raw.githubusercontent.com/sonus21/rqueue/master/rqueue/src/main/resources/public/rqueue/img/android-chrome-192x192.png" alt="Rqueue Logo" width="110">

# Rqueue: Redis Queue,Task Queue, Delayed Queue for Spring and Spring Boot
 
[![Build Status](https://travis-ci.org/sonus21/rqueue.svg?branch=master)](https://travis-ci.org/sonus21/rqueue)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)
![Maven Central](https://img.shields.io/maven-central/v/com.github.sonus21/rqueue)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring framework's messaging library backed by Redis.

### Some of the features

* A message can be delayed for an arbitrary period of time or delivered immediately.
* Multiple messages can be consumed in parallel by different workers.
* Message delivery: It's guaranteed that a message is consumed **at least once**.  (Message would be consumed by a worker more than once due to the failure in the underlying worker/restart-process etc, otherwise exactly one delivery)
* Support Redis cluster
* Queue metrics
* Different Redis connection for application and worker
* Web interface for queue management and  queue stats
* Automatic message serialization and deserialization



### Adding a task
Rqueue supports two types of tasks.
1. Execute tasks as soon as possible
2. Delayed tasks (task that would be scheduled at given delay, run task at 2:30PM)


### Task execution  configuration
Task execution can be configured in different ways
1. By default a tasks would be retried for Integer.MAX_VALUE number of times
2. If we do not need retry then we need to set retry count to zero
3. After retrying/executing a task N (>=1) times if we can't execute the given task then the task can be discarded or push to dead-letter-queue

## Usage

### Application configuration

#### Spring-boot

Add Dependency

* Get latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter)
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring-boot-starter:2.0.0-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
        <groupId>com.github.sonus21</groupId>
        <artifactId>rqueue-spring-boot-starter</artifactId>
        <version>2.0.0-RELEASE</version>
    </dependency>
    ```
    
#### Spring framework

1. Add Dependency
    Get latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring)
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring:2.0.0-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
       <groupId>com.github.sonus21</groupId>
       <artifactId>rqueue-spring</artifactId>
       <version>2.0.0-RELEASE</version>
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


### Worker/Task executor
A  method can be marked as worker/message listener using `RqueueListener` annotation


```java
@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "simple-queue")
  public void simpleMessage(String message) {
    log.info("simple-queue: {}", message);
  }

  // Scheduled Job notification
  @RqueueListener(value = "job-queue",
   numRetries="3", deadLetterQueue="failed-job-queue")
  public void onMessage(Job job) {
    log.info("Job created: {}", job);
  }
  
  // Scheduled push notification
  @RqueueListener(value = "push-notification-queue", 
  numRetries="3", deadLetterQueue="failed-notification-queue")
  public void onMessage(Notification notification) {
    log.info("Notification message: {}", notification);
  }
  
  // asynchronously send otp to the user
  @RqueueListener(value = "otp")
  public void onMessage(Otp otp) {
    log.info("Otp message: {}", otp);
  }
}
```

### Message publishing or task submission
All messages can be send using `RqueueMessageSender` bean's methods. It has handful number of `enqueue` and `enqueueIn` methods, we can use one of them based on the use case.


```java
public class MessageService {
  @AutoWired private RqueueMessageSender rqueueMessageSender;
  
  public void doSomething(){
    rqueueMessageSender.enqueue("simple-queue", "Rqueue is configured");
  }
  
  public void createJOB(Job job){
    //do something
    rqueueMessageSender.enqueue("job-queue", job);
  }
  
  // send notification in 30 seconds
   public void sendNotification(Notification notification){
    //do something
    rqueueMessageSender.enqueueIn("notification-queue", notification, 30*1000L);
  }
}
```

#### ** Key points: **
* A task would be retried without any further configuration
* Method arguments are handled automatically, as in above example even task of `Job` type can be executed by workers.


#### Demo Applications

1. [Rqueue Spring Boot-1](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example) 
2. [Rqueue Spring Boot-2](https://github.com/sonus21/rqueue-task-exector) 
2. [Rqueue Spring ](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example) 

## Advance Configuration

Apart from basic configuration, it can be customized heavily, like number of tasks it would be executing concurrently.
More and more configurations can be provided using `SimpleRqueueListenerContainerFactory` class.

```java
class Application{
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(){
    // return SimpleRqueueListenerContainerFactory object
  } 
}
```

---
**Configure Task store**  
    All tasks are stored in Redis database, either we can utilise the same Redis database for entire application or we can provide a separate one for task store. Nonetheless if we need different database then we can configure using `setRedisConnectionFactory` method.  
    **It's highly recommended to provide master connection as it reads and writes to the Redis database**
    
    
```java
  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory(RqueueMessageHandler rqueueMessageHandler){
    // Create redis connection factory of your choice either Redission or Lettuce or Jedis 
     // Get redis configuration 
     RedisConfiguration redisConfiguration  = new RedisConfiguration();
     // Set fields of redis configuration
     // Create lettuce connection factory
     LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(redisConfiguration);
     factory.setRedisConnectionFactory(lettuceConnectionFactory);
     facory.setRqueueMessageHandler(rqueueMessageHandler);
     return factory;
  }
``` 
---

**Redis connection failure and retry**  

Whenever a call to Redis failed then it would be retried in 5 seconds,  to change that we can set back off to some different value. 

```java
// set backoff time to 100 milli second
factory.setBackOffTime( 100 );
```

---

**Task executor**  
  
Number of workers can be configured using  `setMaxNumWorkers` method. For example to configure 10 workers we can do

```java
SimpleRqueueListenerContainerFactory factory =  new SimpleRqueueListenerContainerFactory();
factory.setMaxNumWorkers(10);
```

By default number of task executors are twice of the number of queues. A custom or shared task executor can be configured using factory's `setTaskExecutor` method, we need to provide an implementation of AsyncTaskExecutor


```java
ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
threadPoolTaskExecutor.setThreadNamePrefix( "taskExecutor" );
threadPoolTaskExecutor.setCorePoolSize(10);
threadPoolTaskExecutor.setMaxPoolSize(50);
threadPoolTaskExecutor.setQueueCapacity(0);
threadPoolTaskExecutor.afterPropertiesSet();
factory.setTaskExecutor(threadPoolTaskExecutor);
```

---
**Manual/Auto start of the container**
Whenever container is refreshed or application is started then it is be started automatically and it  comes with a graceful shutdown.  
Automatic start of the container can be controlled using `autoStartup` flag, when autoStartup is false then application must call start and stop  
method of container to start and stop the container, for further graceful shutdown you should call destroy method as well.

```java
factory.setAutoStartup(false);

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

**Message converters configuration**  
Generally any message can be converted to and from without any problems, though it can be customized by providing an implementation `org.springframework.messaging.converter.MessageConverter`, this message converter must implement both the methods of `MessageConverter` interface.
Implementation must make sure the return type of method `toMessage` is `Message<String>` while as in the case of `fromMessage` a object can be returned as well.


```java
MessageConverter messageConverter = new SomeFancyMessageConverter();
List<MessageConverter> messageConverters = new ArrayList<>();
messageConverters.add(messageConverter);
factory.setMessageConverters(messageConverters);
```
   
More than one message converter can be used as well, when more than one message converters are provided then they are used in the order, whichever returns **non null** value is used.

## Monitoring Queue Statistics
NOTE: Rqueue support micrometer library for monitoring. 

**It provides 4 types gauge of metrics.**
```
1. queue.size : number of tasks to be run
2. dead.letter.queue.size : number of tasks in the dead letter queue
3. delayed.queue.size : number of tasks scheduled for later time, it's an approximate number, since some tasks might not have moved to be processed despite best efforts
4. processing.queue.size : number of tasks are being processed. It's also an approximate number due to retry and tasks acknowledgements.
```

**Execution and failure counters can be enabled (by default this is disabled).**

We need to set count.execution and count.failure fields of RqueueMetricsProperties
```
1. execution.count
2. failure.count 
```

All these metrics are tagged 

**Spring Boot Application**
1.  Add micrometer and the exporter dependencies
2.  Set tags if any using `rqueue.metrics.tags.<name> = <value>`
3.  Enable counting features using `rqueue.metrics.count.execution=true`, `rqueue.metrics.count.failure=true` 

**Spring Application**

1. Add micrometer and the exporter dependencies provide MeterRegistry as bean
2. Provide bean of RqueueMetricsProperties, in this bean set all the required fields.


[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)


## Web Interface
* Queue stats (Scheduled, waiting to run, running, moved to dead letter queue)
* Latency:  Min/Max and Average task execution time
* Queue Management: Move tasks from one queue to another
* Delete enqueued tasks

Link: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

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

## Support
Please report problem, bug or feature(s) to [issue](https://github.com/sonus21/rqueue/issues/new/choose) tracker. You are most welcome for any pull requests for feature, issue or enhancement.

## License
© [Sonu Kumar](mailto:sonunitw12@gmail.com) 2019-Instant.now

The Rqueue is released under version 2.0 of the Apache License.

    
