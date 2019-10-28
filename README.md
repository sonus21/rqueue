# Rqueue aka RedisQueue  
[![Build Status](https://travis-ci.org/sonus21/rqueue.svg?branch=master)](https://travis-ci.org/sonus21/rqueue)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)

Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring framework's messaging library backed by Redis.


* A message can be delayed for an arbitrary period of time or delivered immediately. 
* Multiple messages can be consumed in parallel by different workers.
* Message delivery: It's guaranteed that a message is consumed **at max once**.  (Message would be consumed by a worker at max once due to the failure in the worker and resumes process, otherwise exactly one delivery, there's no ACK mechanism like Redis)


### Adding a task
Rqueue supports two types of tasks.
1. Execute method as soon as possible
2. Delayed tasks (task that would be scheduled at given delay)


### Task execution  configuration
Task execution can be configured in different ways
1. By default a tasks would be retried for Integer.MAX_VALUE number of times
2. If we do not need retry then we need to set retry count to zero
3. After retrying/executing a task N (>=1) times if we can't execute the given task then the task can be discarded or push to dead-later-queue

## Usage

### Application configuration

#### Spring-boot

Add Dependency

* Get latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring-boot-starter)
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring-boot-starter:1.0-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
        <groupId>com.github.sonus21</groupId>
        <artifactId>rqueue-spring-boot-starter</artifactId>
        <version>1.0-RELEASE</version>
        <type>pom</type>
    </dependency>
    ```
    
#### Spring framework

1. Add Dependency
    Get latest one from [Maven central](https://search.maven.org/search?q=g:com.github.sonus21%20AND%20a:rqueue-spring)
    * Gradle
    ```groovy
        implementation 'com.github.sonus21:rqueue-spring:1.0-RELEASE'
    ```
    * Maven
    ```xml
     <dependency>
       <groupId>com.github.sonus21</groupId>
       <artifactId>rqueue-spring</artifactId>
       <version>1.0-RELEASE</version>
       <type>pom</type>
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

  @RqueueListener(value = "delayed-queue", delayedQueue = "true")
  public void delayedMessage(String message) {
    log.info("delayedMessage: {}", message);
  }
  
  @RqueueListener(value = "delayed-queue-2", delayedQueue = "true",
   numRetries="3", deadLaterQueue="failed-delayed-queue")
  public void delayedMessageWithDlq(String message) {
    log.info("delayedMessageWithDlq: {}", message);
  }
  
  @RqueueListener(value = "job-queue", delayedQueue = "true",
   numRetries="3", deadLaterQueue="failed-job-queue")
  public void onMessage(Job job) {
    log.info("Job created: {}", job);
  }
  
  @RqueueListener(value = "notification-queue", delayedQueue = "true", 
  numRetries="3", deadLaterQueue="failed-notification-queue")
  public void onMessage(Notification notification) {
    log.info("Notification message: {}", notification);
  }
}
```

### Message publishing or task submission
All messages can be send using `RqueueMessageSender` bean's methods. It has handful number of put methods, we can use one of them based on the use case.


```java
public class MessageService {
  @AutoWired private RqueueMessageSender rqueueMessageSender;
  
  public void doSomething(){
    rqueueMessageSender.put("simple-queue", "Rqueue is configured");
  }
  
  public void createJOB(Job job){
    //do something
    rqueueMessageSender.put("job-queue", job);
  }
  
  // send notification in 30 seconds
   public void sendNotification(Notification notification){
    //do something
    rqueueMessageSender.put("notification-queue", notification, 30*1000);
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
 // Create redis connection factory of your choice either Redission or Lettuce or Jedis 
 // Get redis configuration 
 RedisConfiguration redisConfiguration  = new RedisConfiguration();
 // Set fields of redis configuration
 // Create lettuce connection factory
 LettuceConnectionFactory lettuceConnectionFactory = new LettuceConnectionFactory(redisConfiguration);
 factory.setRedisConnectionFactory(lettuceConnectionFactory);
``` 
---

**Redis connection failure and retry**  

Whenever a call to Redis failed then it would be retried in 1 second,  to change that we can set back off to some different value. 

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

By default number of task executors are same as number of queues. A custom or shared task executor can be configured using factory's `setTaskExecutor` method, we need to provide an implementation of AsyncTaskExecutor


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

Whenever container is refreshed then it can be started automatically or manfully. Default behaviour is to start automatically, to change this behaviour set auto-start to false.

```java
factory.setAutoStartup(false);
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



## Support
Please report problems/bugs to issue tracker. You are most welcome for any pull requests for feature/issue.

## License
The Rqueue is released under version 2.0 of the Apache License.



    
