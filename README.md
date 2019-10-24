Rqueue aka RedisQueue
----------------------
[![Build Status](https://travis-ci.org/sonus21/rqueue.svg?branch=master)](https://travis-ci.org/sonus21/rqueue)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)

Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring framework's messaging library backed by Redis.


* A message can be delayed for an arbitrary period of time or delivered immediately. 
* Message can be consumed from queue in parallel by multiple workers.
* Message delivery: It's guaranteed that a message is consumed **at max once**.  (Message would be consumed by a worker at max once due to the failure in the worker and resumes the process, otherwise exactly one delivery, there's no ACK mechanism like Redis)


### Adding a task to task queue
Rqueue supports two types of tasks.
1. Asynchronous task
2. Delayed tasks (asynchronous task that would be scheduled at given delay)


### Task configuration
A task can be configured in different ways
1. By default a tasks would be retried for Integer.MAX_VALUE of times
2. If we do not need retry then set retry count to zero
3. After retrying/executing the task N (>=1) times if we can't execute then whether the tasks detail would be discarded or push to dead-later-queue

## Usage
 # Configuration

**Configuration is very very simple based on the annotation**

## Application configuration

#### Spring-boot

1. Add annotation `EnableRqueue` on application config or just add the dependency `rqueue-spring-boot-starter` in the code 

```java
@EnableRqueue
public class Application{
...
}

```

#### spring-framework

1. Add annotation `EnableRqueue` on application config
2. Provide a RedisConnectionFactory bean



```java
@EnableRqueue
public class Application{
  @Bean
  public RedisConnectionFactory redisConnectionFactory(){
    // return a redis connection factory
  }
}

```


## Worker/Task executor

1. Create any component class
2. Add `RqueueListener` annotation on methods


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
  
  @RqueueListener(value = "delayed-queue", delayedQueue = "true", numRetries="3", deadLaterQueue="failed-delayed-queue")
  public void delayedMessageWithDlq(String message) {
    log.info("delayedMessageWithDlq: {}", message);
  }
  
  @RqueueListener(value = "job-queue", delayedQueue = "true", numRetries="3", deadLaterQueue="failed-job-queue")
  public void onMessage(Job job) {
    log.info("Job created: {}", job);
  }
  
  @RqueueListener(value = "notification-queue", delayedQueue = "true", numRetries="3", deadLaterQueue="failed-notification-queue")
  public void onMessage(Notification notification) {
    log.info("Notification message: {}", notification);
  }
}
```

### Message publishing or Task Submission
`AutoWire` bean of type `RqueueMessageSender` and send message using one of the `put` methods.


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

Different types of task can be submitted on the same queue. Method arguments are handled automatically, as in above example even task of `Job` type can be executed by workers.


#### Demo Applications

1. [Rqueue Spring Boot-1](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example) 
2. [Rqueue Spring Boot-2](https://github.com/sonus21/rqueue-task-exector) 
2. [Rqueue Spring ](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example) 

### Advance Configuration
Apart from basic configuration, it can be customized heavily, like number of executors it would be running for task execution.
More and more configurations can be provided using `SimpleRqueueListenerContainerFactory`.

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
    All tasks are stored in Redis database, either we can utilise the same Redis datastore for entire application or we can provide a separate one for task store. Nonetheless if we need different datasstore then we can configure using `setRedisConnectionFactory` method.  
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

Whenever a call to Redis failed then it would be retried in 5 seconds,  to change that we can set back off to some different value. 

```java
// set backoff time to 1 second
factory.setBackOffTime( 1000 );
```

---

**Task executor**  
  
Number of workers can be configured using  `setMaxNumWorkers` method for example to configured 10 workers we can set as

```java
SimpleRqueueListenerContainerFactory factory =  new SimpleRqueueListenerContainerFactory();
factory.setMaxNumWorkers(10);
```

By default number of task executors are same as number of queues, it can be more than the number of workers if more than one queue has been configured for single worker.


Even in some cases if we want to use some custom task executor then create an async task executor and set that to `factory`



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

Whenever container is refreshed then it can be started automatically or manully. Default behaviour is to start automatically, to change this behaviour set autostart to false.

```java
factory.setAutoStartup(false);
```

---

**Message converters configuration**  
Generally message can be converted to and from the redis data, but in some cases if we need to customize message converters, one use case could be compatibility in the task details.
In such cases a custom message converter can be provided. Any message converter has to implement  `org.springframework.messaging.converter.MessageConverter` interfaces.
Implementation must make sure the return type of  `Message<String>` is for method `toMessage`  where as in the case of `fromMessage` a object can be returned as well.

Implemented method can be added to factory as

```java
MessageConverter messageConverter = new SomeFancyMessageConverter();
List<MessageConverter> messageConverters = new ArrayList<>();
messageConverters.add(messageConverter);
factory.setMessageConverters(messageConverters);
```
   
More than one message converter can be specified as well.



## Support
Please report problems/bugs to issue tracker. You are most welcome for any pull requests for feature/issue.

## License
The Rqueue is released under version 2.0 of the Apache License.



    
