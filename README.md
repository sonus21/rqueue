Rqueue aka RedisQueue
----------------------
What's Rqueue?

Rqueue is a worker built for spring framework based on the spring framework's messaging library backed by Redis.


* A message can be delayed for arbitary period of time or delivered immediately. 
* Message can be consumed from queue parallelly by multiple workers.
* Message delivery: It's guaranteed that a message is consumed **at max once**.  (Message would be consumed by worker at max once due to the failure in the worker and resumes the process, otherwise exactly one delivery, there's no ACK mechanism like Redis)


# Usage 

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

```java
class Job{
  private String id; 
  private String message;
  public String getId() {
    return id;
  }
  public void setId(String id) {
      this.id = id;
  }
  public void setMessage(String message) {
      this.message = message;
  }
  public String getMessage() {
      return message;
  }
  @Override
  public String toString(){
    return "Job[id=" +id +" , message=" +message + "]";
  }
}
```


## Worker Configuration

1. Create component class
2. Add `RqueueListener` annotation on methods


```java
@Component
@Slf4j
public class MessageListener {
  @RqueueListener(value = "simple-queue")
  public void simpleMessage(Object message) {
    log.info("simple-queue: {}", message);
  }

  @RqueueListener(value = "delayed-queue", delayedQueue = "true")
  public void delayedMessage(Object message) {
    log.info("delayedMessage: {}", message);
  }
  
  @RqueueListener(value = "delayed-queue", delayedQueue = "true", numRetries="3", deadLaterQueue="failed-delayed-queue")
  public void delayedMessageWithDlq(Object message) {
    log.info("delayedMessageWithDlq: {}", message);
  }
  
  @RqueueListener(value = "job-queue", delayedQueue = "true", numRetries="3", deadLaterQueue="failed-job-queue")
  public void onMessage(Job job) {
    log.info("delayedMessageWithDlq: {}", job);
  }
}
```

### Message publishing
AutoWire bean of type `RqueueMessageSender` and send message using one of the `put` methods.


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
}
```






## Support
Please report problems/bugs to issue tracker. You are most welcome for any pull requests for feature/issue.

## License
The Rqueue is released under version 2.0 of the Apache License.



    
