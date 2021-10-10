## Rqueue Spring Example Project

This example project should be used only for spring-mvc project **not for** spring boot. For
spring-boot refer to one of the following samples

* For spring-boot refer
  to [rqueue-spring-boot-example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-example)
* For spring-boot reactive/webflux
  to [rqueue-spring-boot-reactive-example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)

### How to Run this App?

#### Requirements

* Redis
* Java 8
* Gradle

Run Spring app as

` gradle rqueue-spring-example:run`

Multiple Queues are configured for this application

* simple-queue
* delay-queue
* delay-queue2
* job-queue
* job-morgue

It has three APIs to play with

| Request | Description | Query Params |
| --- | ----------- |------------------|
| http://localhost:8080/job-delay | Schedule job notification with delay | q=> queue name, options are job-queue and job-morgue, delay=> default notification delay is 2000 millisecond, msg => any text that should be sent |
| http://localhost:8080/job | Send job notification in background |q=> queue name, options are job-queue and job-morgue, msg => any text that should be sent |
| http://localhost:8080/push?q=test&msg=test | schedule/send simple string notification on any queue |q=>queue name, options are simple-queue,delay-queue,delay-queue2,msg=message that should be enqueued, numRetries=> number of retry count (optional), delay => optional delay for message     |

Application MessageListener logs the job in console, so we can see in the console what's happening
with the scheduled job.