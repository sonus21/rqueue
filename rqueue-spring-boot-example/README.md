## Rqueue Spring Boot Sample App

This sample project should be used only for spring-boot application.

* For spring-mvc refer
  to [rqueue-spring-example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-example)
* For spring-boot reactive/webflux refer
  to [rqueue-spring-boot-reactive-example](https://github.com/sonus21/rqueue/tree/master/rqueue-spring-boot-reactive-example)

### How to Run this App?

#### Requirements

* Redis
* Java 8
* Gradle

Run boot app as

`gradle rqueue-spring-boot-example:bootRun`

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



