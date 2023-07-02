---
layout: default
title: WEB UI 
nav_order: 5
description: Rqueue Dashboard
permalink: /dashboard
---


The Rqueue has got a dashboard to see what’s going on, dashboard has multiple components for
multiple features.

* **Latency graph**: Latency for all queues or specific queues on daily basis upto 90 days. Latency
  has min, max and average latency.
* **Queue statistics**: How many messages were retried, executed, moved to dead letter, discarded
  due to retry limit exhaust.
* **Task Deletion** : Allows to delete any enqueued messages, either it’s in scheduled to be run, or
  it’s running, or it’s waiting to run.
* **Queue Insight** : It also allows us to see queue internal messages like SQS dashboard.
* **Queue Management** : Move tasks from one queue to another

Link: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

## Configuration

Add resource handler to handle the static resources.

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

All paths are under `/rqueue/**`. for authentication add interceptor(s) that would check for the
session etc.

### Adding Path Prefix

Rqueue endpoints and dashboards are available at `/rqueue/**` but it can handle `x-forwarded-prefix`
header, `x-forwarded-prefix` HTTP header is added as prefix in all urls. Rqueue can be configured to
use prefix using `rqueue.web.url.prefix` like `rqueue.web.url.prefix=/my-application/`

If path prefix is configured then static file resource handler must be configured to handle the
static files otherwise dashboard will not work.

Link: [http://localhost:8080/my-application/rqueue](http://localhost:8080/my-application/rqueue)


```java
public class MvcConfig implements WebMvcConfigurer {

  @Value("${rqueue.web.url.prefix:}")
  private String rqueueWebUrlPrefix;

  @Override
  public void addResourceHandlers(ResourceHandlerRegistry registry) {
    if (!StringUtils.isEmpty(rqueueWebUrlPrefix)) {
      registry
          .addResourceHandler(rqueueWebUrlPrefix + "/**")
          .addResourceLocations("classpath:/public/");
    } else if (!registry.hasMappingForPattern("/**")) {
      registry.addResourceHandler("/**").addResourceLocations("classpath:/public/");
    }
  }
}
```

## Dashboard Configurations

* `rqueue.web.enable` : Whether web dashboard is enabled or not, by default dashboard is enabled.
* `rqueue.web.max.message.move.count` : From utility tab one or more messages can be moved to other
  queue, by default on one request it will move `1000` messages, if you want larger/smaller number
  of messages to be moved then change this number.
* `rqueue.web.collect.listener.stats` : Whether the task execution status should be collected or
  not, when this is enabled than one or more background threads are put in place to aggregate
  metrics.
* `rqueue.web.collect.listener.stats.thread.count` : This controls thread count for metrics
  aggregation.
* `rqueue.web.statistic.history.day` : By default metrics are stored for 90 days, change this number
  to accommodate your use case.
* `rqueue.web.collect.statistic.aggregate.event.count` : The metrics of 500 events are aggregated at
  once, if you want fast update than you can change this to smaller one while if you want delay the
  metrics update than you need to increase this number. **Reducing this number will cause more
  write**
* `rqueue.web.collect.statistic.aggregate.event.wait.time` : Listeners event are aggregated when one
  of these two things either we have enough number of events, or the first event was occurred a long
  time ago, by default an event would wait upto 1 minute of time. If you want aggregation can be
  delayed then increase this to some higher value or reduce it. Provided time is in **second**.
* `rqueue.web.collect.statistic.aggregate.shutdown.wait.time` : At the time of application shutdown,
  there could be some events in the queue those require aggregation. If there are any pending events
  than a force aggregation is triggered, this parameter controls the waiting time for the force
  aggregation task. The provided time is in **millisecond**.

Home Page
---
[![Execution Page](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)

Queue Page
---
[![Queues Page](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)

Tasks waiting for execution
---
[![Explore Queue](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)

Running tasks
---
[![Running tasks](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)
