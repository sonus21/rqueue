---
layout: default
title: Rqueue Dashboard
nav_order: 5
description: Dashboard for Rqueue Monitoring and Management
permalink: /dashboard
---

The Rqueue dashboard provides several components to monitor and manage various aspects of message
processing.

### Components of the Rqueue Dashboard

* **Latency Graph**: Displays latency metrics for all queues or specific queues over a daily basis
  for up to 90 days, showing minimum, maximum, and average latency.

* **Queue Statistics**: Provides insights into message retry counts, executions, movements to
  dead-letter queues, and discards due to retry limit exhaustion.

* **Task Deletion**: Allows deletion of enqueued messages, whether scheduled to run, currently
  running, or waiting to run.

* **Queue Insight**: Offers visibility into internal queue messages, akin to an SQS dashboard.

* **Queue Management**: Facilitates moving tasks from one queue to another.

Link to access the dashboard: [http://localhost:8080/rqueue](http://localhost:8080/rqueue)

## Configuration

### Adding Resource Handlers

To handle static resources, configure a resource handler in your MVC configuration.

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

### Adding Path Prefix

Rqueue endpoints and dashboards are available at `/rqueue/**`, but they can handle
the `x-forwarded-prefix` HTTP header for custom prefixing. Configure the prefix
using `rqueue.web.url.prefix`, for example: `rqueue.web.url.prefix=/my-application/`.

If a path prefix is configured, ensure that static file resource handling is also configured
accordingly, or the dashboard may not function correctly.

Link with configured path
prefix: [http://localhost:8080/my-application/rqueue](http://localhost:8080/my-application/rqueue)

## Dashboard Configurations

* `rqueue.web.enable`: Controls whether the web dashboard is enabled (default: `true`).
* `rqueue.web.max.message.move.count`: Specifies the number of messages to move on a single request
  from the utility tab (default: `1000`).
* `rqueue.web.collect.listener.stats`: Enables collection of task execution status metrics (
  default: `false`).
* `rqueue.web.collect.listener.stats.thread.count`: Controls the number of threads for metrics
  aggregation.
* `rqueue.web.statistic.history.day`: Specifies the number of days to store metrics data (
  default: `90`).
* `rqueue.web.collect.statistic.aggregate.event.count`: Aggregates metrics for a specified number of
  events at once (default: `500`).
* `rqueue.web.collect.statistic.aggregate.event.wait.time`: Specifies the wait time in seconds for
  metrics aggregation based on event occurrence or elapsed time (default: `60` seconds).
* `rqueue.web.collect.statistic.aggregate.shutdown.wait.time`: Sets the wait time in milliseconds
  for force aggregation of pending events during application shutdown.

### Dashboard Screenshots

#### Latency Graph
<br/>

[![Latency Graph](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/stats-graph.png)

#### Queue Statistics
<br/>

[![Queue Statistics](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queues.png)

#### Tasks Waiting for Execution
<br/>

[![Tasks Waiting for Execution](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/queue-explore.png)

#### Running Tasks
<br/>

[![Running Tasks](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/running-tasks.png)
