---
layout: default
title: Middleware
nav_order: 3
parent: Producer Consumer
description: Rqueue Middleware
permalink: /middleware
---

In many of the situations, we want to run some specific block of codes repetitively. Some use cases
are

* Log job detail
* Profile listener method
* Start some arbitrary transaction like newrelic transaction, db transaction, distributed
  transaction, Micrometer tracer etc
* Rate limiting: some queue can only process 10 jobs in a minute while another queue can process any
  number of messages.
* Concurrent job execution: A user's account updater job should not run concurrently.
* Permission/Role details: There could be case like user has been banned, user's role has changed in
  such case some tasks should run.

Middleware can be used in any of these cases, one or more middlewares can be configured
using `SimpleRqueueListenerContainerFactory` object. Middlewares are called in the order they are
added.

```java
public class RqueueConfiguration {

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory factory = new SimpleRqueueListenerContainerFactory();
    factory.useMiddleware(new LoggingMiddleware());
    // add other middlewares here
    return factory;
  }
}
```
