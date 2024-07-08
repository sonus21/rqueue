---
layout: default
title: Middleware
nav_order: 3
parent: Producer Consumer
description: Rqueue Middleware
permalink: /middleware
---

In many scenarios, there's a need to execute specific blocks of code repetitively. Some common use
cases include:

- Logging job details
- Profiling listener methods
- Initiating transactions (e.g., New Relic, database, distributed transactions, Micrometer tracer)
- Implementing rate limiting (e.g., restricting a queue to process only 10 jobs per minute)
- Managing concurrent job execution (e.g., ensuring a user's account updater job runs sequentially)
- Handling permission or role changes (e.g., reacting to user bans or role updates)

Middleware can effectively handle these situations by configuring one or more middlewares through
the `SimpleRqueueListenerContainerFactory` object. These middlewares are invoked in the order they
are added, enabling structured and consistent execution of tasks across different scenarios.

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
