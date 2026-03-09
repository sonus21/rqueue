---
layout: default
title: Middleware
nav_order: 3
parent: Producer Consumer
description: Rqueue Middleware
permalink: /middleware
---

Middleware allows you to execute common logic across your message listeners. Common 
use cases include:

- Logging and auditing job details.
- Profiling listener performance.
- Managing transaction boundaries (e.g., database transactions, New Relic, 
  distributed tracing).
- Implementing rate limiting (e.g., restricting a queue to 10 jobs per minute).
- Ensuring sequential job execution (e.g., forcing a specific user's tasks to run 
  one at a time).
- Enforcing access control or handling status changes (e.g., checking for user bans).

You can register one or more middleware implementations via the 
`SimpleRqueueListenerContainerFactory`. Middleware is invoked in the order it is 
added, providing a structured way to intercept and process messages.

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
