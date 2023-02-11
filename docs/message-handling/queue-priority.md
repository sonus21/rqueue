---
layout: default
title: Queue Priority
nav_order: 4
parent: Producer Consumer
description: Queue Priority Setup in Rqueue
permalink: /priority
---

There are many occasions when we need to group queues based on the priority like critical queue
should be consumed first then low priority queues. Rqueue supports two types of priority

1. Weighted
2. Strict

You need to set `priorityMode` in container factory either `STRICT` or `WEIGHTED`.

Priority needs to be specified using RqueueListener annotation's field `priority`. Multiple queues
can be grouped together using `priorityGroup` field, by default any queue having priority is added
to the default priority group. Queue can be classified into multiple priority groups as well for
example sms queue has 4 types of sub queues `critical`, `high`, `medium` and `low` their priority
needs to be specified as `priority="critical=10, high=6, medium=4,low=1"`.

If you want to use simple priority mechanism, then you need to set priority to some number and their
group, for example.

`priority="60"`
`priorityGroup="critical-group"`

Let's say there're three queues

| Queue | Priority |
|-------|----------|
| Q1    | 6        |
| Q2    | 4        |
| Q3    | 2        | 

Weighted Priority
-
Weighted priority works in round-robin fashion, in this example Q1 would be polled 6 times, Q2 4
times and Q3 2 times and again this process repeats. After polling if it's found that any queue does
not have more jobs than their weight is reduced by `Δ` e.g Q1 does not have any item then it's
weight is reduces by `Δ = currentWeight * (1-(6/(6+4+2))`. As soon as the weight becomes <= 0, queue
is inactive and weight is reinitialised when all queues become inactive.

This algorithm is implemented in [WeightedPriorityPoller][WeightedPriorityPoller]

Strict Priority
-
In Strict priority case, poller would always first poll from the highest priority queue, that's Q1
here. After polling if it encounters that if queue does not have any element than that queue becomes
inactive for **polling interval**, to avoid starvation a queue can be inactive for maximum of 1
minute.

This algorithm is implemented in [StrictPriorityPoller][StrictPriorityPoller]

### Additional Configuration

* `rqueue.add.default.queue.with.queue.level.priority`:  This flags controls whether the default
  queue should be added in the listeners where priority has been defined
  like `priority="critical=5,high=3"`, if we add then the such queues will have one additional level
  that would be the default one, the default value is `true`.
* `rqueue.default.queue.with.queue.level.priority`:  The priority of the default queue,
  when `rqueue.add.default.queue.with.queue.level.priority` is true, by default queue is placed in
  the middle.


[WeightedPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/WeightedPriorityPoller.java

[StrictPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/StrictPriorityPoller.java
