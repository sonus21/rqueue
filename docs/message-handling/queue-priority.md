---
layout: default
title: Queue Priority
nav_order: 4
parent: Producer Consumer
description: Queue Priority Setup in Rqueue
permalink: /priority
---

There are situations where queues need to be grouped based on priority, ensuring critical tasks are
handled before lower priority ones. Rqueue provides two types of priority handling:

Weighted: Allows specifying different numeric priorities for queues.
Strict: Enforces strict ordering of queues based on priority.
To configure priority handling in Rqueue:

Set priorityMode in the container factory to either STRICT or WEIGHTED.
Use the priority field in the RqueueListener annotation to assign priorities to individual queues.
Use the priorityGroup field to group multiple queues together. By default, any queue with a
specified priority is added to the default priority group.
You can categorize queues into multiple priority groups. For instance, a SMS queue might have
sub-queues like critical, high, medium, and low, with priorities specified as priority="critical=10,
high=6, medium=4, low=1".
If you prefer a simpler priority mechanism, assign a numeric priority directly to each queue and its
group. For example:

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
Weighted priority operates in a round-robin manner, where each queue's polling frequency is
determined by its assigned weight. In this example, Q1 is polled 6 times, Q2 4 times, and Q3 2 times
before the cycle repeats. If a queue is found to have no items during polling, its weight is reduced
by `Δ`. For instance, if Q1 has no items, its weight decreases
by `Δ = currentWeight * (1 - (6 / (6 + 4 + 2)))`. When a queue's weight drops to or below 0, it
becomes inactive. The weights are reinitialized when all queues are inactive.

This algorithm is implemented in [WeightedPriorityPoller][WeightedPriorityPoller]

Strict Priority
-
In strict priority mode, the poller always starts by polling from the highest priority queue, which
is Q1 in this case. After polling, if the queue is found to have no elements, it becomes inactive
for a designated **polling interval**. To prevent starvation, a queue can remain inactive for a
maximum of 1 minute before becoming eligible for polling again.

This algorithm is implemented in [StrictPriorityPoller][StrictPriorityPoller]

### Additional Configuration

* `rqueue.add.default.queue.with.queue.level.priority`: This flag determines whether the default
  queue should be added to listeners where a priority has been defined
  using `priority="critical=5,high=3"`. If enabled (`true`), such queues will include an additional
  default level. The default value for this flag is `true`.

* `rqueue.default.queue.with.queue.level.priority`: This setting specifies the priority of the
  default queue when `rqueue.add.default.queue.with.queue.level.priority` is set to `true`. By
  default, the queue is positioned in the middle of the defined priority levels.

[WeightedPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/WeightedPriorityPoller.java

[StrictPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/StrictPriorityPoller.java
