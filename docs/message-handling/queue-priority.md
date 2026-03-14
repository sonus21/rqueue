---
layout: default
title: Queue Priority
nav_order: 4
parent: Producer Consumer
description: Queue Priority Setup in Rqueue
permalink: /priority
---

Queue prioritization ensures that critical tasks are handled before lower-priority 
ones. Rqueue supports three modes of priority handling:

1. **Weighted**: Polling frequency is based on numeric weights assigned to queues.
2. **Strict**: Higher-priority queues are polled first, but starvation is considered
   so lower-priority queues still get polling opportunities.
3. **Hard Strict**: Messages are always fetched from the highest-priority queue first,
   moving top-down only when that queue has no available message.

### Configuring Priority

To enable priority handling:
- Set `priorityMode` in the container factory to `WEIGHTED`, `STRICT`, or `HARD_STRICT`.
- Use the `priority` field in the `@RqueueListener` annotation to assign weights or levels.
- Use the `priorityGroup` field to group multiple related queues. By default, queues with 
  specified priorities are added to a default group.

#### Examples:
- **Sub-queues**: A single listener can handle multiple priority levels for the same 
  logical task: `priority="critical=10,high=6,medium=4,low=1"`.
- **Simple Numeric**: Assign a weight directly: `priority="60"`, `priorityGroup="critical-group"`.

---

### Weighted Priority

Weighted priority uses a round-robin approach where the polling frequency is 
proportional to the assigned weights.

Consider three queues:
| Queue | Weight |
|-------|--------|
| Q1    | 6      |
| Q2    | 4      |
| Q3    | 2      |

In one cycle, Q1 is polled 6 times, Q2 4 times, and Q3 2 times. If a queue is empty 
during a poll, its effective weight is dynamically reduced to prevent wasting 
resources on empty queues. Weights are reset once all queues in the group have 
become inactive or finished their cycle.

For implementation details, see [WeightedPriorityPoller][WeightedPriorityPoller].

### Strict Priority

In strict mode, the poller prefers the highest-priority queue first, but it also
considers starvation so every queue continues to get polling chances over time.

- The poller starts with Q1.
- If Q1 is empty, it moves to Q2, then Q3.
- If a queue is empty, it becomes inactive for the duration of the
  **polling interval**.
- If messages are not fetched from a lower-priority queue for a certain interval,
  that queue becomes eligible again so it can be polled even while higher-priority
  queues continue to receive traffic.
- This gives lower-priority queues a chance to make progress and prevents permanent
  starvation.

For implementation details, see [StrictPriorityPoller][StrictPriorityPoller].

### Hard Strict Priority

In hard strict mode, the poller always follows a top-down priority order.

- The poller starts with the highest-priority queue.
- It continues to fetch from that queue as long as messages are available.
- It moves to the next queue only when the current higher-priority queue has no
  message available.
- This approach keeps the queue order fully top-down and may starve lower-priority
  queues while higher-priority queues continue to receive traffic.

### Additional Configuration

* **`rqueue.add.default.queue.with.queue.level.priority`**: Determines if a "default" 
  priority level should be automatically added to listeners using the 
  `priority="key=value"` syntax. Default: `true`.

* **`rqueue.default.queue.with.queue.level.priority`**: Specifies the weight/priority 
  of the automatically added default queue. By default, it is placed in the middle 
  of the defined priority levels.

[WeightedPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/WeightedPriorityPoller.java

[StrictPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/StrictPriorityPoller.java
