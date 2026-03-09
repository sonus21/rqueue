---
layout: default
title: Queue Priority
nav_order: 4
parent: Producer Consumer
description: Queue Priority Setup in Rqueue
permalink: /priority
---

Queue prioritization ensures that critical tasks are handled before lower-priority 
ones. Rqueue supports two modes of priority handling:

1. **Weighted**: Polling frequency is based on numeric weights assigned to queues.
2. **Strict**: Higher-priority queues are always polled first.

### Configuring Priority

To enable priority handling:
- Set `priorityMode` in the container factory to `STRICT` or `WEIGHTED`.
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

In strict mode, the poller always attempts to fetch messages from the highest-priority 
queue first.

- The poller starts with Q1.
- If Q1 is empty, it moves to Q2, then Q3.
- If a queue is empty, it becomes inactive for the duration of the 
  **polling interval**.
- To prevent total starvation of lower-priority queues, inactive queues become 
  eligible for polling again after a maximum of 1 minute, even if higher-priority 
  queues still have work.

For implementation details, see [StrictPriorityPoller][StrictPriorityPoller].

### Additional Configuration

* **`rqueue.add.default.queue.with.queue.level.priority`**: Determines if a "default" 
  priority level should be automatically added to listeners using the 
  `priority="key=value"` syntax. Default: `true`.

* **`rqueue.default.queue.with.queue.level.priority`**: Specifies the weight/priority 
  of the automatically added default queue. By default, it is placed in the middle 
  of the defined priority levels.

[WeightedPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/WeightedPriorityPoller.java

[StrictPriorityPoller]: https://github.com/sonus21/rqueue/tree/master/rqueue-core/src/main/java/com/github/sonus21/rqueue/listener/StrictPriorityPoller.java
