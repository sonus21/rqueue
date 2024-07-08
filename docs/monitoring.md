---
layout: default
nav_order: 7
title: Monitoring and Alerting
description: Rqueue Health Monitoring and Alerting
permalink: /monitoring-alerting
---

## Monitoring Queue Statistics

{: .note}

Rqueue supports monitoring via the **Micrometer** library.

### Gauge Metrics

Rqueue provides the following gauge metrics:

* **queue.size**: Number of tasks waiting to be processed.
* **dead.letter.queue.size**: Number of tasks in the dead letter queue.
* **scheduled.queue.size**: Approximate number of tasks scheduled for future execution.
* **processing.queue.size**: Approximate number of tasks currently being processed.

### Execution and Failure Counters

Execution and failure counters can be enabled (disabled by default) by
configuring `RqueueMetricsProperties`.

```properties
rqueue.metrics.count.execution=true
rqueue.metrics.count.failure=true
```

### Integration

#### Spring Boot Application

1. Include Micrometer dependencies and relevant exporters.
2. Set tags using `rqueue.metrics.tags.<name> = <value>` if needed.
3. Enable counting features as required.

#### Spring Application

1. Include Micrometer dependencies and provide `MeterRegistry` as a bean.
2. Configure a `RqueueMetricsProperties` bean with necessary settings.

### Example Grafana Dashboard

[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)
