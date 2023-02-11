---
layout: default
nav_order: 7
title: Monitoring/Alerting
description: Rqueue Health Monitoring and Alerting
permalink: /monitoring-alerting
---

## Monitoring Queue Statistics
{: .note}
Rqueue support **micrometer** library for monitoring. 

**It provides 4 types of gauge metrics.**

* **queue.size** : number of tasks to be run
* **dead.letter.queue.size** : number of tasks in the dead letter queue
* **scheduled.queue.size** : number of tasks scheduled for later time, it's an approximate number,
  since some tasks might not have moved to be processed despite best efforts
* **processing.queue.size** : number of tasks are being processed. It's also an approximate number
  due to retry and tasks acknowledgements.

**Execution and failure counters can be enabled (by default this is disabled).**

We need to set `count.execution` and `count.failure` fields of RqueueMetricsProperties

```
1. execution.count
2. failure.count 
```

All these metrics are tagged

**Spring Boot Application**

1. Add `micrometer` and the exporter dependencies
2. Set tags if any using `rqueue.metrics.tags.<name> = <value>`
3. Enable counting features using `rqueue.metrics.count.execution=true`
   , `rqueue.metrics.count.failure=true`

**Spring Application**

1. Add `micrometer` and the exporter dependencies provide `MeterRegistry` as bean
2. Provide a bean of `RqueueMetricsProperties`, in this bean set all the required fields.

[![Grafana Dashboard](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)](https://raw.githubusercontent.com/sonus21/rqueue/master/docs/static/grafana-dashboard.png)
