---
layout: default
title: Migration from Older Versions
description: Migrating between Rqueue versions
permalink: /migration
---

## Migration Guide

This guide outlines the configuration changes required when upgrading between major versions of Rqueue.

---

## Upgrading from 2.9.0 to 2.10+

Starting with version **2.10**, several configuration keys were renamed for consistency with the
introduction of **scheduled queues**.

If you are using any of the following configuration keys, please update them as shown below:

| Older Configuration Key                             | New Configuration Key                                 | Purpose                                                              |
|-----------------------------------------------------|-------------------------------------------------------|----------------------------------------------------------------------|
| `delayed.queue.size`                                | `scheduled.queue.size`                                | Monitoring metric name                                               |
| `rqueue.scheduler.delayed.message.thread.pool.size` | `rqueue.scheduler.scheduled.message.thread.pool.size` | Thread pool used for moving scheduled messages to the main queue     |
| `rqueue.scheduler.delayed.message.time.interval`    | `rqueue.scheduler.scheduled.message.time.interval`    | Frequency at which scheduled messages are pulled into the main queue |
| `rqueue.scheduled.queue.prefix`                     | `rqueue.delayed.queue.prefix`                         | Redis key prefix used for scheduled queues                           |
| `rqueue.delayed.queue.channel.prefix`               | `rqueue.scheduled.queue.channel.prefix`               | Redis pub/sub channel prefix used for scheduled queue events         |

{: .note}
If your application does **not** use any of these configuration properties, you can upgrade to 
**2.10+** without any changes.

---

## Upgrading from 1.x to 2.x

When upgrading from **Rqueue 1.x to 2.x**, you must configure the Redis database version so that
Rqueue can correctly interpret existing queue data.

Choose one of the following methods:

### Option 1: Redis Key

Set the following key in Redis:

```

__rq::version=1

````

### Option 2: Application Configuration

Add the following property to your configuration:

```properties
rqueue.db.version=1
````

This setting ensures that Rqueue correctly handles data in queues created by older versions.

{: .important}
If your existing queues are empty, you can safely upgrade to **version 2.x** without 
configuring the database version.

To check if your queues contain pending tasks, run the following Redis commands:

```
LLEN <queueName>
ZCARD rqueue-delay::<queueName>
ZCARD rqueue-processing::<queueName>
```

If all commands return **0**, your queues are empty and you can proceed with the 
migration without additional configuration.
