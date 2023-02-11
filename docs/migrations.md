---
layout: default
title: Migration from older to new version
description: Rqueue Dashboard
permalink: /migration
---

## 2.9.0 to 2.10+

In 2.10 we changed some configuration keys used for Rqueue configuration. Following config keys were renamed.


| Older                                             | New                                                 | Purpopse                                                                 |
|---------------------------------------------------|-----------------------------------------------------|--------------------------------------------------------------------------|
| delayed.queue.size                                | scheduled.queue.size                                | Monitoring metrics name                                                  |
| rqueue.scheduler.delayed.message.thread.pool.size | rqueue.scheduler.scheduled.message.thread.pool.size | used to pull message from scheduled message to normal queue              |
| rqueue.scheduler.delayed.message.time.interval    | rqueue.scheduler.scheduled.message.time.interval    | how frequently message should be pulled from scheduled queue             |
| rqueue.scheduled.queue.prefix                     | rqueue.delayed.queue.prefix                         | scheduled queue name prefix used in Redis                                |
| rqueue.delayed.queue.channel.prefix               | queue.scheduled.queue.channel.prefix                | scheduled queue channel name prefix, used by Rqueue for immediate action |


If you do not use these configuration keys then you can directly migrate without doing anything.


--- 

## 1.x to 2.x

Set redis key **__rq::version=1** or add `rqueue.db.version=1` in the properties file, this is
required to handle data present in the old queues. It's safe to use version 2.x if existing queues
do not have any tasks to process. Look for the results of following redis commands.

```
1. LLEN <queueName> 
2. ZCARD rqueue-delay::<queueName>
3. ZCARD rqueue-processing::<queueName>
```

If all of these commands gives zero then all tasks have been consumed.


