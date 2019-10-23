Rqueue aka RedisQueue
----------------------
[![Build Status](https://travis-ci.org/sonus21/rqueue.svg?branch=master)](https://travis-ci.org/sonus21/rqueue)
[![Coverage Status](https://coveralls.io/repos/github/sonus21/rqueue/badge.svg?branch=master)](https://coveralls.io/github/sonus21/rqueue?branch=master)

Rqueue is an asynchronous task executor(worker) built for spring framework based on the spring framework's messaging library backed by Redis.


* A message can be delayed for an arbitrary period of time or delivered immediately. 
* Message can be consumed from queue in parallel by multiple workers.
* Message delivery: It's guaranteed that a message is consumed **at max once**.  (Message would be consumed by a worker at max once due to the failure in the worker and resumes the process, otherwise exactly one delivery, there's no ACK mechanism like Redis)


### Adding a task to task queue
Rqueue supports two types of tasks.
1. Asynchronous task
2. Delayed tasks (asynchronous task that would be scheduled at given delay)


### Task configuration
A task can be configured in different ways
1. By default a tasks would be retried for Integer.MAX_VALUE of times
2. If we do not need retry then set retry count to zero
3. After retrying/executing the task N (>=1) times if we can't execute then whether the tasks detail would be discarded or push to dead-later-queue

## Wiki

[Wiki](https://github.com/sonus21/rqueue/wiki)

## Support
Please report problems/bugs to issue tracker. You are most welcome for any pull requests for feature/issue.

## License
The Rqueue is released under version 2.0 of the Apache License.



    
