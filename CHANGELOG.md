# [Rqueue] New and Notable Changes

**NOTE**: The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


###[2.0.4] - 2-Aug-2020

### Added
- Allow a listener to be added on dead letter queue 

#### Fixes:
- Rqueue views/apis not accessible via api gateway


###[2.0.2] - 13-July-2020

### Fixes
- JDK dynamic proxy
- AoP profiler issue


## [2.0.1] - 17-May-2020

### Added
- Allow registering a queue, that can be in push only mode
- Apis to schedule task at the given time 
- Refine enqueueIn apis to support Duration and TimeUnit

### Fixes
- Arguments mismatch due to multiple class loaders.
- Dead letter queue clear lead to clearing all the messages related to that queue. 


## [2.0.0] - 10-May-2020
### Added
- Web interface to visualize queue
- Move message from one queue to another
- Latency visualizer
- Delete one or more message(s) from the queue
- Allow deactivating a consumer in a given environment
- Single or multiple execution of polled messages
- Queue level concurrency
- BackOff for failed messages, linear or exponential
- Group level queue priority
- Multi level queue priority
- Strict or weighted algorithm for message execution

### Breaking Changes
- Queue names are prefixed, that can lead to error.  1.x users set REDIS key `__rq::version` to `1`. It does try to find the version using key prefix, but if all queues are empty or no key exist in REDIS with prefix `rqueue-` then it will consider version 2.
- Renamed annotation field `maxJobExecutionTime` to `visibilityTimeout`

### Fixes
- **Spring** Optional Micrometer, in older version config class was importing micrometer related classes, that could lead to error if classes are not found. In this version now code depends on bean name using DependsOn annotation.

## [1.4.0] - 08-Apr-2020
* Allow queue level configuration of job execution time.
* Support to add Message processor for discard and dead letter queue

## [1.3.2] - 01-Apr-2020
* Support lower version of spring 2.1.x

## [1.3.1] - 27-Feb-2020
* **Fixed** Bootstrap issue due to optional dependencies of micrometer

## [1.3] - 11-Dec-2019
* Expose multiple queue metrics using micrometer. (queue-size, delay queue size, processing queue size, dead letter queue size, execution counter, failure counter)
* An api to move messages from dead letter queue to other queue. (Any source queue to target queue).

### Fixed
* An issue in the scheduler that's always scheduling job at the delay of 5 seconds. (this leads to messages are not copied from delayed queue to main queue on high load)


## [1.2] - 03-Nov-2019
* Typo of *Later* to *Letter*


## [1.1] - 02-Nov-2019
* At least once message guarantee
* Reduced ZSET calls
* Lua script to make atomic operation

## [1.0] - 23-Oct-2019
* The basic version of Asynchronous task execution using Redis for Spring and Spring Boot

[1.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.0-RELEASE
[1.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.1-RELEASE
[1.2]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.2-RELEASE
[1.3]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.3-RELEASE
[1.3.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.3.1-RELEASE
[1.3.2]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.3.2-RELEASE
[1.4.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/1.4.0-RELEASE
[2.0.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/2.0.0-RELEASE
[2.0.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/2.0.1-RELEASE
[2.0.2]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/2.0.2-RELEASE
[2.0.4]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue/2.0.4-RELEASE
