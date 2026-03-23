---
title: CHANGELOG
layout: default

---

# CHANGELOG

All notable user-facing changes to this project are documented in this file.

## Release [4.0.0.RC2] 24-Mar-2026

{: .highlight}
This is a release candidate for 4.0.0. It targets Spring Boot 4.x and Spring Framework 7.x.
Please test thoroughly before using in production.

### Features
* **Pluggable message ID generation** — added `RqueueMessageIdGenerator` with a
  default UUIDv4 implementation so applications can override message ID generation
  with a custom bean, including time-ordered strategies such as UUIDv7.
* **Worker registry for dashboard visibility** — added an optional
  `rqueue.worker.registry.enabled` registry that tracks worker metadata and
  queue-level poller activity for dashboard use.
* **Workers dashboard page** — added a dedicated workers view showing worker
  identity, queue pollers, last poll activity, and recent capacity exhaustion.
* **Queue and workers pagination** — added server-side pagination for dashboard
  queue and worker listings, with configurable page sizes.
* **Dashboard enqueue controls for scheduled messages** — messages in scheduled
  queues can now be moved back to the main queue from the dashboard, including
  explicit front/rear enqueue options for non-periodic messages.
* **Dashboard refresh and usability improvements** — refreshed queue, worker, and
  explorer UI with improved layouts, duration formatting, feedback modals, and
  more readable queue metadata.

## Release [4.0.0.RC1] 18-Mar-2026

{: .highlight}
This is a release candidate for 4.0.0. It targets Spring Boot 4.x and Spring Framework 7.x.
Please test thoroughly before using in production.

### Features
* **Spring Boot 4.x support** — compatible with Spring Boot 4.0.1 and above.
* **Spring Framework 7.x support** — built against Spring Framework 7.0.3, taking
  advantage of the updated messaging and context APIs.
* **Java 21 baseline** — Java 21 is now the minimum supported runtime.
* **Jackson 3.x support** — updated serialization layer to use Jackson 3.x
  (`tools.jackson` packages).
* **Lettuce 7.x support** — Redis client updated to Lettuce 7.2.x.
* `GenericMessageConverter` now supports generic envelope types such as `Event<T>`.
  The type parameter is resolved from the runtime class of the corresponding field
  value, enabling transparent round-trip serialization without requiring a custom
  message converter.

### Migration Notes
* Requires Java 21+.
* Requires Spring Boot 4.x / Spring Framework 7.x. Not backward compatible with
  Spring Boot 3.x — use the 3.x release line for older Spring Boot versions.
* Jackson package namespace changed from `com.fasterxml.jackson` to `tools.jackson`
  in Jackson 3.x. Update any custom `ObjectMapper` configuration accordingly.

## Release [3.4.0] 22-July-2025
### Fixes
* Improved unique message enqueuing to reject duplicates upfront rather than during
  processing. #259


## Release [3.3.0] 29-June-2025
### Fixes
* Custom Message Converter was being ignored #256
* LockKey prefix was not being used #239

## Release [3.2.0] 10-July-2024
### Fixes
* Fixed typo #218

### Feature
* Do not retry some exceptions

## Release [3.1.1] 1-Mar-2024
### Fixes
* Fixed issue for spring boot 3.2 #218


## Release [3.1.0] 24-June-2023
{: .highlight } 
Migrate to this version to reduce Redis resource utilization.

This release fixes a critical bug where task multiplication caused excessive Redis 
resource usage. For details, see issue #[193].

## Release [2.13.1] 24-June-2023
{: .highlight }
Migrate to this version to reduce Redis resource utilization.

This release fixes a critical bug where task multiplication caused excessive Redis 
resource usage. For details, see issue #[193].


## Release [4.0.0] 17-Jan-2022

We're so excited to release Rqueue `4.0.0`. This release supports Java 21,
Spring Boot 4.x and Spring Framework 7.x.

### [2.13.0] - 25-Dec-2022
### Fixes
{: .highlight}
Migrate to this version immediately to prevent duplicate message consumption after deletion.

* Fixed an issue with parallel message deletion or deletion from within a message listener.
* Improved message poller efficiency when no threads are available.
* Corrected the UI to use the system's local time zone.

### [2.12.0] - 14-Dec-2022

### Fixes

* Upgraded Pebble version for CVE
* Use System Zone ID for UI display

### [2.11.1] - 18-Nov-2022
### Fixes
{: .highlight}
Migrate to this version immediately to prevent scheduled message buildup. Messages could 
fail to be consumed if the poller encountered a Redis connection error.

* Improved reliability of the message mover during Redis connection errors.
* Upgraded jQuery version.

### [2.10.2] - 16-Jul-2022
### Fixes
* Fixed message status reporting (previously only showed 'enqueued').
* Fixed an issue where weighted queues with size 1 were not working.
* Fixed a bug where deleted messages could reappear.

### Features
* Added the `rqueue.enabled` flag to disable Rqueue if needed.

### [2.10.1] - 18-Oct-2021

* Fixes for concurrency when task executor is provided see issue #[122]

### [2.10.0] - 10-Oct-2021
{: .warning }
**Breaking Change**: Renamed several configuration keys. If you use custom Rqueue 
settings, please refer to the [Migration Guide](./migration#290-to-210) to avoid 
application failure.

### Fixes
* Fixed an issue where post-processor calls were not being triggered.
* Corrected the default message move count in the dashboard to 1000.
* Fixed a potential naming issue in `rename collection`.
* Fixed a dashboard UI bug showing multiple minus signs.
* Improved support for server context paths. Rqueue endpoints are now relative to the 
  `x-forwarded-prefix` or `server.servlet.context-path`.

### Features

* Display completed jobs in the dashboard
* Option to choose number of days in the chart

### [2.9.0] - 30-Jul-2021

### Fixes

* Option to add rqueue web url prefix, the prefix is configured from application.properties file
  using `rqueue.web.url.prefix=/my-application/`, now rqueue dashboard would be served
  at  `/my-application/rquque` instead of `/rqueue`, the configuration has higher priority than the
  HTTP request header `x-forwarded-prefix`.
* Custom message converter is not working
* RedisCommandExecutionException : command arguments must be strings or integers

### [2.8.0] - 08-Jun-2021

### Fixes

* Producer mode is not honored in Message scheduler
* Message scheduler disable flag is not honored
* Aggregator should not be running in producer mode
* Listener concurrency is not reached, even though messages are in queue
* Register queue in producer mode for all listener methods

### Added

* Pause/Unpause queue from dashboard
* Pause/Unpause queue programmatically
* Batch message fetching
* Default queue priority to WEIGHTED
* Added an API to update the visibility timeout of running job

## [2.7.0] - 13-Apr-2021

### Fixes

* Spring Boot App could not start due to class not found error **Boot 2.0**
* Utility UI message move not working due to invalid data type

### Added

* Support for Reactive Redis and Spring Webflux
* Delete message metadata when `rqueue.message.durability.in-terminal-state` is less than equal to
  zero
* Delete job detail when `rqueue.job.durability.in-terminal-state` is less tha equal to zero

## [2.6.1] - 1-Mar-2021

### Fixes

* Graph not rendering in firefox due to unsafe csp rule
* Crash in dashboard due to Twig template, changed it to Pebble template

## [2.6.0] - 22-Feb-2021

Message counts api

## [2.5.0] - 9-Feb-2021

### Added

* Attach more than one message listeners to the same queue

## [2.4.0] - 3-Feb-2021

### Added

* Job Middlewares
* Delay execution of message when it's moved to enqueue instead of consuming it immediately.

## [2.3.0] - 2-Jan-2021

### Added

* Job checkin for long-running tasks
* Display job and failure details in UI for each message
* Allow deleting messages from normal and scheduled queues instead of only dead letter queue.
* Scan only required beans for RqueueListener annotated methods

### Fixes

* Redis string deserialization issue, string were inserted without quote''
* Dashboard CSP rule error for inline javascript
* Double minus sign (--) in UI

### Miscellaneous

* Delete message metadata along with messages using background job
* Potential error for a periodic message, if period was longer than 24 hours
* Add retry limit exceeded messages at the front of dead letter queue instead at the back.

## [2.2.0] - 6-Dec-2020

### Added

* New API to enqueue periodic message. Periodic jobs are like cron jobs that would run at the
  certain interval.

## [2.1.1] - 24-Sep-2020

### Added

* More apis to enqueue unique message

## [2.1.0] - 16-Sep-2020

### Added

* Allow application to provide message id while enqueuing messages
* Unique message enqueue
* Api to check if message was enqueued or not
* Api to delete single message
* Proxy for outbound http connection
* Enqueue list of objects and process them, like batch-processing

Fixes:

* Registered queues should not be deleted when used in producer mode

## [2.0.4] - 2-Aug-2020

### Added

- Allow a listener to be added on dead letter queue

### Fixes:

- Rqueue views/apis not accessible via api gateway

## [2.0.2] - 13-July-2020

### Fixes

- JDK dynamic proxy
- AoP profiler

## [2.0.1] - 17-May-2020

### Added

- Allow registering a queue, that can be in push only mode
- Apis to schedule task at the given time
- Refine enqueueIn apis to support Duration and TimeUnit

### Fixes

- Arguments mismatch due to multiple class loaders.
- Dead letter queue clear lead to clearing all the messages related to that queue.

## [2.0.0] - 10-May-2020

{: .warning}
Breaking change, for migration [see](./migration#1x-to-2x)

- Queue names are prefixed, that can lead to error. 1.x users set REDIS key `__rq::version` to `1`.
  It does try to find the version using key prefix, but if all queues are empty or no key exist in
  REDIS with prefix `rqueue-` then it will consider version 2.
- Renamed annotation field `maxJobExecutionTime` to `visibilityTimeout`

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

### Fixes

- **Spring** Optional Micrometer, in older version config class was importing micrometer related
  classes, that could lead to error if classes are not found. In this version now code depends on
  bean name using DependsOn annotation.

## [1.4.0] - 08-Apr-2020

* Allow queue level configuration of job execution time.
* Support to add Message processor for discard and dead letter queue

## [1.3.2] - 01-Apr-2020

* Support lower version of spring 2.1.x

## [1.3.1] - 27-Feb-2020

* **Fixed** Bootstrap issue due to optional dependencies of micrometer

## [1.3] - 11-Dec-2019

* Expose multiple queue metrics using micrometer. (queue-size, delay queue size, processing queue
  size, dead letter queue size, execution counter, failure counter)
* An api to move messages from dead letter queue to other queue. (Any source queue to target queue).

### Fixed

* An issue in the scheduler that's always scheduling job at the delay of 5 seconds. (this leads to
  messages are not copied from scheduled queue to main queue on high load)

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

[2.0.4]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.0.4-RELEASE

[2.1.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.1.0-RELEASE

[2.1.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.1.1-RELEASE

[2.2.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.2.0-RELEASE

[2.3.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.3.0-RELEASE

[2.4.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.4.0-RELEASE

[2.5.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.5.0-RELEASE

[2.6.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.6.0-RELEASE

[2.6.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.6.1-RELEASE

[2.7.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.7.0-RELEASE

[2.8.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.8.0-RELEASE

[2.9.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.9.0-RELEASE

[2.10.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.10.0-RELEASE

[2.10.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.10.1-RELEASE

[2.10.2]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.10.2-RELEASE

[2.11]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.11-RELEASE

[2.11.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.11.1-RELEASE

[2.12.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.12.0-RELEASE

[2.13.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.13.0-RELEASE

[2.13.1]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/2.13.1-RELEASE

[4.0.0]: https://repo1.maven.org/maven2/com/github/sonus21/rqueue-core/4.0.0-RELEASE

[122]: https://github.com/sonus21/rqueue/issues/122
[193]: https://github.com/sonus21/rqueue/issues/193
