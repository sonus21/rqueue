# Changelog
All notable changes to Rqueue project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.4.0] - 08-Apr-2020
#### Added
- Allow queue level configuration of job execution time.
- Support to add Message processor for discard and dead letter queue

## [1.3.2] - 01-Apr-2020
### Added
- Support lower version of spring 2.1.x


## [1.3.1] - 27-Feb-2020
### Fixed
- Bootstrap issue due to optional dependencies of micrometer


## [1.3] - 11-Dec-2019
### Added 
- Expose 6 queue metrics using micrometer. (queue-size, delay queue size, processing queue size, dead letter queue size, execution counter, failure counter)
- An api to move messages from dead letter queue to other queue. (Any source queue to target queue).

### Fixed
- An issue in scheduler that's always scheduling job at the delay of 5 seconds. (this leads to messages are not copied from delayed queue to main queue on high load)


## [1.2] - 03-Nov-2019
- Fixed typo of *Later* to *Letter*


## [1.1] - 02-Nov-2019
### Added 
- At least once message guarantee
- Reduced ZSET calls
- Use Lua script to execute synchronized task




## [1.0] - 23-Oct-2019
- Basic version of Asynchronous task execution

