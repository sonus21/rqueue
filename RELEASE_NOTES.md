# Rqueue 4.0.0-RC4 Release Notes

**Release Date:** May 2, 2026

## Overview

Rqueue 4.0.0-RC4 continues the development of multi-backend support with significant improvements to the NATS JetStream backend and enhanced documentation for core APIs.

## What's New

### 🚀 Features

#### NATS Backend Enhancements
- **JetStream Consumer Configuration**: Honor per-queue `visibilityTimeout` and `numRetry` settings on JetStream consumers
  - Visibility timeout now maps to JetStream's `ackWait` parameter
  - `numRetry` configures the consumer's `maxDeliver` setting
  - Fallback to global consumer defaults when queue-level settings are not configured
  
- **Queue Name Validation**: Added backend-specific queue name validation for NATS
  - Prevents invalid queue names from reaching the NATS server
  - Early error detection at queue registration time
  
- **Stream Metadata**: JetStream streams now include descriptive metadata
  - Stream descriptions identify the associated Rqueue queue
  - Improves operational visibility in `nats stream info` output

- **Producer Mode Wiring**: Improved configuration of the NATS backend in producer-only mode
  - Cleaner initialization of transport-specific beans
  - Support for enqueueing without starting listeners

#### Multi-Backend Architecture
- **QueueType Support**: Introduce `QueueType` enum for backend-specific queue modes
  - `QUEUE`: Standard queue mode (default)
  - `STREAM`: Backend-specific streaming mode
  - `@RqueueListener` can declare queue type via annotation parameter
  
- **MessageBroker as Required Dependency**: Refactored `BaseMessageSender` to require `MessageBroker` at construction time
  - Cleaner dependency injection patterns
  - Earlier detection of misconfiguration

### 📚 Documentation

- **Enhanced Javadoc for Public APIs**:
  - `RqueueMessage`: Comprehensive documentation of message envelope structure, timing fields, and failure tracking
  - `QueueDetail`: Configuration metadata for queue behavior including polling, error handling, and priority support
  - `RqueueMessageHandler`: Internal handler responsibilities and argument injection patterns
  
- **Fixed Broken Documentation Links**: Resolved 6+ broken `@link` references in javadoc
  - All cross-module references converted to proper inline documentation
  - Module-specific javadoc generation now passes without warnings

### 🔧 Bug Fixes

- Fixed NATS poller fetch-wait handling for non-positive durations
- Resolved transport configuration conflicts in `RqueueListenerAutoConfig` test setup
- Corrected priority queue consumer creation in `NatsStreamValidator`
- Fixed test configuration issues in reactive and NATS test suites

### 🧹 Maintenance

- Applied Palantir code formatting across the codebase
- Updated copyright notices for 2026
- Cleaned up test infrastructure and mock configurations

## Breaking Changes

None in this release candidate.

## Deprecations

None in this release candidate.

## Known Issues

None reported at this time. Please report issues on [GitHub](https://github.com/sonus21/rqueue/issues).

## Upgrading

No special steps required to upgrade from 4.0.0-RC3. Existing code will continue to work without modification.

## Contributors

Special thanks to all contributors to this release.

## System Requirements

- Java 21+
- Spring Framework 5.3.x / 6.x
- Redis 4.x+ (for Redis backend)
- NATS Server 2.10.x+ (for NATS backend)

## What's Next

The Rqueue project is continuing work on:
- Stabilizing the NATS backend for GA release
- Performance optimizations
- Additional backend support exploration

---

For the full list of changes, see the [commit history](https://github.com/sonus21/rqueue/commits/nats-backend).
