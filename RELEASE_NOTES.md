# Rqueue 4.0.0-RC4 Release Notes

**Release Date:** May 2, 2026

## Overview

Rqueue 4.0.0-RC4 introduces **production-ready NATS JetStream backend support**, bringing multi-backend message queue capabilities to Rqueue. This release adds a fully-featured alternative to Redis, enabling users to choose the backend that best fits their infrastructure and requirements.

## What's New

### 🎯 Major Features

#### 🚀 NATS JetStream Backend (NEW)
Complete implementation of a high-performance, cloud-native message broker alternative to Redis:

- **Full Feature Parity**: Supports all core Rqueue features including message enqueueing, polling, retry logic, and dead-letter queues
- **Stream-based Architecture**: Leverages NATS JetStream for persistent, replicated message storage
- **Cloud-Native Design**: Stateless brokers with durable message persistence
- **Horizontal Scalability**: JetStream clustering enables distributed deployments

**Key Capabilities:**
- Multi-priority queue support via stream multiplexing
- Configurable consumer settings (ack-wait, max-deliver)
- Queue name validation and stream descriptions for operational visibility
- Producer and consumer mode support (enqueue-only or full listener deployment)

#### NATS Backend Implementation Details
- **JetStream Consumer Configuration**: Full honors per-queue `visibilityTimeout` and `numRetry` settings
  - `visibilityTimeout` → JetStream `ackWait` (message visibility window)
  - `numRetry` → Consumer `maxDeliver` (retry attempts before DLQ)
  - Automatic fallback to global defaults when queue-level settings omitted
  - Compatible with Redis backend parameter semantics
  
- **Queue Name Validation**: Backend-specific validation at registration time
  - Early detection of invalid queue names before server rejection
  - Clear error messages for misconfiguration
  
- **Stream Metadata & Discoverability**: Descriptive JetStream stream information
  - Automatic stream descriptions tie back to Rqueue queue names
  - Operational visibility via `nats stream info` commands
  
- **Flexible Deployment Modes**:
  - **Consumer Mode**: Full listener infrastructure with message processing
  - **Producer Mode**: Enqueue-only deployments for message senders
  - Independent backend configuration per deployment

#### Multi-Backend SPI Framework
- **Backend Abstraction**: Clean `MessageBroker` SPI enables pluggable implementations
  - Redis backend: Established, production-hardened (v3.x legacy support)
  - NATS backend: New high-performance alternative
  - Plugin-ready architecture for future backends
  
- **QueueType Enumeration**: Backend-specific queue mode selection
  - `QUEUE`: Traditional queue semantics (default)
  - `STREAM`: Native stream/topic semantics (NATS JetStream streams)
  - Declared via `@RqueueListener(queueType=QueueType.STREAM)`
  
- **Dependency Injection Improvements**: `MessageBroker` as required constructor dependency
  - Eliminates late-binding configuration errors
  - Explicit backend selection at application startup
  - Type-safe Spring bean wiring

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

## Migration Guide

### From Rqueue 3.x to 4.0.0-RC4

**No breaking changes** for existing Redis-based deployments. Applications using the Redis backend will continue to work without modification.

**Opting into NATS Backend:**
1. Add `rqueue-nats` dependency
2. Set `rqueue.backend=nats` in application properties
3. Configure NATS connection URL via `spring.nats.urls` or `NATS_URL`
4. JetStream will be auto-configured on startup

**Example:**
```yaml
rqueue:
  backend: nats
spring:
  nats:
    urls: nats://localhost:4222
```

## Breaking Changes

None in this release candidate. Full backward compatibility with Rqueue 3.4.x.

## Deprecations

None in this release candidate.

## Known Issues

None reported at this time. Please report issues on [GitHub](https://github.com/sonus21/rqueue/issues).

## Upgrading

No special steps required to upgrade from 4.0.0-RC3. Existing code will continue to work without modification.

## Contributors

Special thanks to all contributors to this release.

## Why Choose NATS Backend?

| Feature | Redis | NATS JetStream |
|---------|-------|----------------|
| **Persistence** | Configurable (RDB/AOF) | Built-in durability |
| **Replication** | Cluster mode (complex) | Native clustering |
| **Cloud-Native** | Traditional | Kubernetes-friendly |
| **Message Priority** | Via sorted sets | Native streams |
| **Consumer Groups** | Manual management | First-class citizens |
| **Operational Insight** | Limited | Rich metadata/introspection |

**Choose Redis when:** You have existing Redis infrastructure, need established ecosystem, or require maximum feature stability.

**Choose NATS when:** You want cloud-native architecture, need horizontal scalability, prefer JetStream's semantics, or are running Kubernetes.

## System Requirements

- Java 21+
- Spring Framework 5.3.x / 6.x
- Redis 4.x+ (for Redis backend) — optional, not required
- NATS Server 2.10.x+ (for NATS backend) — optional, not required

## What's Next

The Rqueue project roadmap includes:
- **NATS Backend GA**: Final production hardening and performance tuning for 4.0.0 GA release
- **Backend Comparisons**: Performance benchmarks (Redis vs NATS) and deployment guides
- **Advanced Features**: Dead-letter queue improvements, custom serialization options
- **Ecosystem**: Dashboard/monitoring enhancements for NATS deployments
- **Future Backends**: Exploration of Kafka, Pulsar, and other cloud-native message systems

---

For the full list of changes, see the [commit history](https://github.com/sonus21/rqueue/commits/nats-backend).
