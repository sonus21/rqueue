/**
 * NATS / JetStream backend for Rqueue (preview, v1).
 *
 * <p>This module provides only the {@code MessageBroker} SPI implementation backed by NATS
 * JetStream and a small builder API for plain-Java use. It has no dependency on Spring or Spring
 * Boot. Spring wiring lives in {@code rqueue-spring} (via {@code RqueueListenerConfig} +
 * {@code @EnableRqueue}); Spring Boot auto-config lives in {@code rqueue-spring-boot-starter}.
 * Both gate their NATS code behind {@code @ConditionalOnClass(io.nats.client.JetStream.class)}
 * and {@code @ConditionalOnProperty(name = "rqueue.backend", havingValue = "nats")}, so adding
 * the {@code rqueue-nats} jar plus setting {@code rqueue.backend=nats} is sufficient to switch.
 *
 * <h2>Supported in v1</h2>
 * <ul>
 *   <li>Immediate enqueue, ack, retry-with-delay (via {@code nak}), DLQ (via {@code MaxDeliver}).</li>
 *   <li>Competing consumers across processes (shared durable pull consumer).</li>
 *   <li>Independent consumers per {@link com.github.sonus21.rqueue.annotation.RqueueListener}
 *       method (replaces the Redis "fan-out" pattern; the {@code @RqueueHandler} annotation is
 *       ignored on this backend).</li>
 *   <li>Message-id deduplication via {@code Nats-Msg-Id} headers.</li>
 *   <li>Reactive enqueue via {@code JetStream.publishAsync}.</li>
 *   <li>Dashboard peek via short-lived ephemeral consumers.</li>
 * </ul>
 *
 * <h2>Not supported in v1</h2>
 * <ul>
 *   <li>Delayed / scheduled / cron messages. Calls to {@code enqueueIn}, {@code enqueueAt},
 *       {@code addMessageWithDelay}, and periodic-listener registration throw {@link
 *       UnsupportedOperationException}.</li>
 *   <li>Push consumers and {@code DeliverGroup} routing. All workers use pull, durable consumers.</li>
 * </ul>
 */
package com.github.sonus21.rqueue.nats;
