/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.core.spi;

/**
 * Per-subscriber row surfaced by {@link MessageBroker#subscribers} for the queue-detail
 * dashboard. Each entry corresponds to one logical consumer attached to the queue:
 *
 * <ul>
 *   <li>On Redis, one entry per {@code @RqueueListener} method (handlers all share the
 *       same backing list, so {@code pending} is the same on every row and
 *       {@code pendingShared} is {@code true}).
 *   <li>On NATS JetStream, one entry per durable consumer. {@code pending} is the
 *       per-consumer {@code numPending} for Limits retention (exact, divergent across
 *       rows) and the shared {@code msgCount} for WorkQueue retention (same on every
 *       row, {@code pendingShared = true}).
 * </ul>
 *
 * @param consumerName logical consumer / handler name (from {@code @RqueueListener.consumerName}
 *     when set, otherwise a backend-derived name like {@code rqueue-<queue>}).
 * @param pending messages waiting to be processed by this subscriber.
 * @param inFlight messages this subscriber has received but not yet acknowledged.
 * @param pendingShared {@code true} when {@code pending} is a queue-wide aggregate rather
 *     than this subscriber's exclusive backlog. The dashboard renders these with a
 *     "(shared)" hint so it's clear the figure is not per-consumer.
 */
public record SubscriberView(
    String consumerName, long pending, long inFlight, boolean pendingShared) {

  public SubscriberView(String consumerName, long pending, long inFlight) {
    this(consumerName, pending, inFlight, false);
  }
}
