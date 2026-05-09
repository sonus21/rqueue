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
package com.github.sonus21.rqueue.nats.js;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Bootstrap-time installer for the NATS-native dead-letter advisory bridge. For every active queue
 * registered in {@link EndpointRegistry}, calls
 * {@link JetStreamMessageBroker#installDeadLetterBridge(QueueDetail, String)} so that messages
 * exceeding {@code maxDeliver} on the durable consumer are republished onto the queue's DLQ
 * stream via the {@code $JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES} advisory subject.
 *
 * <p>This is the NATS-side equivalent of the Redis backend's {@code RqueueDeadLetterPublisher}:
 * the rqueue post-processing handler already routes failed deliveries to a configured Rqueue-level
 * DLQ ({@code @RqueueListener(deadLetterQueue=...)}); the advisory bridge registered here is an
 * additional, independent path that catches messages whose handler exhausted retries without the
 * post-processor noticing (e.g. listener restart, container shutdown mid-retry, JetStream-driven
 * redelivery exhaustion outside the rqueue retry counter).
 *
 * <p><b>Lifecycle.</b> Implements {@link SmartInitializingSingleton} so it runs after every
 * {@code @RqueueListener} bean has registered with {@link EndpointRegistry} and the
 * {@link com.github.sonus21.rqueue.nats.js.NatsStreamValidator} has provisioned the underlying
 * streams — but before {@code SmartLifecycle.start()} spawns the message pollers, so the bridge
 * is in place before the first delivery attempt. Implements {@link DisposableBean} so the
 * advisory dispatchers are torn down on context shutdown.
 *
 * <p><b>Producer-only mode.</b> When {@link RqueueConfig#isProducer()} is true the application
 * has no listeners and therefore no consumers that could exhaust retries; the registrar exits
 * early and installs nothing.
 *
 * <p><b>Backend gating.</b> Only does its work when the active broker is a
 * {@link JetStreamMessageBroker}; on Redis or other backends the bean simply no-ops, so it is safe
 * to wire unconditionally from the NATS auto-config (which is itself gated on
 * {@code rqueue.backend=nats}).
 */
public class NatsDeadLetterBridgeRegistrar
    implements SmartInitializingSingleton, DisposableBean {

  private static final Logger log =
      Logger.getLogger(NatsDeadLetterBridgeRegistrar.class.getName());

  private final MessageBroker broker;
  private final RqueueConfig rqueueConfig;
  private final List<AutoCloseable> bridges = new ArrayList<>();

  public NatsDeadLetterBridgeRegistrar(MessageBroker broker, RqueueConfig rqueueConfig) {
    this.broker = broker;
    this.rqueueConfig = rqueueConfig;
  }

  @Override
  public void afterSingletonsInstantiated() {
    if (rqueueConfig != null && rqueueConfig.isProducer()) {
      log.log(Level.FINE,
          "NatsDeadLetterBridgeRegistrar: producer-only mode — skipping bridge installation");
      return;
    }
    if (!(broker instanceof JetStreamMessageBroker)) {
      // Defensive — the bean is wired only by the NATS auto-config, but other backends could
      // theoretically substitute a different MessageBroker via @Primary.
      return;
    }
    JetStreamMessageBroker nb = (JetStreamMessageBroker) broker;
    List<QueueDetail> queues = EndpointRegistry.getActiveQueueDetails();
    if (queues.isEmpty()) {
      return;
    }
    int installed = 0;
    for (QueueDetail q : queues) {
      String consumerName = q.resolvedConsumerName();
      try {
        bridges.add(nb.installDeadLetterBridge(q, consumerName));
        installed++;
      } catch (RuntimeException e) {
        // Best-effort: a single failure must not abort listener startup. The rqueue-level DLQ
        // path (PostProcessingHandler.moveToDlq) still works regardless.
        log.log(Level.WARNING,
            "Failed to install dead-letter advisory bridge for queue " + q.getName()
                + " consumer " + consumerName + ": " + e.getMessage(), e);
      }
    }
    log.log(Level.INFO,
        "NatsDeadLetterBridgeRegistrar: installed {0} advisory bridge(s) across {1} queue(s)",
        new Object[] {installed, queues.size()});
  }

  @Override
  public void destroy() {
    for (AutoCloseable c : bridges) {
      try {
        c.close();
      } catch (Exception ignore) {
        // best-effort close; we are shutting down anyway
      }
    }
    bridges.clear();
  }
}
