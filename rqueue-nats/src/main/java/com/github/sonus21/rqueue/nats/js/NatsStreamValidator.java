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
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.beans.factory.SmartInitializingSingleton;

/**
 * Boot-time JetStream stream / DLQ existence guard. Mirrors the role
 * {@code NatsKvBucketValidator} plays for KV buckets — moves stream existence checks off the
 * publish / pop hot path (where they cost a {@code getStreamInfo} round-trip per message) onto
 * the bootstrap path so a running broker never has to ask "does this stream exist?" again.
 *
 * <p><b>When this runs.</b> Implements {@link SmartInitializingSingleton} so provisioning fires
 * after every singleton bean — including {@code RqueueMessageListenerContainer} — has finished
 * its {@code afterPropertiesSet}, which is when {@link EndpointRegistry} is populated, and
 * <em>before</em> {@code SmartLifecycle.start()} spawns the message pollers. Listening on
 * {@code RqueueBootstrapEvent} would race against the pollers because that event fires
 * <em>after</em> {@code doStart()} has already submitted them, and a poll on a not-yet-created
 * stream surfaces as {@code stream not found [10059]}. {@code InitializingBean} would be too
 * early — the registry is still empty when this bean's own {@code afterPropertiesSet} would run.
 *
 * <p><b>What it walks.</b> For every queue in {@link EndpointRegistry#getActiveQueueDetails()}:
 * <ul>
 *   <li>the main stream {@code <streamPrefix><queue>},
 *   <li>one stream per declared priority sub-queue ({@code <streamPrefix><queue>-<priority>}),
 *   <li>when the listener declared a Rqueue-level DLQ ({@link QueueDetail#isDlqSet()}): the
 *       target DLQ queue's stream ({@code <streamPrefix><deadLetterQueueName>}), so that
 *       {@code PostProcessingHandler} can publish there after retry exhaustion.
 *   <li>when no Rqueue DLQ is declared and {@code RqueueNatsConfig.isAutoCreateDlqStream()} is
 *       true: the NATS-native DLQ stream ({@code <streamPrefix><queue><dlqStreamSuffix>}) as a
 *       safety net for messages that exhaust JetStream {@code maxDeliver}.
 * </ul>
 *
 * <p><b>Behaviour by flag.</b> All work is delegated to
 * {@link NatsProvisioner#ensureStream(String, java.util.List)} /
 * {@link NatsProvisioner#ensureDlqStream(String, java.util.List)}, so the validator inherits the
 * existing flag semantics without re-implementing them:
 * <ul>
 *   <li>{@code autoCreateStreams=true} (default) — any missing stream is created using
 *       {@link RqueueNatsConfig.StreamDefaults}.
 *   <li>{@code autoCreateStreams=false} — every missing stream surfaces an
 *       {@link RqueueNatsException}; the validator collects all of them and raises one
 *       {@link IllegalStateException} listing every missing stream so operators can run a
 *       single batch of {@code nats stream add} commands rather than chase failures one queue
 *       at a time.
 * </ul>
 */
public class NatsStreamValidator implements SmartInitializingSingleton {

  private static final Logger log = Logger.getLogger(NatsStreamValidator.class.getName());

  private final NatsProvisioner provisioner;
  private final RqueueNatsConfig config;
  private final RqueueConfig rqueueConfig;

  public NatsStreamValidator(NatsProvisioner provisioner, RqueueNatsConfig config) {
    this(provisioner, config, null);
  }

  public NatsStreamValidator(
      NatsProvisioner provisioner, RqueueNatsConfig config, RqueueConfig rqueueConfig) {
    this.provisioner = provisioner;
    this.config = config;
    this.rqueueConfig = rqueueConfig;
  }

  @Override
  public void afterSingletonsInstantiated() {
    List<QueueDetail> queues = EndpointRegistry.getActiveQueueDetails();
    if (queues.isEmpty()) {
      log.log(Level.FINE, "NatsStreamValidator: no active queues registered; nothing to do");
      return;
    }
    RqueueNatsConfig.ConsumerDefaults cd = config.getConsumerDefaults();
    boolean producerOnly = rqueueConfig != null && rqueueConfig.isProducer();
    List<String> failures = new ArrayList<>();
    int total = 0;
    for (QueueDetail q : queues) {
      String mainStream = config.getStreamPrefix() + q.getName();
      String mainSubject = config.getSubjectPrefix() + q.getName();
      total += tryEnsure(failures, mainStream, mainSubject, q);
      if (!producerOnly) {
        String consumerName = resolveConsumerName(q);
        tryEnsureConsumer(failures, mainStream, consumerName, q, cd);
      }

      if (q.getPriority() != null) {
        for (String priority : q.getPriority().keySet()) {
          if (Constants.DEFAULT_PRIORITY_KEY.equals(priority)) {
            continue; // DEFAULT entry is the queue itself; already handled above
          }
          String pStream =
              config.getStreamPrefix() + q.getName() + PriorityUtils.getSuffix(priority);
          String pSubject =
              config.getSubjectPrefix() + q.getName() + PriorityUtils.getSuffix(priority);
          total += tryEnsure(failures, pStream, pSubject, q);
          // Consumer is NOT created here: each priority sub-queue has its own QueueDetail
          // in the registry and is processed as its own mainStream entry above, so exactly
          // one consumer is created per stream. Adding a second one here would fail on
          // WorkQueue streams (error 10099).
        }
      }

      if (q.isDlqSet()) {
        // User declared a Rqueue-level DLQ: ensure the target queue's JetStream stream exists
        // so that PostProcessingHandler can publish to it after retry exhaustion. The
        // NATS-native "job-queue-dlq" stream is unrelated and must not be created here —
        // Rqueue routes the message explicitly, not via advisory bridging.
        String dlqQueueStream = config.getStreamPrefix() + q.getDeadLetterQueueName();
        String dlqQueueSubject = config.getSubjectPrefix() + q.getDeadLetterQueueName();
        total += tryEnsure(failures, dlqQueueStream, dlqQueueSubject, q);
        // No consumer needed for the DLQ stream here — the DLQ queue registers its own listener.
      }
    }
    if (!failures.isEmpty()) {
      String hint = config.isAutoCreateStreams()
          ? "Stream creation failed — verify NATS is running with JetStream enabled"
              + " (start the server with `nats-server -js`) and that the account has"
              + " `add_stream` permission."
          : "With rqueue.nats.auto-create-streams=false every required stream must exist"
              + " before the application starts. Run `nats stream add` for each missing"
              + " stream or set rqueue.nats.auto-create-streams=true to let rqueue create"
              + " them automatically.";
      throw new IllegalStateException("NATS JetStream provisioning failed for "
          + failures.size()
          + " of "
          + total
          + " stream(s) at startup. "
          + hint
          + " Failed streams:\n"
          + "  - "
          + String.join("\n  - ", failures));
    }
    log.log(
        Level.INFO,
        "NatsStreamValidator: ensured {0} JetStream stream(s) across {1} queue(s)",
        new Object[] {total, queues.size()});
  }

  private String resolveConsumerName(QueueDetail q) {
    String customName = q.getConsumerName();
    if (q.getType() == com.github.sonus21.rqueue.enums.QueueType.QUEUE
        && (customName == null || customName.isEmpty())) {
      String sanitized = q.getName().replaceAll("[^A-Za-z0-9_-]", "-");
      return sanitized + "-consumer";
    }
    return q.resolvedConsumerName();
  }

  private void tryEnsureConsumer(
      List<String> failures,
      String streamName,
      String consumerName,
      QueueDetail q,
      RqueueNatsConfig.ConsumerDefaults cd) {
    Duration ackWait = JetStreamMessageBroker.resolveAckWait(q, config);
    long maxDeliver = JetStreamMessageBroker.resolveMaxDeliver(q, config);
    try {
      provisioner.ensureConsumer(
          streamName, consumerName, ackWait, maxDeliver, cd.getMaxAckPending());
    } catch (RqueueNatsException e) {
      failures.add("consumer " + consumerName + " on " + streamName + ": " + rootCause(e));
    }
  }

  private int tryEnsure(List<String> failures, String streamName, String subject, QueueDetail q) {
    try {
      provisioner.ensureStream(
          streamName, List.of(subject), q.getType(), "rqueue queue: " + q.getName());
      return 1;
    } catch (RqueueNatsException e) {
      failures.add(streamName + " (subject " + subject + "): " + rootCause(e));
      return 1;
    }
  }

  private int tryEnsureDlq(List<String> failures, String dlqStream, String dlqSubject) {
    try {
      provisioner.ensureDlqStream(dlqStream, List.of(dlqSubject));
      return 1;
    } catch (RqueueNatsException e) {
      failures.add(dlqStream + " (DLQ subject " + dlqSubject + "): " + rootCause(e));
      return 1;
    }
  }

  /** Returns the deepest non-null message in the cause chain for diagnostics. */
  private static String rootCause(Throwable t) {
    Throwable cause = t;
    while (cause.getCause() != null) {
      cause = cause.getCause();
    }
    String msg = cause.getMessage();
    return (msg != null && !msg.isEmpty()) ? msg : cause.getClass().getSimpleName();
  }
}
