/*
 * Copyright (c) 2024-2026 Sonu Kumar
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

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.nats.RqueueNatsException;
import com.github.sonus21.rqueue.nats.internal.NatsProvisioner;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.springframework.context.ApplicationListener;

/**
 * Boot-time JetStream stream / DLQ existence guard. Mirrors the role
 * {@code NatsKvBucketValidator} plays for KV buckets — moves stream existence checks off the
 * publish / pop hot path (where they cost a {@code getStreamInfo} round-trip per message) onto
 * the bootstrap path so a running broker never has to ask "does this stream exist?" again.
 *
 * <p><b>When this runs.</b> Listens for {@link RqueueBootstrapEvent} (start). That event fires
 * from {@code RqueueMessageListenerContainer.afterPropertiesSet} after every
 * {@code @RqueueListener} method has registered its queue with {@link EndpointRegistry}, which
 * is the first moment the full queue / priority / DLQ set is known. {@code InitializingBean}
 * would be too early — the registry is still empty.
 *
 * <p><b>What it walks.</b> For every queue in {@link EndpointRegistry#getActiveQueueDetails()}:
 * <ul>
 *   <li>the main stream {@code <streamPrefix><queue>},
 *   <li>one stream per declared priority sub-queue ({@code <streamPrefix><queue>-<priority>}),
 *   <li>when the listener declared a Rqueue-level DLQ ({@link QueueDetail#isDlqSet()}): the
 *       target DLQ queue's stream ({@code <streamPrefix><deadLetterQueueName>}), so that
 *       {@code PostProcessingHandler} can publish there after retry exhaustion.
 *   <li>when no Rqueue DLQ is declared and {@link RqueueNatsConfig#isAutoCreateDlqStream()} is
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
 *
 * <p>This class consumes {@link RqueueBootstrapEvent} via {@link ApplicationListener} rather
 * than {@code @EventListener} so it works under both Spring Boot and plain Spring without
 * pulling in a stereotype scan.
 */
public class NatsStreamValidator implements ApplicationListener<RqueueBootstrapEvent> {

  private static final Logger log = Logger.getLogger(NatsStreamValidator.class.getName());

  private final NatsProvisioner provisioner;
  private final RqueueNatsConfig config;

  public NatsStreamValidator(NatsProvisioner provisioner, RqueueNatsConfig config) {
    this.provisioner = provisioner;
    this.config = config;
  }

  @Override
  public void onApplicationEvent(RqueueBootstrapEvent event) {
    if (!event.isStart()) {
      return; // shutdown event; nothing to provision
    }
    List<QueueDetail> queues = EndpointRegistry.getActiveQueueDetails();
    if (queues.isEmpty()) {
      log.log(Level.FINE, "NatsStreamValidator: no active queues registered; nothing to do");
      return;
    }
    RqueueNatsConfig.ConsumerDefaults cd = config.getConsumerDefaults();
    List<String> failures = new ArrayList<>();
    int total = 0;
    for (QueueDetail q : queues) {
      String mainStream = config.getStreamPrefix() + q.getName();
      String mainSubject = config.getSubjectPrefix() + q.getName();
      String configuredConsumer = resolveConsumerName(q);
      total += tryEnsure(failures, mainStream, mainSubject);
      tryEnsureConsumer(failures, mainStream, configuredConsumer, cd, mainSubject);

      if (q.getPriority() != null) {
        for (String priority : q.getPriority().keySet()) {
          String pStream = mainStream + "-" + priority;
          String pSubject = mainSubject + "." + priority;
          total += tryEnsure(failures, pStream, pSubject);
          tryEnsureConsumer(failures, pStream, configuredConsumer, cd, pSubject);
        }
      }

      if (q.isDlqSet()) {
        // User declared a Rqueue-level DLQ: ensure the target queue's JetStream stream exists
        // so that PostProcessingHandler can publish to it after retry exhaustion. The
        // NATS-native "job-queue-dlq" stream is unrelated and must not be created here —
        // Rqueue routes the message explicitly, not via advisory bridging.
        String dlqQueueStream = config.getStreamPrefix() + q.getDeadLetterQueueName();
        String dlqQueueSubject = config.getSubjectPrefix() + q.getDeadLetterQueueName();
        total += tryEnsure(failures, dlqQueueStream, dlqQueueSubject);
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

  /** Returns the configured consumer name override, or the default derived from the queue name. */
  static String resolveConsumerName(QueueDetail q) {
    String override = q.getNatsConsumerName();
    return (override != null && !override.isEmpty()) ? override : consumerName(q.getName());
  }

  /** Derives the default consumer name when no override is configured. */
  static String consumerName(String queueName) {
    return "rqueue-" + queueName;
  }

  private void tryEnsureConsumer(
      List<String> failures,
      String streamName,
      String consumerName,
      RqueueNatsConfig.ConsumerDefaults cd,
      String filterSubject) {
    try {
      provisioner.ensureConsumer(
          streamName,
          consumerName,
          cd.getAckWait(),
          cd.getMaxDeliver(),
          cd.getMaxAckPending(),
          filterSubject);
    } catch (RqueueNatsException e) {
      failures.add("consumer " + consumerName + " on " + streamName + ": " + rootCause(e));
    }
  }

  private int tryEnsure(List<String> failures, String streamName, String subject) {
    try {
      provisioner.ensureStream(streamName, List.of(subject));
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
