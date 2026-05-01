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
import io.nats.client.JetStreamManagement;
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
 *   <li>the DLQ stream ({@code <streamPrefix><queue><dlqStreamSuffix>}) — but only when the
 *       listener declared a DLQ (i.e. {@link QueueDetail#isDlqSet()}) and
 *       {@link RqueueNatsConfig#isAutoCreateDlqStream()} is true.
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

  public NatsStreamValidator(JetStreamManagement jsm, RqueueNatsConfig config) {
    this.provisioner = new NatsProvisioner(jsm, config);
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
    List<String> failures = new ArrayList<>();
    int total = 0;
    for (QueueDetail q : queues) {
      String mainStream = config.getStreamPrefix() + q.getName();
      String mainSubject = config.getSubjectPrefix() + q.getName();
      total += tryEnsure(failures, mainStream, mainSubject);

      if (q.getPriority() != null) {
        for (String priority : q.getPriority().keySet()) {
          total += tryEnsure(failures, mainStream + "-" + priority, mainSubject + "." + priority);
        }
      }

      if (q.isDlqSet() && config.isAutoCreateDlqStream()) {
        String dlqStream = mainStream + config.getDlqStreamSuffix();
        String dlqSubject = mainSubject + config.getDlqSubjectSuffix();
        total += tryEnsureDlq(failures, dlqStream, dlqSubject);
      }
    }
    if (!failures.isEmpty()) {
      throw new IllegalStateException("NATS JetStream provisioning failed for "
          + failures.size()
          + " of "
          + total
          + " stream(s) at startup. With rqueue.nats.autoCreateStreams=false, every required"
          + " stream must exist before the application starts. Failed streams:\n"
          + "  - "
          + String.join("\n  - ", failures));
    }
    log.log(
        Level.INFO,
        "NatsStreamValidator: ensured {0} JetStream stream(s) across {1} queue(s)",
        new Object[] {total, queues.size()});
  }

  private int tryEnsure(List<String> failures, String streamName, String subject) {
    try {
      provisioner.ensureStream(streamName, List.of(subject));
      return 1;
    } catch (RqueueNatsException e) {
      failures.add(streamName + " (subject " + subject + "): " + e.getMessage());
      return 1;
    }
  }

  private int tryEnsureDlq(List<String> failures, String dlqStream, String dlqSubject) {
    try {
      provisioner.ensureDlqStream(dlqStream, List.of(dlqSubject));
      return 1;
    } catch (RqueueNatsException e) {
      failures.add(dlqStream + " (DLQ subject " + dlqSubject + "): " + e.getMessage());
      return 1;
    }
  }
}
