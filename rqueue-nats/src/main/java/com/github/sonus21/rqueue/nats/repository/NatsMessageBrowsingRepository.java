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

package com.github.sonus21.rqueue.nats.repository;

import com.github.sonus21.rqueue.exception.BackendCapabilityException;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NATS-backend impl of {@link MessageBrowsingRepository}. Maps Redis-style queue-name keys to
 * JetStream streams and returns actual message counts from JetStream {@link StreamInfo}. The
 * size queries provide real values for:
 *
 * <ul>
 *   <li>Main queue pending: looks up stream by extracting queue name and prefixing with stream
 *       prefix
 *   <li>Dead-letter counts: looks up DLQ stream with DLQ suffix
 *   <li>Processing/Scheduled queues: returns 0 (NATS doesn't expose these separately; consumers
 *       tracks in-flight implicitly, scheduled delivery unsupported)
 * </ul>
 *
 * <p>{@link #viewData} throws {@link BackendCapabilityException} (mapped to HTTP 501 by the web
 * advice) since JetStream KV doesn't expose positional reads on arbitrary keys; the
 * data-explorer panel is Redis-only in v1.
 */
public class NatsMessageBrowsingRepository implements MessageBrowsingRepository {

  private static final Logger log = Logger.getLogger(NatsMessageBrowsingRepository.class.getName());

  private final JetStreamManagement jsm;
  private final RqueueNatsConfig config;

  public NatsMessageBrowsingRepository(JetStreamManagement jsm, RqueueNatsConfig config) {
    this.jsm = jsm;
    this.config = config;
  }

  @Override
  public long getDataSize(String name, DataType type) {
    if (name == null || name.isEmpty()) {
      return 0L;
    }
    try {
      // Try to extract queue name from Redis-style key patterns and look up the NATS stream.
      String queueName = extractQueueName(name);
      if (queueName == null) {
        return 0L; // Key pattern not recognized; return 0.
      }

      // Check if this is a DLQ by matching against known DLQ patterns.
      if (isDlqName(name, queueName)) {
        String dlqStream = config.getStreamPrefix() + queueName + config.getDlqStreamSuffix();
        return getStreamMessageCount(dlqStream);
      }

      // For main queue, processing, and scheduled queue patterns, return sizes.
      // Processing/scheduled counts return 0 in v1 (not exposed by NATS/JetStream natively).
      if (isProcessingQueue(name) || isScheduledQueue(name)) {
        return 0L;
      }

      // Assume it's a main queue pending count.
      String stream = config.getStreamPrefix() + queueName;
      return getStreamMessageCount(stream);
    } catch (IOException | JetStreamApiException e) {
      log.log(Level.WARNING, "Failed to get data size for name=" + name, e);
      return 0L; // Fail gracefully; return 0 if stream lookup fails.
    }
  }

  @Override
  public List<Long> getDataSizes(List<String> names, List<DataType> types) {
    if (names == null || names.isEmpty()) {
      return new ArrayList<>();
    }
    List<Long> out = new ArrayList<>(names.size());
    for (String name : names) {
      out.add(getDataSize(name, null));
    }
    return out;
  }

  @Override
  public DataViewResponse viewData(
      String name, DataType type, String key, int pageNumber, int itemPerPage) {
    throw new BackendCapabilityException(
        "nats",
        "viewData",
        "JetStream does not expose positional reads on arbitrary keys; the dashboard's"
            + " data-explorer panel is Redis-only in v1.");
  }

  /**
   * Extract the base queue name from a Redis-style key. Recognizes patterns like:
   * <ul>
   *   <li>{@code __rq::queue::queueName} → {@code queueName}
   *   <li>{@code __rq::p-queue::queueName} → {@code queueName}
   *   <li>{@code __rq::d-queue::queueName} → {@code queueName}
   *   <li>{@code queueName-dlq} → {@code queueName} (if it's a DLQ)
   *   <li>Other patterns → {@code null}
   * </ul>
   */
  private String extractQueueName(String name) {
    // Pattern: __rq::queue::queueName or __rq::p-queue::queueName or __rq::d-queue::queueName
    if (name.startsWith("__rq::")) {
      int lastSeparator = name.lastIndexOf("::");
      if (lastSeparator >= 0) {
        return name.substring(lastSeparator + 2);
      }
    }
    // Assume it's a direct queue name (e.g., for DLQ patterns).
    return name;
  }

  /**
   * Check if the name matches a processing-queue pattern ({@code __rq::p-queue::...}).
   */
  private boolean isProcessingQueue(String name) {
    return name != null && name.contains("::p-queue::");
  }

  /**
   * Check if the name matches a scheduled-queue pattern ({@code __rq::d-queue::...}).
   */
  private boolean isScheduledQueue(String name) {
    return name != null && name.contains("::d-queue::");
  }

  /**
   * Check if the extracted queue name matches a DLQ name pattern. For now, we assume any name not
   * matching the {@code __rq::} prefix patterns is a DLQ candidate.
   */
  private boolean isDlqName(String originalName, String extractedQueueName) {
    return !originalName.startsWith("__rq::");
  }

  /**
   * Safely query a JetStream stream for its message count. Returns 0 if the stream doesn't
   * exist or on errors.
   */
  private long getStreamMessageCount(String streamName) throws IOException, JetStreamApiException {
    try {
      StreamInfo info = jsm.getStreamInfo(streamName);
      return info.getStreamState().getMsgCount();
    } catch (JetStreamApiException e) {
      // 10059 = stream not found; return 0 rather than failing.
      if (e.getApiErrorCode() == 10059) {
        return 0L;
      }
      throw e;
    }
  }
}
