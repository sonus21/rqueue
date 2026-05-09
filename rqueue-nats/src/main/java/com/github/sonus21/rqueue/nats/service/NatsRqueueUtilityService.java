/*
 * Copyright (c) 2026 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

package com.github.sonus21.rqueue.nats.service;

import com.github.sonus21.rqueue.config.NatsBackendCondition;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.serdes.RqueueSerDes;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.nats.RqueueNatsConfig;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
import io.nats.client.JetStream;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.api.MessageInfo;
import io.nats.client.api.StreamInfo;
import io.nats.client.impl.Headers;
import java.io.IOException;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * NATS-backend implementation of {@link RqueueUtilityService}.
 *
 * <p>The implementation supports operations that map cleanly onto JetStream's model:
 * <ul>
 *   <li>{@link #pauseUnpauseQueue(PauseUnpauseQueueRequest)} — flips the {@code paused} flag on
 *       {@link QueueConfig} in the queue-config KV bucket and propagates the change to the local
 *       {@link RqueueMessageListenerContainer} so the poller stops polling. Multi-instance fan-out
 *       is a follow-up (NATS pub/sub bridge).
 *   <li>{@link #deleteMessage(String, String)} — soft delete: marks the metadata record in the
 *       message-metadata KV bucket. The stream message persists; the dashboard hides it via the
 *       {@code deleted} flag, matching the Redis impl's semantics.
 *   <li>{@link #aggregateDataCounter(AggregationType)} — pure date-selector logic, no backend
 *       dependency.
 *   <li>{@link #getDataType(String)} — reports {@code "STREAM"} since JetStream subjects map to
 *       stream messages, not Redis-shaped data structures.
 * </ul>
 *
 * <p>{@link #moveMessage(MessageMoveRequest)} reads up to {@code maxMessages} from the source
 * JetStream stream, republishes each to the destination stream, and hard-deletes the source copy
 * via {@link JetStreamManagement#deleteMessage}. {@link #enqueueMessage(String, String, String)}
 * looks up the message in the metadata store and republishes it immediately (without a
 * {@code Nats-Next-Deliver-Time} header) so the worker picks it up on its next poll.
 *
 * <p>{@link #makeEmpty(String, String)} still returns "not supported" — purging a stream is a
 * destructive admin operation best performed via {@code nats stream purge}.
 */
@Service
@Conditional(NatsBackendCondition.class)
@Slf4j
public class NatsRqueueUtilityService implements RqueueUtilityService {

  private static final String NOT_SUPPORTED_SUFFIX =
      " is not supported with rqueue.backend=nats in v1";

  /** Error code returned by JetStream when a sequence does not exist in the stream. */
  private static final int JS_NO_MESSAGE_FOUND = 10037;

  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueSystemConfigDao systemConfigDao;
  private final RqueueMessageMetadataService messageMetadataService;
  private final RqueueMessageListenerContainer rqueueMessageListenerContainer;
  private final JetStreamManagement jsm;
  private final JetStream js;
  private final RqueueSerDes serdes;
  private final String streamPrefix;
  private final String subjectPrefix;

  @Autowired
  public NatsRqueueUtilityService(
      RqueueWebConfig rqueueWebConfig,
      RqueueSystemConfigDao systemConfigDao,
      RqueueMessageMetadataService messageMetadataService,
      RqueueMessageListenerContainer rqueueMessageListenerContainer,
      JetStreamManagement jsm,
      JetStream js,
      RqueueSerDes serdes,
      RqueueNatsConfig natsConfig) {
    this.rqueueWebConfig = rqueueWebConfig;
    this.systemConfigDao = systemConfigDao;
    this.messageMetadataService = messageMetadataService;
    this.rqueueMessageListenerContainer = rqueueMessageListenerContainer;
    this.jsm = jsm;
    this.js = js;
    this.serdes = serdes;
    this.streamPrefix = natsConfig.getStreamPrefix();
    this.subjectPrefix = natsConfig.getSubjectPrefix();
  }

  private static <T extends BaseResponse> T notSupported(T response, String op) {
    response.setCode(1);
    response.setMessage(op + NOT_SUPPORTED_SUFFIX);
    return response;
  }

  /**
   * Soft-delete: marks the message metadata as deleted. The underlying stream message persists
   * (JetStream streams are immutable), but the dashboard and consumers honor the deleted flag.
   */
  @Override
  public BooleanResponse deleteMessage(String queueName, String id) {
    BooleanResponse response = new BooleanResponse();
    if (StringUtils.isEmpty(queueName) || StringUtils.isEmpty(id)) {
      response.setCode(1);
      response.setMessage("queueName and id are required");
      return response;
    }
    try {
      boolean ok = messageMetadataService.deleteMessage(
          queueName, id, Duration.ofDays(Constants.DAYS_IN_A_MONTH));
      if (!ok) {
        response.setCode(1);
        response.setMessage("Message metadata not found for queue=" + queueName + " id=" + id);
        return response;
      }
      response.setValue(true);
      return response;
    } catch (Exception e) {
      log.warn("deleteMessage failed for queue={} id={}", queueName, id, e);
      response.setCode(1);
      response.setMessage("deleteMessage failed: " + e.getMessage());
      return response;
    }
  }

  /**
   * Re-enqueue a message for immediate delivery. Looks up the {@link RqueueMessage} from the
   * metadata store by {@code queueName + id}, then republishes the raw bytes to the queue's
   * JetStream stream without a {@code Nats-Next-Deliver-Time} header so the poller picks it up
   * on its next fetch. A fresh {@code Nats-Msg-Id} ({@code id-requeue-<millis>}) prevents
   * JetStream from deduplicating against the original scheduled publish. The {@code position}
   * hint (FRONT / BACK) is ignored — JetStream pull consumers deliver in stream-sequence order.
   */
  @Override
  public BooleanResponse enqueueMessage(String queueName, String id, String position) {
    BooleanResponse r = new BooleanResponse();
    if (StringUtils.isEmpty(queueName) || StringUtils.isEmpty(id)) {
      r.setCode(1);
      r.setMessage("queueName and id are required");
      return r;
    }
    MessageMetadata meta = messageMetadataService.getByMessageId(queueName, id);
    if (meta == null || meta.getRqueueMessage() == null) {
      r.setCode(1);
      r.setMessage("Message not found for queue=" + queueName + " id=" + id);
      return r;
    }
    RqueueMessage message = meta.getRqueueMessage();
    String subject = toSubject(queueName);
    try {
      byte[] payload = serdes.serialize(message);
      Headers headers = new Headers();
      // fresh dedup key so JetStream doesn't drop this as a duplicate of the original publish
      headers.add("Nats-Msg-Id", id + "-requeue-" + System.currentTimeMillis());
      js.publish(subject, headers, payload);
      r.setValue(true);
      return r;
    } catch (Exception e) {
      log.warn("enqueueMessage failed queue={} id={}", queueName, id, e);
      r.setCode(1);
      r.setMessage("enqueueMessage failed: " + e.getMessage());
      return r;
    }
  }

  /**
   * Move up to {@code maxMessages} messages from the source stream to the destination stream.
   * For each message: re-publishes the raw bytes (with original headers minus {@code Nats-Msg-Id})
   * to the destination subject, then hard-deletes the source sequence via
   * {@link JetStreamManagement#deleteMessage}. Both {@code src} and {@code dst} can be either
   * bare queue names (e.g. {@code "orders"}) or fully-prefixed stream names
   * (e.g. {@code "rqueue-js-orders"}) — the prefix is added if absent.
   */
  @Override
  public MessageMoveResponse moveMessage(MessageMoveRequest request) {
    String error = request.validationMessage();
    if (error != null) {
      MessageMoveResponse r = new MessageMoveResponse();
      r.setCode(1);
      r.setMessage(error);
      return r;
    }
    int maxCount = request.getMessageCount(rqueueWebConfig);
    String srcStream = toStream(request.getSrc());
    String dstStream = toStream(request.getDst());
    String dstSubject = toSubject(request.getDst());
    try {
      StreamInfo info = jsm.getStreamInfo(srcStream);
      long seq = info.getStreamState().getFirstSequence();
      long last = info.getStreamState().getLastSequence();
      int moved = 0;
      while (seq <= last && moved < maxCount) {
        try {
          MessageInfo mi = jsm.getMessage(srcStream, seq);
          if (mi != null && mi.getData() != null) {
            Headers h = new Headers();
            if (mi.getHeaders() != null) {
              mi.getHeaders().forEach((k, v) -> {
                if (!"Nats-Msg-Id".equals(k)) { // avoid dedup collision on destination
                  h.add(k, v);
                }
              });
            }
            js.publish(dstSubject, h, mi.getData());
            jsm.deleteMessage(srcStream, seq, false);
            moved++;
          }
        } catch (JetStreamApiException e) {
          if (e.getApiErrorCode() == JS_NO_MESSAGE_FOUND) {
            // already consumed or deleted by another process — skip
          } else {
            throw e;
          }
        }
        seq++;
      }
      MessageMoveResponse r = new MessageMoveResponse(moved);
      r.setValue(moved > 0);
      return r;
    } catch (IOException | JetStreamApiException e) {
      log.warn("moveMessage failed src={} dst={}", srcStream, dstStream, e);
      MessageMoveResponse r = new MessageMoveResponse();
      r.setCode(1);
      r.setMessage("moveMessage failed: " + e.getMessage());
      return r;
    }
  }

  private String toStream(String name) {
    return name.startsWith(streamPrefix) ? name : streamPrefix + name;
  }

  private String toSubject(String name) {
    return name.startsWith(subjectPrefix) ? name : subjectPrefix + name;
  }

  /**
   * NATS would require destructive stream re-creation to empty a queue. Out-of-band admin op
   * (e.g. {@code nats stream purge}) is the recommended path; surfaces "not supported" for now.
   */
  @Override
  public BooleanResponse makeEmpty(String queueName, String dataName) {
    return notSupported(new BooleanResponse(), "makeEmpty");
  }

  @Override
  public Pair<String, String> getLatestVersion() {
    return new Pair<>("", "");
  }

  /**
   * NATS-backed queues are always JetStream streams; report a fixed type rather than probing the
   * KV / stream layer per call.
   */
  @Override
  public StringResponse getDataType(String name) {
    StringResponse response = new StringResponse();
    response.setVal("STREAM");
    return response;
  }

  @Override
  public Mono<BooleanResponse> makeEmptyReactive(String queueName, String datasetName) {
    return Mono.just(makeEmpty(queueName, datasetName));
  }

  @Override
  public Mono<BooleanResponse> deleteReactiveMessage(String queueName, String messageId) {
    return Mono.just(deleteMessage(queueName, messageId));
  }

  @Override
  public Mono<BooleanResponse> enqueueReactiveMessage(
      String queueName, String messageId, String position) {
    return Mono.just(enqueueMessage(queueName, messageId, position));
  }

  @Override
  public Mono<StringResponse> getReactiveDataType(String name) {
    return Mono.just(getDataType(name));
  }

  @Override
  public Mono<MessageMoveResponse> moveReactiveMessage(MessageMoveRequest request) {
    return Mono.just(moveMessage(request));
  }

  @Override
  public Mono<BaseResponse> reactivePauseUnpauseQueue(PauseUnpauseQueueRequest request) {
    return Mono.just(pauseUnpauseQueue(request));
  }

  /**
   * Toggle the {@code paused} flag on the queue's {@link QueueConfig} (persisted in the
   * {@code rqueue-queue-config} KV bucket) and propagate the change to the local listener
   * container so the poller stops dispatching new work.
   *
   * <p>Multi-instance fan-out (i.e. propagating the pause across worker JVMs) is a follow-up;
   * single-instance deployments are fully covered by this path.
   */
  @Override
  public BaseResponse pauseUnpauseQueue(PauseUnpauseQueueRequest request) {
    log.info("Queue PauseUnpause request {}", request);
    BaseResponse response = new BaseResponse();
    if (request == null || StringUtils.isEmpty(request.getName())) {
      response.set(400, "Queue name is required");
      return response;
    }
    QueueConfig queueConfig = systemConfigDao.getConfigByName(request.getName(), true);
    if (queueConfig == null) {
      response.set(404, "Queue does not exist");
      return response;
    }
    boolean targetState = request.isPause();
    if (queueConfig.isPaused() == targetState) {
      // No-op: state already matches; respond OK and skip the listener call to avoid the
      // "duplicate pause" / "not paused but unpause" warnings in QueueStateMgr.
      return response;
    }
    queueConfig.setPaused(targetState);
    systemConfigDao.saveQConfig(queueConfig);
    try {
      rqueueMessageListenerContainer.pauseUnpauseQueue(request.getName(), targetState);
    } catch (Exception e) {
      // QueueConfig is already persisted; surface the pause-propagation failure to the caller
      // but do not roll back — the next listener restart will pick up the persisted flag.
      log.warn("pauseUnpauseQueue listener notification failed for queue={}", request.getName(), e);
      response.set(500, "Persisted but listener notification failed: " + e.getMessage());
    }
    return response;
  }

  @Override
  public Mono<DataSelectorResponse> reactiveAggregateDataCounter(AggregationType type) {
    return Mono.just(aggregateDataCounter(type));
  }

  @Override
  public DataSelectorResponse aggregateDataCounter(AggregationType type) {
    String title;
    List<Pair<String, String>> data;
    if (type == AggregationType.DAILY) {
      data = getDailyDateCounter();
      title = "Select Number of Days";
    } else if (type == AggregationType.WEEKLY) {
      data = getWeeklyDateCounter();
      title = "Select Number of Weeks";
    } else {
      data = getMonthlyDateCounter();
      title = "Select Number of Months";
    }
    return new DataSelectorResponse(title, data);
  }

  private List<Pair<String, String>> getDailyDateCounter() {
    List<Pair<String, String>> dateSelector = new LinkedList<>();
    int[] dates = new int[] {1, 2, 3, 4, 6, 7};
    int step = 15;
    int stepAfter = 15;
    int i = 1;
    dateSelector.add(new Pair<>("0", "Select"));
    while (i <= rqueueWebConfig.getHistoryDay()) {
      if (i >= stepAfter) {
        if (i <= rqueueWebConfig.getHistoryDay()) {
          dateSelector.add(new Pair<>(String.valueOf(i), String.format("Last %d days", i)));
        }
        i += step;
      } else {
        for (int date : dates) {
          if (date == i) {
            String suffix = i == 1 ? "day" : "days";
            dateSelector.add(
                new Pair<>(String.valueOf(date), String.format("Last %d %s", date, suffix)));
            break;
          }
        }
        i += 1;
      }
    }
    return dateSelector;
  }

  private List<Pair<String, String>> getWeeklyDateCounter() {
    List<Pair<String, String>> dateSelector = new LinkedList<>();
    dateSelector.add(new Pair<>("0", "Select"));
    int nWeek =
        (int) Math.ceil(rqueueWebConfig.getHistoryDay() / (double) Constants.DAYS_IN_A_WEEK);
    for (int week = 1; week <= nWeek; week++) {
      String suffix = week == 1 ? "week" : "weeks";
      dateSelector.add(new Pair<>(String.valueOf(week), String.format("Last %d %s", week, suffix)));
    }
    return dateSelector;
  }

  private List<Pair<String, String>> getMonthlyDateCounter() {
    List<Pair<String, String>> dateSelector = new LinkedList<>();
    dateSelector.add(new Pair<>("0", "Select"));
    int nMonths =
        (int) Math.ceil(rqueueWebConfig.getHistoryDay() / (double) Constants.DAYS_IN_A_MONTH);
    for (int month = 1; month <= nMonths; month++) {
      String suffix = month == 1 ? "month" : "months";
      dateSelector.add(
          new Pair<>(String.valueOf(month), String.format("Last %d %s", month, suffix)));
    }
    return dateSelector;
  }
}
