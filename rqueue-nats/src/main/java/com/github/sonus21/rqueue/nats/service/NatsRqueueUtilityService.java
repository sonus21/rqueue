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
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer;
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
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
 * <p>Operations that have no JetStream equivalent return a structured "not supported" response:
 * {@link #moveMessage(MessageMoveRequest)}, {@link #enqueueMessage(String, String, String)} (no
 * scheduled-queue ZSET to re-enqueue from), and {@link #makeEmpty(String, String)} (would require
 * stream re-creation, which is destructive and out-of-band).
 */
@Service
@Conditional(NatsBackendCondition.class)
@Slf4j
public class NatsRqueueUtilityService implements RqueueUtilityService {

  private static final String NOT_SUPPORTED_SUFFIX =
      " is not supported with rqueue.backend=nats in v1";

  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueSystemConfigDao systemConfigDao;
  private final RqueueMessageMetadataService messageMetadataService;
  private final RqueueMessageListenerContainer rqueueMessageListenerContainer;

  @Autowired
  public NatsRqueueUtilityService(
      RqueueWebConfig rqueueWebConfig,
      RqueueSystemConfigDao systemConfigDao,
      RqueueMessageMetadataService messageMetadataService,
      RqueueMessageListenerContainer rqueueMessageListenerContainer) {
    this.rqueueWebConfig = rqueueWebConfig;
    this.systemConfigDao = systemConfigDao;
    this.messageMetadataService = messageMetadataService;
    this.rqueueMessageListenerContainer = rqueueMessageListenerContainer;
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
   * NATS does not support arbitrary message re-enqueue: stream sequences are immutable and there
   * is no scheduled-queue ZSET to pull from. Surfaces a structured "not supported" response.
   */
  @Override
  public BooleanResponse enqueueMessage(String queueName, String id, String position) {
    return notSupported(new BooleanResponse(), "enqueueMessage");
  }

  /**
   * NATS does not support cross-queue positional moves: streams are independent, sequences are
   * immutable. Surfaces a structured "not supported" response.
   */
  @Override
  public MessageMoveResponse moveMessage(MessageMoveRequest messageMoveRequest) {
    return notSupported(new MessageMoveResponse(), "moveMessage");
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
