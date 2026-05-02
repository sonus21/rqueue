/*
 * Copyright (c) 2024-2026 Sonu Kumar
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
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.utils.Constants;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/**
 * NATS-backend stub for {@link RqueueUtilityService}. Admin/dashboard utility methods are
 * Redis-only in v1; this stub returns "not supported" responses uniformly so the rest of the
 * bean graph stays consistent. Replace with a NATS-native implementation in a follow-up.
 */
@Service
@Conditional(NatsBackendCondition.class)
public class NatsRqueueUtilityService implements RqueueUtilityService {

  @Autowired
  private RqueueWebConfig rqueueWebConfig;

  private static <T extends BaseResponse> T notSupported(T response, String op) {
    response.setCode(1);
    response.setMessage(op + " is not supported with rqueue.backend=nats in v1");
    return response;
  }

  @Override
  public BooleanResponse deleteMessage(String queueName, String id) {
    return notSupported(new BooleanResponse(), "deleteMessage");
  }

  @Override
  public BooleanResponse enqueueMessage(String queueName, String id, String position) {
    return notSupported(new BooleanResponse(), "enqueueMessage");
  }

  @Override
  public MessageMoveResponse moveMessage(MessageMoveRequest messageMoveRequest) {
    return notSupported(new MessageMoveResponse(), "moveMessage");
  }

  @Override
  public BooleanResponse makeEmpty(String queueName, String dataName) {
    return notSupported(new BooleanResponse(), "makeEmpty");
  }

  @Override
  public Pair<String, String> getLatestVersion() {
    return new Pair<>("", "");
  }

  @Override
  public StringResponse getDataType(String name) {
    return notSupported(new StringResponse(), "getDataType");
  }

  @Override
  public Mono<BooleanResponse> makeEmptyReactive(String queueName, String datasetName) {
    return Mono.just(notSupported(new BooleanResponse(), "makeEmptyReactive"));
  }

  @Override
  public Mono<BooleanResponse> deleteReactiveMessage(String queueName, String messageId) {
    return Mono.just(notSupported(new BooleanResponse(), "deleteReactiveMessage"));
  }

  @Override
  public Mono<BooleanResponse> enqueueReactiveMessage(
      String queueName, String messageId, String position) {
    return Mono.just(notSupported(new BooleanResponse(), "enqueueReactiveMessage"));
  }

  @Override
  public Mono<StringResponse> getReactiveDataType(String name) {
    return Mono.just(notSupported(new StringResponse(), "getReactiveDataType"));
  }

  @Override
  public Mono<MessageMoveResponse> moveReactiveMessage(MessageMoveRequest request) {
    return Mono.just(notSupported(new MessageMoveResponse(), "moveReactiveMessage"));
  }

  @Override
  public Mono<BaseResponse> reactivePauseUnpauseQueue(PauseUnpauseQueueRequest request) {
    return Mono.just(notSupported(new BaseResponse(), "reactivePauseUnpauseQueue"));
  }

  @Override
  public BaseResponse pauseUnpauseQueue(PauseUnpauseQueueRequest request) {
    return notSupported(new BaseResponse(), "pauseUnpauseQueue");
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
