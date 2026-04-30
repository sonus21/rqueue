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
import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
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
    return Mono.just(notSupported(new DataSelectorResponse(), "reactiveAggregateDataCounter"));
  }

  @Override
  public DataSelectorResponse aggregateDataCounter(AggregationType type) {
    return notSupported(new DataSelectorResponse(), "aggregateDataCounter");
  }
}
