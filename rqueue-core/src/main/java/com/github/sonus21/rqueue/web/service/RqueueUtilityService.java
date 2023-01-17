/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.rqueue.models.Pair;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import reactor.core.publisher.Mono;

public interface RqueueUtilityService {

  BooleanResponse deleteMessage(String queueName, String id);

  MessageMoveResponse moveMessage(MessageMoveRequest messageMoveRequest);

  BooleanResponse makeEmpty(String queueName, String dataName);

  Pair<String, String> getLatestVersion();

  StringResponse getDataType(String name);

  Mono<BooleanResponse> makeEmptyReactive(String queueName, String datasetName);

  Mono<BooleanResponse> deleteReactiveMessage(String queueName, String messageId);

  Mono<StringResponse> getReactiveDataType(String name);

  Mono<MessageMoveResponse> moveReactiveMessage(MessageMoveRequest request);

  Mono<BaseResponse> reactivePauseUnpauseQueue(PauseUnpauseQueueRequest request);

  BaseResponse pauseUnpauseQueue(PauseUnpauseQueueRequest request);

  Mono<DataSelectorResponse> reactiveAggregateDataCounter(AggregationType type);

  DataSelectorResponse aggregateDataCounter(AggregationType type);
}
