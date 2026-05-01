/*
 * Copyright (c) 2020-2026 Sonu Kumar
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

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
import com.github.sonus21.rqueue.models.request.MessageEnqueueRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.PauseUnpauseQueueRequest;
import com.github.sonus21.rqueue.models.request.QueueExploreRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.DataSelectorResponse;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.service.RqueueUtilityService;
import com.github.sonus21.rqueue.utils.condition.ReactiveEnabled;
import com.github.sonus21.rqueue.web.service.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.service.RqueueJobService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Conditional(ReactiveEnabled.class)
@RestController
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue/api/v1")
public class ReactiveRqueueRestController extends BaseReactiveController {

  private final RqueueDashboardChartService rqueueDashboardChartService;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueUtilityService rqueueUtilityService;
  private final RqueueSystemManagerService rqueueQManagerService;
  private final RqueueJobService rqueueJobService;
  private final org.springframework.beans.factory.ObjectProvider<
          com.github.sonus21.rqueue.core.spi.MessageBroker>
      messageBrokerProvider;

  @Autowired
  public ReactiveRqueueRestController(
      RqueueDashboardChartService rqueueDashboardChartService,
      RqueueQDetailService rqueueQDetailService,
      RqueueUtilityService rqueueUtilityService,
      RqueueSystemManagerService rqueueQManagerService,
      RqueueWebConfig rqueueWebConfig,
      RqueueJobService rqueueJobService,
      org.springframework.beans.factory.ObjectProvider<
              com.github.sonus21.rqueue.core.spi.MessageBroker>
          messageBrokerProvider) {
    super(rqueueWebConfig);
    this.rqueueDashboardChartService = rqueueDashboardChartService;
    this.rqueueQDetailService = rqueueQDetailService;
    this.rqueueUtilityService = rqueueUtilityService;
    this.rqueueQManagerService = rqueueQManagerService;
    this.rqueueJobService = rqueueJobService;
    this.messageBrokerProvider = messageBrokerProvider;
  }

  @PostMapping("chart")
  @ResponseBody
  public Mono<ChartDataResponse> getDashboardData(
      @RequestBody @Valid ChartDataRequest chartDataRequest, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueDashboardChartService.getReactiveDashBoardData(chartDataRequest);
    }
    return null;
  }

  @GetMapping("jobs")
  @ResponseBody
  public Mono<DataViewResponse> getJobs(
      @RequestParam(name = "message-id") @NotEmpty String messageId, ServerHttpResponse response)
      throws ProcessingException {
    if (isEnabled(response)) {
      return rqueueJobService.getReactiveJobs(messageId);
    }
    return null;
  }

  @PostMapping("queue-data")
  @ResponseBody
  public Mono<DataViewResponse> exploreQueue(
      @RequestBody @Valid QueueExploreRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueQDetailService.getReactiveExplorePageData(
          request.getSrc(),
          request.getName(),
          request.getType(),
          request.getPageNumber(),
          request.getItemPerPage());
    }
    return null;
  }

  @PostMapping("view-data")
  @ResponseBody
  public Mono<DataViewResponse> viewData(
      @RequestBody @Valid DateViewRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueQDetailService.viewReactiveData(
          request.getName(),
          request.getType(),
          request.getKey(),
          request.getPageNumber(),
          request.getItemPerPage());
    }
    return null;
  }

  @PostMapping("delete-message")
  @ResponseBody
  public Mono<BooleanResponse> deleteMessage(
      @RequestBody @Valid MessageDeleteRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.deleteReactiveMessage(
          request.getQueueName(), request.getMessageId());
    }
    return null;
  }

  @PostMapping("enqueue-message")
  @ResponseBody
  public Mono<BooleanResponse> enqueueMessage(
      @RequestBody @Valid MessageEnqueueRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.enqueueReactiveMessage(
          request.getQueueName(), request.getMessageId(), request.getPosition());
    }
    return null;
  }

  @PostMapping("delete-queue")
  @ResponseBody
  public Mono<BaseResponse> deleteQueue(
      @RequestBody @Valid DataTypeRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueQManagerService.deleteReactiveQueue(request.getName());
    }
    return null;
  }

  @PostMapping("delete-queue-part")
  @ResponseBody
  public Mono<BooleanResponse> deleteAll(
      @RequestBody @Valid DataDeleteRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.makeEmptyReactive(
          request.getQueueName(), request.getDatasetName());
    }
    return null;
  }

  @PostMapping("data-type")
  @ResponseBody
  public Mono<StringResponse> dataType(
      @Valid @RequestBody DataTypeRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.getReactiveDataType(request.getName());
    }
    return null;
  }

  @PostMapping("move-data")
  @ResponseBody
  public Mono<MessageMoveResponse> dataType(
      @RequestBody @Valid MessageMoveRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.moveReactiveMessage(request);
    }
    return null;
  }

  @PostMapping("pause-unpause-queue")
  @ResponseBody
  public Mono<BaseResponse> pauseUnpauseQueue(
      @RequestBody @Valid PauseUnpauseQueueRequest request, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.reactivePauseUnpauseQueue(request);
    }
    return null;
  }

  @GetMapping("aggregate-data-selector")
  @ResponseBody
  public Mono<DataSelectorResponse> aggregateDataCounter(
      @RequestParam("type") AggregationType type, ServerHttpResponse response) {
    if (isEnabled(response)) {
      return rqueueUtilityService.reactiveAggregateDataCounter(type);
    }
    return null;
  }

  /** Reactive twin of {@code RqueueRestController#capabilities}. See javadoc there. */
  @GetMapping("capabilities")
  @ResponseBody
  public Mono<com.github.sonus21.rqueue.core.spi.Capabilities> capabilities(
      ServerHttpResponse response) {
    if (!isEnabled(response)) {
      return Mono.empty();
    }
    com.github.sonus21.rqueue.core.spi.MessageBroker broker =
        messageBrokerProvider.getIfAvailable();
    return Mono.just(
        broker == null
            ? com.github.sonus21.rqueue.core.spi.Capabilities.REDIS_DEFAULTS
            : broker.capabilities());
  }
}
