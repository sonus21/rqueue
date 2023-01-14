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

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
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
import com.github.sonus21.rqueue.utils.condition.ReactiveDisabled;
import com.github.sonus21.rqueue.web.service.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.service.RqueueJobService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue/api/v1")
@Conditional(ReactiveDisabled.class)
public class RqueueRestController extends BaseController {

  private final RqueueDashboardChartService rqueueDashboardChartService;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueUtilityService rqueueUtilityService;
  private final RqueueSystemManagerService rqueueQManagerService;
  private final RqueueJobService rqueueJobService;

  @Autowired
  public RqueueRestController(
      RqueueDashboardChartService rqueueDashboardChartService,
      RqueueQDetailService rqueueQDetailService,
      RqueueUtilityService rqueueUtilityService,
      RqueueSystemManagerService rqueueQManagerService,
      RqueueWebConfig rqueueWebConfig,
      RqueueJobService rqueueJobService) {
    super(rqueueWebConfig);
    this.rqueueDashboardChartService = rqueueDashboardChartService;
    this.rqueueQDetailService = rqueueQDetailService;
    this.rqueueUtilityService = rqueueUtilityService;
    this.rqueueQManagerService = rqueueQManagerService;
    this.rqueueJobService = rqueueJobService;
  }

  @PostMapping("chart")
  @ResponseBody
  public ChartDataResponse getDashboardData(
      @RequestBody @Valid ChartDataRequest chartDataRequest, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
    }
    return null;
  }

  @GetMapping("jobs")
  @ResponseBody
  public DataViewResponse getJobs(
      @RequestParam(name = "message-id") @NotEmpty String messageId, HttpServletResponse response)
      throws ProcessingException {
    if (isEnable(response)) {
      return rqueueJobService.getJobs(messageId);
    }
    return null;
  }

  @PostMapping("queue-data")
  @ResponseBody
  public DataViewResponse exploreQueue(
      @Valid @RequestBody QueueExploreRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueQDetailService.getExplorePageData(
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
  public DataViewResponse viewData(
      @RequestBody @Valid DateViewRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueQDetailService.viewData(
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
  public BooleanResponse deleteMessage(
      @Valid @RequestBody MessageDeleteRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.deleteMessage(request.getQueueName(), request.getMessageId());
    }
    return null;
  }

  @PostMapping("delete-queue")
  @ResponseBody
  public BaseResponse deleteQueue(
      @Valid @RequestBody DataTypeRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueQManagerService.deleteQueue(request.getName());
    }
    return null;
  }

  @PostMapping("delete-queue-part")
  @ResponseBody
  public BooleanResponse deleteAll(
      @RequestBody @Valid DataDeleteRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.makeEmpty(request.getQueueName(), request.getDatasetName());
    }
    return null;
  }

  @PostMapping("data-type")
  @ResponseBody
  public StringResponse dataType(
      @RequestBody @Valid DataTypeRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.getDataType(request.getName());
    }
    return null;
  }

  @PostMapping("move-data")
  @ResponseBody
  public MessageMoveResponse dataType(
      @RequestBody @Valid MessageMoveRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.moveMessage(request);
    }
    return null;
  }

  @PostMapping("pause-unpause-queue")
  @ResponseBody
  public BaseResponse pauseUnpauseQueue(
      @RequestBody @Valid PauseUnpauseQueueRequest request, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.pauseUnpauseQueue(request);
    }
    return null;
  }

  @GetMapping("aggregate-data-selector")
  @ResponseBody
  public DataSelectorResponse aggregateDataCounter(
      @RequestParam AggregationType type, HttpServletResponse response) {
    if (isEnable(response)) {
      return rqueueUtilityService.aggregateDataCounter(type);
    }
    return null;
  }
}
