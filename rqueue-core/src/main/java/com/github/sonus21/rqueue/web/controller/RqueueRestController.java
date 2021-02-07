/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.web.service.RqueueDashboardChartService;
import com.github.sonus21.rqueue.web.service.RqueueJobService;
import com.github.sonus21.rqueue.web.service.RqueueQDetailService;
import com.github.sonus21.rqueue.web.service.RqueueSystemManagerService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping("rqueue/api/v1")
public class RqueueRestController {
  private final RqueueDashboardChartService rqueueDashboardChartService;
  private final RqueueQDetailService rqueueQDetailService;
  private final RqueueUtilityService rqueueUtilityService;
  private final RqueueSystemManagerService rqueueQManagerService;
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueJobService rqueueJobService;

  @Autowired
  public RqueueRestController(
      RqueueDashboardChartService rqueueDashboardChartService,
      RqueueQDetailService rqueueQDetailService,
      RqueueUtilityService rqueueUtilityService,
      RqueueSystemManagerService rqueueQManagerService,
      RqueueWebConfig rqueueWebConfig,
      RqueueJobService rqueueJobService) {
    this.rqueueDashboardChartService = rqueueDashboardChartService;
    this.rqueueQDetailService = rqueueQDetailService;
    this.rqueueUtilityService = rqueueUtilityService;
    this.rqueueQManagerService = rqueueQManagerService;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueJobService = rqueueJobService;
  }

  @PostMapping("chart")
  @ResponseBody
  public ChartDataResponse getDashboardData(
      @RequestBody ChartDataRequest chartDataRequest, HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueDashboardChartService.getDashboardChartData(chartDataRequest);
  }

  @GetMapping("jobs")
  @ResponseBody
  public DataViewResponse getJobs(
      @RequestParam(name = "message-id") @NotEmpty String messageId, HttpServletResponse response)
      throws ProcessingException {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueJobService.getJobs(messageId);
  }

  @GetMapping("explore")
  @ResponseBody
  public DataViewResponse exploreQueue(
      @RequestParam @NotNull DataType type,
      @RequestParam @NotEmpty String name,
      @RequestParam @NotEmpty String src,
      @RequestParam(defaultValue = "0", name = "page") int pageNumber,
      @RequestParam(defaultValue = "20", name = "count") int itemPerPage,
      HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueQDetailService.getExplorePageData(src, name, type, pageNumber, itemPerPage);
  }

  @DeleteMapping("data-set/{queueName}/{datasetName}")
  @ResponseBody
  public BooleanResponse deleteAll(
      @PathVariable String queueName,
      @PathVariable String datasetName,
      HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueUtilityService.makeEmpty(queueName, datasetName);
  }

  @DeleteMapping("data-set/{queueName}/message/{messageId}")
  @ResponseBody
  public BooleanResponse deleteMessage(
      @PathVariable String queueName,
      @PathVariable String messageId,
      HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueUtilityService.deleteMessage(queueName, messageId);
  }

  @GetMapping("data-type")
  @ResponseBody
  public StringResponse dataType(
      @RequestParam @NotEmpty String name, HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueUtilityService.getDataType(name);
  }

  @PutMapping("move")
  @ResponseBody
  public MessageMoveResponse dataType(
      @RequestBody MessageMoveRequest request, HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueUtilityService.moveMessage(request);
  }

  @GetMapping("data")
  @ResponseBody
  public DataViewResponse viewData(
      @RequestParam @NotEmpty String name,
      @RequestParam @NotNull DataType type,
      @RequestParam(required = false) String key,
      @RequestParam(defaultValue = "0", name = "page") int pageNumber,
      @RequestParam(defaultValue = "20", name = "count") int itemPerPage,
      HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueQDetailService.viewData(name, type, key, pageNumber, itemPerPage);
  }

  @DeleteMapping("queues/{queueName}")
  @ResponseBody
  public BaseResponse deleteQueue(@PathVariable String queueName, HttpServletResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    return rqueueQManagerService.deleteQueue(queueName);
  }
}
