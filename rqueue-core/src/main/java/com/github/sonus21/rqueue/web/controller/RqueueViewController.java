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
import com.github.sonus21.rqueue.utils.ReactiveDisabled;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueViewControllerService;
import com.mitchellbosecke.pebble.spring.servlet.PebbleViewResolver;
import java.util.Locale;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.View;

@Controller
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue")
@Conditional(ReactiveDisabled.class)
public class RqueueViewController {

  private final RqueueWebConfig rqueueWebConfig;
  private final PebbleViewResolver rqueueViewResolver;
  private final RqueueViewControllerService rqueueViewControllerService;

  @Autowired
  public RqueueViewController(
      @Qualifier("rqueueViewResolver") PebbleViewResolver rqueueViewResolver,
      RqueueWebConfig rqueueWebConfig,
      RqueueViewControllerService rqueueViewControllerService) {
    this.rqueueViewControllerService = rqueueViewControllerService;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueViewResolver = rqueueViewResolver;
  }

  private String xForwardedPrefix(HttpServletRequest request) {
    String prefix = rqueueWebConfig.getUrlPrefix();
    if (StringUtils.isEmpty(prefix)) {
      return request.getHeader("x-forwarded-prefix");
    }
    return prefix;
  }

  @GetMapping
  public View index(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.index(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("index", Locale.ENGLISH);
  }

  @GetMapping("queues")
  public View queues(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.queues(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("queues", Locale.ENGLISH);
  }

  @GetMapping("queues/{queueName}")
  public View queueDetail(
      @PathVariable String queueName,
      Model model,
      HttpServletRequest request,
      HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.queueDetail(model, xForwardedPrefix(request), queueName);
    return rqueueViewResolver.resolveViewName("queue_detail", Locale.ENGLISH);
  }

  @GetMapping("running")
  public View running(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.running(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("scheduled")
  public View scheduled(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.scheduled(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("dead")
  public View dead(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.dead(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("pending")
  public View pending(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.pending(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
  }

  @GetMapping("utility")
  public View utility(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
      return null;
    }
    rqueueViewControllerService.utility(model, xForwardedPrefix(request));
    return rqueueViewResolver.resolveViewName("utility", Locale.ENGLISH);
  }
}
