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
import com.github.sonus21.rqueue.utils.condition.ReactiveDisabled;
import com.github.sonus21.rqueue.web.service.RqueueViewControllerService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;

@Controller
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue")
@Conditional(ReactiveDisabled.class)
public class RqueueViewController extends BaseController {

  private final ViewResolver rqueueViewResolver;
  private final RqueueViewControllerService rqueueViewControllerService;

  @Autowired
  public RqueueViewController(
      @Qualifier("rqueueViewResolver") ViewResolver rqueueViewResolver,
      RqueueWebConfig rqueueWebConfig,
      RqueueViewControllerService rqueueViewControllerService) {
    super(rqueueWebConfig);
    this.rqueueViewControllerService = rqueueViewControllerService;
    this.rqueueViewResolver = rqueueViewResolver;
  }

  private String xForwardedPrefix(HttpServletRequest request) {
    return request.getHeader("x-forwarded-prefix");
  }

  @GetMapping
  public View index(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.index(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("index", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("queues")
  public View queues(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.queues(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("queues", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("queues/{queueName}")
  public View queueDetail(
      @PathVariable String queueName,
      Model model,
      HttpServletRequest request,
      HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.queueDetail(model, xForwardedPrefix(request), queueName);
      return rqueueViewResolver.resolveViewName("queue_detail", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("running")
  public View running(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.running(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("scheduled")
  public View scheduled(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.scheduled(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("dead")
  public View dead(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.dead(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("pending")
  public View pending(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.pending(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("utility")
  public View utility(Model model, HttpServletRequest request, HttpServletResponse response)
      throws Exception {
    if (isEnable(response)) {
      rqueueViewControllerService.utility(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("utility", Locale.ENGLISH);
    }
    return null;
  }
}
