/*
 * Copyright (c) 2021-2023 Sonu Kumar
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
import com.github.sonus21.rqueue.utils.condition.ReactiveEnabled;
import com.github.sonus21.rqueue.web.service.RqueueViewControllerService;
import java.util.Locale;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Conditional;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.reactive.result.view.View;
import org.springframework.web.reactive.result.view.ViewResolver;
import reactor.core.publisher.Mono;

@Controller
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue")
@Conditional(ReactiveEnabled.class)
public class ReactiveRqueueViewController extends BaseReactiveController {

  private final ViewResolver rqueueViewResolver;
  private final RqueueViewControllerService rqueueViewControllerService;

  @Autowired
  public ReactiveRqueueViewController(
      RqueueWebConfig rqueueWebConfig,
      RqueueViewControllerService rqueueViewControllerService,
      @Qualifier("reactiveRqueueViewResolver") ViewResolver rqueueViewResolver) {
    super(rqueueWebConfig);
    this.rqueueViewResolver = rqueueViewResolver;
    this.rqueueViewControllerService = rqueueViewControllerService;
  }

  private String xForwardedPrefix(ServerHttpRequest request) {
    return request.getHeaders().getFirst("x-forwarded-prefix");
  }

  @GetMapping
  public Mono<View> index(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.index(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("index", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("queues")
  public Mono<View> queues(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.queues(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("queues", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("queues/{queueName}")
  public Mono<View> queueDetail(
      @PathVariable String queueName,
      Model model,
      ServerHttpRequest request,
      ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.queueDetail(model, xForwardedPrefix(request), queueName);
      return rqueueViewResolver.resolveViewName("queue_detail", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("running")
  public Mono<View> running(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.running(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("scheduled")
  public Mono<View> scheduled(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.scheduled(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("dead")
  public Mono<View> dead(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.dead(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("pending")
  public Mono<View> pending(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.pending(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("running", Locale.ENGLISH);
    }
    return null;
  }

  @GetMapping("utility")
  public Mono<View> utility(Model model, ServerHttpRequest request, ServerHttpResponse response)
      throws Exception {
    if (isEnabled(response)) {
      rqueueViewControllerService.utility(model, xForwardedPrefix(request));
      return rqueueViewResolver.resolveViewName("utility", Locale.ENGLISH);
    }
    return null;
  }
}
