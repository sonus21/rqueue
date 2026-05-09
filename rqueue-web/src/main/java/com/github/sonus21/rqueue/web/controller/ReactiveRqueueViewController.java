/*
 * Copyright (c) 2021-2026 Sonu Kumar
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
import com.github.sonus21.rqueue.web.RqueueViewControllerService;
import com.github.sonus21.rqueue.web.view.RqueueHtmlRenderer;
import java.nio.charset.StandardCharsets;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import reactor.core.publisher.Mono;

@Conditional(ReactiveEnabled.class)
@Controller
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue")
public class ReactiveRqueueViewController extends BaseReactiveController {

  private final RqueueHtmlRenderer renderer;
  private final RqueueViewControllerService rqueueViewControllerService;

  @Autowired
  public ReactiveRqueueViewController(
      RqueueWebConfig rqueueWebConfig,
      RqueueViewControllerService rqueueViewControllerService,
      RqueueHtmlRenderer renderer) {
    super(rqueueWebConfig);
    this.renderer = renderer;
    this.rqueueViewControllerService = rqueueViewControllerService;
  }

  private String xForwardedPrefix(ServerHttpRequest request) {
    return request.getHeaders().getFirst("x-forwarded-prefix");
  }

  private Mono<Void> writeHtml(ServerHttpResponse response, String html) {
    response.getHeaders().setContentType(MediaType.TEXT_HTML);
    byte[] bytes = html.getBytes(StandardCharsets.UTF_8);
    DataBuffer buffer = response.bufferFactory().wrap(bytes);
    return response.writeWith(Mono.just(buffer));
  }

  @GetMapping
  public Mono<Void> index(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.index(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderIndex(model.asMap()));
  }

  @GetMapping("queues")
  public Mono<Void> queues(
      Model model,
      @RequestParam(name = "page", defaultValue = "1") int pageNumber,
      ServerHttpRequest request,
      ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.queues(model, xForwardedPrefix(request), pageNumber);
    return writeHtml(response, renderer.renderQueues(model.asMap()));
  }

  @GetMapping("queues/{queueName}")
  public Mono<Void> queueDetail(
      @PathVariable("queueName") String queueName,
      Model model,
      ServerHttpRequest request,
      ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.queueDetail(model, xForwardedPrefix(request), queueName);
    return writeHtml(response, renderer.renderQueueDetail(model.asMap()));
  }

  @GetMapping("workers")
  public Mono<Void> workers(
      Model model,
      @RequestParam(name = "page", defaultValue = "1") int pageNumber,
      ServerHttpRequest request,
      ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.workers(model, xForwardedPrefix(request), pageNumber);
    return writeHtml(response, renderer.renderWorkers(model.asMap()));
  }

  @GetMapping("running")
  public Mono<Void> running(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.running(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("scheduled")
  public Mono<Void> scheduled(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.scheduled(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("dead")
  public Mono<Void> dead(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.dead(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("pending")
  public Mono<Void> pending(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.pending(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("utility")
  public Mono<Void> utility(Model model, ServerHttpRequest request, ServerHttpResponse response) {
    if (!isEnabled(response)) return Mono.empty();
    rqueueViewControllerService.utility(model, xForwardedPrefix(request));
    return writeHtml(response, renderer.renderUtility(model.asMap()));
  }
}
