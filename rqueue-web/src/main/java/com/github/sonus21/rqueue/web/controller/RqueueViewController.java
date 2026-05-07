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
import com.github.sonus21.rqueue.utils.condition.ReactiveDisabled;
import com.github.sonus21.rqueue.web.RqueueViewControllerService;
import com.github.sonus21.rqueue.web.view.RqueueHtmlRenderer;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Conditional(ReactiveDisabled.class)
@Controller
@RequestMapping(path = "${rqueue.web.url.prefix:}rqueue")
public class RqueueViewController extends BaseController {

  private final RqueueHtmlRenderer renderer;
  private final RqueueViewControllerService rqueueViewControllerService;

  @Autowired
  public RqueueViewController(
      RqueueHtmlRenderer renderer,
      RqueueWebConfig rqueueWebConfig,
      RqueueViewControllerService rqueueViewControllerService) {
    super(rqueueWebConfig);
    this.renderer = renderer;
    this.rqueueViewControllerService = rqueueViewControllerService;
  }

  private String xForwardedPrefix(HttpServletRequest request) {
    return request.getHeader("x-forwarded-prefix");
  }

  private void writeHtml(HttpServletResponse response, String html) throws IOException {
    response.setContentType("text/html;charset=UTF-8");
    response.getWriter().write(html);
  }

  @GetMapping
  public void index(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.index(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderIndex(model.asMap()));
  }

  @GetMapping("queues")
  public void queues(
      Model model,
      @RequestParam(name = "page", defaultValue = "1") int pageNumber,
      HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.queues(model, xForwardedPrefix(request), pageNumber);
    writeHtml(response, renderer.renderQueues(model.asMap()));
  }

  @GetMapping("workers")
  public void workers(
      Model model,
      @RequestParam(name = "page", defaultValue = "1") int pageNumber,
      HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.workers(model, xForwardedPrefix(request), pageNumber);
    writeHtml(response, renderer.renderWorkers(model.asMap()));
  }

  @GetMapping("queues/{queueName}")
  public void queueDetail(
      @PathVariable("queueName") String queueName,
      Model model,
      HttpServletRequest request,
      HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.queueDetail(model, xForwardedPrefix(request), queueName);
    writeHtml(response, renderer.renderQueueDetail(model.asMap()));
  }

  @GetMapping("running")
  public void running(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.running(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("scheduled")
  public void scheduled(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.scheduled(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("dead")
  public void dead(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.dead(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("pending")
  public void pending(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.pending(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderRunning(model.asMap()));
  }

  @GetMapping("utility")
  public void utility(Model model, HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    if (!isEnable(response)) return;
    rqueueViewControllerService.utility(model, xForwardedPrefix(request));
    writeHtml(response, renderer.renderUtility(model.asMap()));
  }
}
