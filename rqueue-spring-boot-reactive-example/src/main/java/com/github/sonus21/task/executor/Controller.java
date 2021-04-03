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

package com.github.sonus21.task.executor;

import com.github.sonus21.rqueue.core.ReactiveRqueueMessageEnqueuer;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
@Slf4j
public class Controller {

  private @NonNull
  final ReactiveRqueueMessageEnqueuer reactiveRqueueMessageEnqueuer;

  @Value("${email.queue.name}")
  private String emailQueueName;

  @Value("${invoice.queue.name}")
  private String invoiceQueueName;

  @Value("${invoice.queue.delay}")
  private Long invoiceDelay;

  @GetMapping("email")
  public Mono<String> sendEmail(
      @RequestParam String email, @RequestParam String subject, @RequestParam String content) {
    log.info("Sending email");
    Mono<String> mono = reactiveRqueueMessageEnqueuer
        .enqueue(emailQueueName, new Email(email, subject, content));
    return mono.zipWith(Mono.just("Please check your inbox!"), (a, b) -> b);
  }

  @GetMapping("invoice")
  public Mono<String> generateInvoice(@RequestParam String id, @RequestParam String type) {
    log.info("Generate invoice");
    Mono<String> mono = reactiveRqueueMessageEnqueuer
        .enqueueIn(invoiceQueueName, new Invoice(id, type), invoiceDelay);
    return mono
        .zipWith(Mono.just("Invoice would be generated in " + invoiceDelay + " milliseconds"),
            (a, b) -> b);
  }
}
