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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.spring.boot.reactive.ReactiveWebApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;

@ContextConfiguration(classes = ReactiveWebApplication.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1000",
        "spring.data.redis.port=8020",
        "list.email.queue.enabled=true",
        "mysql.db.name=ReactiveWebTest",
        "use.system.redis=false",
        "spring.main.web-application-type=reactive"
    })
@SpringBootIntegrationTest
@EnabledIfEnvironmentVariable(named = "RQUEUE_REACTIVE_ENABLED", matches = "true")
@AutoConfigureWebTestClient(timeout = "10000")
@TestInstance(Lifecycle.PER_CLASS)
class ReactiveWebViewTest extends BasicListenerTest {

  @Autowired
  private WebTestClient webTestClient;
  @Autowired
  private RqueueConfig rqueueConfig;
  private boolean initialized = false;

  private void initialize() throws TimedOutException {
    verifyListMessageListener(); // list email queue
    verifySimpleTaskExecution(); // notification queue
    verifyScheduledTaskExecution(); // job queue
    enqueueIn(
        rqueueConfig.getScheduledQueueName(emailQueue),
        (i) -> Email.newInstance(),
        (i) -> 30_000L,
        10,
        true);
  }

  @BeforeEach
  public void init() throws TimedOutException {
    if (!initialized) {
      initialize();
      initialized = true;
    }
  }

  @Test
  void home() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void queues() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/queues")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void queueDetail() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/queues/" + emailQueue)
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void running() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/running")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void scheduled() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/scheduled")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void dead() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/dead")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void pending() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/pending")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }

  @Test
  void utility() throws Exception {
    FluxExchangeResult<String> result =
        this.webTestClient
            .get()
            .uri("/rqueue/utility")
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(String.class);
    List<String> body = result.getResponseBody().collectList().block();
  }
}
