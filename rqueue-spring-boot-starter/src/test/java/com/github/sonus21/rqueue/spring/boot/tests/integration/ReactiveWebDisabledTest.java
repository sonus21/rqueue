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

import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.request.DataDeleteRequest;
import com.github.sonus21.rqueue.models.request.DataTypeRequest;
import com.github.sonus21.rqueue.models.request.DateViewRequest;
import com.github.sonus21.rqueue.models.request.MessageDeleteRequest;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.request.QueueExploreRequest;
import com.github.sonus21.rqueue.spring.boot.reactive.ReactiveWebApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

@ContextConfiguration(classes = ReactiveWebApplication.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Slf4j
@TestPropertySource(
    properties = {
        "rqueue.retry.per.poll=1000",
        "spring.data.redis.port=8018",
        "reservation.request.dead.letter.consumer.enabled=true",
        "reservation.request.active=true",
        "list.email.queue.enabled=true",
        "mysql.db.name=ReactiveWebDisabledTest",
        "use.system.redis=false",
        "user.banned.queue.active=true",
        "spring.main.web-application-type=reactive",
        "rqueue.reactive.enabled=true",
        "rqueue.web.enable=false",
    })
@SpringBootIntegrationTest
@EnabledIfEnvironmentVariable(named = "RQUEUE_REACTIVE_ENABLED", matches = "true")
class ReactiveWebDisabledTest extends BasicListenerTest {

  @Autowired
  private WebTestClient webTestClient;

  @ParameterizedTest
  @ValueSource(
      strings = {
          "",
          "/queues",
          "/running",
          "/scheduled",
          "/dead",
          "/pending",
          "/utility",
          "/queues/test-queue",
          "/api/v1/aggregate-data-selector?type=WEEKLY",
          "/api/v1/jobs?message-id=1234567890"
      })
  void testPath(String path) throws Exception {
    this.webTestClient
        .get()
        .uri("/rqueue" + path)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
    ;
  }

  @Test
  void getChartLatency() throws Exception {
    ChartDataRequest chartDataRequest =
        new ChartDataRequest(ChartType.LATENCY, AggregationType.DAILY);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/chart")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Mono.just(chartDataRequest), ChartDataRequest.class)
        .accept(MediaType.APPLICATION_JSON)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void exploreData() throws Exception {
    QueueExploreRequest request = new QueueExploreRequest();
    request.setType(DataType.LIST);
    request.setSrc(emailQueue);
    request.setName(emailDeadLetterQueue);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/queue-data")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), QueueExploreRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void deleteDataSet() throws Exception {
    DataDeleteRequest request = new DataDeleteRequest();
    request.setQueueName(emailQueue);
    request.setDatasetName(emailDeadLetterQueue);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/delete-queue-part")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), DataDeleteRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void dataType() throws Exception {
    DataTypeRequest request = new DataTypeRequest();
    request.setName(emailDeadLetterQueue);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/data-type")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), DataTypeRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void moveMessage() throws Exception {
    MessageMoveRequest request =
        new MessageMoveRequest(emailDeadLetterQueue, DataType.LIST, emailQueue, DataType.LIST);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/move-data")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), MessageMoveRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void viewData() throws Exception {
    DateViewRequest dateViewRequest = new DateViewRequest();
    dateViewRequest.setName(emailDeadLetterQueue);
    dateViewRequest.setType(DataType.LIST);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/view-data")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(dateViewRequest), DateViewRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void deleteQueue() throws Exception {
    DataTypeRequest request = new DataTypeRequest();
    request.setName(jobQueue);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/delete-queue")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), DataTypeRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }

  @Test
  void deleteMessage() throws Exception {
    MessageDeleteRequest request = new MessageDeleteRequest();
    request.setMessageId(UUID.randomUUID().toString());
    request.setQueueName(emailQueue);
    this.webTestClient
        .post()
        .uri("/rqueue/api/v1/delete-message")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .body(Mono.just(request), MessageDeleteRequest.class)
        .exchange()
        .expectStatus()
        .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE)
        .expectBody()
        .isEmpty();
  }
}
