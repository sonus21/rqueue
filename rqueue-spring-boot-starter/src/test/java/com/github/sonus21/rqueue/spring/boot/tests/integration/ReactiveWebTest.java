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

package com.github.sonus21.rqueue.spring.boot.tests.integration;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.enums.AggregationType;
import com.github.sonus21.rqueue.models.enums.ChartDataType;
import com.github.sonus21.rqueue.models.enums.ChartType;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.ChartDataRequest;
import com.github.sonus21.rqueue.models.response.ChartDataResponse;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.spring.boot.reactive.ReactiveWebApplication;
import com.github.sonus21.rqueue.spring.boot.tests.SpringBootIntegrationTest;
import com.github.sonus21.rqueue.test.dto.Email;
import com.github.sonus21.rqueue.test.tests.BasicListenerTest;
import java.util.Collections;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.MediaType;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.FluxExchangeResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.util.DefaultUriBuilderFactory;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

@ContextConfiguration(classes = ReactiveWebApplication.class)
@SpringBootTest(webEnvironment = WebEnvironment.DEFINED_PORT)
@Slf4j
@TestPropertySource(
    properties = {
      "server.port=8000",
      "rqueue.retry.per.poll=1000",
      "spring.redis.port=8018",
      "reservation.request.dead.letter.consumer.enabled=true",
      "reservation.request.active=true",
      "list.email.queue.enabled=true",
      "mysql.db.name=ReactiveWebTest",
      "use.system.redis=false",
      "user.banned.queue.active=true",
      "spring.main.web-application-type=reactive",
      "rqueue.reactive.enabled=true"
    })
@SpringBootIntegrationTest
class ReactiveWebTest extends BasicListenerTest {
  @Autowired private WebTestClient webTestClient;
  @Autowired private RqueueConfig rqueueConfig;

  @PostConstruct
  public void init() throws TimedOutException {
    verifyListMessageListener(); // list email queue
    verifySimpleTaskExecution(); // notification queue
    verifyDelayedTaskExecution(); // job queue
    enqueueIn(
        rqueueConfig.getDelayedQueueName(emailQueue),
        (i) -> Email.newInstance(),
        (i) -> 30_000L,
        10);
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

  @Test
  void restGetDashboardData() {
    ChartDataRequest request = new ChartDataRequest();
    request.setQueue(jobQueue);
    request.setDateTypes(Collections.singletonList(ChartDataType.EXECUTION));
    request.setAggregationType(AggregationType.DAILY);
    request.setType(ChartType.STATS);
    FluxExchangeResult<ChartDataResponse> result =
        this.webTestClient
            .post()
            .uri("/rqueue/api/v1/chart")
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .body(Mono.just(request), ChartDataRequest.class)
            .exchange()
            .expectStatus()
            .is2xxSuccessful()
            .returnResult(ChartDataResponse.class);
    List<ChartDataResponse> body = result.getResponseBody().collectList().block();
    System.out.println(body.get(0));
  }

  @Test
  void restExplore() {
    DefaultUriBuilderFactory factory = new DefaultUriBuilderFactory();
    factory.setEncodingMode(DefaultUriBuilderFactory.EncodingMode.VALUES_ONLY);
    String path =
        UriComponentsBuilder.fromUriString("/rqueue/api/v1/explore")
            .queryParam("type", DataType.LIST)
            .queryParam("name", rqueueConfig.getDelayedQueueName(emailQueue))
            .queryParam("src", rqueueConfig.getQueueName(emailQueue))
            .build()
            .toUriString();
    FluxExchangeResult<DataViewResponse> result =
        this.webTestClient
            .get()
            .uri(path)
            .accept(MediaType.APPLICATION_JSON_UTF8)
            .exchange()
            //            .expectStatus()
            //            .is2xxSuccessful()
            .returnResult(DataViewResponse.class);
    List<DataViewResponse> body = result.getResponseBody().collectList().block();
    System.out.println(body.get(0));
  }
}
