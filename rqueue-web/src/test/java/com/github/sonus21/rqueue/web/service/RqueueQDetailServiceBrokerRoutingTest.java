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

package com.github.sonus21.rqueue.web.service;

import static com.github.sonus21.rqueue.utils.TestUtils.createQueueConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.spi.Capabilities;
import com.github.sonus21.rqueue.core.spi.MessageBroker;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.enums.NavTab;
import com.github.sonus21.rqueue.models.response.DataViewResponse;
import com.github.sonus21.rqueue.models.response.RedisDataDetail;
import com.github.sonus21.rqueue.repository.MessageBrowsingRepository;
import com.github.sonus21.rqueue.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.web.service.impl.RqueueQDetailServiceImpl;
import com.github.sonus21.rqueue.worker.RqueueWorkerRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@CoreUnitTest
class RqueueQDetailServiceBrokerRoutingTest extends TestBase {

  @Mock
  private MessageBrowsingRepository messageBrowsingRepository;

  @Mock
  private RqueueMessageTemplate rqueueMessageTemplate;

  @Mock
  private RqueueSystemManagerService rqueueSystemManagerService;

  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;

  @Mock
  private RqueueWorkerRegistry rqueueWorkerRegistry;

  @Mock
  private MessageBroker messageBroker;

  private final RqueueConfig rqueueConfig = new RqueueConfig(null, null, false, 2);
  private RqueueQDetailServiceImpl service;
  private QueueConfig queueConfig;
  private QueueDetail queueDetail;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    service = new RqueueQDetailServiceImpl(
        messageBrowsingRepository,
        rqueueMessageTemplate,
        rqueueSystemManagerService,
        rqueueMessageMetadataService,
        rqueueConfig,
        rqueueWorkerRegistry);
    queueConfig = createQueueConfig("brokerRouted", 3, 900_000L, null);
    queueDetail = TestUtils.createQueueDetail("brokerRouted");
    EndpointRegistry.delete();
    EndpointRegistry.register(queueDetail);
  }

  @AfterEach
  void tearDown() {
    EndpointRegistry.delete();
  }

  @Test
  void sizeUsesBrokerWhenSet() {
    service.setMessageBroker(messageBroker);
    when(messageBroker.capabilities()).thenReturn(Capabilities.REDIS_DEFAULTS);
    when(messageBroker.size(any(QueueDetail.class))).thenReturn(42L);
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getProcessingQueueName(),
            com.github.sonus21.rqueue.models.enums.DataType.ZSET))
        .thenReturn(0L);
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getScheduledQueueName(),
            com.github.sonus21.rqueue.models.enums.DataType.ZSET))
        .thenReturn(0L);

    List<Entry<NavTab, RedisDataDetail>> details = service.getQueueDataStructureDetail(queueConfig);

    RedisDataDetail pending = details.stream()
        .filter(e -> e.getKey() == NavTab.PENDING)
        .findFirst()
        .orElseThrow()
        .getValue();
    assertEquals(42L, pending.getSize());
    verify(messageBroker, atLeastOnce()).size(any(QueueDetail.class));
    verify(messageBrowsingRepository, never())
        .getDataSize(
            queueConfig.getQueueName(), com.github.sonus21.rqueue.models.enums.DataType.LIST);
  }

  @Test
  void sizeFallsBackToRedisWhenNoBroker() {
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getQueueName(), com.github.sonus21.rqueue.models.enums.DataType.LIST))
        .thenReturn(7L);
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getProcessingQueueName(),
            com.github.sonus21.rqueue.models.enums.DataType.ZSET))
        .thenReturn(0L);
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getScheduledQueueName(),
            com.github.sonus21.rqueue.models.enums.DataType.ZSET))
        .thenReturn(0L);

    List<Entry<NavTab, RedisDataDetail>> details = service.getQueueDataStructureDetail(queueConfig);

    RedisDataDetail pending = details.stream()
        .filter(e -> e.getKey() == NavTab.PENDING)
        .findFirst()
        .orElseThrow()
        .getValue();
    assertEquals(7L, pending.getSize());
  }

  @Test
  void scheduledTabHiddenAndEmptyWhenIntrospectionUnsupported() {
    Capabilities natsCaps = new Capabilities(true, false, false, false);
    service.setMessageBroker(messageBroker);
    when(messageBroker.capabilities()).thenReturn(natsCaps);
    when(messageBroker.size(any(QueueDetail.class))).thenReturn(0L);
    when(messageBrowsingRepository.getDataSize(
            queueConfig.getProcessingQueueName(),
            com.github.sonus21.rqueue.models.enums.DataType.ZSET))
        .thenReturn(0L);

    List<Entry<NavTab, RedisDataDetail>> details = service.getQueueDataStructureDetail(queueConfig);
    boolean scheduledPresent = details.stream().anyMatch(e -> e.getKey() == NavTab.SCHEDULED);
    assertFalse(scheduledPresent, "scheduled nav tab should be hidden");

    List<NavTab> tabs = service.getNavTabs(queueConfig);
    assertFalse(tabs.contains(NavTab.SCHEDULED));

    when(rqueueSystemManagerService.getQueueConfig(queueConfig.getName())).thenReturn(queueConfig);
    DataViewResponse explore = service.getExplorePageData(
        queueConfig.getName(), queueConfig.getScheduledQueueName(), DataType.ZSET, 0, 10);
    assertTrue(explore.isHideScheduledPanel());
    assertTrue(explore.isHideCronJobs());
    assertTrue(explore.getRows() == null || explore.getRows().isEmpty());
  }

  @Test
  void peekRoutesThroughBrokerForReadyList() {
    service.setMessageBroker(messageBroker);
    when(messageBroker.capabilities()).thenReturn(Capabilities.REDIS_DEFAULTS);
    when(messageBroker.peek(any(QueueDetail.class), anyLong(), anyLong()))
        .thenReturn(Collections.emptyList());
    when(rqueueSystemManagerService.getQueueConfig(queueConfig.getName())).thenReturn(queueConfig);

    DataViewResponse response = service.getExplorePageData(
        queueConfig.getName(), queueConfig.getQueueName(), DataType.LIST, 0, 10);

    verify(messageBroker, atLeastOnce()).peek(any(QueueDetail.class), anyLong(), anyLong());
    assertTrue(response.getRows() == null || response.getRows().isEmpty());
  }

  @Test
  void hideFlagsDefaultFalseWithRedisBroker() {
    service.setMessageBroker(messageBroker);
    when(messageBroker.capabilities()).thenReturn(Capabilities.REDIS_DEFAULTS);
    when(messageBroker.peek(any(QueueDetail.class), anyLong(), anyLong()))
        .thenReturn(Collections.emptyList());
    when(rqueueSystemManagerService.getQueueConfig(queueConfig.getName())).thenReturn(queueConfig);

    DataViewResponse response = service.getExplorePageData(
        queueConfig.getName(), queueConfig.getQueueName(), DataType.LIST, 0, 10);

    assertFalse(response.isHideScheduledPanel());
    assertFalse(response.isHideCronJobs());
  }
}
