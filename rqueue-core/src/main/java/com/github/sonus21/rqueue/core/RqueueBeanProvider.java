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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.listener.RqueueMessageHandler;
import com.github.sonus21.rqueue.metrics.RqueueMetricsCounter;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEventPublisher;

@Getter
@Setter
public class RqueueBeanProvider {

  @Autowired
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Autowired
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Autowired
  private RqueueJobDao rqueueJobDao;
  @Autowired
  private RqueueWebConfig rqueueWebConfig;
  @Autowired
  private ApplicationEventPublisher applicationEventPublisher;
  @Autowired
  private RqueueLockManager rqueueLockManager;

  @Autowired(required = false)
  private RqueueMetricsCounter rqueueMetricsCounter;

  @Autowired
  private RqueueMessageHandler rqueueMessageHandler;

  private MessageProcessor preExecutionMessageProcessor;
  @Autowired
  private RqueueMessageTemplate rqueueMessageTemplate;
  @Autowired
  private RqueueConfig rqueueConfig;
}
