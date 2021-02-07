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

package com.github.sonus21.rqueue.core.impl;

import com.github.sonus21.rqueue.core.EndpointRegistry;
import com.github.sonus21.rqueue.core.RqueueEndpointManager;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.exception.QueueDoesNotExist;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.util.CollectionUtils;

public class RqueueEndpointManagerImpl extends BaseMessageSender implements RqueueEndpointManager {

  public RqueueEndpointManagerImpl(
      RqueueMessageTemplate messageTemplate,
      MessageConverter messageConverter,
      MessageHeaders messageHeaders) {
    super(messageTemplate, messageConverter, messageHeaders);
  }

  @Override
  public void registerQueue(String name, String... priorities) {
    registerQueueInternal(name, priorities);
  }

  @Override
  public boolean isQueueRegistered(String queueName) {
    try {
      EndpointRegistry.get(queueName);
      return true;
    } catch (QueueDoesNotExist e) {
      return false;
    }
  }

  @Override
  public List<QueueDetail> getQueueConfig(String queueName) {
    QueueDetail queueDetail = EndpointRegistry.get(queueName);
    Map<String, Integer> priorityMap = queueDetail.getPriority();
    if (CollectionUtils.isEmpty(priorityMap)) {
      return Collections.singletonList(queueDetail);
    }
    Map<String, Integer> localPriorityMap = new HashMap<>(priorityMap);
    localPriorityMap.remove(Constants.DEFAULT_PRIORITY_KEY);
    List<QueueDetail> queueDetails = new ArrayList<>();
    queueDetails.add(queueDetail);
    for (String priority : localPriorityMap.keySet()) {
      queueDetails.add(
          EndpointRegistry.get(PriorityUtils.getQueueNameForPriority(queueName, priority)));
    }
    return queueDetails;
  }
}
