/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import java.util.Collection;
import java.util.List;
import org.springframework.context.ApplicationListener;
import reactor.core.publisher.Mono;

public interface RqueueSystemManagerService extends ApplicationListener<RqueueBootstrapEvent> {

  BaseResponse deleteQueue(String queueName);

  List<String> getQueues();

  List<QueueConfig> getQueueConfigs(Collection<String> queues);

  List<QueueConfig> getQueueConfigs();

  List<QueueConfig> getSortedQueueConfigs();

  QueueConfig getQueueConfig(String queueName);

  Mono<BaseResponse> deleteReactiveQueue(String queueName);
}
