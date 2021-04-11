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

package com.github.sonus21.rqueue.web.service;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import reactor.core.publisher.Mono;

public interface RqueueMessageMetadataService {
  MessageMetadata get(String id);

  void delete(String id);

  void deleteAll(Collection<String> ids);

  List<MessageMetadata> findAll(Collection<String> ids);

  void save(MessageMetadata messageMetadata, Duration duration);

  MessageMetadata getByMessageId(String queueName, String messageId);

  void deleteMessage(String queueName, String messageId, Duration duration);

  MessageMetadata getOrCreateMessageMetadata(RqueueMessage rqueueMessage);

  Mono<Boolean> saveReactive(MessageMetadata messageMetadata, Duration duration);
}
