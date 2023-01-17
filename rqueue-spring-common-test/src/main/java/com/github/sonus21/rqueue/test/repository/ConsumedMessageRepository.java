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

package com.github.sonus21.rqueue.test.repository;

import com.github.sonus21.rqueue.test.entity.ConsumedMessage;
import java.util.Collection;
import java.util.List;
import org.springframework.data.repository.CrudRepository;

public interface ConsumedMessageRepository extends CrudRepository<ConsumedMessage, String> {

  List<ConsumedMessage> findByQueueName(String queueName);

  List<ConsumedMessage> findByMessageId(String messageId);

  List<ConsumedMessage> findByMessageIdIn(Collection<String> messageIds);

  ConsumedMessage findByMessageIdAndTag(String messageId, String tag);

  void deleteAllByIdIn(Collection<String> ids);
}
