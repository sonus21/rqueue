/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.MessageUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

@Service
public class RqueueMessageMetadataServiceImpl implements RqueueMessageMetadataService {
  private final RqueueRedisTemplate<MessageMetadata> template;

  public RqueueMessageMetadataServiceImpl(RqueueRedisTemplate<MessageMetadata> template) {
    this.template = template;
  }

  @Autowired
  public RqueueMessageMetadataServiceImpl(RqueueConfig rqueueConfig) {
    this.template = new RqueueRedisTemplate<>(rqueueConfig.getConnectionFactory());
  }

  @Override
  public MessageMetadata get(String id) {
    return template.get(id);
  }

  @Override
  public List<MessageMetadata> findAll(Collection<String> ids) {
    return template.mget(ids).stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public void save(MessageMetadata messageMetadata, Duration duration) {
    Assert.notNull(messageMetadata.getId(), "messageMetadata id cannot be null");
    template.set(messageMetadata.getId(), messageMetadata, duration);
  }

  @Override
  public void save(Collection<MessageMetadata> messageMetadata, Duration duration) {
    if (CollectionUtils.isEmpty(messageMetadata)) {
      return;
    }
    template.getRedisTemplate().multi();
    messageMetadata.forEach(e -> template.set(e.getId(), e, duration));
    template.getRedisTemplate().exec();
  }

  @Override
  public void deleteMessage(String messageId, Duration duration) {
    String id = MessageUtils.getMessageMetaId(messageId);
    MessageMetadata messageMetadata = get(id);
    if (messageMetadata == null) {
      messageMetadata = new MessageMetadata(id, messageId);
    }
    messageMetadata.setDeleted(true);
    messageMetadata.setDeletedOn(System.currentTimeMillis());
    template.set(messageMetadata.getId(), messageMetadata, duration);
  }

  @Override
  public void delete(String id) {
    template.delete(id);
  }

  @Override
  public Map<String, MessageMetadata> getMessageMetaMap(Collection<String> ids) {
    return null;
  }
}
