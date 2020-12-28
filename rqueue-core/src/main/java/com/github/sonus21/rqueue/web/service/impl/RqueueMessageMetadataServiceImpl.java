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

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueMessageMetadataDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class RqueueMessageMetadataServiceImpl implements RqueueMessageMetadataService {
  private final RqueueMessageMetadataDao rqueueMessageMetadataDao;

  @Autowired
  public RqueueMessageMetadataServiceImpl(RqueueMessageMetadataDao rqueueMessageMetadataDao) {
    this.rqueueMessageMetadataDao = rqueueMessageMetadataDao;
  }

  @Override
  public MessageMetadata get(String id) {
    return rqueueMessageMetadataDao.get(id);
  }

  @Override
  public void delete(String id) {
    rqueueMessageMetadataDao.delete(id);
  }

  @Override
  public void deleteAll(Collection<String> ids) {
    rqueueMessageMetadataDao.deleteAll(ids);
  }

  @Override
  public List<MessageMetadata> findAll(Collection<String> ids) {
    return rqueueMessageMetadataDao.findAll(ids).stream()
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public void save(MessageMetadata messageMetadata, Duration duration) {
    saveAll(Collections.singletonList(messageMetadata), duration);
  }

  @Override
  public void saveAll(List<MessageMetadata> messageMetadata, Duration duration) {
    rqueueMessageMetadataDao.saveAll(messageMetadata, duration);
  }

  @Override
  public MessageMetadata getByMessageId(String queueName, String messageId) {
    String id = RqueueMessageUtils.getMessageMetaId(queueName, messageId);
    return rqueueMessageMetadataDao.get(id);
  }

  @Override
  public void deleteMessage(String queueName, String messageId, Duration duration) {
    String id = RqueueMessageUtils.getMessageMetaId(queueName, messageId);
    MessageMetadata messageMetadata = rqueueMessageMetadataDao.get(id);
    if (messageMetadata == null) {
      messageMetadata = new MessageMetadata(id, MessageStatus.DELETED);
    }
    messageMetadata.setDeleted(true);
    messageMetadata.setDeletedOn(System.currentTimeMillis());
    rqueueMessageMetadataDao.save(messageMetadata, duration);
  }

  @Override
  public MessageMetadata getOrCreateMessageMetadata(RqueueMessage rqueueMessage) {
    MessageMetadata messageMetadata =
        getByMessageId(rqueueMessage.getQueueName(), rqueueMessage.getId());
    if (messageMetadata == null) {
      messageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    }
    return messageMetadata;
  }

  @Override
  public Map<String, MessageMetadata> getMessageMetaMap(Collection<String> ids) {
    return findAll(ids).stream().collect(Collectors.toMap(MessageMetadata::getId, e -> e));
  }
}
