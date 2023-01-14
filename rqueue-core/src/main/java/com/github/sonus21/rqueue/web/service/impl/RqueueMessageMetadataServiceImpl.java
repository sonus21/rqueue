/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.web.service.impl;

import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueMessageMetadataDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class RqueueMessageMetadataServiceImpl implements RqueueMessageMetadataService {

  private final RqueueMessageMetadataDao rqueueMessageMetadataDao;
  private final RqueueStringDao rqueueStringDao;
  private final RqueueLockManager lockManager;

  @Autowired
  public RqueueMessageMetadataServiceImpl(
      RqueueMessageMetadataDao rqueueMessageMetadataDao,
      RqueueStringDao rqueueStringDao,
      RqueueLockManager rqueueLockManager) {
    this.rqueueMessageMetadataDao = rqueueMessageMetadataDao;
    this.rqueueStringDao = rqueueStringDao;
    this.lockManager = rqueueLockManager;
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
    rqueueMessageMetadataDao.save(messageMetadata, duration);
  }

  @Override
  public MessageMetadata getByMessageId(String queueName, String messageId) {
    String id = RqueueMessageUtils.getMessageMetaId(queueName, messageId);
    return rqueueMessageMetadataDao.get(id);
  }

  @Override
  public boolean deleteMessage(String queueName, String messageId, Duration duration) {
    String lockValue = UUID.randomUUID().toString();
    try {
      if (lockManager.acquireLock(messageId, lockValue, Duration.ofSeconds(1))) {
        String id = RqueueMessageUtils.getMessageMetaId(queueName, messageId);
        MessageMetadata messageMetadata = rqueueMessageMetadataDao.get(id);
        if (messageMetadata == null) {
          messageMetadata = new MessageMetadata(id, MessageStatus.DELETED);
        }
        messageMetadata.setDeleted(true);
        messageMetadata.setDeletedOn(System.currentTimeMillis());
        save(messageMetadata, duration);
        return true;
      }
    } finally {
      lockManager.releaseLock(messageId, lockValue);
    }
    return false;
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
  public Mono<Boolean> saveReactive(MessageMetadata messageMetadata, Duration duration) {
    return rqueueMessageMetadataDao.saveReactive(messageMetadata, duration);
  }

  @Override
  public List<TypedTuple<MessageMetadata>> readMessageMetadataForQueue(
      String queueName, long start, long end) {
    List<TypedTuple<String>> metaIds =
        rqueueStringDao.readFromOrderedSetWithScoreBetween(queueName, start, end);
    Map<String, Double> metaIdToScoreMap =
        metaIds.stream().collect(Collectors.toMap(TypedTuple::getValue, TypedTuple::getScore));
    List<MessageMetadata> messageMetadata = findAll(metaIdToScoreMap.keySet());
    return messageMetadata.stream()
        .map(
            metadata -> {
              Double score = metaIdToScoreMap.get(metadata.getId());
              if (score == null) {
                return null;
              } else {
                return new DefaultTypedTuple<>(metadata, score);
              }
            })
        .filter(Objects::nonNull)
        .sorted(
            Comparator.comparingLong(
                (DefaultTypedTuple<MessageMetadata> e1) ->
                    -(Objects.requireNonNull(e1.getValue()).getUpdatedOn())))
        .collect(Collectors.toList());
  }

  @Override
  public void saveMessageMetadataForQueue(
      String queueName, MessageMetadata messageMetadata, Long ttlInMillisecond) {
    messageMetadata.setUpdatedOn(System.currentTimeMillis());
    save(messageMetadata, Duration.ofMillis(ttlInMillisecond));
    rqueueStringDao.addToOrderedSetWithScore(
        queueName, messageMetadata.getId(), -(System.currentTimeMillis() + ttlInMillisecond));
  }

  @Override
  public void deleteQueueMessages(String queueName, long before) {
    rqueueStringDao.deleteAll(queueName, -before, 0);
  }
}
