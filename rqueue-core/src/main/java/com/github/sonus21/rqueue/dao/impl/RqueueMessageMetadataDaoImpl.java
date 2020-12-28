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

package com.github.sonus21.rqueue.dao.impl;

import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.dao.RqueueMessageMetadataDao;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.utils.RedisUtils;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

@Repository
public class RqueueMessageMetadataDaoImpl implements RqueueMessageMetadataDao {
  private final RqueueRedisTemplate<MessageMetadata> template;

  public RqueueMessageMetadataDaoImpl(RqueueConfig rqueueConfig) {
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
  public void saveAll(Collection<MessageMetadata> messageMetadata, Duration duration) {
    for (MessageMetadata metadata : messageMetadata) {
      Assert.notNull(metadata.getId(), "messageMetadata id cannot be null");
    }
    RedisUtils.executePipeLine(
        template.getRedisTemplate(),
        (connection, keySerializer, valueSerializer) -> {
          for (MessageMetadata metadata : messageMetadata) {
            byte[] key = keySerializer.serialize(metadata.getId());
            byte[] value = valueSerializer.serialize(metadata);
            connection.set(key, value, Expiration.from(duration), SetOption.UPSERT);
          }
        });
  }

  @Override
  public void delete(String id) {
    template.delete(id);
  }

  @Override
  public void deleteAll(Collection<String> ids) {
    template.delete(ids);
  }
}
