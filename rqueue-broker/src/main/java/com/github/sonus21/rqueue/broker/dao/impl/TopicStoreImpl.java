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

package com.github.sonus21.rqueue.broker.dao.impl;

import com.github.sonus21.rqueue.broker.config.RqueueBrokerSystemConfig;
import com.github.sonus21.rqueue.broker.dao.TopicStore;
import com.github.sonus21.rqueue.broker.models.db.SubscriptionConfig;
import com.github.sonus21.rqueue.broker.models.db.TopicConfig;
import com.github.sonus21.rqueue.broker.models.request.Subscription;
import com.github.sonus21.rqueue.broker.models.request.Topic;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
public class TopicStoreImpl implements TopicStore {
  private final RqueueBrokerSystemConfig rqueueBrokerSystemConfig;
  private final RqueueRedisTemplate<String> redisTemplate;
  private final RqueueRedisTemplate<TopicConfig> topicConfigRqueueRedisTemplate;
  private final RqueueRedisTemplate<SubscriptionConfig> subscriptionConfigRqueueRedisTemplate;

  @Autowired
  public TopicStoreImpl(
      RqueueBrokerSystemConfig rqueueBrokerSystemConfig,
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> redisTemplate) {
    RedisConnectionFactory connectionFactory =
        redisTemplate.getRedisTemplate().getConnectionFactory();
    this.rqueueBrokerSystemConfig = rqueueBrokerSystemConfig;
    this.redisTemplate = redisTemplate;
    this.topicConfigRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.subscriptionConfigRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
  }

  @Override
  public Set<Topic> getTopics() {
    String key = rqueueBrokerSystemConfig.getTopicsKey();
    Set<String> topics = redisTemplate.getMembers(key);
    if (CollectionUtils.isEmpty(topics)) {
      return Collections.emptySet();
    }
    return topics.stream().map(Topic::new).collect(Collectors.toSet());
  }

  @Override
  public List<TopicConfig> getTopicConfigs() {
    String key = rqueueBrokerSystemConfig.getTopicsKey();
    Set<String> topics = redisTemplate.getMembers(key);
    if (CollectionUtils.isEmpty(topics)) {
      return Collections.emptyList();
    }
    List<String> keys =
        topics.stream()
            .map(rqueueBrokerSystemConfig::getTopicConfigurationKey)
            .collect(Collectors.toList());
    List<TopicConfig> topicConfigs = topicConfigRqueueRedisTemplate.mget(keys);
    return topicConfigs.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  @Override
  public TopicConfig getTopicConfig(Topic topic) {
    String key = rqueueBrokerSystemConfig.getTopicConfigurationKey(topic.getName());
    return topicConfigRqueueRedisTemplate.get(key);
  }

  @Override
  public List<SubscriptionConfig> getSubscriptionConfig(Topic topic) {
    String key = rqueueBrokerSystemConfig.getTopicSubscriptionKey(topic.getName());
    Set<SubscriptionConfig> configs = subscriptionConfigRqueueRedisTemplate.getMembers(key);
    if (CollectionUtils.isEmpty(configs)) {
      return Collections.emptyList();
    }
    return new ArrayList<>(configs);
  }

  @Override
  public boolean isExist(Topic topic) {
    String key = rqueueBrokerSystemConfig.getTopicsKey();
    return redisTemplate.isSetMember(key, topic.getName());
  }

  @Override
  public List<Subscription> getSubscriptions(Topic topic) {
    List<SubscriptionConfig> configs = getSubscriptionConfig(topic);
    return configs.stream()
        .map(
            e -> {
              Subscription subscription = new Subscription();
              subscription.setDelay(e.getDelay());
              return subscription;
            })
        .collect(Collectors.toList());
  }

  @Override
  public void saveSubscription(Topic topic, List<Subscription> subscriptions) {}

  @Override
  public void addTopics(List<Topic> topics) {}

  @Override
  public void addSubscriptions(Topic topic, List<Subscription> subscriptions) {}

  @Override
  public void removeTopic(Topic topic) {}
}
