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

import com.github.sonus21.rqueue.broker.config.CacheConfig;
import com.github.sonus21.rqueue.broker.config.SystemConfig;
import com.github.sonus21.rqueue.broker.dao.TopicStore;
import com.github.sonus21.rqueue.broker.models.db.SubscriptionConfig;
import com.github.sonus21.rqueue.broker.models.db.TopicConfig;
import com.github.sonus21.rqueue.broker.models.request.Subscription;
import com.github.sonus21.rqueue.broker.models.request.Topic;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.exception.ProcessingException;
import com.github.sonus21.rqueue.utils.PriorityUtils;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
@Slf4j
public class TopicStoreImpl implements TopicStore {
  private static final String CONFIGS_CACHE_KEY = "__configs__";
  private static final String TOPICS_CACHE_KEY = "__topics__";

  private final SystemConfig systemConfig;
  private final RqueueConfig rqueueConfig;
  private final RqueueRedisTemplate<String> redisTemplate;
  private final RqueueRedisTemplate<TopicConfig> topicConfigRqueueRedisTemplate;
  private final RqueueRedisTemplate<SubscriptionConfig> subscriptionConfigRqueueRedisTemplate;
  private final Cache topicCache;

  @Autowired
  public TopicStoreImpl(
      SystemConfig systemConfig,
      RqueueConfig rqueueConfig,
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> redisTemplate,
      CacheManager cacheManager) {
    this.rqueueConfig = rqueueConfig;
    RedisConnectionFactory connectionFactory =
        redisTemplate.getRedisTemplate().getConnectionFactory();
    this.systemConfig = systemConfig;
    this.redisTemplate = redisTemplate;
    this.topicCache = cacheManager.getCache(CacheConfig.TOPIC_CACHE);
    this.topicConfigRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.subscriptionConfigRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
  }

  @Override
  public Set<Topic> getTopics() {
    Set<Topic> topics = (Set<Topic>) topicCache.get(TOPICS_CACHE_KEY);
    if (topics != null) {
      return topics;
    }
    String key = systemConfig.getTopicsKey();
    Set<String> topicsFromDb = redisTemplate.getMembers(key);
    if (CollectionUtils.isEmpty(topicsFromDb)) {
      return Collections.emptySet();
    }
    topics = topicsFromDb.stream().map(Topic::new).collect(Collectors.toSet());
    topicCache.put(TOPICS_CACHE_KEY, topics);
    return topics;
  }

  @Override
  public List<TopicConfig> getTopicConfigs() {
    List<TopicConfig> configs = (List<TopicConfig>) topicCache.get(CONFIGS_CACHE_KEY);
    if (configs != null) {
      return configs;
    }
    Set<Topic> topics = getTopics();
    if (CollectionUtils.isEmpty(topics)) {
      return Collections.emptyList();
    }
    List<String> keys =
        topics.stream()
            .map(e -> systemConfig.getTopicConfigurationKey(e.getName()))
            .collect(Collectors.toList());
    List<TopicConfig> topicConfigs = topicConfigRqueueRedisTemplate.mget(keys);
    configs = topicConfigs.stream().filter(Objects::nonNull).collect(Collectors.toList());
    topicCache.put(CONFIGS_CACHE_KEY, configs);
    return configs;
  }

  private String configKey(Topic topic) {
    return "__config__" + topic.getName();
  }

  private String topicKey(Topic topic) {
    return "__topic__" + topic.getName();
  }

  private String subscriptionKey(Topic topic) {
    return "__subscription__" + topic.getName();
  }

  @Override
  public TopicConfig getTopicConfig(Topic topic) {
    String cacheKey = configKey(topic);
    TopicConfig topicConfig = topicCache.get(cacheKey, TopicConfig.class);
    if (topicConfig != null) {
      return topicConfig;
    }
    String key = systemConfig.getTopicConfigurationKey(topic.getName());
    topicConfig = topicConfigRqueueRedisTemplate.get(key);
    if (topicConfig != null) {
      topicCache.put(cacheKey, topicConfig);
    }
    return topicConfig;
  }

  @Override
  public List<SubscriptionConfig> getSubscriptionConfig(Topic topic) {
    String cacheKey = subscriptionKey(topic);
    List<SubscriptionConfig> configs = (List<SubscriptionConfig>) topicCache.get(cacheKey);
    if (configs != null) {
      return configs;
    }
    String key = systemConfig.getTopicSubscriptionKey(topic.getName());
    Set<SubscriptionConfig> configSet = subscriptionConfigRqueueRedisTemplate.getMembers(key);
    if (CollectionUtils.isEmpty(configSet)) {
      configs = Collections.emptyList();
    } else {
      configs = new ArrayList<>(configSet);
    }
    topicCache.put(cacheKey, configs);
    return configs;
  }

  @Override
  public boolean isExist(Topic topic) {
    String cacheKey = topicKey(topic);
    Object object = topicCache.get(cacheKey);
    if (object == null) {
      String key = systemConfig.getTopicsKey();
      return redisTemplate.isSetMember(key, topic.getName());
    }
    return true;
  }

  @Override
  public List<Subscription> getSubscriptions(Topic topic) {
    List<SubscriptionConfig> configs = getSubscriptionConfig(topic);
    return configs.stream().map(SubscriptionConfig::toSubscription).collect(Collectors.toList());
  }

  @Override
  public void addTopics(List<Topic> topics) {
    log.info("Adding topics {} BEGIN", topics);
    topicCache.evict(TOPICS_CACHE_KEY);
    topicCache.evict(CONFIGS_CACHE_KEY);
    Map<String, TopicConfig> topicConfigs = new HashMap<>();
    for (Topic topic : topics) {
      String id = systemConfig.getTopicConfigurationKey(topic.getName());
      TopicConfig topicConfig = new TopicConfig(id, topic.getName());
      topicConfig.setSystemName(systemConfig.getTopicName(topic.getName()));
      topicConfigs.put(id, topicConfig);
    }
    redisTemplate.addToSet(
        systemConfig.getTopicsKey(),
        topics.stream().map(Topic::getName).collect(Collectors.toList()));
    topicConfigRqueueRedisTemplate.mset(topicConfigs);
    topics.forEach(e -> topicCache.put(topicKey(e), e));
    topicCache.evict(TOPICS_CACHE_KEY);
    topicCache.evict(CONFIGS_CACHE_KEY);
    log.info("Adding topics {} END", topics);
  }

  private String getSystemTarget(Subscription subscription) {
    String queueName;
    if (subscription.getDelay() == null) {
      queueName = rqueueConfig.getQueueName(subscription.getTarget());
    } else {
      queueName = rqueueConfig.getDelayedQueueName(subscription.getTarget());
    }
    if (StringUtils.isEmpty(subscription.getPriority())) {
      return queueName;
    }
    return PriorityUtils.getQueueNameForPriority(queueName, subscription.getPriority());
  }

  private SubscriptionConfig getSubscriptionConfig(Subscription subscription) {
    SubscriptionConfig subscriptionConfig = SubscriptionConfig.fromSubscription(subscription);
    String systemTarget = getSystemTarget(subscription);
    subscriptionConfig.setSystemTarget(systemTarget);
    return subscriptionConfig;
  }

  @Override
  public void addSubscriptions(Topic topic, List<Subscription> subscriptions) {
    log.info("Adding Subscription Topic: {} Subscriptions: {}", topic.getName(), subscriptions);
    List<SubscriptionConfig> subscriptionConfigs = getSubscriptionConfig(topic);
    List<SubscriptionConfig> newSubscriptionConfigs = new ArrayList<>();
    for (Subscription subscription : subscriptions) {
      SubscriptionConfig subscriptionConfig = getSubscriptionConfig(subscription);
      subscriptionConfigs.add(subscriptionConfig);
      newSubscriptionConfigs.add(subscriptionConfig);
    }
    String subscriptionCacheKey = subscriptionKey(topic);
    String dbKey = systemConfig.getTopicSubscriptionKey(topic.getName());
    subscriptionConfigRqueueRedisTemplate.addToSet(dbKey, newSubscriptionConfigs);
    topicCache.put(subscriptionCacheKey, subscriptionConfigs);
  }

  @Override
  public void removeTopic(Topic topic) {
    log.info("Removing topic {}", topic.getName());
    topicCache.evict(TOPICS_CACHE_KEY);
    topicCache.evict(CONFIGS_CACHE_KEY);
    redisTemplate.removeFromSet(systemConfig.getTopicsKey(), topic.getName());
    redisTemplate.delete(systemConfig.getTopicConfigurationKey(topic.getName()));
    redisTemplate.delete(systemConfig.getTopicSubscriptionKey(topic.getName()));
    topicCache.evict(topicKey(topic));
    topicCache.evict(subscriptionKey(topic));
    topicCache.evict(configKey(topic));
    topicCache.evict(TOPICS_CACHE_KEY);
    topicCache.evict(CONFIGS_CACHE_KEY);
  }

  @Override
  public void removeSubscription(Topic topic, SubscriptionConfig subscriptionConfig) {
    log.info(
        "Removing subscription, topic: {}, subscription: {}", topic.getName(), subscriptionConfig);
    String subscriptionCacheKey = subscriptionKey(topic);
    String subscriptionDbKey = systemConfig.getTopicSubscriptionKey(topic.getName());
    topicCache.evict(subscriptionCacheKey);
    subscriptionConfigRqueueRedisTemplate.removeFromSet(subscriptionDbKey, subscriptionConfig);
    topicCache.evict(subscriptionCacheKey);
  }

  @Override
  public void updateSubscription(
      Topic topic, SubscriptionConfig subscriptionConfig, Subscription subscription)
      throws ProcessingException {
    log.info(
        "Updating subscription, topic: {},OldSubscription: {} NewSubscription: {}",
        topic.getName(),
        subscriptionConfig,
        subscription);
    String subscriptionCacheKey = subscriptionKey(topic);
    String subscriptionDbKey = systemConfig.getTopicSubscriptionKey(topic.getName());
    subscriptionConfigRqueueRedisTemplate.removeFromSet(subscriptionDbKey, subscriptionConfig);
    SubscriptionConfig subscriptionConfigNew;
    try {
      subscriptionConfigNew = subscriptionConfig.clone();
    } catch (CloneNotSupportedException e) {
      throw new ProcessingException(e);
    }
    subscriptionConfigNew.setDelay(subscription.getDelay());
    subscriptionConfigNew.setSystemTarget(getSystemTarget(subscription));
    subscriptionConfigRqueueRedisTemplate.addToSet(subscriptionDbKey, subscriptionConfigNew);
    topicCache.evict(subscriptionCacheKey);
  }
}
