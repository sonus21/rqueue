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

package com.github.sonus21.rqueue.broker.dao;

import com.github.sonus21.rqueue.broker.models.db.SubscriptionConfig;
import com.github.sonus21.rqueue.broker.models.db.TopicConfig;
import com.github.sonus21.rqueue.broker.models.request.Subscription;
import com.github.sonus21.rqueue.broker.models.request.Topic;
import com.github.sonus21.rqueue.exception.ProcessingException;
import java.util.List;
import java.util.Set;

public interface TopicStore {
  Set<Topic> getTopics();

  List<TopicConfig> getTopicConfigs();

  TopicConfig getTopicConfig(Topic topic);

  List<SubscriptionConfig> getSubscriptionConfig(Topic topic);

  boolean isExist(Topic topic);

  List<Subscription> getSubscriptions(Topic topic);

  void addTopics(List<Topic> topics);

  void addSubscriptions(Topic topic, List<Subscription> subscriptions);

  void removeTopic(Topic topic);

  void removeSubscription(Topic topic, SubscriptionConfig subscriptionConfig);

  void updateSubscription(
      Topic topic, SubscriptionConfig subscriptionConfig, Subscription subscription)
      throws ProcessingException;
}
