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
import java.util.List;
import java.util.Set;

public class TopicStoreImpl implements TopicStore {

  @Override
  public Set<Topic> getTopics() {
    return null;
  }

  @Override
  public List<TopicConfig> getTopicConfigs() {
    return null;
  }

  @Override
  public TopicConfig getTopicConfig(Topic topic) {
    return null;
  }

  @Override
  public List<SubscriptionConfig> getSubscriptionConfig(Topic topic) {
    return null;
  }

  @Override
  public boolean isExist(Topic topic) {
    return false;
  }

  @Override
  public List<Subscription> getSubscriptions(Topic topic) {
    return null;
  }

  @Override
  public void saveSubscription(Topic topic, List<Subscription> subscriptions) {

  }

  @Override
  public void addTopics(List<Topic> topics) {

  }

  @Override
  public void addSubscriptions(Topic topic, List<Subscription> subscriptions) {

  }

  @Override
  public void removeTopic(Topic topic) {

  }
}
