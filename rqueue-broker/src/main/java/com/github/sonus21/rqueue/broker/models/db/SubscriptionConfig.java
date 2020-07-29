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

package com.github.sonus21.rqueue.broker.models.db;

import com.github.sonus21.rqueue.broker.models.request.DestinationType;
import com.github.sonus21.rqueue.broker.models.request.Subscription;
import com.github.sonus21.rqueue.models.SerializableBase;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@Getter
@Setter
@ToString
public class SubscriptionConfig extends SerializableBase implements Cloneable {
  private static final long serialVersionUID = -1261404662305659322L;
  private String id;
  private String target;
  private DestinationType targetType;
  private String priority;
  private String systemTarget;
  private Long delay;
  private long createdOn;
  private long updatedOn;

  public Subscription toSubscription() {
    Subscription subscription = new Subscription();
    subscription.setDelay(delay);
    subscription.setTargetType(targetType);
    subscription.setTarget(target);
    subscription.setPriority(priority);
    return subscription;
  }

  public static SubscriptionConfig fromSubscription(Subscription subscription) {
    SubscriptionConfig subscriptionConfig = new SubscriptionConfig();
    subscriptionConfig.setDelay(subscription.getDelay());
    subscriptionConfig.setTarget(subscription.getTarget());
    subscriptionConfig.setPriority(subscription.getPriority());
    subscriptionConfig.setCreatedOn(System.currentTimeMillis());
    subscriptionConfig.setUpdatedOn(subscriptionConfig.createdOn);
    return subscriptionConfig;
  }

  @Override
  @SuppressWarnings("squid:S2975")
  public SubscriptionConfig clone() throws CloneNotSupportedException {
    return (SubscriptionConfig) super.clone();
  }
}
