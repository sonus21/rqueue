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

package com.github.sonus21.rqueue.listener;

import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.utils.BaseLogger;
import java.lang.ref.WeakReference;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.event.Level;

abstract class MessageContainerBase extends BaseLogger implements Runnable {
  protected final WeakReference<RqueueMessageListenerContainer> container;

  MessageContainerBase(Logger log, String groupName, RqueueMessageListenerContainer container) {
    this(log, groupName, new WeakReference<>(container));
  }

  MessageContainerBase(
      Logger log, String groupName, WeakReference<RqueueMessageListenerContainer> container) {
    super(log, groupName);
    this.container = container;
  }

  protected RqueueMessageTemplate getRqueueMessageTemplate() {
    return Objects.requireNonNull(container.get()).getRqueueMessageTemplate();
  }

  boolean isQueueActive(String queueName) {
    return Objects.requireNonNull(container.get()).isQueueActive(queueName);
  }

  @Override
  public void run() {
    try {
      start();
    } catch (Exception e) {
      log(Level.ERROR, "Failed {}", e, e.getMessage());
    }
  }

  abstract void start();
}
