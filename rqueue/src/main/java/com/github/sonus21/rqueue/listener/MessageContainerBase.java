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
import java.lang.ref.WeakReference;
import java.util.List;
import org.slf4j.Logger;
import org.springframework.messaging.converter.MessageConverter;

public class MessageContainerBase {
  protected final WeakReference<RqueueMessageListenerContainer> container;

  MessageContainerBase(RqueueMessageListenerContainer container) {
    this.container = new WeakReference<>(container);
  }

  MessageContainerBase(WeakReference<RqueueMessageListenerContainer> container) {
    this.container = container;
  }

  Logger getLogger() {
    container.get();
    return RqueueMessageListenerContainer.logger;
  }

  @SuppressWarnings("ConstantConditions")
  RqueueMessageHandler getMessageHandler() {
    return container.get().getRqueueMessageHandler();
  }

  protected List<MessageConverter> getMessageConverters() {
    return getMessageHandler().getMessageConverters();
  }

  @SuppressWarnings("ConstantConditions")
  protected RqueueMessageTemplate getRqueueMessageTemplate() {
    return container.get().getRqueueMessageTemplate();
  }

  @SuppressWarnings("ConstantConditions")
  boolean isQueueActive(String queueName) {
    return container.get().isQueueActive(queueName);
  }
}
