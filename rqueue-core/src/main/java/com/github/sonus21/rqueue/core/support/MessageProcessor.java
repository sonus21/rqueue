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

package com.github.sonus21.rqueue.core.support;

import com.github.sonus21.rqueue.core.Job;

/**
 * This interface can be used to take some action whenever a message is processed
 *
 * <p>Rqueue supports many types of action that can be taken by application whenever some events
 * occur about the enqueued tasks.
 *
 * <p>You can set one or more message processor in factory or container. Each message processor is
 * called based upon the status {@link com.github.sonus21.rqueue.models.enums.MessageStatus}.
 *
 * @see com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory
 * @see com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer
 */
public interface MessageProcessor {

  /**
   * This method would be called with the specified object, this will happen only when message is
   * deserialize successfully.
   *
   * <p>Return value is used only when it's called from pre-processing, if the method returns true
   * then only message would be consumed otherwise it will be discarded, no further processing of
   * the message would be done.
   *
   * @param job message
   * @return true/false.
   */
  boolean process(Job job);
}
