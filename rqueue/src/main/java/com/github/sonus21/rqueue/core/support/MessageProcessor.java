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

package com.github.sonus21.rqueue.core.support;

/**
 * This interface can be used to take some action when ever a message is processed
 *
 * <p>Rqueue supports many types of action that can be taken by application whenever some events
 * occur about the enqueued tasks.
 *
 * <p>You can set one or more message processor in factory or container based. Each message
 * processor is called based on the event status.
 *
 * @see com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory
 * @see com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer
 */
public interface MessageProcessor {

  /**
   * This method would be called with the specified object, this will happen only when message is
   * deserialize successfully. Return value is used for pre-processing, where if caller returns true
   * then only message would be executed otherwise it will be discarded, returning false means task
   * to ignore. It's considered that message has to be deleted.
   *
   * @param message message
   * @return true/false.
   */
  default boolean process(Object message) {
    return true;
  }
}
