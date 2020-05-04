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

package com.github.sonus21.rqueue.models.event;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.db.TaskStatus;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class QueueTaskEvent extends ApplicationEvent {
  private static final long serialVersionUID = -7762050873209497221L;
  private final TaskStatus status;
  private final RqueueMessage rqueueMessage;
  private final MessageMetadata messageMetadata;

  /**
   * Create a new QueueTaskEvent.
   *
   * @param status task status
   * @param rqueueMessage rqueue message object
   * @param queueName the queue on which event occur
   * @param messageMetadata message metadata.
   */
  public QueueTaskEvent(
      String queueName,
      TaskStatus status,
      RqueueMessage rqueueMessage,
      MessageMetadata messageMetadata) {
    super(queueName);
    this.status = status;
    this.rqueueMessage = rqueueMessage;
    this.messageMetadata = messageMetadata;
  }
}
