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

package com.github.sonus21.rqueue.models.db;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = true)
@Builder(toBuilder = true)
public class MessageMetadata extends SerializableBase {

  private static final long serialVersionUID = 4200184682879443328L;
  private String id;
  private long totalExecutionTime;
  private boolean deleted;
  private Long deletedOn;
  // latest RqueueMessage
  private RqueueMessage rqueueMessage;
  // Rqueue message current status
  private MessageStatus status;
  private long updatedOn;

  public MessageMetadata(String id, MessageStatus messageStatus) {
    this.id = id;
    this.status = messageStatus;
  }

  public MessageMetadata(RqueueMessage rqueueMessage, MessageStatus messageStatus) {
    this.id =
        RqueueMessageUtils.getMessageMetaId(rqueueMessage.getQueueName(), rqueueMessage.getId());
    this.rqueueMessage = rqueueMessage;
    this.status = messageStatus;
  }

  public void merge(MessageMetadata other) {
    if (other == null) {
      return;
    }
    if (other.isDeleted() && !this.isDeleted()) {
      this.deleted = true;
      if (MessageStatus.DELETED.equals(other.getStatus()) && !MessageStatus.DELETED.equals(
          this.getStatus())) {
        this.status = MessageStatus.DELETED;
        if (this.updatedOn < other.updatedOn) {
          this.updatedOn = other.updatedOn;
        } else if (this.updatedOn == 0) {
          this.updatedOn = System.currentTimeMillis();
        }
      }
    }
  }
}
