/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.test.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
@Entity
@Table(
    name = "consumed_messages",
    uniqueConstraints = {
        @UniqueConstraint(
            name = "message_id_and_tag_unique",
            columnNames = {"message_id", "tag"})
    })
public class ConsumedMessage {

  @Id
  @Column
  private String id;

  @Column(name = "message_id")
  private String messageId;

  // Tag can be more than the message
  @Column(name = "tag", length = 1000000)
  private String tag;

  @Column(name = "queue_name")
  private String queueName;

  // Around 1 MB of data
  @Column(length = 1000000)
  private String message;

  @Column
  private Long createdAt;

  @Column
  private Long updatedAt;

  @Column
  private int count;

  public ConsumedMessage(String messageId, String tag, String queueName, String message) {
    this(
        UUID.randomUUID().toString(),
        messageId,
        tag,
        queueName,
        message,
        System.currentTimeMillis(),
        System.currentTimeMillis(),
        1);
  }

  @PreUpdate
  public void update() {
    this.updatedAt = System.currentTimeMillis();
  }

  public void incrementCount() {
    this.count += 1;
  }
}
