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

package com.github.sonus21.rqueue.models.event;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.github.sonus21.rqueue.converter.GenericMessageConverter.SmartMessageSerDes;
import com.github.sonus21.rqueue.models.SerializableBase;
import com.github.sonus21.rqueue.models.enums.PubSubType;
import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class RqueuePubSubEvent extends SerializableBase {

  private PubSubType type;
  private String senderId;
  private String message;

  @JsonIgnore
  public <T> T messageAs(SmartMessageSerDes serDes, Class<T> clazz) {
    return serDes.deserialize(getMessage().getBytes(StandardCharsets.UTF_8), clazz);
  }
}
