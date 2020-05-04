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

import com.github.sonus21.rqueue.listener.QueueDetail;
import java.util.Map;
import lombok.Getter;
import org.springframework.context.ApplicationEvent;

@Getter
public class QueueInitializationEvent extends ApplicationEvent {

  private static final long serialVersionUID = 1955427920805054136L;
  private final Map<String, QueueDetail> queueDetailMap;
  private final boolean start;

  public QueueInitializationEvent(
      Object source, Map<String, QueueDetail> queueDetailMap, boolean start) {
    super(source);
    this.queueDetailMap = queueDetailMap;
    this.start = start;
  }
}
