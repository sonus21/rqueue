/*
 *  Copyright 2023 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
@Setter
public class RqueueEventBusConfig {

  @Value("${rqueue.event.bus.core.pool.size:2}")
  private int corePoolSize;

  @Value("${rqueue.event.bus.max.pool.size:10}")
  private int maxPoolSize;

  @Value("${rqueue.event.bus.queue.capacity:100}")
  private int queueCapacity;

  @Value("${rqueue.event.bus.keep.alive.time:60000}")
  private int keepAliveTime;
}
