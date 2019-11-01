/*
 * Copyright (c)  2019-2019, Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.github.sonus21.rqueue.utils;

import static com.github.sonus21.rqueue.utils.RedisUtil.getRedisTemplate;

import java.io.Serializable;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

public abstract class RqueueRedisTemplate<V extends Serializable> {
  protected RedisTemplate<String, V> redisTemplate;

  public RqueueRedisTemplate(RedisConnectionFactory redisConnectionFactory) {
    this.redisTemplate = getRedisTemplate(redisConnectionFactory);
  }
}
