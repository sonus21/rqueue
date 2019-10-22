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

package com.github.sonus21.rqueue.core;

import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.connection.RedisConnectionFactory;

public class StringMessageTemplate extends RqueueRedisTemplate<String, String> {

  public StringMessageTemplate(RedisConnectionFactory redisConnectionFactory) {
    super(redisConnectionFactory);
  }

  public boolean putIfAbsent(String key, long time, TimeUnit timeUnit) {
    Boolean result = redisTemplate.opsForValue().setIfAbsent(key, key, time, timeUnit);
    if (result == null) {
      return false;
    }
    return result;
  }

  public boolean delete(String key) {
    Boolean result = redisTemplate.delete(key);
    if (result == null) {
      return false;
    }
    return result;
  }
}
