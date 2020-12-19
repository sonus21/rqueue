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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import lombok.ToString;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@SuppressWarnings("unchecked")
@ToString
public class RedisScriptFactory {
  public static <T> RedisScript<T> getScript(ScriptType type) {
    Resource resource = new ClassPathResource(type.getPath());
    DefaultRedisScript<T> script = new DefaultRedisScript<>();
    script.setLocation(resource);
    switch (type) {
      case ENQUEUE_MESSAGE:
      case MOVE_MESSAGE:
      case MOVE_EXPIRED_MESSAGE:
      case MOVE_MESSAGE_LIST_TO_LIST:
      case MOVE_MESSAGE_LIST_TO_ZSET:
      case MOVE_MESSAGE_ZSET_TO_ZSET:
      case MOVE_MESSAGE_ZSET_TO_LIST:
      case SCHEDULE_MESSAGE:
        script.setResultType((Class<T>) Long.class);
        return script;
      case DELETE_IF_SAME:
        script.setResultType((Class<T>) Boolean.class);
        return script;
      case DEQUEUE_MESSAGE:
        script.setResultType((Class<T>) RqueueMessage.class);
        return script;
      default:
        throw new UnknownSwitchCase(type.toString());
    }
  }

  public enum ScriptType {
    ENQUEUE_MESSAGE("scripts/enqueue_message.lua"),
    DEQUEUE_MESSAGE("scripts/dequeue_message.lua"),
    MOVE_MESSAGE("scripts/move_message.lua"),
    MOVE_EXPIRED_MESSAGE("scripts/move_expired_message.lua"),
    MOVE_MESSAGE_LIST_TO_LIST("scripts/move_message_list_to_list.lua"),
    MOVE_MESSAGE_LIST_TO_ZSET("scripts/move_message_list_to_zset.lua"),
    MOVE_MESSAGE_ZSET_TO_ZSET("scripts/move_message_zset_to_zset.lua"),
    MOVE_MESSAGE_ZSET_TO_LIST("scripts/move_message_zset_to_list.lua"),
    SCHEDULE_MESSAGE("scripts/schedule_message.lua"),
    DELETE_IF_SAME("scripts/delete_if_same.lua");
    private final String path;

    ScriptType(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }
  }
}
