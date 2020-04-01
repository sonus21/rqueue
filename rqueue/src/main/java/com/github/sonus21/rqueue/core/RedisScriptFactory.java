/*
 * Copyright 2019 Sonu Kumar
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

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

@SuppressWarnings("unchecked")
class RedisScriptFactory {
  static RedisScript getScript(ScriptType type) {
    Resource resource = new ClassPathResource(type.getPath());
    DefaultRedisScript script = new DefaultRedisScript();
    script.setLocation(resource);
    switch (type) {
      case ADD_MESSAGE:
      case MOVE_MESSAGE:
      case REPLACE_MESSAGE:
      case PUSH_MESSAGE:
        script.setResultType(Long.class);
        return script;
      case REMOVE_MESSAGE:
        script.setResultType(RqueueMessage.class);
        return script;
    }
    return null;
  }

  enum ScriptType {
    ADD_MESSAGE("scripts/add-message.lua"),
    REMOVE_MESSAGE("scripts/remove-message.lua"),
    REPLACE_MESSAGE("scripts/replace-message.lua"),
    MOVE_MESSAGE("scripts/move-message.lua"),
    PUSH_MESSAGE("scripts/push-message.lua");

    private String path;

    ScriptType(String path) {
      this.path = path;
    }

    public String getPath() {
      return path;
    }
  }
}
