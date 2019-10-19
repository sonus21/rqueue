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

/**
 * A global lock manager, it relies on Redis for lock guarantee, even in the case of master slave
 * configuration it relies only {@link
 * org.springframework.data.redis.core.ValueOperations#setIfAbsent(Object, Object) method}
 */
public class LockManager {
  private StringMessageTemplate stringMessageTemplate;
  private static final long LOCK_EXPIRY_TIME = 10;

  public LockManager(StringMessageTemplate stringMessageTemplate) {
    this.stringMessageTemplate = stringMessageTemplate;
  }

  /**
   * Acquire a lock from redis
   *
   * @param lock lock identifier
   * @return true or false whether lock was acquired or not
   */
  public boolean acquireLock(String lock) {
    try {
      return stringMessageTemplate.putIfAbsent(lock, LOCK_EXPIRY_TIME, TimeUnit.SECONDS);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Release given lock, in case of failure it'll be propagated to the caller.
   *
   * @param lock lock identifier
   */
  public void releaseLock(String lock) {
    stringMessageTemplate.delete(lock);
  }
}
