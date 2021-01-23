/*
 *  Copyright 2021 Sonu Kumar
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

package com.github.sonus21.rqueue.core.context;

public class DefaultContext implements Context {

  private final Context parentContext;
  public static final Context EMPTY = new DefaultContext(null, null, null);
  private final Object key;
  private final Object value;

  private DefaultContext(Context parentContext, Object key, Object value) {
    this.parentContext = parentContext;
    this.key = key;
    this.value = value;
  }

  public static Context withValue(Context parentContext, Object key, Object value) {
    if (key == null) {
      throw new IllegalArgumentException("key can not be null");
    }
    return new DefaultContext(parentContext, key, value);
  }

  @Override
  public Object getValue(Object key) {
    if (key == null) {
      throw new IllegalArgumentException("key can not be null");
    }
    if (this == EMPTY) {
      return null;
    }
    if (key.equals(this.key)) {
      return value;
    }
    if (parentContext != null) {
      return parentContext.getValue(key);
    }
    return null;
  }
}
