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

import java.util.HashMap;
import java.util.Map;

public class DefaultContext implements Context {

  private final Context parentContext;
  private final Map<Object, Object> map = new HashMap<>(3);

  public DefaultContext(Context parentContext) {
    this.parentContext = parentContext;
  }

  @Override
  public synchronized Object getValue(Object key) {
    if (map.containsKey(key)) {
      return map.get(key);
    }
    if (parentContext != null) {
      return parentContext.getValue(key);
    }
    return null;
  }

  @Override
  public synchronized Context setValue(Object key, Object value) {
    map.put(key, value);
    return this;
  }
}
