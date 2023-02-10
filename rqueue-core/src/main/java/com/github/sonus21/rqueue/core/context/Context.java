/*
 * Copyright (c) 2021-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core.context;

import com.github.sonus21.rqueue.core.middleware.ContextMiddleware;

/**
 * A context that supports getValue method. Context is used inside job and middleware
 *
 * @see DefaultContext
 * @see ContextMiddleware
 */
public interface Context {

  /**
   * Return value form the context.
   *
   * @param key context key to be searched.
   * @return value or null
   */
  Object getValue(Object key);
}
