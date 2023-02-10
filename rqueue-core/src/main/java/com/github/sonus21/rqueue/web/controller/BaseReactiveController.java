/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.web.controller;

import com.github.sonus21.rqueue.config.RqueueWebConfig;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpResponse;

public class BaseReactiveController {

  protected final RqueueWebConfig rqueueWebConfig;

  public BaseReactiveController(RqueueWebConfig rqueueWebConfig) {
    this.rqueueWebConfig = rqueueWebConfig;
  }

  protected boolean isEnabled(ServerHttpResponse response) {
    if (!rqueueWebConfig.isEnable()) {
      response.setStatusCode(HttpStatus.SERVICE_UNAVAILABLE);
    }
    return rqueueWebConfig.isEnable();
  }
}
