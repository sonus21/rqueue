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

package com.github.sonus21.rqueue.broker.config;


import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RqueueSystemConfig {
  Value("${rqueue.root.password:admin}")
  private String rootPassword;

  @Value("${rqueue.root.username:admin}")
  private String rootUsername;

  @Value("${rqueue.root.credentials.key.suffix:auth:root}")
  private String rootCredentialsKeySuffix;

  @Value("${rqueue.authentication.tokens.key.suffix:auth::tokens}")
  private String tokenKeySuffix;

}
