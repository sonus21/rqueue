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
public class AuthConfig {

  @Value("${rqueue.authentication.session.expiry:604800}")
  private int sessionExpiry;

  @Value("${rqueue.authentication.session.close.on.browser.close:false}")
  private boolean closeSessionOnBrowserClose;

  @Value("${rqueue.authentication.cookie.secure:false}")
  private boolean cookieSecure;

  @Value("${rqueue.root.username:admin}")
  private String rootUsername;

  @Value("${rqueue.root.password:admin}")
  private String rootPassword;

  public int getSessionExpiry() {
    return sessionExpiry;
  }

  public boolean isCloseSessionOnBrowserClose() {
    return closeSessionOnBrowserClose;
  }

  public boolean isCookieSecure() {
    return cookieSecure;
  }

  public String getRootPassword() {
    return rootPassword;
  }

  public String getRootUsername() {
    return rootUsername;
  }
}
