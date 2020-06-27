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

package com.github.sonus21.rqueue.broker.models;

import com.github.sonus21.rqueue.broker.models.db.Session;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public final class AuthUtils {
  private AuthUtils() {}

  private static final String SESSION_ID = "SESSION-ID";
  public static final String AUTHORIZATION_HEADER = "Authorization";

  public static void addSession(HttpServletResponse response, Session session, int expiry) {
    Cookie cookie = new Cookie(SESSION_ID, session.getId());
    cookie.setMaxAge(expiry);
    response.addCookie(cookie);
  }

  public static String getSessionId(HttpServletRequest request) {
    Cookie[] cookies = request.getCookies();
    if (cookies == null || cookies.length == 0) {
      return null;
    }
    for (Cookie cookie : cookies) {
      if (cookie.getName().equals(SESSION_ID)) {
        return cookie.getValue();
      }
    }
    return null;
  }

  public static String getToken(HttpServletRequest request) {
    return request.getHeader(AUTHORIZATION_HEADER);
  }
}
