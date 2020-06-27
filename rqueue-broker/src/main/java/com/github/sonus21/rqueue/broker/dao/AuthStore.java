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

package com.github.sonus21.rqueue.broker.dao;

import com.github.sonus21.rqueue.broker.models.db.RootUser;
import com.github.sonus21.rqueue.broker.models.db.Session;
import com.github.sonus21.rqueue.broker.models.db.Token;

public interface AuthStore {
  RootUser getRootUser();

  boolean deleteToken(String tokenName);

  boolean addToken(Token token);

  boolean isTokenExist(String token);

  void updateRootUser(RootUser rootUser);

  Session createSession(String username, int sessionExpiry);

  boolean isSessionExist(String sessionId);

  void deleteSession(String sessionId);

  void cleanUserSessions(String rootUserName);

  Session getSession(String sessionId);
}
