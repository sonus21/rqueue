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

package com.github.sonus21.rqueue.broker.dao.impl;

import com.github.sonus21.rqueue.broker.config.RqueueBrokerSystemConfig;
import com.github.sonus21.rqueue.broker.dao.AuthStore;
import com.github.sonus21.rqueue.broker.models.db.RootUser;
import com.github.sonus21.rqueue.broker.models.db.Session;
import com.github.sonus21.rqueue.broker.models.db.Token;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
public class AuthStoreImpl implements AuthStore {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final RqueueBrokerSystemConfig rqueueBrokerSystemConfig;
  private final RqueueRedisTemplate<Session> sessionRqueueRedisTemplate;
  private final RqueueRedisTemplate<RootUser> rootUserRqueueRedisTemplate;
  private final RqueueRedisTemplate<Token> tokenRqueueRedisTemplate;

  @Autowired
  public AuthStoreImpl(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      RqueueBrokerSystemConfig rqueueBrokerSystemConfig) {
    RedisConnectionFactory connectionFactory =
        stringRqueueRedisTemplate.getRedisTemplate().getConnectionFactory();
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.rqueueBrokerSystemConfig = rqueueBrokerSystemConfig;
    this.sessionRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.rootUserRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.tokenRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
  }

  @Override
  public RootUser getRootUser() {
    RootUser rootUser = rootUserRqueueRedisTemplate.get(rqueueBrokerSystemConfig.getRootUserKey());
    if (rootUser == null || rootUser.getUsername() == null || rootUser.getPassword() == null) {
      rootUser = null;
    }
    return rootUser;
  }

  @Override
  public boolean deleteToken(String tokenName) {
    String tokenKey = rqueueBrokerSystemConfig.getTokenKey(tokenName);
    Token token = tokenRqueueRedisTemplate.get(tokenKey);
    if (token == null) {
      return false;
    }
    String tokensKey = rqueueBrokerSystemConfig.getTokensKey();
    String tokenNamesKey = rqueueBrokerSystemConfig.getTokenNamesKey();
    stringRqueueRedisTemplate.removeFromSet(tokensKey, token.getValue());
    stringRqueueRedisTemplate.removeFromSet(tokenNamesKey, tokenName);
    tokenRqueueRedisTemplate.delete(tokenKey);
    return true;
  }

  @Override
  public boolean addToken(Token token) {
    String tokenKey = rqueueBrokerSystemConfig.getTokenKey(token.getName());
    String tokensKey = rqueueBrokerSystemConfig.getTokensKey();
    String tokenNamesKey = rqueueBrokerSystemConfig.getTokenNamesKey();
    tokenRqueueRedisTemplate.set(tokenKey, token);
    stringRqueueRedisTemplate.addToSet(tokensKey, token.getValue());
    stringRqueueRedisTemplate.addToSet(tokenNamesKey, token.getName());
    return true;
  }

  @Override
  public boolean isTokenExist(String token) {
    String tokensKey = rqueueBrokerSystemConfig.getTokensKey();
    return stringRqueueRedisTemplate.isSetMember(tokensKey, token);
  }

  @Override
  public void updateRootUser(RootUser rootUser) {
    String rootUserKey = rqueueBrokerSystemConfig.getRootUserKey();
    rootUserRqueueRedisTemplate.set(rootUserKey, rootUser);
  }

  @Override
  public Session createSession(String username, int sessionExpiry) {
    Session session = new Session();
    session.setId(UUID.randomUUID().toString());
    session.setUserId(username);
    session.setCreatedAt(System.currentTimeMillis());
    String userSessionKey = rqueueBrokerSystemConfig.getUserSessionKey(username);
    String sessionKey = rqueueBrokerSystemConfig.getSessionKey(session.getId());
    stringRqueueRedisTemplate.addToSet(userSessionKey, session.getId());
    sessionRqueueRedisTemplate.set(sessionKey, session);
    return session;
  }

  @Override
  public boolean isSessionExist(String sessionId) {
    String key = rqueueBrokerSystemConfig.getSessionKey(sessionId);
    return stringRqueueRedisTemplate.exist(key);
  }

  @Override
  public void deleteSession(String sessionId) {
    Session session = getSession(sessionId);
    if (session != null && session.getId() != null) {
      String key = rqueueBrokerSystemConfig.getSessionKey(sessionId);
      String userSessionKey = rqueueBrokerSystemConfig.getUserSessionKey(session.getUserId());
      stringRqueueRedisTemplate.delete(key);
      stringRqueueRedisTemplate.removeFromSet(userSessionKey, sessionId);
    }
  }

  @Override
  public void cleanUserSessions(String userName) {
    String userSessionKey = rqueueBrokerSystemConfig.getUserSessionKey(userName);
    Set<String> sessionIds = stringRqueueRedisTemplate.getMembers(userSessionKey);
    if (CollectionUtils.isEmpty(sessionIds)) {
      List<String> sessionKeys =
          sessionIds.stream()
              .map(rqueueBrokerSystemConfig::getSessionKey)
              .collect(Collectors.toList());
      stringRqueueRedisTemplate.delete(sessionKeys);
    }
    stringRqueueRedisTemplate.delete(userSessionKey);
  }

  @Override
  public Session getSession(String sessionId) {
    String key = rqueueBrokerSystemConfig.getSessionKey(sessionId);
    return sessionRqueueRedisTemplate.get(key);
  }
}
