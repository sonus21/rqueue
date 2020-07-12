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

import com.github.sonus21.rqueue.broker.config.CacheConfig;
import com.github.sonus21.rqueue.broker.config.SystemConfig;
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
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.stereotype.Repository;
import org.springframework.util.CollectionUtils;

@Repository
public class AuthStoreImpl implements AuthStore {
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate;
  private final SystemConfig systemConfig;
  private final RqueueRedisTemplate<Session> sessionRqueueRedisTemplate;
  private final RqueueRedisTemplate<RootUser> rootUserRqueueRedisTemplate;
  private final RqueueRedisTemplate<Token> tokenRqueueRedisTemplate;
  private final Cache authTokenCache;
  private final Cache sessionCache;

  @Autowired
  public AuthStoreImpl(
      @Qualifier("stringRqueueRedisTemplate") RqueueRedisTemplate<String> stringRqueueRedisTemplate,
      SystemConfig systemConfig,
      CacheManager cacheManager) {
    RedisConnectionFactory connectionFactory =
        stringRqueueRedisTemplate.getRedisTemplate().getConnectionFactory();
    this.stringRqueueRedisTemplate = stringRqueueRedisTemplate;
    this.systemConfig = systemConfig;
    this.sessionRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.rootUserRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.tokenRqueueRedisTemplate = new RqueueRedisTemplate<>(connectionFactory);
    this.authTokenCache = cacheManager.getCache(CacheConfig.AUTH_TOKEN_CACHE_NAME);
    this.sessionCache = cacheManager.getCache(CacheConfig.SESSION_CACHE_NAME);
  }

  @Override
  public RootUser getRootUser() {
    RootUser rootUser = rootUserRqueueRedisTemplate.get(systemConfig.getRootUserKey());
    if (rootUser == null || rootUser.getUsername() == null || rootUser.getPassword() == null) {
      rootUser = null;
    }
    return rootUser;
  }

  @Override
  public boolean deleteToken(String tokenName) {
    String tokenKey = systemConfig.getTokenKey(tokenName);
    Token token = tokenRqueueRedisTemplate.get(tokenKey);
    if (token == null) {
      return false;
    }
    String tokensKey = systemConfig.getTokensKey();
    String tokenNamesKey = systemConfig.getTokenNamesKey();
    stringRqueueRedisTemplate.removeFromSet(tokensKey, token.getValue());
    stringRqueueRedisTemplate.removeFromSet(tokenNamesKey, tokenName);
    tokenRqueueRedisTemplate.delete(tokenKey);
    authTokenCache.evict(token.getValue());
    return true;
  }

  @Override
  public boolean addToken(Token token) {
    String tokenKey = systemConfig.getTokenKey(token.getName());
    String tokensKey = systemConfig.getTokensKey();
    String tokenNamesKey = systemConfig.getTokenNamesKey();
    tokenRqueueRedisTemplate.set(tokenKey, token);
    stringRqueueRedisTemplate.addToSet(tokensKey, token.getValue());
    stringRqueueRedisTemplate.addToSet(tokenNamesKey, token.getName());
    authTokenCache.put(token.getValue(), Boolean.TRUE);
    return true;
  }

  @Override
  public boolean isTokenExist(String token) {
    Boolean tokenExist = authTokenCache.get(token, Boolean.class);
    if (tokenExist == null) {
      String tokensKey = systemConfig.getTokensKey();
      if (stringRqueueRedisTemplate.isSetMember(tokensKey, token)) {
        authTokenCache.put(token, Boolean.TRUE);
        return true;
      }
      return false;
    }
    return true;
  }

  @Override
  public void updateRootUser(RootUser rootUser) {
    String rootUserKey = systemConfig.getRootUserKey();
    rootUserRqueueRedisTemplate.set(rootUserKey, rootUser);
  }

  @Override
  public Session createSession(String username, int sessionExpiry) {
    Session session = new Session();
    session.setId(UUID.randomUUID().toString());
    session.setUserId(username);
    session.setCreatedAt(System.currentTimeMillis());
    String userSessionKey = systemConfig.getUserSessionKey(username);
    String sessionKey = systemConfig.getSessionKey(session.getId());
    stringRqueueRedisTemplate.addToSet(userSessionKey, session.getId());
    sessionRqueueRedisTemplate.set(sessionKey, session);
    return session;
  }

  @Override
  public boolean isSessionExist(String sessionId) {
    Boolean sessionExist = sessionCache.get(sessionId, Boolean.class);
    if (sessionExist != null) {
      String key = systemConfig.getSessionKey(sessionId);
      if (stringRqueueRedisTemplate.exist(key)) {
        sessionCache.put(sessionId, true);
        return true;
      }
      return false;
    }
    return true;
  }

  @Override
  public void deleteSession(String sessionId) {
    Session session = getSession(sessionId);
    if (session != null && session.getId() != null) {
      String key = systemConfig.getSessionKey(sessionId);
      String userSessionKey = systemConfig.getUserSessionKey(session.getUserId());
      stringRqueueRedisTemplate.delete(key);
      stringRqueueRedisTemplate.removeFromSet(userSessionKey, sessionId);
      sessionCache.evict(session);
    }
  }

  @Override
  public void cleanUserSessions(String userName) {
    String userSessionKey = systemConfig.getUserSessionKey(userName);
    Set<String> sessionIds = stringRqueueRedisTemplate.getMembers(userSessionKey);
    if (CollectionUtils.isEmpty(sessionIds)) {
      List<String> sessionKeys =
          sessionIds.stream().map(systemConfig::getSessionKey).collect(Collectors.toList());
      stringRqueueRedisTemplate.delete(sessionKeys);
      for (String sessionId : sessionIds) {
        sessionCache.evict(sessionId);
      }
    }
    stringRqueueRedisTemplate.delete(userSessionKey);
  }

  @Override
  public Session getSession(String sessionId) {
    String key = systemConfig.getSessionKey(sessionId);
    return sessionRqueueRedisTemplate.get(key);
  }
}
