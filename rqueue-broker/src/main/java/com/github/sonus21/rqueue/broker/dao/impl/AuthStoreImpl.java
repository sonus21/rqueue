package com.github.sonus21.rqueue.broker.dao.impl;

import com.github.sonus21.rqueue.broker.dao.AuthStore;
import com.github.sonus21.rqueue.broker.models.db.RootUser;
import com.github.sonus21.rqueue.broker.models.db.Session;

public class AuthStoreImpl implements AuthStore {

  @Override
  public RootUser getRootUser() {
    return null;
  }

  @Override
  public boolean deleteToken(String token) {
    return false;
  }

  @Override
  public boolean addToken(String token) {
    return false;
  }

  @Override
  public boolean isTokenExist(String token) {
    return false;
  }

  @Override
  public void updateRootUser(RootUser rootUser) {

  }

  @Override
  public Session createSession(String username) {
    return null;
  }

  @Override
  public boolean isSessionExist(String sessionId) {
    return false;
  }

  @Override
  public void deleteSession(String sessionId) {

  }

  @Override
  public void cleanUserSessions(String rootUserName) {

  }

  @Override
  public Session getSession(String sessionId) {
    return null;
  }
}
