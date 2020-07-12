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

package com.github.sonus21.rqueue.broker.service.impl;

import com.github.sonus21.rqueue.broker.config.AuthConfig;
import com.github.sonus21.rqueue.broker.dao.AuthStore;
import com.github.sonus21.rqueue.broker.models.db.RootUser;
import com.github.sonus21.rqueue.broker.models.db.Session;
import com.github.sonus21.rqueue.broker.models.request.DeleteTokenRequest;
import com.github.sonus21.rqueue.broker.models.request.NewTokenRequest;
import com.github.sonus21.rqueue.broker.models.request.UpdateRootPassword;
import com.github.sonus21.rqueue.broker.models.request.UpdateRootUsername;
import com.github.sonus21.rqueue.broker.models.request.UsernamePassword;
import com.github.sonus21.rqueue.broker.service.AuthenticationService;
import com.github.sonus21.rqueue.broker.service.utils.AuthUtils;
import com.github.sonus21.rqueue.exception.ErrorCode;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.StringUtils;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class AuthenticationServiceImpl implements AuthenticationService {
  private final AuthConfig authConfig;
  private final AuthStore authStore;
  private final PasswordEncoder passwordEncoder;

  @Autowired
  public AuthenticationServiceImpl(
      AuthConfig authConfig, AuthStore authStore, PasswordEncoder passwordEncoder) {
    this.authConfig = authConfig;
    this.authStore = authStore;
    this.passwordEncoder = passwordEncoder;
  }

  @Override
  public boolean isValidToken(String token) {
    if (StringUtils.isEmpty(token)) {
      return false;
    }
    String[] tokens = token.split(" ");
    return authStore.isTokenExist(tokens[tokens.length - 1]);
  }

  private RootUser getRootUser() {
    RootUser rootUser = authStore.getRootUser();
    if (rootUser == null) {
      rootUser = new RootUser();
      rootUser.setUsername(authConfig.getRootUsername());
      rootUser.setPassword(passwordEncoder.encode(authConfig.getRootPassword()));
      authStore.updateRootUser(rootUser);
    }
    return rootUser;
  }

  @Override
  public BaseResponse login(UsernamePassword usernamePassword, HttpServletResponse response) {
    RootUser rootUser = getRootUser();
    if (rootUser.getUsername().equals(usernamePassword.getUsername())
        && rootUser.getPassword().equals(usernamePassword.getPassword())) {
      Session session =
          authStore.createSession(rootUser.getUsername(), authConfig.getSessionExpiry());
      int expiry = authConfig.getSessionExpiry();
      if (authConfig.isCloseSessionOnBrowserClose()) {
        expiry = -1;
      }
      AuthUtils.addSession(response, session, expiry, authConfig.isCookieSecure());
      return new BaseResponse();
    }
    return new BaseResponse(ErrorCode.INVALID_USERNAME_OR_PASSWORD);
  }

  @Override
  public BaseResponse logout(HttpServletRequest request, HttpServletResponse response) {
    String sessionId = AuthUtils.getSessionId(request);
    if (sessionId == null) {
      log.warn("Session does not exist");
    } else {
      authStore.deleteSession(sessionId);
    }
    return new BaseResponse(ErrorCode.SUCCESS);
  }

  private BaseResponse updateUser(
      RootUser rootUser,
      String rootUserName,
      HttpServletRequest request,
      HttpServletResponse response) {
    Session session = authStore.getSession(AuthUtils.getSessionId(request));
    AuthUtils.removeSession(response, session, authConfig.isCookieSecure());
    authStore.cleanUserSessions(rootUserName);
    authStore.updateRootUser(rootUser);
    return new BaseResponse(ErrorCode.SUCCESS);
  }

  @Override
  public BaseResponse updateRootPassword(
      UpdateRootPassword updateRootPassword,
      HttpServletRequest request,
      HttpServletResponse response) {
    if (StringUtils.isEmpty(updateRootPassword.getNewPassword())) {
      return new BaseResponse(ErrorCode.PASSWORD_IS_REQUIRED);
    }
    if (updateRootPassword.getNewPassword().length() < 5) {
      return new BaseResponse(ErrorCode.PASSWORD_DOES_NOT_SATISFY_REQUIREMENTS);
    }
    RootUser rootUser = getRootUser();
    if (!passwordEncoder
        .encode(rootUser.getPassword())
        .equals(updateRootPassword.getOldPassword())) {
      return new BaseResponse(ErrorCode.OLD_PASSWORD_NOT_MATCHING);
    }
    String rootUserName = rootUser.getUsername();
    rootUser.setPassword(passwordEncoder.encode(updateRootPassword.getNewPassword()));
    return updateUser(rootUser, rootUserName, request, response);
  }

  @Override
  public BaseResponse updateRootUsername(
      UpdateRootUsername updateRootUsername,
      HttpServletRequest request,
      HttpServletResponse response) {
    if (StringUtils.isEmpty(updateRootUsername.getUsername())) {
      return new BaseResponse(ErrorCode.USERNAME_IS_REQUIRED);
    }
    RootUser rootUser = getRootUser();
    String rootUserName = rootUser.getUsername();
    rootUser.setUsername(updateRootUsername.getUsername());
    return updateUser(rootUser, rootUserName, request, response);
  }

  @Override
  public BaseResponse createNewToken(NewTokenRequest request) {
    if (authStore.addToken(request)) {
      return new BaseResponse();
    }
    return new BaseResponse(ErrorCode.ERROR);
  }

  @Override
  public BaseResponse deleteToken(DeleteTokenRequest request) {
    if (authStore.deleteToken(request.getName())) {
      return new BaseResponse(ErrorCode.SUCCESS);
    }
    return new BaseResponse(ErrorCode.TOKEN_DOES_NOT_EXIST);
  }

  @Override
  public boolean isValidSessionId(String sessionId) {
    if (sessionId == null) {
      return false;
    }
    return authStore.isSessionExist(sessionId);
  }
}
