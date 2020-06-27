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

package com.github.sonus21.rqueue.broker.service;

import com.github.sonus21.rqueue.broker.models.request.DeleteTokenRequest;
import com.github.sonus21.rqueue.broker.models.request.NewTokenRequest;
import com.github.sonus21.rqueue.broker.models.request.UpdateRootPassword;
import com.github.sonus21.rqueue.broker.models.request.UpdateRootUsername;
import com.github.sonus21.rqueue.broker.models.request.UsernamePassword;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public interface AuthenticationService {
  boolean isValidToken(String token);

  BaseResponse login(UsernamePassword usernamePassword, HttpServletResponse response);

  BaseResponse logout(HttpServletRequest request, HttpServletResponse response);

  BaseResponse updateRootPassword(
      UpdateRootPassword updateRootPassword,
      HttpServletRequest request,
      HttpServletResponse response);

  BaseResponse updateRootUsername(
      UpdateRootUsername updateRootUsername,
      HttpServletRequest request,
      HttpServletResponse response);

  BaseResponse createNewToken(NewTokenRequest request);

  BaseResponse deleteToken(DeleteTokenRequest request);

  boolean isValidSessionId(String value);
}
