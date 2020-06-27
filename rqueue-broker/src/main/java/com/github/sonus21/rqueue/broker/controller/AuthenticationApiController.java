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

package com.github.sonus21.rqueue.broker.controller;

import com.github.sonus21.rqueue.broker.models.request.UpdateRootPassword;
import com.github.sonus21.rqueue.broker.models.request.UpdateRootUsername;
import com.github.sonus21.rqueue.broker.models.request.UsernamePassword;
import com.github.sonus21.rqueue.broker.service.AuthenticationService;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping
public class AuthenticationController {
  private final AuthenticationService authenticationService;

  @Autowired
  public AuthenticationController(AuthenticationService authenticationService) {

    this.authenticationService = authenticationService;
  }

  @PostMapping("login")
  public BaseResponse login(
      @Valid @RequestBody UsernamePassword usernamePassword, HttpServletResponse response) {
    return authenticationService.login(usernamePassword, response);
  }

  @DeleteMapping("logout")
  public BaseResponse logout(HttpServletRequest request, HttpServletResponse response) {
    return authenticationService.logout(request, response);
  }

  @PutMapping("root-user-name")
  public BaseResponse updateRootUserName(
      @Valid UpdateRootUsername username, HttpServletRequest request) {
    return authenticationService.updateRootUsername(username, request);
  }

  @PutMapping("root-user-password")
  public BaseResponse updateRootUserPassword(
      @Valid UpdateRootPassword password, HttpServletRequest request) {
    return authenticationService.updateRootPassword(password, request);
  }
}
