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

package com.github.sonus21.rqueue.broker.aop;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.broker.service.AuthenticationService;
import com.github.sonus21.rqueue.broker.service.utils.AuthUtils;
import com.github.sonus21.rqueue.exception.ErrorCode;
import com.github.sonus21.rqueue.models.response.BaseResponse;
import com.github.sonus21.rqueue.utils.StringUtils;
import java.io.IOException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.servlet.ModelAndView;

@Slf4j
public class AuthenticationInterceptor implements HandlerInterceptor {
  @Autowired private AuthenticationService authenticationService;
  @Autowired ObjectMapper objectMapper;

  private void sendLoginDetails(HttpServletRequest request, HttpServletResponse response)
      throws IOException {
    String requestContentType = request.getContentType();
    MediaType mediaType = MediaType.APPLICATION_JSON;
    try {
      mediaType = MediaType.parseMediaType(requestContentType);
    } catch (Exception e) {
      log.warn("Invalid mime type", e);
    }
    if (mediaType.includes(MediaType.APPLICATION_JSON)) {
      BaseResponse baseResponse = new BaseResponse(ErrorCode.UNAUTHORIZED_ACCESS);
      String data = objectMapper.writeValueAsString(baseResponse);
      response.getWriter().write(data);
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
      response.flushBuffer();
    } else {
      response.sendRedirect(AuthUtils.LOGIN_PAGE_URL);
    }
  }

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    log.info("[preHandle] HTTP: {}, URL: {} ", request.getMethod(), request.getRequestURI());
    String token = AuthUtils.getToken(request);
    boolean authorized;
    if (StringUtils.isEmpty(token)) {
      String sessionId = AuthUtils.getSessionId(request);
      authorized = authenticationService.isValidSessionId(sessionId);
    } else {
      authorized = authenticationService.isValidToken(token);
    }
    if (authorized) {
      return true;
    }
    sendLoginDetails(request, response);
    return false;
  }

  @Override
  public void postHandle(
      HttpServletRequest request,
      HttpServletResponse response,
      Object handler,
      ModelAndView modelAndView)
      throws Exception {
    log.info("[postHandle] HTTP: {}, URL: {} ", request.getMethod(), request.getRequestURI());
  }
}
