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

package com.github.sonus21.rqueue.exception;

import com.fasterxml.jackson.annotation.JsonValue;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public enum ErrorCode {
  SUCCESS(0, "Success"),
  ERROR(1, "Error"),
  VALIDATION_ERROR(2, "Validation error"),
  CONCURRENT_REQUEST(101, "Concurrent request"),
  TOPIC_LIST_CAN_NOT_BE_EMPTY(102, "Topic list cannot be empty"),
  TOPIC_ALREADY_EXIST(103, "Topic already exist."),
  DUPLICATE_TOPIC(104, "Duplicate topic"),
  TOPIC_DOES_NOT_EXIST(105, "Topic does not exist"),
  DUPLICATE_SUBSCRIPTION(106, "Already subscribed"),
  SUBSCRIPTION_DOES_NOT_EXIST(107, "Already subscribed"),
  NO_MESSAGE_PROVIDED(108, "No message provided"),
  INVALID_USERNAME_OR_PASSWORD(109, "Invalid username or password"),
  TOKEN_DOES_NOT_EXIST(110, "Token does not exist"),
  USERNAME_IS_REQUIRED(111, "Username is required"),
  PASSWORD_IS_REQUIRED(112, "Password is required"),
  PASSWORD_DOES_NOT_SATISFY_REQUIREMENTS(113, "Password does not satisfy requirement"),
  UNAUTHORIZED_ACCESS(114, "Unauthorized access(Either login or provide Authorization headers)"),
  OLD_PASSWORD_NOT_MATCHING(115, "Old password not matching"),
  QUEUE_ALREADY_EXIST(116, "Queue already exist"),
  QUEUE_DOES_NOT_EXIST(117, "Queue does not exist"),
  QUEUE_UPDATE_PARAMETERS_MISSING(118, "Queue update parameters are missing"),
  NOTHING_TO_UPDATE(119, "nothing to be updated"),
  INVALID_QUEUE_PRIORITY(120, "Invalid queue priority");
  private int code;
  private String message;

  @JsonValue
  public int getCode() {
    return code;
  }

  public String getMessage() {
    return message;
  }
}
