/*
 * Copyright (c) 2021-2023 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.sonus21.rqueue.annotation.RqueueHandler;
import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.test.dto.UserBanned;
import com.github.sonus21.rqueue.test.service.ConsumedMessageStore;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@RqueueListener(value = "${user.banned.queue.name}", active = "${user.banned.queue.active}")
@Slf4j
@Component
@RequiredArgsConstructor
public class UserBannedMessageListener {

  @NonNull
  private final ConsumedMessageStore consumedMessageStore;

  @Value("${user.banned.queue.name}")
  private String userBannedQueue;

  @RqueueHandler
  public void handleMessage1(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessage1", userBannedQueue);
    log.info("handleMessage1 {}", userBanned);
  }

  @RqueueHandler
  public void handleMessage2(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessage2", userBannedQueue);
    log.info("handleMessage2 {}", userBanned);
  }

  @RqueueHandler(primary = true)
  public void handleMessagePrimary(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleMessagePrimary", userBannedQueue);
    log.info("handleMessagePrimary {}", userBanned);
  }

  @RqueueHandler
  public void handleUserBanned(UserBanned userBanned) throws JsonProcessingException {
    consumedMessageStore.save(userBanned, "handleUserBanned", userBannedQueue);
    log.info("handleUserBanned {}", userBanned);
  }
}
