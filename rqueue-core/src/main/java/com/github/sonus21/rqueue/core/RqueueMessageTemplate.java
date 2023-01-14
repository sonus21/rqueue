/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.rqueue.core;

import com.github.sonus21.rqueue.models.MessageMoveResult;
import java.util.List;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations.TypedTuple;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Rqueue message template is used to send messages
 *
 * <p>Methods from this class should not be used in the application code, these methods are bound
 * to change as new and new features are added to Rqueue.
 */
public interface RqueueMessageTemplate {

  List<RqueueMessage> pop(
      String queueName,
      String processingQueueName,
      String processingChannelName,
      long visibilityTimeout,
      int count);

  Long addMessageWithDelay(
      String scheduleQueueName, String scheduleQueueChannelName, RqueueMessage rqueueMessage);

  void moveMessageWithDelay(
      String srcZsetName, String tgtZsetName, RqueueMessage src, RqueueMessage tgt, long delay);

  void moveMessage(String srcZsetName, String tgtListName, RqueueMessage src, RqueueMessage tgt);

  Long addMessage(String queueName, RqueueMessage rqueueMessage);

  Boolean addToZset(String zsetName, RqueueMessage rqueueMessage, long score);

  List<RqueueMessage> getAllMessages(
      String queueName, String processingQueueName, String scheduleQueueName);

  Long getScore(String zsetName, RqueueMessage message);

  boolean addScore(String zsetName, RqueueMessage message, long delta);

  MessageMoveResult moveMessageListToList(
      String srcQueueName, String dstQueueName, int numberOfMessage);

  MessageMoveResult moveMessageZsetToList(
      String sourceZset, String destinationList, int maxMessage);

  MessageMoveResult moveMessageListToZset(
      String sourceList, String destinationZset, int maxMessage, long score);

  MessageMoveResult moveMessageZsetToZset(
      String sourceZset, String destinationZset, int maxMessage, long newScore, boolean fixedScore);

  List<RqueueMessage> readFromZset(String name, long start, long end);

  List<RqueueMessage> readFromList(String name, long start, long end);

  RedisTemplate<String, RqueueMessage> getTemplate();

  Long removeElementFromZset(String zsetName, RqueueMessage rqueueMessage);

  List<TypedTuple<RqueueMessage>> readFromZsetWithScore(String name, long start, long end);

  Long scheduleMessage(
      String queueName, String messageId, RqueueMessage rqueueMessage, Long expiryInSeconds);

  boolean renameCollection(String srcName, String tgtName);

  boolean renameCollections(List<String> srcNames, List<String> tgtNames);

  void deleteCollection(String name);

  Mono<Long> addReactiveMessage(String queueName, RqueueMessage rqueueMessage);

  Flux<Long> addReactiveMessageWithDelay(
      String scheduledQueueName, String scheduledQueueChannelName, RqueueMessage rqueueMessage);
}
