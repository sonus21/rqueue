/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.web.service.impl;

import static com.github.sonus21.rqueue.utils.HttpUtils.readUrl;

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.impl.MessageSweeper;
import com.github.sonus21.rqueue.core.impl.MessageSweeper.MessageDeleteRequest;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.exception.UnknownSwitchCase;
import com.github.sonus21.rqueue.models.MessageMoveResult;
import com.github.sonus21.rqueue.models.db.QueueConfig;
import com.github.sonus21.rqueue.models.enums.DataType;
import com.github.sonus21.rqueue.models.request.MessageMoveRequest;
import com.github.sonus21.rqueue.models.response.BooleanResponse;
import com.github.sonus21.rqueue.models.response.MessageMoveResponse;
import com.github.sonus21.rqueue.models.response.StringResponse;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.StringUtils;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.github.sonus21.rqueue.web.service.RqueueUtilityService;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.util.Pair;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class RqueueUtilityServiceImpl implements RqueueUtilityService {
  private final RqueueWebConfig rqueueWebConfig;
  private final RqueueStringDao rqueueStringDao;
  private final RqueueSystemConfigDao rqueueSystemConfigDao;
  private final RqueueMessageTemplate rqueueMessageTemplate;
  private final RqueueMessageMetadataService messageMetadataService;
  private final RqueueConfig rqueueConfig;
  private String latestVersion = "NA";
  private String releaseLink = "#";
  private long versionFetchTime = 0;

  @Autowired
  public RqueueUtilityServiceImpl(
      RqueueConfig rqueueConfig,
      RqueueWebConfig rqueueWebConfig,
      RqueueStringDao rqueueStringDao,
      RqueueSystemConfigDao rqueueSystemConfigDao,
      RqueueMessageTemplate rqueueMessageTemplate,
      RqueueMessageMetadataService messageMetadataService) {
    this.rqueueStringDao = rqueueStringDao;
    this.rqueueSystemConfigDao = rqueueSystemConfigDao;
    this.rqueueWebConfig = rqueueWebConfig;
    this.rqueueConfig = rqueueConfig;
    this.rqueueMessageTemplate = rqueueMessageTemplate;
    this.messageMetadataService = messageMetadataService;
  }

  @Override
  public BooleanResponse deleteMessage(String queueName, String id) {
    String queueConfigKey = rqueueConfig.getQueueConfigKey(queueName);
    QueueConfig queueConfig = rqueueSystemConfigDao.getQConfig(queueConfigKey, true);
    BooleanResponse booleanResponse = new BooleanResponse();
    if (queueConfig == null) {
      booleanResponse.setCode(1);
      booleanResponse.setMessage("Queue config not found!");
      return booleanResponse;
    }
    messageMetadataService.deleteMessage(queueName, id, Duration.ofDays(Constants.DAYS_IN_A_MONTH));
    booleanResponse.setValue(true);
    return booleanResponse;
  }

  private MessageMoveResponse moveMessageToZset(MessageMoveRequest messageMoveRequest) {
    String src = messageMoveRequest.getSrc();
    String dst = messageMoveRequest.getDst();
    int requestMessageCount = messageMoveRequest.getMessageCount(rqueueWebConfig);
    String newScore = (String) messageMoveRequest.getOthers().get("newScore");
    Boolean isFixedScore = (Boolean) messageMoveRequest.getOthers().get("fixedScore");
    long scoreInMilli = 0;
    boolean fixedScore = false;
    if (newScore != null) {
      scoreInMilli = Long.parseLong(newScore);
    }
    if (isFixedScore != null) {
      fixedScore = isFixedScore;
    }
    MessageMoveResult result;
    if (messageMoveRequest.getSrcType() == DataType.ZSET) {
      result =
          rqueueMessageTemplate.moveMessageZsetToZset(
              src, dst, requestMessageCount, scoreInMilli, fixedScore);
    } else {
      result =
          rqueueMessageTemplate.moveMessageListToZset(src, dst, requestMessageCount, scoreInMilli);
    }
    MessageMoveResponse response = new MessageMoveResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  private MessageMoveResponse moveMessageToList(MessageMoveRequest messageMoveRequest) {
    String src = messageMoveRequest.getSrc();
    String dst = messageMoveRequest.getDst();
    int requestMessageCount = messageMoveRequest.getMessageCount(rqueueWebConfig);
    MessageMoveResult result;
    if (messageMoveRequest.getSrcType() == DataType.ZSET) {
      result = rqueueMessageTemplate.moveMessageZsetToList(src, dst, requestMessageCount);
    } else {
      result = rqueueMessageTemplate.moveMessageListToList(src, dst, requestMessageCount);
    }
    MessageMoveResponse response = new MessageMoveResponse(result.getNumberOfMessages());
    response.setValue(result.isSuccess());
    return response;
  }

  @Override
  public MessageMoveResponse moveMessage(MessageMoveRequest messageMoveRequest) {
    String message = messageMoveRequest.validationMessage();
    if (!StringUtils.isEmpty(message)) {
      MessageMoveResponse transferResponse = new MessageMoveResponse();
      transferResponse.setCode(1);
      transferResponse.setMessage(message);
      return transferResponse;
    }
    DataType dstType = messageMoveRequest.getDstType();
    switch (dstType) {
      case ZSET:
        return moveMessageToZset(messageMoveRequest);
      case LIST:
        return moveMessageToList(messageMoveRequest);
      default:
        throw new UnknownSwitchCase(dstType.name());
    }
  }

  @Override
  public BooleanResponse makeEmpty(String queueName, String dataName) {
    org.springframework.data.redis.connection.DataType type = rqueueStringDao.type(dataName);
    // empty data structure
    if (type == null || type == org.springframework.data.redis.connection.DataType.NONE) {
      return new BooleanResponse(true);
    }
    return new BooleanResponse(
        MessageSweeper.getInstance(rqueueConfig, rqueueMessageTemplate, messageMetadataService)
            .deleteMessage(
                MessageDeleteRequest.builder()
                    .dataName(dataName)
                    .queueName(queueName)
                    .dataType(type)
                    .build()));
  }

  private boolean shouldFetchVersionDetail() {
    if (!rqueueConfig.isLatestVersionCheckEnabled()) {
      return false;
    }
    return System.currentTimeMillis() - versionFetchTime > Constants.MILLIS_IN_A_DAY;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Pair<String, String> getLatestVersion() {
    if (shouldFetchVersionDetail()) {
      Map<String, Object> response =
          readUrl(rqueueConfig, Constants.GITHUB_API_FOR_LATEST_RELEASE, LinkedHashMap.class);
      if (response != null) {
        String tagName = (String) response.get("tag_name");
        if (tagName != null && !tagName.isEmpty()) {
          if (Character.toLowerCase(tagName.charAt(0)) == 'v') {
            releaseLink = (String) response.get("html_url");
            latestVersion = tagName.substring(1);
            versionFetchTime = System.currentTimeMillis();
          }
        }
      }
    }
    return Pair.of(releaseLink, latestVersion);
  }

  @Override
  public StringResponse getDataType(String name) {
    return new StringResponse(
        DataType.convertDataType(rqueueStringDao.type(StringUtils.clean(name))).name());
  }

  @Override
  public Mono<BooleanResponse> makeEmptyReactive(String queueName, String datasetName) {
    return Mono.just(makeEmpty(queueName, datasetName));
  }

  @Override
  public Mono<BooleanResponse> deleteReactiveMessage(String queueName, String messageId) {
    return Mono.just(deleteMessage(queueName, messageId));
  }

  @Override
  public Mono<StringResponse> getReactiveDataType(String name) {
    return Mono.just(getDataType(name));
  }

  @Override
  public Mono<MessageMoveResponse> moveReactiveMessage(MessageMoveRequest request) {
    return Mono.just(moveMessage(request));
  }
}
