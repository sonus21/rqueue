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

package com.github.sonus21.rqueue.listener;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueRedisTemplate;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageTemplate;
import com.github.sonus21.rqueue.core.context.Context;
import com.github.sonus21.rqueue.core.context.DefaultContext;
import com.github.sonus21.rqueue.core.middleware.ContextMiddleware;
import com.github.sonus21.rqueue.core.middleware.Middleware;
import com.github.sonus21.rqueue.core.middleware.PermissionMiddleware;
import com.github.sonus21.rqueue.core.middleware.ProfilerMiddleware;
import com.github.sonus21.rqueue.core.middleware.RateLimiterMiddleware;
import com.github.sonus21.rqueue.core.support.MessageProcessor;
import com.github.sonus21.rqueue.core.support.RqueueMessageUtils;
import com.github.sonus21.rqueue.dao.RqueueJobDao;
import com.github.sonus21.rqueue.dao.RqueueStringDao;
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.google.common.util.concurrent.RateLimiter;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueMiddlewareTest extends TestBase {

  private final RqueueMessageListenerContainer container =
      mock(RqueueMessageListenerContainer.class);
  private final WeakReference<RqueueMessageListenerContainer> containerWeakReference =
      new WeakReference<>(container);
  private final RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
  private final RqueueConfig rqueueConfig = mock(RqueueConfig.class);
  private final RqueueMessageMetadataService rqueueMessageMetadataService =
      mock(RqueueMessageMetadataService.class);
  private final RqueueRedisTemplate<String> stringRqueueRedisTemplate =
      mock(RqueueRedisTemplate.class);
  private final RqueueJobDao rqueueJobDao = mock(RqueueJobDao.class);
  private final RqueueStringDao rqueueStringDao = mock(RqueueStringDao.class);
  private final RqueueMessageTemplate messageTemplate = mock(RqueueMessageTemplate.class);
  private final RqueueMessageHandler messageHandler = mock(RqueueMessageHandler.class);
  private final Semaphore semaphore = new Semaphore(100);
  private final TaskExecutionBackOff taskBackOff = new FixedTaskExecutionBackOff();
  private final ApplicationEventPublisher applicationEventPublisher =
      mock(ApplicationEventPublisher.class);
  private final RqueueSystemConfigDao rqueueSystemConfigDao = mock(RqueueSystemConfigDao.class);
  private final String queueName = "test-queue";
  private final Object payload = "test message";
  private final MessageConverter messageConverter = new GenericMessageConverter();
  private RqueueMessage rqueueMessage = new RqueueMessage();
  private PostProcessingHandler postProcessingHandler;
  private MessageMetadata defaultMessageMetadata;

  @BeforeEach
  public void init() throws IllegalAccessException {

    rqueueMessage =
        RqueueMessageUtils.buildMessage(
            messageConverter,
            queueName,
            null,
            payload,
            null,
            null,
            RqueueMessageHeaders.emptyMessageHeaders());
    defaultMessageMetadata = new MessageMetadata(rqueueMessage, MessageStatus.ENQUEUED);
    MessageProcessorHandler messageProcessorHandler =
        new MessageProcessorHandler(
            null,
            new MessageProcessor() {
              @Override
              public boolean process(Job job) {
                return false;
              }
            },
            new MessageProcessor() {
              @Override
              public boolean process(Job job) {
                return false;
              }
            },
            null);
    postProcessingHandler =
        new PostProcessingHandler(
            rqueueConfig,
            rqueueWebConfig,
            applicationEventPublisher,
            messageTemplate,
            taskBackOff,
            messageProcessorHandler,
            rqueueSystemConfigDao);
    doReturn(rqueueMessageMetadataService).when(container).rqueueMessageMetadataService();
    doReturn(true).when(container).isQueueActive(anyString());
    doReturn(
        new MessageProcessor() {
          @Override
          public boolean process(Job job) {
            return true;
          }
        })
        .when(container)
        .getPreExecutionMessageProcessor();
    doReturn(messageHandler).when(container).getRqueueMessageHandler();
    doReturn(messageConverter).when(messageHandler).getMessageConverter();
    doReturn(rqueueJobDao).when(container).rqueueJobDao();
    doReturn(messageTemplate).when(container).getRqueueMessageTemplate();
    doReturn(rqueueStringDao).when(container).rqueueStringDao();
    doReturn(1).when(rqueueConfig).getRetryPerPoll();
    doReturn("test-job::" + UUID.randomUUID().toString()).when(rqueueConfig).getJobId();
  }

  @Test
  void logMiddleware() {
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    doReturn(newArrayList(logMiddleware)).when(container).getMiddleWares();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        containerWeakReference,
        rqueueConfig,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        semaphore)
        .run();
    verify(messageHandler, times(1)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(1, logMiddleware.jobs.size());
  }

  @Test
  void logAndContextMiddleware() {
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    TestContextMiddleware contextMiddleware = new TestContextMiddleware();
    doReturn(newArrayList(logMiddleware, contextMiddleware)).when(container).getMiddleWares();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        containerWeakReference,
        rqueueConfig,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        semaphore)
        .run();
    verify(messageHandler, times(1)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(1, logMiddleware.jobs.size());
    assertEquals(1, contextMiddleware.jobs.size());
    assertNotNull(logMiddleware.jobs.get(0).getId());
    assertEquals(
        logMiddleware.jobs.get(0).getId(),
        logMiddleware.jobs.get(0).getContext().getValue("JobId"));
  }

  @Test
  void logContextAndPermissionMiddleware() {
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    TestContextMiddleware contextMiddleware = new TestContextMiddleware();
    TestPermissionMiddleware permissionMiddleware = new TestPermissionMiddleware();
    doReturn(newArrayList(logMiddleware, contextMiddleware, permissionMiddleware))
        .when(container)
        .getMiddleWares();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    RqueueMessage rqueueMessage1 =
        RqueueMessageUtils.buildMessage(
            messageConverter,
            null,
            queueName,
            payload,
            null,
            null,
            RqueueMessageHeaders.emptyMessageHeaders());
    permissionMiddleware.allowedMessageIds.add(rqueueMessage.getId());
    MessageMetadata messageMetadata = new MessageMetadata(rqueueMessage1, MessageStatus.ENQUEUED);
    doAnswer(
        invocation -> {
          RqueueMessage message = invocation.getArgument(0);
          if (message.getId().equals(defaultMessageMetadata.getRqueueMessage().getId())) {
            return defaultMessageMetadata;
          }
          return messageMetadata;
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        containerWeakReference,
        rqueueConfig,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        semaphore)
        .run();

    new RqueueExecutor(
        containerWeakReference,
        rqueueConfig,
        postProcessingHandler,
        rqueueMessage1,
        queueDetail,
        semaphore)
        .run();

    verify(messageHandler, times(1)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(2, logMiddleware.jobs.size());
    assertEquals(2, contextMiddleware.jobs.size());
    assertEquals(2, permissionMiddleware.jobs.size());
    assertEquals(1, permissionMiddleware.declined.size());
    assertEquals(permissionMiddleware.declined.get(0).getMessageId(), rqueueMessage1.getId());
  }

  @Test
  void logAndRateLimiterMiddleware() throws TimedOutException {
    int permissionRate = 10;
    RateLimiter rateLimiter = RateLimiter.create(permissionRate);
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    TestRateLimiter testRateLimiter = new TestRateLimiter(rateLimiter);
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    Map<String, MessageMetadata> map = new HashMap<>();
    List<RqueueMessage> messages = new ArrayList<>();
    int jobCount = 100;
    for (int i = 0; i < jobCount; i++) {
      RqueueMessage message =
          RqueueMessageUtils.buildMessage(
              messageConverter,
              queueName,
              null,
              payload,
              null,
              null,
              RqueueMessageHeaders.emptyMessageHeaders());
      MessageMetadata messageMetadata = new MessageMetadata(message, MessageStatus.ENQUEUED);
      messages.add(message);
      map.put(message.getId(), messageMetadata);
    }
    doReturn(newArrayList(logMiddleware, testRateLimiter)).when(container).getMiddleWares();
    doAnswer(
        invocation -> {
          RqueueMessage message = invocation.getArgument(0);
          return map.get(message.getId());
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    doAnswer(
        invocation -> {
          TimeoutUtils.sleep(randomTime(20, 100));
          return null;
        })
        .when(messageHandler)
        .handleMessage(any());
    Executor executor = Executors.newSingleThreadExecutor();
    long startTime = System.currentTimeMillis();
    for (RqueueMessage message : messages) {
      executor.execute(
          new RqueueExecutor(
              containerWeakReference,
              rqueueConfig,
              postProcessingHandler,
              message,
              queueDetail,
              semaphore));
    }
    TimeoutUtils.waitFor(() -> testRateLimiter.jobs.size() == jobCount, "all jobs to proceed");
    long endTime = System.currentTimeMillis();
    // we need to round since rate is at second resolution and execution in millis so we round this
    // to next second
    int maxJobsExecution =
        (int) (permissionRate * Math.ceil(1.0 * (endTime - startTime) / Constants.ONE_MILLI));
    int minimumRejection = jobCount - maxJobsExecution;
    assertTrue(testRateLimiter.throttled.size() >= minimumRejection);
    verify(messageHandler, times(testRateLimiter.jobs.size() - testRateLimiter.throttled.size()))
        .handleMessage(any());
  }

  @Test
  void logAndProfilerMiddleware() {
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    TestProfilerMiddleware profilerMiddleware = new TestProfilerMiddleware();
    doReturn(newArrayList(logMiddleware, profilerMiddleware)).when(container).getMiddleWares();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        containerWeakReference,
        rqueueConfig,
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        semaphore)
        .run();
    verify(messageHandler, times(1)).handleMessage(any());
    verify(messageTemplate, times(1))
        .removeElementFromZset(queueDetail.getProcessingQueueName(), rqueueMessage);
    assertEquals(1, logMiddleware.jobs.size());
    assertEquals(1, profilerMiddleware.jobs.size());
    assertEquals(rqueueMessage.getId(), profilerMiddleware.jobs.get(0).getMessageId());
    assertEquals(rqueueMessage.getId(), logMiddleware.jobs.get(0).getMessageId());
  }

  static class TestContextMiddleware implements ContextMiddleware {

    List<Job> jobs = new ArrayList<>();

    @Override
    public Context getContext(Job job) {
      this.jobs.add(job);
      Context context = DefaultContext.EMPTY;
      return DefaultContext.withValue(context, "JobId", job.getId());
    }
  }

  static class TestProfilerMiddleware extends ProfilerMiddleware {

    List<Job> jobs = new ArrayList<>();

    @Override
    protected void report(Job job, Duration duration) {
      super.report(job, duration);
      this.jobs.add(job);
    }
  }

  @Slf4j
  static class TestLogMiddleware implements Middleware {

    List<Job> jobs = new ArrayList<>();

    @Override
    public void handle(Job job, Callable<Void> next) throws Exception {
      log.info("Queue: {}, JobId: {}", job.getRqueueMessage().getQueueName(), job.getId());
      this.jobs.add(job);
      next.call();
    }
  }

  static class TestPermissionMiddleware implements PermissionMiddleware {

    List<String> allowedMessageIds = new ArrayList<>();
    List<Job> jobs = new ArrayList<>();
    List<Job> declined = new ArrayList<>();

    @Override
    public boolean hasPermission(Job job) {
      this.jobs.add(job);
      if (allowedMessageIds.contains(job.getMessageId())) {
        return true;
      }
      this.declined.add(job);
      return false;
    }
  }

  static class TestRateLimiter implements RateLimiterMiddleware {

    final RateLimiter rateLimiter;
    List<Job> jobs = Collections.synchronizedList(new ArrayList<>());
    List<Job> throttled = Collections.synchronizedList(new ArrayList<>());

    TestRateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
    }

    @Override
    public boolean isThrottled(Job job) {
      jobs.add(job);
      boolean acquired = rateLimiter.tryAcquire();
      if (!acquired) {
        throttled.add(job);
      }
      return !acquired;
    }
  }
}
