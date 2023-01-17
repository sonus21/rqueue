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

package com.github.sonus21.rqueue.listener;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.common.RqueueLockManager;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueWebConfig;
import com.github.sonus21.rqueue.converter.GenericMessageConverter;
import com.github.sonus21.rqueue.core.Job;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
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
import com.github.sonus21.rqueue.dao.RqueueSystemConfigDao;
import com.github.sonus21.rqueue.exception.TimedOutException;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.models.db.MessageMetadata;
import com.github.sonus21.rqueue.models.enums.MessageStatus;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.rqueue.utils.backoff.FixedTaskExecutionBackOff;
import com.github.sonus21.rqueue.utils.backoff.TaskExecutionBackOff;
import com.github.sonus21.rqueue.web.service.RqueueMessageMetadataService;
import com.google.common.util.concurrent.RateLimiter;
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
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.messaging.converter.MessageConverter;

@CoreUnitTest
class RqueueMiddlewareTest extends TestBase {

  private final QueueThreadPool queueThreadPool = new QueueThreadPool(null, true, 100);
  private final RqueueWebConfig rqueueWebConfig = new RqueueWebConfig();
  private final TaskExecutionBackOff taskBackOff = new FixedTaskExecutionBackOff();
  private final String queueName = "test-queue";
  private final Object payload = "test message";
  private final MessageConverter messageConverter = new GenericMessageConverter();
  @Mock
  private RqueueLockManager rqueueLockManager;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RqueueMessageMetadataService rqueueMessageMetadataService;
  @Mock
  private RqueueBeanProvider rqueueBeanProvider;
  @Mock
  private QueueStateMgr queueStateMgr;
  @Mock
  private RqueueJobDao rqueueJobDao;
  @Mock
  private RqueueMessageTemplate messageTemplate;
  @Mock
  private RqueueMessageHandler messageHandler;
  @Mock
  private RqueueSystemConfigDao rqueueSystemConfigDao;
  @Mock
  private ApplicationEventPublisher applicationEventPublisher;
  private RqueueMessage rqueueMessage = new RqueueMessage();
  private PostProcessingHandler postProcessingHandler;
  private MessageMetadata defaultMessageMetadata;

  @BeforeEach
  public void init() throws IllegalAccessException {
    MockitoAnnotations.openMocks(this);
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
            null, job -> true, job -> true, null);
    postProcessingHandler =
        new PostProcessingHandler(
            rqueueWebConfig,
            applicationEventPublisher,
            messageTemplate,
            taskBackOff,
            messageProcessorHandler,
            rqueueSystemConfigDao);
    doReturn(rqueueMessageMetadataService)
        .when(rqueueBeanProvider)
        .getRqueueMessageMetadataService();
    doReturn(true).when(queueStateMgr).isQueueActive(anyString());
    doReturn((MessageProcessor) job -> true).when(rqueueBeanProvider)
        .getPreExecutionMessageProcessor();
    doReturn(messageHandler).when(rqueueBeanProvider).getRqueueMessageHandler();
    doReturn(messageConverter).when(messageHandler).getMessageConverter();
    doReturn(rqueueJobDao).when(rqueueBeanProvider).getRqueueJobDao();
    doReturn(rqueueConfig).when(rqueueBeanProvider).getRqueueConfig();
    doReturn(1).when(rqueueConfig).getRetryPerPoll();
    doReturn("test-job::" + UUID.randomUUID().toString()).when(rqueueConfig).getJobId();
  }

  @Test
  void logMiddleware() {
    TestLogMiddleware logMiddleware = new TestLogMiddleware();
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        Collections.singletonList(logMiddleware),
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
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
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        newArrayList(logMiddleware, contextMiddleware),
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
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
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
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
        rqueueBeanProvider,
        queueStateMgr,
        newArrayList(logMiddleware, contextMiddleware, permissionMiddleware),
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
        .run();

    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        newArrayList(logMiddleware, contextMiddleware, permissionMiddleware),
        postProcessingHandler,
        rqueueMessage1,
        queueDetail,
        queueThreadPool)
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
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
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
    doAnswer(
        invocation -> {
          RqueueMessage message = invocation.getArgument(0);
          return map.get(message.getId());
        })
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    doAnswer(
        invocation -> defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .get(any());
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
              rqueueBeanProvider,
              queueStateMgr,
              newArrayList(logMiddleware, testRateLimiter),
              postProcessingHandler,
              message,
              queueDetail,
              queueThreadPool));
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
    QueueDetail queueDetail = TestUtils.createQueueDetail(queueName);
    doReturn(rqueueLockManager).when(rqueueBeanProvider).getRqueueLockManager();
    doReturn(true).when(rqueueLockManager).acquireLock(anyString(), any(), any());
    doReturn(defaultMessageMetadata).when(rqueueMessageMetadataService)
        .get(defaultMessageMetadata.getId());
    doReturn(defaultMessageMetadata)
        .when(rqueueMessageMetadataService)
        .getOrCreateMessageMetadata(any());
    new RqueueExecutor(
        rqueueBeanProvider,
        queueStateMgr,
        newArrayList(logMiddleware, profilerMiddleware),
        postProcessingHandler,
        rqueueMessage,
        queueDetail,
        queueThreadPool)
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
