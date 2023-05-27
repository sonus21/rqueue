package com.github.sonus21.rqueue.core;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.core.ScheduledQueueMessageSchedulerTest.TestScheduledQueueMessageScheduler;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.models.event.RqueueBootstrapEvent;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.ThreadUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import com.github.sonus21.test.TestTaskScheduler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sonus21.rqueue.utils.TimeoutUtils.sleep;
import static com.github.sonus21.rqueue.utils.TimeoutUtils.waitFor;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@CoreUnitTest
@Slf4j
class RedisAndNormalSchedulingTest extends TestBase {

  private final String slowQueue = "slow-queue";
  private final String fastQueue = "fast-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private final QueueDetail fastQueueDetail = TestUtils.createQueueDetail(fastQueue);
  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueConfig rqueueConfig;
  @Mock
  private RedisTemplate<String, Long> redisTemplate;
  @Mock
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  @InjectMocks
  private TestScheduledQueueMessageScheduler messageScheduler;

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(fastQueueDetail);
    EndpointRegistry.register(slowQueueDetail);
  }

  @Test
  void redisAndNormalScheduling() throws Exception {
    TestTaskScheduler scheduler = new TestTaskScheduler(2);
    AtomicBoolean startGenerateMessage = new AtomicBoolean(false);
    AtomicBoolean generateMessage = new AtomicBoolean(true);
    long totalTime = 2000L;
    long minDelay = 10L;
    //25% buffer due to short polling intervals, IN CI it runs slowly
    double buffer = 0.25;
    String channelName = messageScheduler.getChannelName(slowQueue);
    doReturn(1).when(rqueueSchedulerConfig).getScheduledMessageThreadPoolSize();
    doReturn(true).when(rqueueSchedulerConfig).isAutoStart();
    doReturn(true).when(rqueueSchedulerConfig).isEnabled();
    doReturn(true).when(rqueueSchedulerConfig).isRedisEnabled();
    doReturn(3000L).when(rqueueSchedulerConfig).getScheduledMessageTimeIntervalInMilli();
    doReturn(100L).when(rqueueSchedulerConfig).getMaxMessageCount();
    doReturn(minDelay).when(rqueueSchedulerConfig).minMessageMoveDelay();

    Runnable messageGenerator = () -> {
      int counter = 0;
      while (generateMessage.get()) {
        if (startGenerateMessage.get()) {
          counter += 1;
          byte[] currentTime = String.valueOf(System.currentTimeMillis()).getBytes();
          messageScheduler.redisScheduleTriggerHandler.messageListener.onMessage(
              new DefaultMessage(channelName.getBytes(), currentTime), null);
        }
        // each message enqueue would lead to one event, so ~250 QPS
        TimeoutUtils.sleep(4L);
      }
      System.out.println("Exiting sent " + counter + "  messages ");
    };
    scheduler.submit(messageGenerator);
    AtomicInteger counter = new AtomicInteger(0);
    doAnswer(invocation -> {
      counter.incrementAndGet();
      sleep(5);
      return System.currentTimeMillis() - Constants.DEFAULT_SCRIPT_EXECUTION_TIME;
    }).when(redisTemplate).execute(any(RedisCallback.class));

    try (MockedStatic<ThreadUtils> threadUtils = Mockito.mockStatic(ThreadUtils.class)) {
      threadUtils.when(() -> ThreadUtils.createTaskScheduler(1, "scheduledQueueMsgScheduler-", 60))
          .thenReturn(scheduler);
      messageScheduler.onApplicationEvent(new RqueueBootstrapEvent("Test", true));
      waitFor(() -> scheduler.submittedTasks() >= 2, "one start task to be submitted");
      startGenerateMessage.set(true);
      // sleep for 2 seconds -> this should send around 500 events
      TimeoutUtils.sleep(totalTime);
      generateMessage.set(false); // disable Redis pub/sub event
      TimeoutUtils.sleep(100);
      int expectedMessageMoveCalls = (int) ((totalTime / minDelay) * (1 - buffer));
      messageScheduler.destroy();
      int ranJobs = counter.get();
      int jobCount = scheduler.submittedTasks();
      log.info("Expected Job={}, Ran Jobs={}, Submitted Jobs={}",
          expectedMessageMoveCalls, ranJobs, jobCount);
      assertTrue(jobCount >= ranJobs);
      assertTrue(jobCount >= expectedMessageMoveCalls);
    }
  }
}
