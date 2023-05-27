package com.github.sonus21.rqueue.core;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.TestUtils;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.data.redis.connection.DefaultMessage;
import org.springframework.data.redis.connection.MessageListener;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@CoreUnitTest
@Slf4j
class RedisScheduleTriggerHandlerTest extends TestBase {

  private final ExecutorService executor = Executors.newFixedThreadPool(1);
  private final String slowQueue = "slow-queue";
  private final QueueDetail slowQueueDetail = TestUtils.createQueueDetail(slowQueue);
  private Scheduler scheduler;
  @Mock
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Mock
  private RqueueRedisListenerContainerFactory rqueueRedisListenerContainerFactory;
  private RedisScheduleTriggerHandler redisScheduleTriggerHandler;
  private long runningTime = 0;


  class Task implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      log.info("Running");
      TimeoutUtils.sleep(runningTime);
      return null;
    }
  }

  class Scheduler implements Function<String, Future<?>> {

    AtomicInteger counter = new AtomicInteger(0);

    @Override
    public Future<?> apply(String s) {
      counter.incrementAndGet();
      return executor.submit(new Task());
    }
  }

  @BeforeEach
  public void init() {
    MockitoAnnotations.openMocks(this);
    EndpointRegistry.delete();
    EndpointRegistry.register(slowQueueDetail);
    scheduler = new Scheduler();
    redisScheduleTriggerHandler = new RedisScheduleTriggerHandler(log,
        rqueueRedisListenerContainerFactory, rqueueSchedulerConfig,
        List.of(slowQueue), scheduler,
        (e) -> {
          return slowQueueDetail.getScheduledQueueChannelName();
        });
    redisScheduleTriggerHandler.initialize();
    redisScheduleTriggerHandler.startQueue(slowQueue);
  }

  @Test
  void onMessageListenerTest() throws Exception {
    MessageListener messageListener = redisScheduleTriggerHandler.messageListener;
    // invalid channel
    messageListener.onMessage(new DefaultMessage(slowQueue.getBytes(), "312".getBytes()), null);
    TimeoutUtils.sleep(50);
    assertEquals(0, scheduler.counter.get());

    // invalid body
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            "sss".getBytes()), null);
    TimeoutUtils.sleep(50);
    assertEquals(0, scheduler.counter.get());

    // future time
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis() + 500).getBytes()), null);
    TimeoutUtils.sleep(50);
    assertEquals(0, scheduler.counter.get());

    // both are correct
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()), null);
    assertEquals(1, scheduler.counter.get());
    // let it run
    TimeoutUtils.sleep(100);

    // send another message while one is running
    runningTime = 200;
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()), null);
    assertEquals(2, scheduler.counter.get());
    TimeoutUtils.sleep(100);

    // this should be rejected as another task is already running
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()), null);
    TimeoutUtils.sleep(50);
    assertEquals(2, scheduler.counter.get());
    TimeoutUtils.sleep(100);

    // this should success
    long lastRunTime = System.currentTimeMillis();
    messageListener.onMessage(
        new DefaultMessage(slowQueueDetail.getScheduledQueueChannelName().getBytes(),
            String.valueOf(System.currentTimeMillis()).getBytes()), null);
    assertEquals(3, scheduler.counter.get());
    verify(rqueueRedisListenerContainerFactory, times(1)).addMessageListener(any(), any());
    doReturn(400L).when(rqueueSchedulerConfig).getTerminationWaitTime();

    assertEquals(1, redisScheduleTriggerHandler.queueNameToFuture.size());
    assertEquals(1, redisScheduleTriggerHandler.channelNameToQueueName.size());
    assertTrue(redisScheduleTriggerHandler.queueNameToLastRunTime.get(slowQueue) >= lastRunTime);

    redisScheduleTriggerHandler.stop();

    assertTrue(redisScheduleTriggerHandler.queueNameToFuture.isEmpty());
    assertEquals(1, redisScheduleTriggerHandler.channelNameToQueueName.size());
    assertEquals(0L, redisScheduleTriggerHandler.queueNameToLastRunTime.get(slowQueue));
    verify(rqueueRedisListenerContainerFactory, times(1)).removeMessageListener(any(), any());
  }
}
