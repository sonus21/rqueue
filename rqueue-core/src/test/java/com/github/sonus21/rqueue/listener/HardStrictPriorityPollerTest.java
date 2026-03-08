package com.github.sonus21.rqueue.listener;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.github.sonus21.TestBase;
import com.github.sonus21.rqueue.CoreUnitTest;
import com.github.sonus21.rqueue.core.RqueueBeanProvider;
import com.github.sonus21.rqueue.listener.RqueueMessageListenerContainer.QueueStateMgr;
import com.github.sonus21.rqueue.utils.Constants;
import com.github.sonus21.rqueue.utils.QueueThreadPool;
import com.github.sonus21.rqueue.utils.TimeoutUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.messaging.MessageHeaders;

@CoreUnitTest
class HardStrictPriorityPollerTest extends TestBase {

  @Mock private RqueueBeanProvider rqueueBeanProvider;
  @Mock private QueueStateMgr queueStateMgr;
  @Mock private PostProcessingHandler postProcessingHandler;

  private final String highPriorityQueue = "high-priority-" + UUID.randomUUID();
  private final String lowPriorityQueue = "low-priority-" + UUID.randomUUID();
  private HardStrictPriorityPoller poller;
  private QueueDetail highDetail;
  private QueueDetail lowDetail;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    highDetail = createQueueDetail(highPriorityQueue, 100);
    lowDetail = createQueueDetail(lowPriorityQueue, 10);

    List<QueueDetail> queueDetails = Arrays.asList(lowDetail, highDetail);
    Map<String, QueueThreadPool> queueNameToThread = new HashMap<>();
    queueNameToThread.put(highPriorityQueue, mock(QueueThreadPool.class));
    queueNameToThread.put(lowPriorityQueue, mock(QueueThreadPool.class));

    poller =
        spy(
            new HardStrictPriorityPoller(
                "test-group",
                queueDetails,
                queueNameToThread,
                rqueueBeanProvider,
                queueStateMgr,
                Collections.emptyList(),
                50L,
                50L,
                postProcessingHandler,
                new MessageHeaders(Collections.emptyMap()),
                new HardStrictPriorityPollerProperties()));

    // КРИТИЧЕСКИ ВАЖНО: Разрешаем опрос очередей
    lenient().doReturn(true).when(poller).eligibleForPolling(anyString());
    // КРИТИЧЕСКИ ВАЖНО: Запрещаем немедленный выход из цикла
    lenient().doReturn(false).when(poller).shouldExit();
  }

  private QueueDetail createQueueDetail(String name, int priority) {
    QueueDetail detail = mock(QueueDetail.class);
    when(detail.getName()).thenReturn(name);
    Map<String, Integer> priorityMap = new HashMap<>();
    priorityMap.put(Constants.DEFAULT_PRIORITY_KEY, priority);
    when(detail.getPriority()).thenReturn(priorityMap);
    return detail;
  }

  @Test
  void testQueuesAreSortedByPriority() {
    List<String> sortedQueues = poller.queues;
    assertTrue(
        sortedQueues.indexOf(highPriorityQueue) < sortedQueues.indexOf(lowPriorityQueue),
        "High priority queue should be first");
  }

  @Test
  void testExistMessagesInHigherPriorityQueueReturnsTrue() {
    List<String> queues = Arrays.asList(highPriorityQueue, lowPriorityQueue);

    // High queue has messages
    lenient().doReturn(true).when(poller).existAvailableMessagesForPoll(highDetail);

    // Checking from low priority perspective
    boolean result =
        poller.existMessagesInCurrentQueueOrHigherPriorityQueue(lowPriorityQueue, queues);
    assertTrue(result, "Should return true because high priority queue has messages");
  }

  @Test
  void testStrictExecutionPreventsLowPriorityPoll() throws Exception {
    AtomicInteger highQueuePollCount = new AtomicInteger(0);
    AtomicInteger lowQueuePollCount = new AtomicInteger(0);

    // High Priority always has messages
    lenient().doReturn(true).when(poller).existAvailableMessagesForPoll(highDetail);
    lenient()
        .doAnswer(
            invocation -> {
              highQueuePollCount.incrementAndGet();
              return 1;
            })
        .when(poller)
        .poll(anyInt(), eq(highPriorityQueue), eq(highDetail), any());

    // Low Priority also has messages
    lenient().doReturn(true).when(poller).existAvailableMessagesForPoll(lowDetail);
    lenient()
        .doAnswer(
            invocation -> {
              lowQueuePollCount.incrementAndGet();
              return 1;
            })
        .when(poller)
        .poll(anyInt(), eq(lowPriorityQueue), eq(lowDetail), any());

    Thread pollerThread = new Thread(poller::start);
    pollerThread.start();

    try {
      // Wait for multiple polls of High priority
      TimeoutUtils.waitFor(() -> highQueuePollCount.get() > 5, 2000, "high priority polls");

      // Low priority must NEVER be polled because 'break' happens after high poll
      assertTrue(lowQueuePollCount.get() == 0, "Low priority queue should not be polled");
    } finally {
      stop(poller, pollerThread);
    }
  }

  @Test
  void testLowPriorityIsPolledWhenHighIsEmpty() throws Exception {
    AtomicInteger lowQueuePollCount = new AtomicInteger(0);

    // High is empty
    lenient().doReturn(false).when(poller).existAvailableMessagesForPoll(highDetail);

    // Low has messages
    lenient().doReturn(true).when(poller).existAvailableMessagesForPoll(lowDetail);
    lenient()
        .doAnswer(
            invocation -> {
              lowQueuePollCount.incrementAndGet();
              return 1;
            })
        .when(poller)
        .poll(anyInt(), eq(lowPriorityQueue), eq(lowDetail), any());

    Thread pollerThread = new Thread(poller::start);
    pollerThread.start();

    try {
      TimeoutUtils.waitFor(() -> lowQueuePollCount.get() > 0, 2000, "low priority poll");
      assertTrue(lowQueuePollCount.get() > 0);
    } finally {
      stop(poller, pollerThread);
    }
  }

  private void stop(HardStrictPriorityPoller poller, Thread thread) {
    lenient().doReturn(true).when(poller).shouldExit();
    if (thread != null && thread.isAlive()) {
      thread.interrupt();
      try {
        thread.join(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
