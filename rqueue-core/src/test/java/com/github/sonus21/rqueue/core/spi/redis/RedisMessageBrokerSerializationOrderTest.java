package com.github.sonus21.rqueue.core.spi.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.converter.RqueueRedisSerializer;
import com.github.sonus21.rqueue.converter.RqueueRedisSerializer.PropertyOrder;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
import com.github.sonus21.rqueue.utils.RedisUtils;
import com.github.sonus21.rqueue.utils.TestUtils;
import java.io.IOException;
import java.net.ServerSocket;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import redis.embedded.RedisServer;

@Tag("core")
class RedisMessageBrokerSerializationOrderTest {

  private static RedisServer redisServer;
  private static int redisPort;

  @BeforeAll
  static void startRedis() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      redisPort = socket.getLocalPort();
    }
    redisServer = new RedisServer(redisPort);
    redisServer.start();
  }

  @AfterAll
  static void stopRedis() throws IOException {
    if (redisServer != null) {
      redisServer.stop();
    }
  }

  private LettuceConnectionFactory connectionFactory;
  private RqueueMessageTemplateImpl messageTemplate;
  private RedisMessageBroker broker;
  // Passthrough serialiser: inserts raw bytes into the ZSET without re-serialising, mirroring
  // how dequeue_message.lua byte-copies messages from q-queue into the processing queue.
  private RedisTemplate<String, byte[]> rawTemplate;

  @BeforeEach
  void setUp() {
    connectionFactory =
        new LettuceConnectionFactory(new RedisStandaloneConfiguration("localhost", redisPort));
    connectionFactory.afterPropertiesSet();
    messageTemplate = new RqueueMessageTemplateImpl(connectionFactory, null);
    broker = new RedisMessageBroker(messageTemplate);

    rawTemplate = new RedisTemplate<>();
    rawTemplate.setConnectionFactory(connectionFactory);
    rawTemplate.setKeySerializer(new StringRedisSerializer());
    rawTemplate.setValueSerializer(RedisSerializer.byteArray());
    rawTemplate.afterPropertiesSet();
  }

  @AfterEach
  void tearDown() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
    messageTemplate.getRedisTemplate().delete(queueDetail.getProcessingQueueName());
    messageTemplate.getRedisTemplate().delete(queueDetail.getScheduledQueueName());
    connectionFactory.destroy();
  }

  // --- property order: ALPHABETICAL (Jackson 3.x default, RQueue 4.x default) ---

  @Test
  void parkForRetry_alphabeticalOrder_4xMessage_isMovedToScheduledQueue() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

    RqueueMessage original = RqueueMessage.builder()
        .id("test-msg-alpha-4x-001")
        .queueName("test-queue")
        .message("{\"payload\":\"test\"}")
        .processAt(1_000_000L)
        .queuedTime(2_000_000L)
        .build();

    messageTemplate.addToZset(
        queueDetail.getProcessingQueueName(), original, System.currentTimeMillis());

    RqueueMessage updated = original.toBuilder().failureCount(1).build().updateReEnqueuedAt();
    broker.parkForRetry(queueDetail, original, updated, 60_000L);

    assertEquals(
        0L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getProcessingQueueName()),
        "Processing queue must be empty after parkForRetry");
    assertEquals(
        1L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getScheduledQueueName()),
        "Scheduled queue must contain the rescheduled message");
  }

  @Test
  void ack_alphabeticalOrder_4xMessage_isRemovedFromProcessingQueue() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

    RqueueMessage original = RqueueMessage.builder()
        .id("test-msg-alpha-4x-ack-001")
        .queueName("test-queue")
        .message("{\"payload\":\"test\"}")
        .processAt(1_000_000L)
        .queuedTime(2_000_000L)
        .build();

    messageTemplate.addToZset(
        queueDetail.getProcessingQueueName(), original, System.currentTimeMillis());

    broker.ack(queueDetail, original);

    assertEquals(
        0L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getProcessingQueueName()),
        "Processing queue must be empty after ack");
  }

  @Test
  void parkForRetry_alphabeticalOrder_v3Message_strandsMessage() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

    rawTemplate
        .opsForZSet()
        .add(
            queueDetail.getProcessingQueueName(),
            v3Bytes(RqueueMessage.builder()
                .id("test-msg-alpha-v3-001")
                .queueName("test-queue")
                .message("{\"payload\":\"test\"}")
                .processAt(1_000_000L)
                .queuedTime(2_000_000L)
                .build()),
            System.currentTimeMillis());

    RqueueMessage original = RqueueMessage.builder()
        .id("test-msg-alpha-v3-001")
        .queueName("test-queue")
        .message("{\"payload\":\"test\"}")
        .processAt(1_000_000L)
        .queuedTime(2_000_000L)
        .build();
    RqueueMessage updated = original.toBuilder().failureCount(1).build().updateReEnqueuedAt();
    broker.parkForRetry(queueDetail, original, updated, 60_000L);

    assertEquals(
        1L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getProcessingQueueName()),
        "3.x message must remain stranded under ALPHABETICAL order");
    assertEquals(
        0L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getScheduledQueueName()),
        "Scheduled queue must be empty: ZSCORE missed due to alphabetical vs declaration mismatch");
  }

  @Test
  void ack_alphabeticalOrder_v3Message_strandsMessage() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

    RqueueMessage original = RqueueMessage.builder()
        .id("test-msg-alpha-v3-ack-001")
        .queueName("test-queue")
        .message("{\"payload\":\"test\"}")
        .processAt(1_000_000L)
        .queuedTime(2_000_000L)
        .build();

    rawTemplate
        .opsForZSet()
        .add(queueDetail.getProcessingQueueName(), v3Bytes(original), System.currentTimeMillis());

    broker.ack(queueDetail, original);

    assertEquals(
        1L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getProcessingQueueName()),
        "3.x message must remain stranded under ALPHABETICAL order");
  }

  // --- property order: DECLARATION (declaration order, matching RQueue 3.x) ---

  @Test
  void parkForRetry_declarationOrder_v3Message_isMovedToScheduledQueue() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
    RedisUtils.RedisTemplateProvider saved = RedisUtils.redisTemplateProvider;
    try {
      RedisMessageBroker declarationBroker = brokerWithOrder(PropertyOrder.DECLARATION);
      RqueueMessageTemplateImpl declarationTemplate = templateWithOrder(PropertyOrder.DECLARATION);

      RqueueMessage original = RqueueMessage.builder()
          .id("test-msg-v3-001")
          .queueName("test-queue")
          .message("{\"payload\":\"test\"}")
          .processAt(1_000_000L)
          .queuedTime(2_000_000L)
          .build();

      rawTemplate
          .opsForZSet()
          .add(queueDetail.getProcessingQueueName(), v3Bytes(original), System.currentTimeMillis());

      RqueueMessage updated = original.toBuilder().failureCount(1).build().updateReEnqueuedAt();
      declarationBroker.parkForRetry(queueDetail, original, updated, 60_000L);

      assertEquals(
          0L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getProcessingQueueName()),
          "Processing queue must be empty after parkForRetry");
      assertEquals(
          1L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getScheduledQueueName()),
          "Scheduled queue must contain the rescheduled message");
    } finally {
      RedisUtils.redisTemplateProvider = saved;
    }
  }

  @Test
  void ack_declarationOrder_v3Message_isRemovedFromProcessingQueue() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
    RedisUtils.RedisTemplateProvider saved = RedisUtils.redisTemplateProvider;
    try {
      RedisMessageBroker declarationBroker = brokerWithOrder(PropertyOrder.DECLARATION);
      RqueueMessageTemplateImpl declarationTemplate = templateWithOrder(PropertyOrder.DECLARATION);

      RqueueMessage original = RqueueMessage.builder()
          .id("test-msg-v3-002")
          .queueName("test-queue")
          .message("{\"payload\":\"test\"}")
          .processAt(1_000_000L)
          .queuedTime(2_000_000L)
          .build();

      rawTemplate
          .opsForZSet()
          .add(queueDetail.getProcessingQueueName(), v3Bytes(original), System.currentTimeMillis());

      declarationBroker.ack(queueDetail, original);

      assertEquals(
          0L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getProcessingQueueName()),
          "Processing queue must be empty after ack");
    } finally {
      RedisUtils.redisTemplateProvider = saved;
    }
  }

  @Test
  void parkForRetry_declarationOrder_4xAlphabeticalMessage_strandsMessage() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
    RedisUtils.RedisTemplateProvider saved = RedisUtils.redisTemplateProvider;
    try {
      RedisMessageBroker declarationBroker = brokerWithOrder(PropertyOrder.DECLARATION);
      RqueueMessageTemplateImpl declarationTemplate = templateWithOrder(PropertyOrder.DECLARATION);

      RqueueMessage original = RqueueMessage.builder()
          .id("test-msg-legacy-4x-001")
          .queueName("test-queue")
          .message("{\"payload\":\"test\"}")
          .processAt(1_000_000L)
          .queuedTime(2_000_000L)
          .build();

      byte[] v4AlphaBytes =
          new RqueueRedisSerializer(PropertyOrder.ALPHABETICAL).serialize(original);
      rawTemplate
          .opsForZSet()
          .add(queueDetail.getProcessingQueueName(), v4AlphaBytes, System.currentTimeMillis());

      RqueueMessage updated = original.toBuilder().failureCount(1).build().updateReEnqueuedAt();
      declarationBroker.parkForRetry(queueDetail, original, updated, 60_000L);

      assertEquals(
          1L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getProcessingQueueName()),
          "4.x alphabetical message must remain stranded under DECLARATION order");
      assertEquals(
          0L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getScheduledQueueName()),
          "Scheduled queue must be empty: ZSCORE missed due to declaration vs alphabetical"
              + " mismatch");
    } finally {
      RedisUtils.redisTemplateProvider = saved;
    }
  }

  @Test
  void ack_declarationOrder_4xAlphabeticalMessage_strandsMessage() {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");
    RedisUtils.RedisTemplateProvider saved = RedisUtils.redisTemplateProvider;
    try {
      RedisMessageBroker declarationBroker = brokerWithOrder(PropertyOrder.DECLARATION);
      RqueueMessageTemplateImpl declarationTemplate = templateWithOrder(PropertyOrder.DECLARATION);

      RqueueMessage original = RqueueMessage.builder()
          .id("test-msg-legacy-4x-ack-001")
          .queueName("test-queue")
          .message("{\"payload\":\"test\"}")
          .processAt(1_000_000L)
          .queuedTime(2_000_000L)
          .build();

      byte[] v4AlphaBytes =
          new RqueueRedisSerializer(PropertyOrder.ALPHABETICAL).serialize(original);
      rawTemplate
          .opsForZSet()
          .add(queueDetail.getProcessingQueueName(), v4AlphaBytes, System.currentTimeMillis());

      declarationBroker.ack(queueDetail, original);

      assertEquals(
          1L,
          declarationTemplate
              .getRedisTemplate()
              .opsForZSet()
              .size(queueDetail.getProcessingQueueName()),
          "4.x alphabetical message must remain stranded under DECLARATION order");
    } finally {
      RedisUtils.redisTemplateProvider = saved;
    }
  }

  private RqueueMessageTemplateImpl templateWithOrder(PropertyOrder order) {
    RqueueRedisSerializer serializer = new RqueueRedisSerializer(order);
    StringRedisSerializer key = new StringRedisSerializer();
    RedisUtils.redisTemplateProvider = new RedisUtils.RedisTemplateProvider() {
      @Override
      public <V> org.springframework.data.redis.core.RedisTemplate<String, V> getRedisTemplate(
          org.springframework.data.redis.connection.RedisConnectionFactory factory) {
        org.springframework.data.redis.core.RedisTemplate<String, V> t =
            new org.springframework.data.redis.core.RedisTemplate<>();
        t.setConnectionFactory(factory);
        t.setKeySerializer(key);
        t.setValueSerializer(serializer);
        t.setHashKeySerializer(key);
        t.setHashValueSerializer(serializer);
        return t;
      }
    };
    return new RqueueMessageTemplateImpl(connectionFactory, null);
  }

  private RedisMessageBroker brokerWithOrder(PropertyOrder order) {
    return new RedisMessageBroker(templateWithOrder(order));
  }

  // Reproduce the bytes RQueue 3.x would have stored using the actual Jackson 2.x ObjectMapper
  // so the serialised form stays in sync with RqueueMessage field changes automatically.
  private static byte[] v3Bytes(RqueueMessage message) throws Exception {
    com.fasterxml.jackson.databind.ObjectMapper jackson2 =
        new com.fasterxml.jackson.databind.ObjectMapper()
            .configure(
                com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
                false);
    //noinspection deprecation — mirrors the API RQueue 3.x RqueueRedisSerDes used
    jackson2.enableDefaultTyping(
        com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping.NON_FINAL,
        com.fasterxml.jackson.annotation.JsonTypeInfo.As.PROPERTY);
    return jackson2.writeValueAsBytes(message);
  }
}
