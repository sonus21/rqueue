package com.github.sonus21.rqueue.core.spi.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.impl.RqueueMessageTemplateImpl;
import com.github.sonus21.rqueue.listener.QueueDetail;
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
class RedisMessageBrokerV3CompatibilityTest {

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

  @Test
  void parkForRetry_v3SerializedMessage_isMovedToScheduledQueue() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

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
  void ack_v3SerializedMessage_isRemovedFromProcessingQueue() throws Exception {
    QueueDetail queueDetail = TestUtils.createQueueDetail("test-queue");

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

    broker.ack(queueDetail, original);

    assertEquals(
        0L,
        messageTemplate.getRedisTemplate().opsForZSet().size(queueDetail.getProcessingQueueName()),
        "Processing queue must be empty after ack");
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
