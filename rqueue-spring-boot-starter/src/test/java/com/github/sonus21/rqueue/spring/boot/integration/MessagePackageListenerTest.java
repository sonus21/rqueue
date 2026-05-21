/*
 * Copyright (c) 2026 Sonu Kumar
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
package com.github.sonus21.rqueue.spring.boot.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sonus21.rqueue.annotation.RqueueListener;
import com.github.sonus21.rqueue.core.RqueueMessage;
import com.github.sonus21.rqueue.core.RqueueMessageEnqueuer;
import com.github.sonus21.rqueue.listener.RqueueMessageHeaders;
import com.github.sonus21.rqueue.test.application.BaseApplication;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.boot.data.redis.autoconfigure.DataRedisReactiveAutoConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Tag("springBootIntegration")
@Tag("integration")
@Tag("springBoot")
class MessagePackageListenerTest {

  private static final String MESSAGE_PACKAGE_QUEUE = "message-package-listener-package";
  private static final String MESSAGE_QUEUE = "message-package-listener-message";
  private static final String NATS_STREAM_PREFIX = "rqueue-js-messagePackageListener-";
  private static final String NATS_SUBJECT_PREFIX = "rqueue.js.messagePackageListener.";
  private static final int REDIS_PORT = 8032;

  @ParameterizedTest(name = "{0}")
  @EnumSource(BackendUnderTest.class)
  void listenerReceivesMsgPackPayloadInsideSpringMessage(BackendUnderTest backend)
      throws Exception {
    try (ConfigurableApplicationContext context = startContext(backend)) {
      TestListener listener = context.getBean(TestListener.class);
      ListenerPayload payload = new ListenerPayload(backend.name(), "msgpack-message-package");

      String messageId =
          context.getBean(RqueueMessageEnqueuer.class).enqueue(MESSAGE_PACKAGE_QUEUE, payload);

      assertThat(listener.messagePackageLatch.await(20, TimeUnit.SECONDS)).isTrue();
      assertThat(listener.messagePackage.get()).isNotNull();
      assertThat(listener.messagePackage.get().getPayload()).isEqualTo(payload);
      assertRqueueMessage(
          listener.messagePackageRqueueMessage.get(), messageId, MESSAGE_PACKAGE_QUEUE, payload);
    }
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(BackendUnderTest.class)
  void listenerReceivesMsgPackPayload(BackendUnderTest backend) throws Exception {
    try (ConfigurableApplicationContext context = startContext(backend)) {
      TestListener listener = context.getBean(TestListener.class);
      ListenerPayload payload = new ListenerPayload(backend.name(), "msgpack-message");

      String messageId =
          context.getBean(RqueueMessageEnqueuer.class).enqueue(MESSAGE_QUEUE, payload);

      assertThat(listener.messageLatch.await(20, TimeUnit.SECONDS)).isTrue();
      assertThat(listener.message.get()).isEqualTo(payload);
      assertRqueueMessage(listener.messageRqueueMessage.get(), messageId, MESSAGE_QUEUE, payload);
    }
  }

  private ConfigurableApplicationContext startContext(BackendUnderTest backend) {
    if (backend.isNats()) {
      AbstractNatsBootIT.startNats();
      AbstractNatsBootIT.deleteStreamsWithPrefix(NATS_STREAM_PREFIX);
    }
    return new SpringApplicationBuilder(backend.applicationClass())
        .web(WebApplicationType.NONE)
        .properties(backend.properties())
        .run();
  }

  private static void assertRqueueMessage(
      RqueueMessage rqueueMessage,
      String messageId,
      String queueName,
      ListenerPayload expectedPayload) {
    assertThat(rqueueMessage).isNotNull();
    assertThat(rqueueMessage.getId()).isEqualTo(messageId);
    assertThat(rqueueMessage.getQueueName()).isEqualTo(queueName);
    assertThat(MsgPackMessageConverterProvider.isMsgPack(rqueueMessage.getMessage()))
        .isTrue();
    assertThat(MsgPackMessageConverterProvider.decode(rqueueMessage.getMessage()))
        .isEqualTo(expectedPayload);
  }

  enum BackendUnderTest {
    REDIS(RedisTestApp.class, new String[] {
      "rqueue.backend=redis",
      "spring.data.redis.host=127.0.0.1",
      "spring.data.redis.port=" + REDIS_PORT,
      "mysql.db.name=MessagePackageListenerTestRedis",
      "use.system.redis=false"
    }),
    NATS(NatsTestApp.class, new String[] {});

    private final Class<?> applicationClass;
    private final String[] properties;

    BackendUnderTest(Class<?> applicationClass, String[] properties) {
      this.applicationClass = applicationClass;
      this.properties = properties;
    }

    Class<?> applicationClass() {
      return applicationClass;
    }

    String[] properties() {
      String[] common = new String[] {
        "rqueue.message.converter.provider.class="
            + MsgPackMessageConverterProvider.class.getName(),
      };
      String[] backendProperties = isNats()
          ? new String[] {
            "rqueue.backend=nats",
            "rqueue.nats.naming.stream-prefix=" + NATS_STREAM_PREFIX,
            "rqueue.nats.naming.subject-prefix=" + NATS_SUBJECT_PREFIX,
            "rqueue.nats.connection.url=" + AbstractNatsBootIT.activeNatsUrl()
          }
          : properties;
      String[] merged = new String[common.length + backendProperties.length];
      System.arraycopy(common, 0, merged, 0, common.length);
      System.arraycopy(backendProperties, 0, merged, common.length, backendProperties.length);
      return merged;
    }

    boolean isNats() {
      return this == NATS;
    }
  }

  @SpringBootApplication
  @Import(TestListener.class)
  static class RedisTestApp extends BaseApplication {}

  @SpringBootApplication(
      exclude = {DataRedisAutoConfiguration.class, DataRedisReactiveAutoConfiguration.class})
  @Import(TestListener.class)
  static class NatsTestApp {}

  @Component
  static class TestListener {

    final CountDownLatch messagePackageLatch = new CountDownLatch(1);
    final CountDownLatch messageLatch = new CountDownLatch(1);
    final AtomicReference<Message<ListenerPayload>> messagePackage = new AtomicReference<>();
    final AtomicReference<RqueueMessage> messagePackageRqueueMessage = new AtomicReference<>();
    final AtomicReference<ListenerPayload> message = new AtomicReference<>();
    final AtomicReference<RqueueMessage> messageRqueueMessage = new AtomicReference<>();

    @RqueueListener(value = MESSAGE_PACKAGE_QUEUE)
    void onMessagePackage(
        Message<ListenerPayload> message,
        @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage) {
      messagePackage.set(message);
      messagePackageRqueueMessage.set(rqueueMessage);
      messagePackageLatch.countDown();
    }

    @RqueueListener(value = MESSAGE_QUEUE)
    void onMessage(
        ListenerPayload message,
        @Header(RqueueMessageHeaders.MESSAGE) RqueueMessage rqueueMessage) {
      this.message.set(message);
      messageRqueueMessage.set(rqueueMessage);
      messageLatch.countDown();
    }
  }

  static class ListenerPayload {

    private String backend;
    private String body;

    ListenerPayload() {}

    ListenerPayload(String backend, String body) {
      this.backend = backend;
      this.body = body;
    }

    public String getBackend() {
      return backend;
    }

    public void setBackend(String backend) {
      this.backend = backend;
    }

    public String getBody() {
      return body;
    }

    public void setBody(String body) {
      this.body = body;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof ListenerPayload)) {
        return false;
      }
      ListenerPayload that = (ListenerPayload) other;
      return Objects.equals(backend, that.backend) && Objects.equals(body, that.body);
    }

    @Override
    public int hashCode() {
      return Objects.hash(backend, body);
    }
  }
}
