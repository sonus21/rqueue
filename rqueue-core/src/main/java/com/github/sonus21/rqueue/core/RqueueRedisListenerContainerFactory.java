/*
 * Copyright (c) 2020-2023 Sonu Kumar
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

import com.github.sonus21.rqueue.config.RqueueConfig;
import com.github.sonus21.rqueue.config.RqueueSchedulerConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;

@Slf4j
public class RqueueRedisListenerContainerFactory
    implements DisposableBean, SmartLifecycle, InitializingBean {

  @Autowired
  private RqueueSchedulerConfig rqueueSchedulerConfig;
  @Autowired
  private RqueueConfig rqueueConfig;

  @Autowired(required = false)
  private RedisMessageListenerContainer systemContainer;

  private RedisMessageListenerContainer container;

  private boolean sharedContainer = false;

  @Override
  public void destroy() throws Exception {
    if (notSharedContainer()) {
      container.destroy();
    }
  }

  @Override
  public void start() {
    if (notSharedContainer()) {
      container.start();
    }
  }

  @Override
  public void stop() {
    if (notSharedContainer()) {
      container.stop();
    }
  }

  @Override
  public boolean isRunning() {
    if (container != null) {
      return container.isRunning();
    }
    return false;
  }

  private RedisMessageListenerContainer getContainer() {
    return this.container;
  }

  public void addMessageListener(MessageListener listener, Topic topic) {
    getContainer().addMessageListener(listener, topic);
  }

  private boolean notSharedContainer() {
    return container != null && !sharedContainer;
  }

  private void createContainer() {
    container = new RedisMessageListenerContainer();
    container.setConnectionFactory(rqueueConfig.getConnectionFactory());
    container.afterPropertiesSet();
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    if (!rqueueConfig.isSharedConnection()) {
      createContainer();
      return;
    }
    if (rqueueSchedulerConfig.isListenerShared()) {
      if (systemContainer != null) {
        container = systemContainer;
        sharedContainer = true;
        return;
      }
    }
    createContainer();
  }

  @Override
  public boolean isAutoStartup() {
    return true;
  }

  @Override
  public void stop(Runnable callback) {
    stop();
    callback.run();
  }

  @Override
  public int getPhase() {
    return Integer.MAX_VALUE;
  }

  public void removeMessageListener(MessageListener messageListener, ChannelTopic channelTopic) {
    getContainer().removeMessageListener(messageListener, channelTopic);
  }
}
