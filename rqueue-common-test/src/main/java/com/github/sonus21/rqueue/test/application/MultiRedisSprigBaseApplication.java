/*
 * Copyright 2020 Sonu Kumar
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.sonus21.rqueue.test.application;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.rqueue.config.SimpleRqueueListenerContainerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import redis.embedded.RedisServer;

@Slf4j
public abstract class MultiRedisSprigBaseApplication {

  @Value("${spring.redis2.port}")
  private int redisPort2;

  @Value("${spring.redis2.host}")
  private String redisHost2;

  private RedisServer redisServer;

  @Value("${mysql.db.name}")
  private String dbName;

  @Value("${spring.redis.port}")
  private int redisPort;

  @Value("${spring.redis.host}")
  private String redisHost;

  @Value("${use.system.redis:false}")
  private boolean useSystemRedis;

  private RedisServer redisServer2;

  private ExecutorService executor;
  private List<String> lines = new ArrayList<>();
  Process process;

  @PostConstruct
  public void postConstruct() {
    if (redisServer == null) {
      redisServer = new RedisServer(redisPort);
      redisServer.start();
    }
    if (redisServer2 == null) {
      redisServer2 = new RedisServer(redisPort2);
      redisServer2.start();
    }
    executor = Executors.newSingleThreadExecutor();
    executor.submit(
        () -> {
          try {
            process = Runtime.getRuntime().exec("redis-cli -p " + redisPort + " monitor");
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s;
            while ((s = br.readLine()) != null) {
              lines.add(s);
            }
            process.waitFor();
          } catch (Exception e) {
            log.error("Process call failed", e);
          }
        });
  }

  @PreDestroy
  public void preDestroy() {
    if (redisServer != null) {
      redisServer.stop();
    }
    if (redisServer2 != null) {
      redisServer2.stop();
    }
    if (process != null) {
      process.destroy();
    }
    for (String line : lines) {
      assert line.equals("OK");
    }
  }

  @Bean
  public LettuceConnectionFactory redisConnectionFactory() {
    return new LettuceConnectionFactory(redisHost, redisPort2);
  }

  @Bean
  public DataSource dataSource() {
    EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
    return builder.setType(EmbeddedDatabaseType.H2).setName(dbName).build();
  }

  @Bean
  public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
    HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
    vendorAdapter.setGenerateDdl(true);
    LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
    factory.setJpaVendorAdapter(vendorAdapter);
    factory.setPackagesToScan("com.github.sonus21.rqueue.test.entity");
    factory.setDataSource(dataSource());
    return factory;
  }

  @Bean
  public RedisMessageListenerContainer container(RedisConnectionFactory redisConnectionFactory) {
    RedisMessageListenerContainer container = new RedisMessageListenerContainer();
    container.setConnectionFactory(redisConnectionFactory);
    return container;
  }

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper();
  }

  @Bean
  public SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory() {
    SimpleRqueueListenerContainerFactory simpleRqueueListenerContainerFactory =
        new SimpleRqueueListenerContainerFactory();
    RedisStandaloneConfiguration redisConfiguration =
        new RedisStandaloneConfiguration(redisHost2, redisPort2);
    LettuceConnectionFactory connectionFactory = new LettuceConnectionFactory(redisConfiguration);
    connectionFactory.afterPropertiesSet();
    simpleRqueueListenerContainerFactory.setRedisConnectionFactory(connectionFactory);
    return simpleRqueueListenerContainerFactory;
  }
}
