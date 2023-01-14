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

package com.github.sonus21.rqueue.test.application;

import com.athaydes.javanna.Javanna;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sonus21.junit.BootstrapRedis;
import com.github.sonus21.junit.RedisBootstrapperBase;
import com.github.sonus21.rqueue.utils.SerializationUtils;
import java.util.HashMap;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

@Slf4j
public abstract class ApplicationBasicConfiguration extends RedisBootstrapperBase {

  @Value("${mysql.db.name}")
  protected String dbName;

  @Value("${spring.data.redis.port}")
  protected int redisPort;

  @Value("${spring.data.redis.host}")
  protected String redisHost;

  @Value("${use.system.redis:false}")
  protected boolean useSystemRedis;

  @Value("${monitor.thread.count:0}")
  protected int monitorThreads;

  @Value("${monitor.enabled:false}")
  protected boolean monitoringEnabled;

  @Value("${rqueue.reactive.enabled:false}")
  protected boolean reactiveEnabled;

  protected void init() {
    final BootstrapRedis bootstrapRedis =
        Javanna.createAnnotation(
            BootstrapRedis.class,
            new HashMap<String, Object>() {
              private static final long serialVersionUID = -786051705319430908L;

              {
                put("port", redisPort);
                put("monitorRedis", monitoringEnabled);
                put("monitorThreadsCount", monitorThreads);
                put("systemRedis", useSystemRedis);
              }
            });
    super.bootstrap(bootstrapRedis);
  }

  @Bean
  public DataSource dataSource() {
    EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
    return builder.setType(EmbeddedDatabaseType.H2).setName(dbName).build();
  }

  @Bean
  public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
    HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
    vendorAdapter.setGenerateDdl(true);
    LocalContainerEntityManagerFactoryBean factory = new LocalContainerEntityManagerFactoryBean();
    factory.setJpaVendorAdapter(vendorAdapter);
    factory.setPackagesToScan("com.github.sonus21.rqueue.test.entity");
    factory.setDataSource(dataSource);
    return factory;
  }

  @Bean
  public ObjectMapper objectMapper() {
    return SerializationUtils.createObjectMapper();
  }
}
