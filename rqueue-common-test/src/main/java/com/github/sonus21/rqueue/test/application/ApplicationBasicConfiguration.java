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
import javax.sql.DataSource;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import redis.embedded.RedisServer;

public abstract class ApplicationBasicConfiguration {
  protected RedisServer redisServer;

  @Value("${mysql.db.name}")
  protected String dbName;

  @Value("${spring.redis.port}")
  protected int redisPort;

  @Value("${spring.redis.host}")
  protected String redisHost;

  @Value("${use.system.redis:false}")
  protected boolean useSystemRedis;

  protected void init() {
    if (useSystemRedis) {
      return;
    }
    if (redisServer == null) {
      redisServer = new RedisServer(redisPort);
      redisServer.start();
    }
  }

  protected void destroy() {
    if (redisServer != null) {
      redisServer.stop();
    }
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
    return new ObjectMapper();
  }
}
