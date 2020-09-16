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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import redis.embedded.RedisServer;

public abstract class ApplicationBasicConfiguration {
  private static final Logger monitorLogger = LoggerFactory.getLogger("monitor");
  protected RedisServer redisServer;
  protected ExecutorService executorService;
  protected List<RProcess> processes;
  @Value("${mysql.db.name}")
  protected String dbName;
  @Value("${spring.redis.port}")
  protected int redisPort;
  @Value("${spring.redis.host}")
  protected String redisHost;
  @Value("${use.system.redis:false}")
  protected boolean useSystemRedis;
  @Value("${monitor.thread.count:0}")
  protected int monitorThreads;

  protected void init() {
    if (monitorThreads > 0) {
      executorService = Executors.newFixedThreadPool(monitorThreads);
      processes = new ArrayList<>();
    }
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

    if (processes != null) {
      for (RProcess rProcess : processes) {
        rProcess.process.destroy();
        monitorLogger.info("RedisNode {} ", rProcess.redisNode);
        for (String line : rProcess.out) {
          monitorLogger.info("{}", line);
        }
      }
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  protected void monitor(String host, int port) {
    executorService.submit(
        () -> {
          try {
            Process process =
                Runtime.getRuntime()
                    .exec("redis-cli " + " -h " + host + " -p " + port + " monitor");
            List<String> lines = new LinkedList<>();
            RProcess rProcess = new RProcess(process, new RedisNode(host, port), lines);
            processes.add(rProcess);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String s;
            while ((s = br.readLine()) != null) {
              lines.add(s);
            }
            process.waitFor();
          } catch (Exception e) {
            monitorLogger.error("Process call failed", e);
          }
        });
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

  @AllArgsConstructor
  public static class RProcess {
    Process process;
    RedisNode redisNode;
    List<String> out;
  }
}
