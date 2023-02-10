/*
 * Copyright (c) 2019-2023 Sonu Kumar
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

package com.github.sonus21.junit;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.util.CollectionUtils;
import redis.embedded.RedisServer;

@Slf4j
public abstract class RedisBootstrapperBase {

  private static final Logger monitorLogger = LoggerFactory.getLogger("monitor");
  protected RedisServer redisServer;
  protected ExecutorService executorService;
  protected List<MonitorProcess> processes = new ArrayList<>();

  protected void bootstrap(BootstrapRedis bootstrapRedis) {
    int monitorThreads = 0;
    if (bootstrapRedis.monitorRedis()) {
      if (bootstrapRedis.monitorThreadsCount() > 0) {
        monitorThreads = bootstrapRedis.monitorThreadsCount();
      } else {
        monitorThreads = 1;
      }
    }
    if (monitorThreads > 0) {
      executorService = Executors.newFixedThreadPool(monitorThreads);
    }
    if (!bootstrapRedis.systemRedis() && redisServer == null) {
      log.info("Starting Redis at port {}", bootstrapRedis.port());
      redisServer = new RedisServer(bootstrapRedis.port());
      redisServer.start();
    }
    if (bootstrapRedis.monitorRedis()) {
      monitor(bootstrapRedis.host(), bootstrapRedis.port());
    }
  }

  protected void cleanup() {
    if (redisServer != null) {
      redisServer.stop();
    }

    if (!CollectionUtils.isEmpty(processes)) {
      for (MonitorProcess monitorProcess : processes) {
        monitorProcess.process.destroy();
        monitorLogger.info("RedisNode {} ", monitorProcess.redisNode);
        for (String line : monitorProcess.out) {
          monitorLogger.info("{}", line);
        }
      }
    }
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  protected void monitor(String host, int port) {
    log.info("Monitor {}:{}", host, port);
    executorService.submit(
        () -> {
          try {
            Process process =
                Runtime.getRuntime()
                    .exec("redis-cli " + " -h " + host + " -p " + port + " monitor");
            List<String> lines = new LinkedList<>();
            MonitorProcess monitorProcess =
                new MonitorProcess(process, new RedisNode(host, port), lines);
            processes.add(monitorProcess);
            BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
            while (process.isAlive()) {
              String s = br.readLine();
              if (s != null) {
                lines.add(s);
              }
            }
          } catch (Exception e) {
            monitorLogger.error("Process call failed", e);
          }
        });
  }

  @AllArgsConstructor
  @Getter
  public static class MonitorProcess {

    public Process process;
    public RedisNode redisNode;
    public List<String> out;
  }
}
