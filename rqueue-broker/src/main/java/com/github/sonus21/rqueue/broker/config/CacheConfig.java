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

package com.github.sonus21.rqueue.broker.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.concurrent.ConcurrentMapCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;

@Configuration
public class CacheConfig {
  public static final String AUTH_TOKEN_CACHE_NAME = "authToken";
  public static final String SESSION_CACHE_NAME = "sessionCache";
  public static final String TOPIC_CONFIG_CACHE_NAME = "topicConfigCache";
  public static final String TOPIC_CACHE =
  public static final String QUEUE_CONFIG_CACHE_NAME = "queueConfigCache";

  @Value("${rqueue.authentication.token.cache.expiry:86400}")
  private Long tokenCacheExpiry;

  @Value("${rqueue.authentication.token.cache.size:10}")
  private int tokenCacheSize;

  @Value("${rqueue.authentication.cookie.cache.expiry:86400}")
  private Long cookieCacheExpiry;

  @Value("${rqueue.authentication.cookie.cache.size:5}")
  private int cookieCacheSize;

  @Value("${rqueue.topic.config.cache.expiry:86400}")
  private Long topicConfigCacheExpiry;

  @Value("${rqueue.topic.config.cache.size:500}")
  private int topicConfigCacheSize;

  @Value("${rqueue.queue.config.cache.expiry:86400}")
  private Long queueConfigCacheExpiry;

  @Value("${rqueue.queue.config.cache.size:500}")
  private int queueConfigCacheSize;

  @Bean
  public CacheManager cacheManager() {
    RqueueCacheManager rqueueCacheManager = new RqueueCacheManager();
    rqueueCacheManager.addCache(
        createCache(SESSION_CACHE_NAME, cookieCacheExpiry, cookieCacheSize));
    rqueueCacheManager.addCache(
        createCache(AUTH_TOKEN_CACHE_NAME, tokenCacheExpiry, tokenCacheSize));
    rqueueCacheManager.addCache(
        createCache(TOPIC_CONFIG_CACHE_NAME, topicConfigCacheExpiry, topicConfigCacheSize));
    rqueueCacheManager.addCache(
        createCache(QUEUE_CONFIG_CACHE_NAME, queueConfigCacheExpiry, queueConfigCacheSize));
    return rqueueCacheManager;
  }

  private CaffeineCache createCache(String name, Long expiry, int size) {
    Caffeine<Object, Object> caffeine = Caffeine.newBuilder();
    caffeine.maximumSize(size);
    if (expiry != null) {
      caffeine.expireAfterWrite(Duration.ofSeconds(expiry));
    }
    return new CaffeineCache(name, caffeine.build());
  }

  static class RqueueCacheManager implements CacheManager {
    private Map<String, CaffeineCache> cacheMap = new HashMap<>();

    RqueueCacheManager(CaffeineCache... caffeineCaches) {
      for (CaffeineCache caffeineCache : caffeineCaches) {
        addCache(caffeineCache);
      }
    }

    void addCache(CaffeineCache caffeineCache) {
      this.cacheMap.put(caffeineCache.getName(), caffeineCache);
    }

    @Override
    @Nullable
    public Cache getCache(String name) {
      return this.cacheMap.get(name);
    }

    @Override
    public Collection<String> getCacheNames() {
      return Collections.unmodifiableSet(this.cacheMap.keySet());
    }
  }

  private Cache createConcurrentMapBasedCache(String name, int cacheSize) {
    ConcurrentMap<Object, Object> concurrentMap = new ConcurrentHashMap<>(Math.min(cacheSize, 8));
    return new ConcurrentMapCache(name, concurrentMap, false);
  }
}
