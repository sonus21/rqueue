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

package com.github.sonus21.rqueue.utils;

import com.github.sonus21.rqueue.config.RqueueConfig;
import java.net.InetSocketAddress;
import java.net.Proxy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Slf4j
public final class HttpUtils {

  private HttpUtils() {
  }

  private static SimpleClientHttpRequestFactory getRequestFactory(RqueueConfig rqueueConfig) {
    SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
    requestFactory.setReadTimeout(2 * Constants.ONE_MILLI_INT);
    requestFactory.setConnectTimeout(2 * Constants.ONE_MILLI_INT);
    if (StringUtils.isEmpty(rqueueConfig.getProxyHost())) {
      return requestFactory;
    }
    Proxy proxy =
        new Proxy(
            rqueueConfig.getProxyType(),
            new InetSocketAddress(rqueueConfig.getProxyHost(), rqueueConfig.getProxyPort()));
    requestFactory.setProxy(proxy);
    return requestFactory;
  }

  public static <T> T readUrl(RqueueConfig rqueueConfig, String url, Class<T> clazz) {
    try {
      RestTemplate restTemplate = new RestTemplate(getRequestFactory(rqueueConfig));
      return restTemplate.getForObject(url, clazz);
    } catch (Exception e) {
      log.error("GET call failed for {}", url, e);
      return null;
    }
  }

  public static String joinPath(String... components) {
    StringBuilder sb = new StringBuilder();
    for (String comp : components) {
      if (StringUtils.isEmpty(comp) || comp.equals(Constants.FORWARD_SLASH)) {
        continue;
      }
      sb.append(Constants.FORWARD_SLASH);
      if (comp.endsWith(Constants.FORWARD_SLASH) && comp.startsWith(Constants.FORWARD_SLASH)) {
        sb.append(comp, 1, comp.length() - 1);
      } else if (comp.endsWith(Constants.FORWARD_SLASH)) {
        sb.append(comp, 0, comp.length() - 1);
      } else if (comp.startsWith(Constants.FORWARD_SLASH)) {
        sb.append(comp.substring(1));
      } else {
        sb.append(comp);
      }
    }
    sb.append(Constants.FORWARD_SLASH);
    return sb.toString();
  }
}
