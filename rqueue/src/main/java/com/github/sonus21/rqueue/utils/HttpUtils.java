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

package com.github.sonus21.rqueue.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

@Slf4j
public final class HttpUtils {

  private HttpUtils() {}

  public static <T> T readUrl(String url, Class<T> clazz) {
    try {
      RestTemplate restTemplate = new RestTemplate();
      SimpleClientHttpRequestFactory rf =
          (SimpleClientHttpRequestFactory) restTemplate.getRequestFactory();
      rf.setReadTimeout(2 * Constants.ONE_MILLI_INT);
      rf.setConnectTimeout(2 * Constants.ONE_MILLI_INT);
      return restTemplate.getForObject(url, clazz);
    } catch (Exception e) {
      log.error("GET call failed for {}", url, e);
      return null;
    }
  }
}
