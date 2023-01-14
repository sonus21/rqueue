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

package com.github.sonus21.rqueue.utils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public final class SerializationUtils {

  public static final byte[] EMPTY_ARRAY = new byte[0];

  private SerializationUtils() {
  }

  public static boolean isEmpty(byte[] bytes) {
    return bytes == null || bytes.length == 0;
  }

  public static boolean isJson(String data) {
    return !StringUtils.isEmpty(data)
        && data.charAt(0) == '{'
        && data.charAt(data.length() - 1) == '}';
  }

  public static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper = mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }
}
