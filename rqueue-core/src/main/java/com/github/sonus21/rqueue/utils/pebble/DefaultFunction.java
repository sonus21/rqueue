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

package com.github.sonus21.rqueue.utils.pebble;

import io.pebbletemplates.pebble.extension.Function;
import io.pebbletemplates.pebble.template.EvaluationContext;
import io.pebbletemplates.pebble.template.PebbleTemplate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DefaultFunction implements Function {

  public static final String FUNCTION_NAME = "default";

  @Override
  public Object execute(
      Map<String, Object> args, PebbleTemplate self, EvaluationContext context, int lineNumber) {
    Object src = args.get("src");
    Object dst = args.get("dst");
    if (src == null) {
      return dst;
    }
    return src;
  }

  @Override
  public List<String> getArgumentNames() {
    return Arrays.asList("src", "dst");
  }
}
