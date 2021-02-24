/*
 *  Copyright 2021 Sonu Kumar
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         https://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and limitations under the License.
 *
 */

package com.github.sonus21.rqueue.utils.pebble;

import com.mitchellbosecke.pebble.attributes.AttributeResolver;
import com.mitchellbosecke.pebble.extension.Extension;
import com.mitchellbosecke.pebble.extension.Filter;
import com.mitchellbosecke.pebble.extension.Function;
import com.mitchellbosecke.pebble.extension.NodeVisitorFactory;
import com.mitchellbosecke.pebble.extension.Test;
import com.mitchellbosecke.pebble.operator.BinaryOperator;
import com.mitchellbosecke.pebble.operator.UnaryOperator;
import com.mitchellbosecke.pebble.tokenParser.TokenParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RqueuePebbleExtension implements Extension {

  @Override
  public Map<String, Filter> getFilters() {
    return null;
  }

  @Override
  public Map<String, Test> getTests() {
    return null;
  }

  @Override
  public Map<String, Function> getFunctions() {
    Map<String, Function> map = new HashMap<>();
    map.put("dlq", new DeadLetterQueuesFunction());
    map.put("time", new DateTimeFunction());
    return map;
  }

  @Override
  public List<TokenParser> getTokenParsers() {
    return null;
  }

  @Override
  public List<BinaryOperator> getBinaryOperators() {
    return null;
  }

  @Override
  public List<UnaryOperator> getUnaryOperators() {
    return null;
  }

  @Override
  public Map<String, Object> getGlobalVariables() {
    return null;
  }

  @Override
  public List<NodeVisitorFactory> getNodeVisitors() {
    return null;
  }

  @Override
  public List<AttributeResolver> getAttributeResolver() {
    return null;
  }
}
