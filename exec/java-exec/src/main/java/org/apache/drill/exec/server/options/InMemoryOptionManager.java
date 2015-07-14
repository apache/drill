/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.server.options;

import java.util.Map;

public abstract class InMemoryOptionManager extends FallbackOptionManager {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InMemoryOptionManager.class);

  final Map<String, OptionValue> options;

  InMemoryOptionManager(OptionManager fallback, Map<String, OptionValue> options) {
    super(fallback);
    this.options = options;
  }

  @Override
  OptionValue getLocalOption(String name) {
    return options.get(name);
  }

  @Override
  boolean setLocalOption(OptionValue value) {
    if(supportsOption(value)){
      options.put(value.name, value);
      return true;
    }else{
      return false;
    }

  }

  @Override
  Iterable<OptionValue> optionIterable() {
    return options.values();
  }

  /**
   * Check (e.g. option type) to see if implementations of this manager support this option.
   *
   * @param value the option value
   * @return true iff the option value is supported
   */
  abstract boolean supportsOption(OptionValue value);

}
