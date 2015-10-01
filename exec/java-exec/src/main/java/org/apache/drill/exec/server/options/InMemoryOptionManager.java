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

import org.apache.drill.exec.server.options.OptionValue.OptionType;

import java.util.Map;

/**
 * {@link OptionManager} that hold options in memory rather than in a persistent store. Option stored in
 * {@link SessionOptionManager}, {@link QueryOptionManager}, and {@link FragmentOptionManager} are held in memory
 * (see {@link #options}) whereas {@link SystemOptionManager} stores options in a persistent store.
 */
public abstract class InMemoryOptionManager extends FallbackOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(InMemoryOptionManager.class);

  protected final Map<String, OptionValue> options;

  InMemoryOptionManager(final OptionManager fallback, final Map<String, OptionValue> options) {
    super(fallback);
    this.options = options;
  }

  @Override
  OptionValue getLocalOption(final String name) {
    return options.get(name);
  }

  @Override
  boolean setLocalOption(final OptionValue value) {
    if (supportsOptionType(value.type)) {
      options.put(value.name, value);
      return true;
    } else {
      return false;
    }
  }

  @Override
  Iterable<OptionValue> getLocalOptions() {
    return options.values();
  }

  @Override
  boolean deleteAllLocalOptions(final OptionType type) {
    if (supportsOptionType(type)) {
      options.clear();
      return true;
    } else {
      return false;
    }
  }

  @Override
  boolean deleteLocalOption(final String name, final OptionType type) {
    if (supportsOptionType(type)) {
      options.remove(name);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Check to see if implementations of this manager support the given option type.
   *
   * @param type option type
   * @return true iff the type is supported
   */
  abstract boolean supportsOptionType(OptionType type);

}
