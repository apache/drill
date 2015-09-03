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

import java.util.Iterator;

import com.google.common.collect.Iterables;

/**
 * An {@link OptionManager} which allows for falling back onto another {@link OptionManager}. This way method calls can
 * be delegated to the fallback manager in case the current manager does not handle the specified option. Also, all
 * options do not need to be stored at every contextual level. For example, if an option isn't changed from its default
 * within a session, then we can get the option from system options.
 * <p/>
 * {@link FragmentOptionManager} and {@link SessionOptionManager} use {@link SystemOptionManager} as the fall back
 * manager. {@link QueryOptionManager} uses {@link SessionOptionManager} as the fall back manager.
 */
public abstract class FallbackOptionManager extends BaseOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FallbackOptionManager.class);

  protected final OptionManager fallback;

  public FallbackOptionManager(OptionManager fallback) {
    this.fallback = fallback;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return Iterables.concat(fallback, getLocalOptions()).iterator();
  }

  @Override
  public OptionValue getOption(final String name) {
    final OptionValue value = getLocalOption(name);
    if (value == null && fallback != null) {
      return fallback.getOption(name);
    } else {
      return value;
    }
  }

  /**
   * Gets the option values managed by this manager as an iterable.
   *
   * @return iterable of option values
   */
  abstract Iterable<OptionValue> getLocalOptions();

  /**
   * Gets the option value from this manager without falling back.
   *
   * @param name the option name
   * @return the option value, or null if the option does not exist locally
   */
  abstract OptionValue getLocalOption(String name);

  /**
   * Sets the option value for this manager without falling back.
   *
   * @param value the option value
   * @return true iff the value was successfully set
   */
  abstract boolean setLocalOption(OptionValue value);

  @Override
  public void setOption(OptionValue value) {
    SystemOptionManager.getValidator(value.name).validate(value); // validate the option

    // fallback if unable to set locally
    if (!setLocalOption(value)) {
      fallback.setOption(value);
    }
  }

  @Override
  public OptionList getOptionList() {
    final OptionList list = new OptionList();
    for (final OptionValue value : getLocalOptions()) {
      list.add(value);
    }
    return list;
  }
}
