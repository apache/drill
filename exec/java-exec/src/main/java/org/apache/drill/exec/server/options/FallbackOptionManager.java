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
import org.apache.drill.exec.server.options.OptionValue.OptionType;

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
    /**
     * TODO(DRILL-2097): Add a Preconditions.checkNotNull(fallback, "A fallback manager must be provided.") and remove
     * the null check in {@link #getOption(String)}. This is not added currently only to avoid modifying the long list
     * of test files.
     */
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

  /**
   * Deletes all options for this manager without falling back.
   *
   * If no options are set, calling this method should be no-op. See {@link OptionManager#deleteAllOptions}.
   *
   * @param type option type
   * @return true iff the option type is supported
   */
  abstract boolean deleteAllLocalOptions(OptionType type);

  /**
   * Deletes the option with given name for this manager without falling back.
   *
   * This method will be called with an option name that is guaranteed to have an option validator. Also, if option
   * with {@param name} does not exist within the manager, calling this method should be a no-op. See
   * {@link OptionManager#deleteOption}.
   *
   * @param name option name
   * @param type option type
   * @return true iff the option type is supported
   */
  abstract boolean deleteLocalOption(String name, OptionType type);

  @Override
  public void setOption(OptionValue value) {
    final OptionValidator validator = SystemOptionManager.getValidator(value.name);

    validator.validate(value, this); // validate the option

    // fallback if unable to set locally
    if (!setLocalOption(value)) {
      fallback.setOption(value);
    }
  }

  @Override
  public void deleteOption(final String name, final OptionType type) {
    SystemOptionManager.getValidator(name); // ensure the option exists

    // fallback if unable to delete locally
    if (!deleteLocalOption(name, type)) {
      fallback.deleteOption(name, type);
    }
  }

  @Override
  public void deleteAllOptions(final OptionType type) {
    // fallback if unable to delete locally
    if (!deleteAllLocalOptions(type)) {
      fallback.deleteAllOptions(type);
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
