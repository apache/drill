/*
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
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.common.map.CaseInsensitiveMap;
import org.apache.drill.exec.rpc.user.UserSession;

/**
 * {@link OptionManager} that holds options within {@link org.apache.drill.exec.rpc.user.UserSession} context. Options
 * set at the session level only apply to queries that you run during the current Drill connection. Session level
 * settings override system level settings.
 *
 * NOTE that currently, the effects of deleting a short lived option (see {@link OptionValidator#isShortLived}) are
 * undefined. For example, we inject an exception (passed through an option), then try to delete the option, depending
 * on where the exception was injected, the reset query could either succeed or the exception could actually be thrown
 * in the reset query itself.
 */
public class SessionOptionManager extends InMemoryOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SessionOptionManager.class);

  private final UserSession session;

  /**
   * Map of short lived options. Key: option name, Value: [ start, end )
   */
  private final Map<String, ImmutablePair<Integer, Integer>> shortLivedOptions =
    CaseInsensitiveMap.newConcurrentMap();

  public SessionOptionManager(OptionManager systemOptions, UserSession session) {
    super(systemOptions, CaseInsensitiveMap.newConcurrentMap());
    this.session = session;
  }

  @Override
  public void setLocalOptionHelper(OptionValue value) {
    super.setLocalOptionHelper(value);
    String name = value.name;
    OptionDefinition definition = getOptionDefinition(name); // if set, validator must exist.
    OptionValidator validator = definition.getValidator();
    boolean shortLived = validator.isShortLived();
    if (shortLived) {
      int start = session.getQueryCount() + 1; // start from the next query
      int ttl = validator.getTtl();
      int end = start + ttl;
      shortLivedOptions.put(name, new ImmutablePair<>(start, end));
    }
  }

  @Override
  OptionValue getLocalOption(String name) {
    OptionValue value = super.getLocalOption(name);
    if (shortLivedOptions.containsKey(name)) {
      if (withinRange(name)) {
        return value;
      }
      int queryNumber = session.getQueryCount();
      int start = shortLivedOptions.get(name).getLeft();
      // option is not in effect if queryNumber < start
      if (queryNumber < start) {
        return fallback.getOption(name);
      // reset if queryNumber <= end
      } else {
        options.remove(name);
        shortLivedOptions.remove(name);
        return null; // fallback takes effect
      }
    }
    return value;
  }

  private boolean withinRange(String name) {
    int queryNumber = session.getQueryCount();
    ImmutablePair<Integer, Integer> pair = shortLivedOptions.get(name);
    int start = pair.getLeft();
    int end = pair.getRight();
    return start <= queryNumber && queryNumber < end;
  }

  private final Predicate<OptionValue> isLive = value -> {
    String name = value.name;
    return !shortLivedOptions.containsKey(name) || withinRange(name);
  };

  @Override
  Iterable<OptionValue> getLocalOptions() {
    return options.values().stream()
        .filter(isLive)
        .collect(Collectors.toList());
  }

  /**
   * Gets the SystemOptionManager.
   * @return The SystemOptionManager.
   */
  public SystemOptionManager getSystemOptionManager() {
    return (SystemOptionManager) fallback;
  }

  @Override
  public OptionValue getDefault(String optionName) {
    return fallback.getDefault(optionName);
  }

  @Override
  protected OptionValue.OptionScope getScope() {
    return OptionValue.OptionScope.SESSION;
  }
}
