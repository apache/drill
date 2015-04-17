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

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.drill.exec.rpc.user.UserSession;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

public class SessionOptionManager extends InMemoryOptionManager {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SessionOptionManager.class);

  private final UserSession session;

  /**
   * Map of short lived options. Key: option name, Value: [ start, end )
   */
  private final ConcurrentHashMap<String, ImmutablePair<Integer, Integer>> shortLivedOptions = new ConcurrentHashMap<>();

  public SessionOptionManager(final OptionManager systemOptions, final UserSession session) {
    super(systemOptions, new ConcurrentHashMap<String, OptionValue>());
    this.session = session;
  }

  @Override
  boolean setLocalOption(final OptionValue value) {
    final boolean set = super.setLocalOption(value);
    final String name = value.name;
    final OptionValidator validator = fallback.getAdmin().getValidator(name);
    final boolean shortLived = validator.isShortLived();
    if (set && shortLived) {
      final int start = session.getQueryCount() + 1; // start from the next query
      final int ttl = validator.getTtl();
      final int end = start + ttl;
      shortLivedOptions.put(name, new ImmutablePair<>(start, end));
    }
    return set;
  }

  @Override
  OptionValue getLocalOption(final String name) {
    final OptionValue value = options.get(name);
    if (shortLivedOptions.containsKey(name)) {
      if (withinRange(value)) {
        return value;
      }
      final int queryNumber = session.getQueryCount();
      final int start = shortLivedOptions.get(name).getLeft();
      // option is not in effect if queryNumber < start
      if (queryNumber < start) {
        return fallback.getAdmin().getValidator(name).getDefault();
      // reset if queryNumber <= end
      } else {
        options.remove(name);
        shortLivedOptions.remove(name);
        return null; // fallback takes effect
      }
    }
    return value;
  }

  private boolean withinRange(final OptionValue value) {
    final int queryNumber = session.getQueryCount();
    final ImmutablePair<Integer, Integer> pair = shortLivedOptions.get(value.name);
    final int start = pair.getLeft();
    final int end = pair.getRight();
    return start <= queryNumber && queryNumber < end;
  }

  private final Predicate<OptionValue> isLive = new Predicate<OptionValue>() {
    @Override
    public boolean apply(final OptionValue value) {
      final String name = value.name;
      return !shortLivedOptions.containsKey(name) || withinRange(value);
    }
  };

  @Override
  Iterable<OptionValue> optionIterable() {
    final Collection<OptionValue> liveOptions = Collections2.filter(options.values(), isLive);
    return liveOptions;
  }

  @Override
  boolean supportsOption(OptionValue value) {
    return value.type == OptionValue.OptionType.SESSION;
  }
}
