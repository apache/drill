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
package org.apache.drill.common.util;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.google.common.base.Function;

/**
 * Collection of hooks that can be set by observers and are executed at various
 * parts of the query preparation process.
 *
 * <p>For testing and debugging rather than for end-users.</p>
 */
public enum Hook {
  /** Called with the logical plan. */
  LOGICAL_PLAN;

  private final List<Function<Object, Object>> handlers =
      new CopyOnWriteArrayList<>();

  /** Adds a handler for this Hook.
   *
   * <p>Returns a {@link Hook.Closeable} so that you can use the following
   * try-finally pattern to prevent leaks:</p>
   *
   * <blockquote><pre>
   *     final Hook.Closeable closeable = Hook.FOO.add(HANDLER);
   *     try {
   *         ...
   *     } finally {
   *         closeable.close();
   *     }</pre>
   * </blockquote>
   */
  public Closeable add(final Function handler) {
    handlers.add(handler);
    return new Closeable() {
      public void close() {
        remove(handler);
      }
    };
  }

  /** Removes a handler from this Hook. */
  private boolean remove(Function handler) {
    return handlers.remove(handler);
  }

  /** Runs all handlers registered for this Hook, with the given argument. */
  public void run(Object arg) {
    for (Function<Object, Object> handler : handlers) {
      handler.apply(arg);
    }
  }

  /** Removes a Hook after use. */
  public interface Closeable extends AutoCloseable {
    void close(); // override, removing "throws"
  }
}

// End Hook.java
