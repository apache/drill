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
package org.apache.drill.common;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for AutoCloseable classes.
 */
public class AutoCloseables {

  private static final Logger LOGGER = LoggerFactory.getLogger(AutoCloseables.class);

  public interface Closeable extends AutoCloseable {
    @Override
    void close();
  }

  public static AutoCloseable all(final Collection<? extends AutoCloseable> autoCloseables) {
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        AutoCloseables.close(autoCloseables);
      }
    };
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t
   * @param t the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, AutoCloseable... autoCloseables) {
    close(t, Arrays.asList(autoCloseables));
  }

  /**
   * Closes all autoCloseables if not null and suppresses exceptions by adding them to t
   * @param t the throwable to add suppressed exception to
   * @param autoCloseables the closeables to close
   */
  public static void close(Throwable t, Collection<? extends AutoCloseable> autoCloseables) {
    try {
      close(autoCloseables);
    } catch (Exception e) {
      t.addSuppressed(e);
    }
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one
   * @param autoCloseables the closeables to close
   */
  public static void close(AutoCloseable... autoCloseables) throws Exception {
    close(Arrays.asList(autoCloseables));
  }

  /**
   * Closes all autoCloseables if not null and suppresses subsequent exceptions if more than one
   * @param autoCloseables the closeables to close
   */
  public static void close(Iterable<? extends AutoCloseable> ac) throws Exception {
    Exception topLevelException = null;
    for (AutoCloseable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else {
          topLevelException.addSuppressed(e);
        }
      }
    }
    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  /**
   * Close all without caring about thrown exceptions
   * @param closeables - array containing auto closeables
   */
  public static void closeSilently(AutoCloseable... closeables) {
    Arrays.stream(closeables).filter(Objects::nonNull)
        .forEach(target -> {
          try {
            target.close();
          } catch (Exception e) {
            LOGGER.warn(String.format("Exception was thrown while closing auto closeable: %s", target), e);
          }
        });
  }

}
