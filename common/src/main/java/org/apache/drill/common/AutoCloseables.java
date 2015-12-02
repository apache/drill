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
package org.apache.drill.common;

import org.slf4j.Logger;

/**
 * Utilities for AutoCloseable classes.
 */
public class AutoCloseables {
  /**
   * Close an {@link AutoCloseable}, catching and logging any exceptions at
   * INFO level.
   *
   * <p>This can be dangerous if there is any possibility of recovery. See
   * the <a href="https://code.google.com/p/guava-libraries/issues/detail?id=1118">
   * notes regarding the deprecation of Guava's
   * {@link com.google.common.io.Closeables#closeQuietly}</a>.
   *
   * @param ac the AutoCloseable to close
   * @param logger the logger to use to record the exception if there was one
   */
  public static void close(final AutoCloseable ac, final Logger logger) {
    if (ac == null) {
      return;
    }

    try {
      ac.close();
    } catch(Exception e) {
      logger.warn("Failure on close(): {}", e);
    }
  }

  public static void close(AutoCloseable... ac) throws Exception {
    Exception topLevelException = null;
    for (AutoCloseable closeable : ac) {
      try {
        closeable.close();
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
}
