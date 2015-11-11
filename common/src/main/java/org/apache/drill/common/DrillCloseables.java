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

import java.io.Closeable;
import java.io.IOException;

/**
 * Provides additional functionality to Guava's Closeables.
 */
public class DrillCloseables {
  /**
   * Constructor. Prevents construction for class of static utilities.
   */
  private DrillCloseables() {
  }

  /**
   * Close() a {@see java.io.Closeable} without throwing a (checked)
   * {@see java.io.IOException}. This wraps the close() call with a
   * try-catch that will rethrow an IOException wrapped with a
   * {@see java.lang.RuntimeException}, providing a way to call close()
   * without having to do the try-catch everywhere or propagate the IOException.
   *
   * <p>Guava has deprecated {@see com.google.common.io.Closeables.closeQuietly()}
   * as described in
   * {@link https://code.google.com/p/guava-libraries/issues/detail?id=1118}.
   *
   * @param closeable the Closeable to close
   * @throws RuntimeException if an IOException occurs; the IOException is
   *   wrapped by the RuntimeException
   */
  public static void closeNoChecked(final Closeable closeable) {
    try {
      closeable.close();
    } catch(final IOException e) {
      throw new RuntimeException("IOException while closing", e);
    }
  }
}
