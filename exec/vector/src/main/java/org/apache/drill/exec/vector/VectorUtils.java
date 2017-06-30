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
package org.apache.drill.exec.vector;

public class VectorUtils {

  /**
   * Vectors cannot be any larger than the Netty memory allocation
   * block size.
   */

  private static final int ABSOLUTE_MAX_SIZE = 16 * 1024 * 1024;

  /**
   * Minimum size selected to prevent pathological performance if vectors
   * are limited to an unusably small size. This limit is a judgment call,
   * not based on any known limits.
   */

  private static final int ABSOLUTE_MIN_SIZE = 16 * 1024;

  private VectorUtils() { }

  /**
   * Static function called once per run to compute the maximum
   * vector size, in bytes. Normally uses the hard-coded limit,
   * but allows setting a system property to override the limit
   * for testing. The configured value must be within reasonable
   * bounds.
   * @return the maximum vector size, in bytes
   */

  static int maxSize() {
    String prop = System.getProperty( ValueVector.MAX_BUFFER_SIZE_KEY );
    int value = ABSOLUTE_MAX_SIZE;
    if (prop != null) {
      try {
        value = Integer.parseInt(prop);
        value = Math.max(value, ABSOLUTE_MIN_SIZE);
        value = Math.min(value, ABSOLUTE_MAX_SIZE);
      } catch (NumberFormatException e) {
        // Ignore
      }
    }
    return value;
  }

}
