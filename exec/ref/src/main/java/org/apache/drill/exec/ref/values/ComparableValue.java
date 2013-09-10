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
package org.apache.drill.exec.ref.values;

public interface ComparableValue extends Comparable<DataValue>, DataValue {

  /**
   * Tells whether or not comparisons between the current ComparableValue and the provided DataValue are possible.
   * @param dv2 The other DataValue
   * @return True if comaprable.  False if not comparable.
   */
  public boolean supportsCompare(DataValue dv2);

  /**
   * Similar to standard comparable. However, the expectation is that supportsCompare should be called first. Likely
   * will have unexpected outcome if you don't call supportsCompare first.
   * 
   * @param other
   * @return
   */
  public int compareTo(DataValue other);

}
