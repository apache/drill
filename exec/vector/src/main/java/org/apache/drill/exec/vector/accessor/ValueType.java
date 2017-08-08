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
package org.apache.drill.exec.vector.accessor;

/**
 * Represents the primitive types supported to read and write data
 * from value vectors. Vectors support many data widths. For simplicity
 * (and because of no difference in performance), the get/set methods
 * use a reduced set of types. In general, each reader and writer
 * supports just one type. Though some may provide more than one
 * (such as access to bytes for a <tt>STRING</tt> value.)
 */

public enum ValueType {
  INTEGER, LONG, DOUBLE, STRING, BYTES, DECIMAL, PERIOD
}
