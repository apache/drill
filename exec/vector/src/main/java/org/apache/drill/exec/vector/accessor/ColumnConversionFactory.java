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

import org.apache.drill.exec.record.metadata.ColumnMetadata;

/**
 * Create a column type converter for the given column and base writer.
 * The new writer is expected to be a "shim" writer that implements
 * additional "set" methods to convert data from the type that the
 * client requires to the type required by the underlying vector as
 * represented by the base writer.
 */
public interface ColumnConversionFactory {
  /**
   * Create a type conversion writer for the given column, converting data
   * to the type needed by the base writer.
   * @param colDefn column metadata definition
   * @param baseWriter base column writer for the column's vector
   * @return a new scalar writer to insert between the client and
   * the base vector
   */
  ScalarWriter newWriter(ColumnMetadata colDefn, ScalarWriter baseWriter);
}
