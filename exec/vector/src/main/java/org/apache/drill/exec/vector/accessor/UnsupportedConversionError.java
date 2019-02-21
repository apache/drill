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
 * Raised when a column accessor reads or writes the value using the wrong
 * Java type (which may indicate an data inconsistency in the input data.)
 */

public class UnsupportedConversionError extends UnsupportedOperationException {

  private static final long serialVersionUID = 1L;

  public UnsupportedConversionError(String message) {
    super(message);
  }

  public static UnsupportedConversionError readError(ColumnMetadata schema, String javaType) {
    return new UnsupportedConversionError(
        String.format("Column `%s`: Unsupported conversion from Drill type %s to Java type %s",
            schema.name(), schema.type().name(), javaType));
  }

  public static UnsupportedConversionError writeError(ColumnMetadata schema, String javaType) {
    return new UnsupportedConversionError(
        String.format("Column `%s`: Unsupported conversion from Java type %s to Drill type %s",
            schema.name(), schema.type().name(), javaType));
  }

  public static UnsupportedConversionError nullError(ColumnMetadata schema) {
    return new UnsupportedConversionError(
        String.format("Column `%s`: Type %s %s is not nullable",
            schema.name(), schema.mode().name(), schema.type().name()));
  }
}
