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
package org.apache.drill.exec.physical.rowSet.impl;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Interface to a mechanism that transforms the schema desired by
 * a reader (or other client of the result set loader) to the schema
 * desired for the output batch. Automates conversions of multiple
 * types, such as parsing a Varchar into a date or INT, etc. The actual
 * conversion policy is provided by the implementation.
 */
public interface SchemaTransformer {

  /**
   * Describes how to transform a column from input type to output type,
   * including the associated projection type
   */
  public interface ColumnTransform extends ColumnConversionFactory {
    ProjectionType projectionType();
    ColumnMetadata inputSchema();
    ColumnMetadata outputSchema();
  }

  TupleMetadata outputSchema();
  ColumnTransform transform(ColumnMetadata inputSchema, ProjectionType projType);
}
