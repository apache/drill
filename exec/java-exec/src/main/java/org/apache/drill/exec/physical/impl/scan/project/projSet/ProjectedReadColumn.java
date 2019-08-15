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
package org.apache.drill.exec.physical.impl.scan.project.projSet;

import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.physical.resultSet.project.ProjectionType;
import org.apache.drill.exec.physical.resultSet.project.RequestedTuple.RequestedColumn;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Projected column. Includes at least the reader schema. May also
 * include projection specification, and output schema and a type
 * conversion.
 */

public class ProjectedReadColumn extends AbstractReadColProj {
  private final RequestedColumn requestedCol;
  private final ColumnMetadata outputSchema;
  private final ColumnConversionFactory conversionFactory;

  public ProjectedReadColumn(ColumnMetadata readSchema) {
    this(readSchema, null, null, null);
  }

  public ProjectedReadColumn(ColumnMetadata readSchema,
      RequestedColumn requestedCol) {
    this(readSchema, requestedCol, null, null);
  }

  public ProjectedReadColumn(ColumnMetadata readSchema,
      ColumnMetadata outputSchema, ColumnConversionFactory conversionFactory) {
    this(readSchema, null, outputSchema, null);
  }

  public ProjectedReadColumn(ColumnMetadata readSchema,
      RequestedColumn requestedCol, ColumnMetadata outputSchema,
      ColumnConversionFactory conversionFactory) {
    super(readSchema);
    this.requestedCol = requestedCol;
    this.outputSchema = outputSchema;
    this.conversionFactory = conversionFactory;
  }

  @Override
  public ColumnMetadata providedSchema() {
    return outputSchema == null ? readSchema : outputSchema;
  }

  @Override
  public ProjectionSet mapProjection() {
    // Should never occur: maps should use the map class.
    return null;
  }

  @Override
  public ProjectionType projectionType() {
    return requestedCol == null ? null : requestedCol.type();
  }

  @Override
  public ColumnConversionFactory conversionFactory() { return conversionFactory; }
}
