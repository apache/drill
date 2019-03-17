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
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.convert.AbstractWriteConverter;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Default schema transformer that maps input types to output types and
 * simply passes along the input schema and projection type. Provides
 * support for an ad-hoc column conversion factory (to create type
 * conversion shims), such as those used in unit tests.
 */
public class DefaultSchemaTransformer implements SchemaTransformer {

  public class DefaultColumnTransformer implements ColumnTransform {

    private final ColumnMetadata columnSchema;
    private final ProjectionType projType;

    public DefaultColumnTransformer(ColumnMetadata inputSchema, ProjectionType projType) {
      columnSchema = inputSchema;
      this.projType = projType;
    }

    @Override
    public AbstractWriteConverter newWriter(ScalarWriter baseWriter) {
      if (conversionFactory == null) {
        return null;
      }
      return conversionFactory.newWriter(baseWriter);
    }

    @Override
    public ProjectionType projectionType() { return projType; }

    @Override
    public ColumnMetadata inputSchema() { return columnSchema; }

    @Override
    public ColumnMetadata outputSchema() { return columnSchema; }
  }

  private final ColumnConversionFactory conversionFactory;

  public DefaultSchemaTransformer(ColumnConversionFactory conversionFactory) {
    this.conversionFactory = conversionFactory;
  }

  @Override
  public ColumnTransform transform(ColumnMetadata inputSchema,
      ProjectionType projType) {
    return new DefaultColumnTransformer(inputSchema, projType);
  }

  @Override
  public TupleMetadata outputSchema() { return null; }
}
