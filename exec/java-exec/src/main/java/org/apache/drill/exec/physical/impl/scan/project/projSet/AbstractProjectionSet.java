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

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.exec.physical.resultSet.ProjectionSet;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;

/**
 * Base class for projection set implementations. Handles an optional
 * type conversion based on a provided schema, custom conversion, or both.
 */

public abstract class AbstractProjectionSet implements ProjectionSet {
  protected final TypeConverter typeConverter;
  protected final TupleMetadata providedSchema;
  protected final boolean isStrict;
  protected CustomErrorContext errorContext;

  public AbstractProjectionSet(TypeConverter typeConverter) {
    this.typeConverter = typeConverter;
    providedSchema = typeConverter == null ? null :
        typeConverter.providedSchema();
    isStrict = providedSchema != null &&
        typeConverter.providedSchema().booleanProperty(TupleMetadata.IS_STRICT_SCHEMA_PROP);
  }

  public AbstractProjectionSet(TypeConverter typeConverter, boolean isStrict) {
    this.typeConverter = typeConverter;
    providedSchema = typeConverter == null ? null :
        typeConverter.providedSchema();
    this.isStrict = isStrict;
  }

  public AbstractProjectionSet() {
    this(null);
  }

  @Override
  public void setErrorContext(CustomErrorContext errorContext) {
    this.errorContext = errorContext;
  }

  protected static boolean isSpecial(ColumnMetadata col) {
    return col.booleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD);
  }

  protected ColumnMetadata outputSchema(ColumnMetadata col) {
    return providedSchema == null ? null :
      providedSchema.metadata(col.name());
  }

  protected ColumnConversionFactory conversion(ColumnMetadata inputSchema, ColumnMetadata outputCol) {
    return typeConverter == null ? null :
      typeConverter.conversionFactory(inputSchema, outputCol);
  }

  protected TypeConverter childConverter(ColumnMetadata outputSchema) {
    TupleMetadata childSchema = outputSchema == null ? null : outputSchema.tupleSchema();
    return typeConverter == null ? null :
      typeConverter.childConverter(childSchema);
  }
}
