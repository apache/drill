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

import java.util.HashMap;
import java.util.Map;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionType;

public class TypeConverter {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TypeConverter.class);

  public static interface CustomTypeTransform {
    ColumnConversionFactory transform(ColumnMetadata inputDefn,
        Map<String, String> properties,
        ColumnMetadata outputDefn, ConversionDefn defn);
  }

  private static class NullTypeTransform implements CustomTypeTransform {
    @Override
    public ColumnConversionFactory transform(ColumnMetadata inputDefn,
        Map<String, String> properties,
        ColumnMetadata outputDefn, ConversionDefn defn) {
      return null;
    }
  }

  public static class Builder {
    private TupleMetadata providedSchema;
    private CustomTypeTransform transform;
    private Map<String, String> properties;
    private CustomErrorContext errorContext;

    public Builder providedSchema(TupleMetadata schema) {
      providedSchema = schema;
      return this;
    }

    public TupleMetadata providedSchema() { return providedSchema; }

    public Builder transform(TypeConverter.CustomTypeTransform transform) {
      this.transform = transform;
      return this;
    }

    public Builder properties(Map<String, String> properties) {
      this.properties = properties;
      return this;
    }

    public Builder setConversionProperty(String key, String value) {
      if (key == null || value == null) {
        return this;
      }
      if (properties == null) {
        properties = new HashMap<>();
      }
      properties.put(key, value);
      return this;
    }

    public Builder errorContext(CustomErrorContext errorContext) {
      this.errorContext = errorContext;
      return this;
    }

    public TypeConverter build() {
      return new TypeConverter(this);
    }
  }

  private final TupleMetadata providedSchema;
  private final CustomTypeTransform customTransform;
  private final Map<String, String> properties;
  private final CustomErrorContext errorContext;

  public static Builder builder() { return new Builder(); }

  public TypeConverter(Builder builder) {
    this.providedSchema = builder.providedSchema;
    this.customTransform = builder.transform == null ?
        new NullTypeTransform() : builder.transform;
    this.properties = builder.properties;
    this.errorContext = builder.errorContext;
  }

  public TypeConverter(TypeConverter parent,
      TupleMetadata childSchema) {
    this.providedSchema = childSchema;
    this.customTransform = parent.customTransform;
    this.properties = parent.properties;
    this.errorContext = parent.errorContext;
  }

  public TupleMetadata providedSchema() { return providedSchema; }

  public ColumnConversionFactory conversionFactory(ColumnMetadata inputSchema,
      ColumnMetadata outputCol) {
    if (outputCol == null) {
      return customConversion(inputSchema);
    } else {
      return schemaBasedConversion(inputSchema, outputCol);
    }
  }

  private ColumnConversionFactory customConversion(ColumnMetadata inputSchema) {
    return customTransform.transform(inputSchema, properties, null, null);
  }

  public ColumnConversionFactory schemaBasedConversion(ColumnMetadata inputSchema,
      ColumnMetadata outputCol) {

    // Custom transforms take priority. Allows replacing the standard
    // conversions. Also allows conversions between the same type, such
    // as rescaling units.

    ConversionDefn defn = StandardConversions.analyze(inputSchema, outputCol);
    ColumnConversionFactory factory = customTransform.transform(inputSchema, properties, outputCol, defn);
    if (factory != null) {
      return factory;
    }

    // Some conversions are automatic.

    if (defn.type != ConversionType.EXPLICIT) {
      return null;
    }

    // If an explicit conversion is needed, but no standard conversion
    // is available, we have no way to do the conversion.

    if (defn.conversionClass == null) {
      throw UserException.validationError()
        .message("Runtime type conversion not available")
        .addContext("Input type", inputSchema.typeString())
        .addContext("Output type", outputCol.typeString())
        .addContext(errorContext)
        .build(logger);
    }

    // Return a factory for the conversion.

    return StandardConversions.factory(defn.conversionClass, properties);
  }

  public TypeConverter childConverter(TupleMetadata childSchema) {
    if (childSchema == null && providedSchema == null) {
      return this;
    }
    return new TypeConverter(this, childSchema);
  }
}
