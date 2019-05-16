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

import java.util.Map;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.project.ScanLevelProjection.ScanProjectionType;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.ProjectionType;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.convert.AbstractWriteConverter;
import org.apache.drill.exec.vector.accessor.convert.ColumnConversionFactory;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions;
import org.apache.drill.exec.vector.accessor.convert.StandardConversions.ConversionDefn;

/**
 * Base class for plugin-specific type transforms. Handles basic type
 * checking. Assumes a type conversion is needed only if the output
 * column is defined and has a type or mode different than the input.
 * Else, assumes no transform is needed. Subclases can change or enhance
 * this policy. The subclass provides the actual per-column transform logic.
 * <p>
 * This class also handles setting default values for required vectors
 * when a default value is available from the column schema.
 */

public class SchemaTransformerImpl implements SchemaTransformer {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(SchemaTransformerImpl.class);

  public static abstract class AbstractColumnTransform implements ColumnTransform {

    private final ColumnMetadata inputSchema;
    private final ColumnMetadata outputSchema;
    private final ProjectionType projType;

    public AbstractColumnTransform(ColumnMetadata colDefn, ProjectionType projType,
        ColumnMetadata outputDefn) {
      inputSchema = colDefn;
      outputSchema = outputDefn;
      this.projType = projType;
    }

    @Override
    public ProjectionType projectionType() { return projType; }

    @Override
    public ColumnMetadata inputSchema() { return inputSchema; }

    @Override
    public ColumnMetadata outputSchema() { return outputSchema; }
  }

  /**
   * A no-op transform that simply keeps the input column schema and
   * writer without any changes.
   */
  public static class PassThroughColumnTransform extends AbstractColumnTransform {

    public PassThroughColumnTransform(ColumnMetadata colDefn, ProjectionType projType,
        ColumnMetadata outputDefn) {
      super(colDefn, projType, outputDefn);
    }

    @Override
    public AbstractWriteConverter newWriter(ScalarWriter baseWriter) {
      return null;
    }
  }

  /**
   * Full column transform that has separate input and output types
   * and provides a type conversion writer to convert between the
   * two. The conversion writer factory is provided via composition,
   * not by subclassing this class.
   */
  public static class ColumnSchemaTransform extends AbstractColumnTransform {

    private final ColumnConversionFactory conversionFactory;

    public ColumnSchemaTransform(ColumnMetadata inputSchema, ColumnMetadata outputSchema,
        ProjectionType projType, ColumnConversionFactory conversionFactory) {
      super(inputSchema, projType, outputSchema);
      this.conversionFactory = conversionFactory;
    }

    @Override
    public AbstractWriteConverter newWriter(ScalarWriter baseWriter) {
      if (conversionFactory == null) {
        return null;
      }
      return conversionFactory.newWriter(baseWriter);
    }
  }

  protected final TupleMetadata outputSchema;
  protected final Map<String, String> properties;

  public SchemaTransformerImpl(TupleMetadata outputSchema,
      Map<String, String> properties) {
    this.outputSchema = outputSchema;
    this.properties = properties;
  }

  /**
   * Implement a basic policy to pass through input columns for which there
   * is no matching output column, and to do a type conversion only if types
   * and modes differ.
   * <p>
   * Subclasses can change this behavior if, say, they want to do conversion
   * even if the types are the same (such as parsing a VARCHAR field to produce
   * another VARCHAR.)
   */
  @Override
  public ColumnTransform transform(ColumnMetadata inputSchema,
      ProjectionType projType) {

    // Should never get an unprojected column; should be handled
    // by the caller.

    assert projType != ProjectionType.UNPROJECTED;

    // If no matching column, assume a pass-through transform

    ColumnMetadata outputCol = outputSchema.metadata(inputSchema.name());
    if (outputCol == null) {
      return new PassThroughColumnTransform(inputSchema, projType, inputSchema);
    }

    ConversionDefn defn = StandardConversions.analyze(inputSchema, outputCol);
    ColumnConversionFactory factory = customTransform(inputSchema, outputCol, defn);
    if (factory == null) {
      switch (defn.type) {
      case NONE:
      case IMPLICIT:
        return new PassThroughColumnTransform(inputSchema, projType, outputCol);
      case EXPLICIT:
        if (defn.conversionClass == null) {
          throw UserException.validationError()
            .message("Runtime type conversion not available")
            .addContext("Column:", outputCol.name())
            .addContext("Input type:", inputSchema.typeString())
            .addContext("Output type:", outputCol.typeString())
            .build(logger);
        }
        factory = StandardConversions.factory(defn.conversionClass, properties);
        break;
      default:
        throw new IllegalStateException("Unexpected conversion type: " + defn.type);
      }
    }
    return new ColumnSchemaTransform(inputSchema, outputCol, projType, factory);
  }

  /**
   * Overridden to provide a custom conversion between input an output types.
   *
   * @param inputDefn the column schema for the input column which the
   * client code (e.g. reader) wants to produce
   * @param outputDefn the column schema for the output vector to be produced
   * by this operator
   * @param defn a description of the required conversion. This method is
   * required to do nothing of conversion type is
   * {@link ScanProjectionType.EXPLICIT} and the conversion class is null, meaning
   * that no standard conversion is available
   * @return a column transformer factory to implement a custom conversion,
   * or null to use the standard conversion
   */
  private ColumnConversionFactory customTransform(ColumnMetadata inputDefn,
      ColumnMetadata outputDefn, ConversionDefn defn) {
    return null;
  }

  @Override
  public TupleMetadata outputSchema() { return outputSchema; }
}
