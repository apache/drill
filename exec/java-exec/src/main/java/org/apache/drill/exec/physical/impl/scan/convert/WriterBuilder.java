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
package org.apache.drill.exec.physical.impl.scan.convert;

import java.util.Map;
import java.util.Objects;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.EmptyErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.ValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds scalar writers, applying standard column conversions
 * if a provided schema is given. Use this class for the simple cases
 * where the input is a Drill-compatible Java type and the conversions
 * are well-defined. Not suitable for conversions from specialized types
 * (binary, data structure) or for specialized conversions.
 */
public abstract class WriterBuilder {
  private static final Logger logger = LoggerFactory.getLogger(WriterBuilder.class);

  protected final TupleMetadata providedSchema;
  private final StandardConversions conversions;
  protected MinorType defaultType;
  protected DataMode defaultMode;
  protected CustomErrorContext errorContext;

  public WriterBuilder(TupleMetadata providedSchema,
      Map<String, String> properties) {
    this.providedSchema = providedSchema;
    this.conversions = StandardConversions.builder()
        .withSchema(providedSchema)
        .withProperties(properties)
        .build();
  }

  public WriterBuilder defaultType(MinorType type) {
    this.defaultType = type;
    return this;
  }

  public WriterBuilder defaultMode(DataMode mode) {
    this.defaultMode = mode;
    return this;
  }

  public WriterBuilder errorContext(CustomErrorContext errorContext) {
    this.errorContext = errorContext;
    return this;
  }

  public CustomErrorContext errorContext() {
    return errorContext == null ? EmptyErrorContext.INSTANCE : errorContext;
  }

  public ValueWriter writerFor(ColumnMetadata readerCol) {
    if (!MetadataUtils.isScalar(readerCol)) {
      throw UserException.internalError()
          .message("WriterBuilder only works with scalar columns, reader column is not scalar")
          .addContext("Column name", readerCol.name())
          .addContext("Column type", readerCol.type().name())
          .addContext(errorContext())
          .build(logger);
    }
    if (!rowWriter().isProjected(readerCol.name())) {
      return makeColumn(readerCol);
    }
    ColumnMetadata providedCol = providedCol(readerCol.name());
    if (providedCol == null) {
      return makeColumn(readerCol);
    }
    if (!MetadataUtils.isScalar(providedCol)) {
      throw UserException.validationError()
        .message("WriterBuilder only works with scalar columns, provided column is not scalar")
        .addContext("Provided column name", providedCol.name())
        .addContext("Provided column type", providedCol.type().name())
        .addContext(errorContext())
        .build(logger);
    }
    if (!compatibleModes(readerCol.mode(), providedCol.mode())) {
      throw UserException.validationError()
        .message("Reader and provided columns have incompatible cardinality")
        .addContext("Column name", providedCol.name())
        .addContext("Provided column mode", providedCol.mode().name())
        .addContext("Reader column mode", readerCol.mode().name())
        .addContext(errorContext())
        .build(logger);
    }
    ScalarWriter colWriter = makeColumn(providedCol);
    return conversions.converterFor(colWriter, readerCol.type());
  }

  private boolean compatibleModes(DataMode source, DataMode dest) {
    return source == dest ||
           dest == DataMode.OPTIONAL && source == DataMode.REQUIRED;
  }

  protected ColumnMetadata providedCol(String name) {
    return providedSchema == null ? null : providedSchema.metadata(name);
  }

  protected ScalarWriter makeColumn(ColumnMetadata colSchema) {
    int posn = rowWriter().addColumn(colSchema);
    return rowWriter().scalar(posn);
  }

  protected abstract RowSetLoader rowWriter();

  public ValueWriter writerFor(String colName) {
    return writerFor(columnSchema(colName));
  }

  protected ColumnMetadata columnSchema(String colName) {
    return MetadataUtils.newScalar(colName,
        Objects.requireNonNull(defaultType),
        Objects.requireNonNull(defaultMode));
  }

  public static class EarlySchemaWriterBuilder extends WriterBuilder {

    public EarlySchemaWriterBuilder(TupleMetadata providedSchema) {
      super(providedSchema, null);
    }

    public EarlySchemaWriterBuilder(TupleMetadata providedSchema,
        Map<String, String> properties) {
      super(providedSchema, properties);
    }

    private RowSetLoader rowWriter;
    private TupleMetadata tableSchema;
    private TupleMetadata readerSchema;

    public void mergeReaderSchema(TupleMetadata readerSchema) {
      this.tableSchema = readerSchema;
      if (providedSchema == null) {
        this.readerSchema = readerSchema;
      } else {
        this.readerSchema = FixedReceiver.Builder.mergeSchemas(
            providedSchema, readerSchema);
      }
    }

    public void mergeColumn(String colName) {
      mergeColumn(columnSchema(colName));
    }

    public void mergeColumn(ColumnMetadata readerCol) {
      if (tableSchema == null) {
        tableSchema = new TupleSchema();
      }
      tableSchema.addColumn(readerCol);
      if (readerSchema == null) {
        readerSchema = new TupleSchema();
      }
      final ColumnMetadata providedCol = providedCol(readerCol.name());
      readerSchema.addColumn(providedCol == null ? readerCol : providedCol);
    }

    public TupleMetadata readerSchema() { return readerSchema; }

    @Override
    protected ColumnMetadata columnSchema(String colName) {
      ColumnMetadata colSchema = tableSchema == null ? null : tableSchema.metadata(colName);
      return colSchema == null ? super.columnSchema(colName) : colSchema;
    }

    @Override
    protected ScalarWriter makeColumn(ColumnMetadata colSchema) {
      ScalarWriter colWriter = rowWriter.scalar(colSchema.name());
      if (colWriter == null) {
        return super.makeColumn(colSchema);
      } else {
        return colWriter;
      }
    }

    public WriterBuilder rowWriter(RowSetLoader rowWriter) {
      this.rowWriter = rowWriter;
      return this;
    }

    @Override
    protected RowSetLoader rowWriter() {
      return rowWriter;
    }
  }

  public static class LateSchemaWriterBuilder extends WriterBuilder {

    private final RowSetLoader rowWriter;

    public LateSchemaWriterBuilder(RowSetLoader rowWriter, TupleMetadata providedSchema) {
      super(providedSchema, null);
      this.rowWriter = rowWriter;
    }

    @Override
    protected RowSetLoader rowWriter() {
      return rowWriter;
    }
  }
}
