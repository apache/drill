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
package org.apache.drill.exec.store.paimon.read;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.PathSegment;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.framework.SchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.ops.OperatorStats;
import org.apache.drill.exec.record.ColumnConverter;
import org.apache.drill.exec.record.ColumnConverterFactory;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.store.paimon.PaimonTableUtils;
import org.apache.drill.exec.store.paimon.PaimonWork;
import org.apache.drill.exec.store.paimon.PaimonReadUtils;
import org.apache.drill.exec.store.paimon.format.PaimonFormatPlugin;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class PaimonRecordReader implements ManagedReader<SchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(PaimonRecordReader.class);

  private final PaimonFormatPlugin formatPlugin;

  private final String path;

  private final List<SchemaPath> columns;

  private final LogicalExpression condition;

  private final PaimonWork work;

  private final int maxRecords;

  private ResultSetLoader loader;

  private ColumnConverter[] converters;

  private InternalRow.FieldGetter[] getters;

  private RecordReader<InternalRow> recordReader;

  private RecordReader.RecordIterator<InternalRow> currentBatch;

  private OperatorStats stats;

  private int lastSchemaVersion = -1;

  public PaimonRecordReader(PaimonFormatPlugin formatPlugin, String path,
    List<SchemaPath> columns, LogicalExpression condition, PaimonWork work, int maxRecords) {
    this.formatPlugin = formatPlugin;
    this.path = path;
    this.columns = columns;
    this.condition = condition;
    this.work = work;
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean open(SchemaNegotiator negotiator) {
    try {
      Table table = PaimonTableUtils.loadTable(formatPlugin, path);
      RowType rowType = table.rowType();
      ReadBuilder readBuilder = table.newReadBuilder();
      PaimonReadUtils.applyFilter(readBuilder, rowType, condition);
      PaimonReadUtils.applyProjection(readBuilder, rowType, columns);
      RowType readType = readBuilder.readType();

      TupleSchema tableSchema = PaimonColumnConverterFactory.convertSchema(readType);
      TupleMetadata providedSchema = negotiator.providedSchema();
      TupleMetadata tupleSchema = FixedReceiver.Builder.mergeSchemas(providedSchema, tableSchema);
      negotiator.tableSchema(tupleSchema, true);
      loader = negotiator.build();

      converters = buildConverters(providedSchema, tableSchema);
      getters = buildGetters(readType);

      recordReader = readBuilder.newRead().executeFilter().createReader(work.getSplit());
      currentBatch = null;
      stats = negotiator.context().getStats();
      return true;
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Failed to open Paimon reader for %s", path)
        .build(logger);
    }
  }

  @Override
  public boolean next() {
    RowSetLoader rowWriter = loader.writer();
    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        updateStats(rowWriter);
        return false;
      }
    }
    updateStats(rowWriter);
    return true;
  }

  @Override
  public void close() {
    if (currentBatch != null) {
      currentBatch.releaseBatch();
    }
    AutoCloseables.closeSilently(recordReader);
    if (loader != null) {
      loader.close();
    }
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    if (rowWriter.limitReached(maxRecords)) {
      return false;
    }

    InternalRow row = nextRow();
    if (row == null) {
      return false;
    }

    rowWriter.start();
    for (int i = 0; i < getters.length; i++) {
      converters[i].convert(getters[i].getFieldOrNull(row));
    }
    rowWriter.save();

    return true;
  }

  private InternalRow nextRow() {
    try {
      while (true) {
        if (currentBatch == null) {
          currentBatch = recordReader.readBatch();
          if (currentBatch == null) {
            return null;
          }
        }
        InternalRow row = currentBatch.next();
        if (row != null) {
          return row;
        }
        currentBatch.releaseBatch();
        currentBatch = null;
      }
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .message("Failed to read Paimon data for %s", path)
        .build(logger);
    }
  }

  private void updateStats(RowSetLoader rowWriter) {
    if (stats == null) {
      return;
    }
    int rowCount = rowWriter.rowCount();
    if (rowCount == 0) {
      return;
    }
    int schemaVersion = loader.schemaVersion();
    boolean isNewSchema = schemaVersion != lastSchemaVersion;
    lastSchemaVersion = schemaVersion;
    stats.batchReceived(0, rowCount, isNewSchema);
  }

  private ColumnConverter[] buildConverters(TupleMetadata providedSchema, TupleSchema tableSchema) {
    ColumnConverterFactory factory = new PaimonColumnConverterFactory(providedSchema);
    ColumnConverter[] columnConverters = new ColumnConverter[tableSchema.size()];
    for (int i = 0; i < tableSchema.size(); i++) {
      ColumnMetadata columnMetadata = tableSchema.metadata(i);
      columnConverters[i] = factory.getConverter(providedSchema, columnMetadata,
        loader.writer().column(columnMetadata.name()));
    }
    return columnConverters;
  }

  private InternalRow.FieldGetter[] buildGetters(RowType readType) {
    List<DataField> fields = readType.getFields();
    InternalRow.FieldGetter[] fieldGetters = new InternalRow.FieldGetter[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      fieldGetters[i] = InternalRow.createFieldGetter(fields.get(i).type(), i);
    }
    return fieldGetters;
  }
}
