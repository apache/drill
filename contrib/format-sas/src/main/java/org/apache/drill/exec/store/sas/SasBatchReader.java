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

package org.apache.drill.exec.store.sas;

import com.epam.parso.Column;
import com.epam.parso.SasFileProperties;
import com.epam.parso.SasFileReader;
import com.epam.parso.impl.SasFileReaderImpl;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class SasBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(SasBatchReader.class);
  private final int maxRecords;
  private final List<SasColumnWriter> writerList;
  private FileSplit split;
  private InputStream fsStream;
  private SasFileReader sasFileReader;
  private SasFileProperties fileProperties;
  private CustomErrorContext errorContext;
  private RowSetLoader rowWriter;


  public static class SasReaderConfig {
    protected final SasFormatPlugin plugin;
    public SasReaderConfig(SasFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public SasBatchReader(int maxRecords) {
    this.maxRecords = maxRecords;
    writerList = new ArrayList<>();
  }

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    openFile(negotiator);

    TupleMetadata schema;
    if (negotiator.hasProvidedSchema()) {
      schema = negotiator.providedSchema();
    } else {
      schema = buildSchema();
    }
    // TODO Add Implicit Columns
    negotiator.tableSchema(schema, true);

    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    buildWriterList(schema);

    return true;
  }

  private void openFile(FileScanFramework.FileSchemaNegotiator negotiator) {
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      sasFileReader = new SasFileReaderImpl(fsStream);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Unable to open SAS File %s", split.getPath())
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }

  private void buildWriterList(TupleMetadata schema) {
    int colIndex = 0;
    for (MaterializedField field : schema.toFieldList()) {
      String fieldName = field.getName();
      MinorType type = field.getType().getMinorType();
      if (type == MinorType.FLOAT8) {
        writerList.add(new NumericSasColumnWriter(colIndex, fieldName, rowWriter));
      } else {
        writerList.add(new StringSasColumnWriter(colIndex, fieldName, rowWriter));
      }
      colIndex++;
    }
  }

  private TupleMetadata buildSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    List<Column> columns = sasFileReader.getColumns();

    for (Column column : columns) {
      String fieldName = column.getName();
      MinorType type = getType(column);
      builder.addNullable(fieldName, type);
    }

    return builder.buildSchema();
  }

  private MinorType getType(Column column) {
    String typeName = column.getType().getName();
    switch (typeName) {
      case "java.lang.String":
        return MinorType.VARCHAR;
      case "java.lang.Number":
        return MinorType.FLOAT8;
    }
    return null;
  }

  private void addImplicitColumnsToSchema() {
    fileProperties = sasFileReader.getSasFileProperties();
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!processNextRow()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(fsStream);
  }

  private boolean processNextRow() {
    if (rowWriter.limitReached(maxRecords)) {
      return false;
    }

    Object[] row;
    try {
      row = sasFileReader.readNext();
      for (int i = 0; i < row.length; i++) {
        writerList.get(i).load(row);
      }
    } catch (IOException e) {
      return false;
    }
    return true;
  }

  public abstract static class SasColumnWriter {
    final String columnName;
    final ScalarWriter writer;
    final int columnIndex;

    public SasColumnWriter(int columnIndex, String columnName, ScalarWriter writer) {
      this.columnIndex = columnIndex;
      this.columnName = columnName;
      this.writer = writer;
    }

    public abstract void load (Object[] row);
  }

  public static class StringSasColumnWriter extends SasColumnWriter {

    StringSasColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(Object[] row) {
      writer.setString((String) row[columnIndex]);
    }
  }

  public static class NumericSasColumnWriter extends SasColumnWriter {

    NumericSasColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(Object[] row) {
      writer.setDouble(((Long) row[columnIndex]).doubleValue());
    }
  }
}
