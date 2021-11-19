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
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
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
  private Object[] firstRow;


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
      firstRow = sasFileReader.readNext();
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
        writerList.add(new DoubleSasColumnWriter(colIndex, fieldName, rowWriter));
      } else if (type == MinorType.BIGINT) {
        writerList.add(new BigIntSasColumnWriter(colIndex, fieldName, rowWriter));
      } else if (type == MinorType.DATE) {
        writerList.add(new DateSasColumnWriter(colIndex, fieldName, rowWriter));
      } else {
        writerList.add(new StringSasColumnWriter(colIndex, fieldName, rowWriter));
      }
      colIndex++;
    }
  }

  private TupleMetadata buildSchema() {
    SchemaBuilder builder = new SchemaBuilder();
    List<Column> columns = sasFileReader.getColumns();

    int counter = 0;
    for (Column column : columns) {
      String fieldName = column.getName();
      try {
        MinorType type = getType(firstRow[counter].getClass().getSimpleName());
        builder.addNullable(fieldName, type);
      } catch (Exception e) {
        System.out.println("Error with column type: " + firstRow[counter].getClass().getSimpleName());
        throw e;
      }
      counter++;
    }

    return builder.buildSchema();
  }

  private MinorType getType(String simpleType) {
    switch (simpleType) {
      case "String":
        return MinorType.VARCHAR;
      case "Double":
        return MinorType.FLOAT8;
      case "Long":
        return MinorType.BIGINT;
      case "Date":
        return MinorType.DATE;
      default:
        throw UserException.dataReadError()
          .message("SAS Reader does not support data type: " + simpleType)
          .addContext(errorContext)
          .build(logger);
    }
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
      // Process first row
      if (firstRow != null) {
        row = firstRow;
        firstRow = null;
      } else {
        row = sasFileReader.readNext();
      }

      if (row == null) {
        return false;
      }

      rowWriter.start();
      for (int i = 0; i < row.length; i++) {
        writerList.get(i).load(row);
      }
      rowWriter.save();
    } catch (IOException e) {
      throw UserException.dataReadError()
        .message("Error reading SAS file: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
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

  public static class BigIntSasColumnWriter extends SasColumnWriter {

    BigIntSasColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(Object[] row) {
      writer.setLong((Long) row[columnIndex]);
    }
  }

  public static class DateSasColumnWriter extends SasColumnWriter {

    DateSasColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(Object[] row) {
      LocalDate value = convertDateToLocalDate((Date)row[columnIndex]);
      writer.setDate(value);
    }

    private LocalDate convertDateToLocalDate(Date date) {
      return Instant.ofEpochMilli(date.toInstant().toEpochMilli())
        .atZone(ZoneOffset.ofHours(0))
        .toLocalDate();
    }
  }


  public static class DoubleSasColumnWriter extends SasColumnWriter {

    DoubleSasColumnWriter (int columnIndex, String columnName, RowSetLoader rowWriter) {
      super(columnIndex, columnName, rowWriter.scalar(columnName));
    }

    @Override
    public void load(Object[] row) {
      // The SAS reader does something strange with zeros. For whatever reason, even if the
      // field is a floating point number, the value is returned as a long.  This causes class
      // cast exceptions.
      if (row[columnIndex].equals(0L)) {
        writer.setDouble(0.0);
      } else {
        writer.setDouble((Double) row[columnIndex]);
      }
    }
  }
}
