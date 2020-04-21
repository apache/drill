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

package org.apache.drill.exec.store.easy;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;
import org.apache.hadoop.mapred.FileSplit;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;


/**
 * To create a format plugin, there is often a great deal of cut/pasted code. The EasyEVFBatchReader
 * is intended to allow developers of new format plugins to focus their energy on code that is truly unique
 * for each format.
 * <p>
 * To create a new format plugin, simply extend this class, and overwrite the open() method as shown below in the
 * snippet below. The code that is unique for the formats will be contained in the iterator class which
 * <p>
 * With respect to schema creation, there are three basic situations:
 * <ol>
 *   <li>Schema is known before opening the file</li>
 *   <li>Schema is known (and unchanging) after reading the first row of data</li>
 *   <li>Schmea is completely flexible, IE: not consistent and not known after first row.</li>
 * </ol>
 *
 * This class inmplements a series of methods to facilitate schema creation. However, to achieve the best
 * possible performance, it is vital to use the correct methods to map the schema. Drill will perform fastest when
 * the schema is known in advance and the writers can be stored in a data structure which minimizes the amount of string
 * comparisons.
 *
 * <b>Current state</b>
 * For the third scenario, where the schema is not consistent, there are a collection of functions, all named <pre>writeDataType()</pre>
 * which accept a ScalarWriter, column name and value. These functions first check to see whether the column has been added to the schema, if not
 * it adds it. If it has been added, the value will be added to the column.
 *
 * <p>
 *
 *   <pre>
 *   public boolean open(FileSchemaNegotiator negotiator) {
 *     super.open(negotiator);
 *     super.fileIterator = new LTSVRecordIterator(getRowWriter(), reader);
 *     return true;
 *   }
 *   </pre>
 *
 */
public abstract class EasyEVFBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(EasyEVFBatchReader.class);

  public FileSplit split;

  public EasyEVFIterator fileIterator;

  public ResultSetLoader loader;

  private RowSetLoader rowWriter;

  public InputStream fsStream;

  public BufferedReader reader;

  public RowSetLoader getRowWriter() {
    return rowWriter;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    this.split = negotiator.split();
    this.loader = negotiator.build();
    this.rowWriter = loader.writer();
    try {
      this.fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      this.reader = new BufferedReader(new InputStreamReader(fsStream, StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message(String.format("Failed to open input file: %s", split.getPath()))
        .addContext(e.getMessage())
        .build(logger);
    }
    return true;
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!fileIterator.next()) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    try {
      AutoCloseables.close(reader, fsStream);
    } catch (Exception e) {
      logger.warn("Error closing Input Streams.");
    }
  }

  /**
   * Writes a string column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeStringColumn(TupleWriter rowWriter, String fieldName, String value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.VARCHAR, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);

    // Case for null string
    if (value == null) {
      colWriter.setNull();
    }

    colWriter.setString(value);
  }

  /**
   * Writes a boolean column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written to the Drill vector
   */
  public static void writeBooleanColumn(TupleWriter rowWriter, String fieldName, boolean value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.BIT, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setBoolean(value);
  }

  /**
   * Writes a string column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written.  Must be a org.joda.time.LocalDate object.
   */
  public static void writeDateColumn(TupleWriter rowWriter, String fieldName, LocalDate value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.DATE, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    if (value == null) {
      colWriter.setNull();
    }

    colWriter.setDate(value);
  }

  /**
   * Writes a timestamp column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written.  Must be a org.joda.time.Instant object.
   */
  public static void writeTimestampColumn(TupleWriter rowWriter, String fieldName, Instant value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.TIMESTAMP, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    if (value == null) {
      colWriter.setNull();
    }
    colWriter.setTimestamp(value);
  }

  /**
   * Writes a vardecimal column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeDecimalColumn(TupleWriter rowWriter, String fieldName, BigDecimal value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.VARDECIMAL, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    if (value == null) {
      colWriter.setNull();
    }

    colWriter.setDecimal(value);
  }

  /**
   * Writes a double column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeDoubleColumn(TupleWriter rowWriter, String fieldName, double value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.FLOAT8, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setDouble(value);
  }

  /**
   * Writes a float column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeFloatColumn(TupleWriter rowWriter, String fieldName, float value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.FLOAT4, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setDouble(value);
  }

  /**
   * Writes an int column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeIntColumn(TupleWriter rowWriter, String fieldName, int value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setInt(value);
  }

  /**
   * Writes a long column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeBigIntColumn(TupleWriter rowWriter, String fieldName, long value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.INT, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    colWriter.setLong(value);
  }

  /**
   * Writes an Interval column.  This function should be used when the schema is not known in advance or cannot be inferred from the first
   * row of data.
   * @param rowWriter The rowWriter object
   * @param fieldName The name of the field to be written
   * @param value The value to be written
   */
  public static void writeIntervalColumn(TupleWriter rowWriter, String fieldName, Period value) {
    int index = rowWriter.tupleSchema().index(fieldName);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(fieldName, TypeProtos.MinorType.INTERVAL, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    if (value == null) {
      colWriter.setNull();
    }
    colWriter.setPeriod(value);
  }
}
