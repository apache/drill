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
import org.joda.time.LocalTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public abstract class EasyEVFBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(EasyEVFBatchReader.class);

  public FileSplit split;

  public Iterator fileIterator;

  public ResultSetLoader loader;

  private RowSetLoader rowWriter;

  public InputStream fsStream;

  public BufferedReader reader;

  public EasyEVFBatchReader() {
  }

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
        .build(logger);
    }
    return true;
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!fileIterator.hasNext()) {
        return false;
      }
      fileIterator.next();
    }
    return true;
  }

  @Override
  public void close() {
    try {
      if (reader != null) {
        reader.close();
        reader = null;
      }
      if (fsStream != null) {
        fsStream.close();
        fsStream = null;
      }
    } catch (IOException e) {
      logger.warn("Error closing batch Record Reader.");
    }
  }

  /**
   * This function should be used when writing Drill columns when the schema is not fixed or known in advance. At present only simple data types are
   * supported with this function.  If the column is not present in the schema, this function will add it first.
   * @param rowWriter The RowSetWriter to which the data is to be written
   * @param name The field name
   * @param value The field value.  Cannot be a primitive.
   * @param type The Drill data type of the field.
   */
  public static void writeColumn(TupleWriter rowWriter, String name, Object value, TypeProtos.MinorType type) {
    int index = rowWriter.tupleSchema().index(name);
    if (index == -1) {
      ColumnMetadata colSchema = MetadataUtils.newScalar(name, type, TypeProtos.DataMode.OPTIONAL);
      index = rowWriter.addColumn(colSchema);
    }
    ScalarWriter colWriter = rowWriter.scalar(index);
    switch (type) {
      case INT:
        colWriter.setInt((Integer)value);
        break;
      case BIGINT:
        colWriter.setLong((Long)value);
        break;
      case VARCHAR:
        colWriter.setString((String)value);
        break;
      case FLOAT4:
      case FLOAT8:
        colWriter.setDouble((Double)value);
        break;
      case DATE:
        colWriter.setDate((LocalDate) value);
        break;
      case BIT:
        colWriter.setBoolean((Boolean)value);
        break;
      case TIMESTAMP:
      case TIMESTAMPTZ:
        colWriter.setTimestamp((Instant) value);
        break;
      case TIME:
        colWriter.setTime((LocalTime) value);
        break;
      case INTERVAL:
        colWriter.setPeriod((Period)value);
        break;
      default:
        logger.warn("Does not support data type {}", type.toString());
    }
  }
}
