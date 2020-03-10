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
package org.apache.drill.exec.store.avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.convert.StandardConversions;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.stream.IntStream;

public class AvroBatchReader implements ManagedReader<FileScanFramework.FileSchemaNegotiator> {
  private static final Logger logger = LoggerFactory.getLogger(AvroBatchReader.class);

  private Path filePath;
  private long endPosition;
  private DataFileReader<GenericRecord> reader;
  private ResultSetLoader loader;
  private List<ColumnConverter> converters;
  // re-use container instance
  private GenericRecord record;

  @Override
  public boolean open(FileScanFramework.FileSchemaNegotiator negotiator) {
    FileSplit split = negotiator.split();
    filePath = split.getPath();

    // Avro files are splittable, define reading start / end positions
    long startPosition = split.getStart();
    endPosition = startPosition + split.getLength();

    logger.debug("Processing Avro file: {}, start position: {}, end position: {}",
      filePath, startPosition, endPosition);

    reader = prepareReader(split, negotiator.fileSystem(),
      negotiator.userName(), negotiator.context().getFragmentContext().getQueryUserName());

    logger.debug("Avro file schema: {}", reader.getSchema());
    TupleMetadata readerSchema = AvroSchemaUtil.convert(reader.getSchema());
    logger.debug("Avro file converted schema: {}", readerSchema);
    TupleMetadata providedSchema = negotiator.providedSchema();
    TupleMetadata tableSchema = StandardConversions.mergeSchemas(providedSchema, readerSchema);
    logger.debug("Avro file table schema: {}", tableSchema);
    negotiator.tableSchema(tableSchema, true);
    loader = negotiator.build();
    ColumnConverterFactory factory = new ColumnConverterFactory(providedSchema);
    converters = factory.initConverters(providedSchema, readerSchema, loader.writer());

    return true;
  }

  @Override
  public boolean next() {
    RowSetLoader rowWriter = loader.writer();
    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (reader == null) {
      return;
    }

    try {
      reader.close();
    } catch (IOException e) {
      logger.warn("Error closing Avro reader: {}", e.getMessage(), e);
    } finally {
      reader = null;
    }
  }

  @Override
  public String toString() {
    long currentPosition = -1L;
    try {
      if (reader != null) {
        currentPosition = reader.tell();
      }
    } catch (IOException e) {
      logger.trace("Unable to obtain Avro reader position: {}", e.getMessage(), e);
    }
    return "AvroBatchReader[File=" + filePath
      + ", Position=" + currentPosition
      + "]";
  }

  /**
   * Initialized Avro data reader based on given file system and file path.
   * Moves reader to the sync point from where to start reading the data.
   *
   * @param fileSplit file split
   * @param fs file system
   * @param opUserName name of the user whom to impersonate while reading the data
   * @param queryUserName name of the user who issues the query
   * @return Avro file reader
   */
  private DataFileReader<GenericRecord> prepareReader(FileSplit fileSplit, FileSystem fs, String opUserName, String queryUserName) {
    try {
      UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(opUserName, queryUserName);
      DataFileReader<GenericRecord> reader = ugi.doAs((PrivilegedExceptionAction<DataFileReader<GenericRecord>>) () ->
        new DataFileReader<>(new FsInput(fileSplit.getPath(), fs.getConf()), new GenericDatumReader<GenericRecord>()));

      // move to sync point from where to read the file
      reader.sync(fileSplit.getStart());
      return reader;
    } catch (IOException | InterruptedException e) {
      throw UserException.dataReadError(e)
        .message("Error preparing Avro reader")
        .addContext("Reader", this)
        .build(logger);
    }
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    try {
      if (!reader.hasNext() || reader.pastSync(endPosition)) {
        return false;
      }
      record = reader.next(record);
    } catch (IOException e) {
      throw UserException.dataReadError(e)
        .addContext("Reader", this)
        .build(logger);
    }

    Schema schema = record.getSchema();

    if (Schema.Type.RECORD != schema.getType()) {
      throw UserException.dataReadError()
        .message("Root object must be record type. Found: %s", schema.getType())
        .addContext("Reader", this)
        .build(logger);
    }

    rowWriter.start();
    IntStream.range(0, rowWriter.size())
      .forEach(i -> converters.get(i).convert(record.get(i)));
    rowWriter.save();

    return true;
  }
}
