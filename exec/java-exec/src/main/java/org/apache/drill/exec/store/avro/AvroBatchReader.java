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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.FsInput;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.PlanStringBuilder;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.v3.FixedReceiver;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.ColumnConverter;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.util.ImpersonationUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroBatchReader implements ManagedReader {
  private static final Logger logger = LoggerFactory.getLogger(AvroBatchReader.class);

  private final Path filePath;
  private final long endPosition;
  private final DataFileReader<GenericRecord> reader;
  private final RowSetLoader loader;
  private final ColumnConverter converter;
  private final CustomErrorContext errorContext;
  // re-use container instance
  private GenericRecord record;

  public AvroBatchReader(AvroFormatConfig config, EasySubScan scan, FileSchemaNegotiator negotiator) {
    errorContext = negotiator.parentErrorContext();
    FileDescrip file = negotiator.file();
    filePath = file.split().getPath();

    // Avro files are splittable, define reading start / end positions.
    long startPosition = file.split().getStart();
    endPosition = startPosition + file.split().getLength();
    logger.debug("Processing Avro file: {}, start position: {}, end position: {}",
        filePath, startPosition, endPosition);

    reader = prepareReader(file.split(), file.fileSystem(),
        negotiator.userName(), negotiator.context().getFragmentContext().getQueryUserName());
    logger.debug("Avro file schema: {}", reader.getSchema());

    TupleMetadata readerSchema = AvroSchemaUtil.convert(reader.getSchema());
    logger.debug("Avro file converted schema: {}", readerSchema);

    TupleMetadata providedSchema = negotiator.providedSchema();
    TupleMetadata tableSchema = FixedReceiver.Builder.mergeSchemas(providedSchema, readerSchema);
    logger.debug("Avro file table schema: {}", tableSchema);

    negotiator.tableSchema(tableSchema, true);
    ResultSetLoader setLoader = negotiator.build();
    loader = setLoader.writer();

    AvroColumnConverterFactory factory = new AvroColumnConverterFactory(providedSchema);
    converter = factory.getRootConverter(providedSchema, readerSchema, loader);
  }

  @Override
  public boolean next() {
    while (!loader.isFull()) {
      if (!nextLine(loader)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(reader);
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
    return new PlanStringBuilder(this)
        .unquotedField("File", filePath.toString())
        .unquotedField("Position", String.valueOf(currentPosition))
        .toString();
  }

  /**
   * Process one row of records.
   * @param rowWriter
   * @return true true if one row is processed, false the EOF is reached.
   */
  private boolean nextLine(RowSetLoader rowWriter) {
    try {
      if (!reader.hasNext() || reader.pastSync(endPosition)) {
        return false;
      }
      record = reader.next(record);
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    Schema schema = record.getSchema();

    if (Schema.Type.RECORD != schema.getType()) {
      throw UserException
        .dataReadError()
        .message("Root object must be record type. Found: %s", schema.getType())
        .addContext(errorContext)
        .build(logger);
    }

    rowWriter.start();
    converter.convert(record);
    rowWriter.save();

    return true;
  }

  /**
   * Initialized Avro data reader based on given file system and file path.
   * Moves reader to the sync point from where to start reading the data.
   *
   * @param fileSplit A section of an input file.
   * @param fs A fairly generic filesystem.
   * @param opUserName name of the user whom to impersonate while reading the data.
   * @param queryUserName name of the user who issues the query.
   * @return Avro file reader
   */
  private DataFileReader<GenericRecord> prepareReader(FileSplit fileSplit, FileSystem fs, String opUserName, String queryUserName) {
    try {
      UserGroupInformation ugi = ImpersonationUtil.createProxyUgi(opUserName, queryUserName);
      DataFileReader<GenericRecord> reader = ugi.doAs((PrivilegedExceptionAction<DataFileReader<GenericRecord>>) () ->
        new DataFileReader<>(new FsInput(fileSplit.getPath(), fs.getConf()), new GenericDatumReader<GenericRecord>()));

      // Move to sync point from where to read the file.
      reader.sync(fileSplit.getStart());
      return reader;
    } catch (IOException | InterruptedException e) {
      throw UserException
        .dataReadError(e)
        .message("Error preparing Avro reader")
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
  }
}
