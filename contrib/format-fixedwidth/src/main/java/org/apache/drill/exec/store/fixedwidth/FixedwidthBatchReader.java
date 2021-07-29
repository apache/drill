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

package org.apache.drill.exec.store.fixedwidth;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

public class FixedwidthBatchReader implements ManagedReader<FileSchemaNegotiator>{

  private static final Logger logger = LoggerFactory.getLogger(FixedwidthBatchReader.class);

  private FileSplit split;

  private final int maxRecords;

  private final FixedwidthFormatConfig config;

  private CustomErrorContext errorContext;

  private InputStream fsStream;

  private ResultSetLoader loader;

  private BufferedReader reader;

  public FixedwidthBatchReader(FixedwidthFormatConfig config, int maxRecords) {
    this.config = config;
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    errorContext = negotiator.parentErrorContext();
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      negotiator.tableSchema(buildSchema(),true);
      loader = negotiator.build();
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to open input file: {}", split.getPath().toString())
              .addContext(errorContext)
              .addContext(e.getMessage())
              .build(logger);
    }

    reader = new BufferedReader(new InputStreamReader(fsStream, Charsets.UTF_8));

    return true;

  }

  @Override
  public boolean next() { // Use loader to read data from file to turn into Drill rows

    String line;

    try {
      line = reader.readLine();
      System.out.println(line);
      RowSetLoader writer = loader.writer();

      while (!writer.isFull() && line != null) {

        writer.start();
        parseLine(line, writer);
        writer.save();

        line = reader.readLine();
      }
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to read input file: {}", split.getPath().toString())
              .addContext(errorContext)
              .addContext(e.getMessage())
              .build(logger);
    }
    return (line != null);
  }

  @Override
  public void close() {
    try {
      fsStream.close();
      loader.close();
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to close input file: {}", split.getPath().toString())
              .addContext(errorContext)
              .addContext(e.getMessage())
              .build(logger);
    }
  }

  private TupleMetadata buildSchema(){
    SchemaBuilder builder = new SchemaBuilder();

    for (FixedwidthFieldConfig field : config.getFields()){
      builder.addNullable(field.getFieldName(),field.getDataType());
    }

      return builder.buildSchema();
  }

  private void parseLine(String line, RowSetLoader writer) {
    int i = 0;
    TypeProtos.MinorType dataType;
    String dateTimeFormat;
    String value;

    for (FixedwidthFieldConfig field : config.getFields()) {
      value = line.substring(field.getStartIndex() - 1, field.getStartIndex() + field.getFieldWidth() - 1);

      // Convert String to data type in field
      dataType = field.getDataType();
      dateTimeFormat = field.getDateTimeFormat();

      switch (dataType) {
        case INT:
          writer.scalar(i).setInt(Integer.parseInt(value));
          break;
        case VARCHAR:
          writer.scalar(i).setString(value);
          break;
        case DATE:
          DateTimeFormatter formatDate = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.ENGLISH);
          LocalDate date = LocalDate.parse(value, formatDate);

          writer.scalar(i).setDate(date);
          break;
        case TIME:
          DateTimeFormatter formatTime = DateTimeFormatter.ofPattern(dateTimeFormat, Locale.ENGLISH);
          LocalTime time = LocalTime.parse(value, formatTime);

          writer.scalar(i).setTime(time);
          break;
        case TIMESTAMP:
          DateTimeFormatter formatTS = DateTimeFormatter.ofPattern(dateTimeFormat,Locale.ENGLISH);
          LocalDateTime ldt = LocalDateTime.parse(value,formatTS);
          ZoneId z = ZoneId.of( "America/Toronto" ) ;
          ZonedDateTime zdt = ldt.atZone( z ) ;
          Instant timeStamp = zdt.toInstant();

          writer.scalar(i).setTimestamp(timeStamp);
          break;
        default:
          throw new RuntimeException("Unknown data type specified in fixed width. Found data type " + dataType);




      }

      i++;
    }
  }

}