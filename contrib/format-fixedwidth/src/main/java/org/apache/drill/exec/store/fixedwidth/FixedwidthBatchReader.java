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

        Object[] row = parseLine(line);

        writer.start();

        for (int i = 0; i < row.length; i++) {
          if (row[i] instanceof Integer) {
            writer.scalar(i).setInt((Integer) row[i]);
          } else if (row[i] instanceof String) {
            writer.scalar(i).setString((String) row[i]);
          }
        }
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

  private Object[] parseLine(String line){
    Object[] row = new Object[config.getFields().size()];
    int i = 0;
    TypeProtos.MinorType dataType;
    String dateTimeFormat;

    for (FixedwidthFieldConfig field : config.getFields()){
      row[i] = line.substring(field.getStartIndex()-1,field.getStartIndex()+field.getFieldWidth()-1);

      // Convert String to data type in field
      dataType = field.getDataType();
      dateTimeFormat = field.getDateTimeFormat();

      if (dataType == TypeProtos.MinorType.INT){
        row[i] = Integer.parseInt((String) row[i]);
      } else if (dataType == TypeProtos.MinorType.VARCHAR){
      } else if (dataType == TypeProtos.MinorType.DATE || dataType == TypeProtos.MinorType.TIME){
        // Check to ensure date time format matches input date?
      } else{

      }

      i++;
    }
      return row;
  }

}