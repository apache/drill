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
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.record.metadata.SchemaBuilder;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;

public class FixedwidthBatchReader implements ManagedReader<FileSchemaNegotiator>{

  private static final Logger logger = LoggerFactory.getLogger(FixedwidthBatchReader.class);

  private FileSplit split;

  private final int maxRecords;

  private final FixedwidthFormatConfig config;

  private CustomErrorContext errorContext;

  private InputStream fsStream;

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
      negotiator.build();
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to open input file: {}", split.getPath().toString())
              .addContext(errorContext)
              .addContext(e.getMessage())
              .build(logger);
    }
    return true;
  }

  @Override
  public boolean next() {
    byte[] byteArray = new byte[10000];
    int bytesRead;

    try {
      bytesRead = fsStream.read(byteArray);
      System.out.println(new String(byteArray));
    } catch (Exception e) {
      throw UserException
              .dataReadError(e)
              .message("Failed to read input file: {}", split.getPath().toString())
              .addContext(errorContext)
              .addContext(e.getMessage())
              .build(logger);
    }
    return (bytesRead != -1);
  }

  @Override
  public void close() {
    try {
      fsStream.close();
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
}