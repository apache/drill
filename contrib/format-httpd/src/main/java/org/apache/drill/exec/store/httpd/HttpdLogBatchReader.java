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

package org.apache.drill.exec.store.httpd;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.MetadataUtils;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Charsets;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class HttpdLogBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(HttpdLogBatchReader.class);
  public static final String RAW_LINE_COL_NAME = "_raw";
  public static final String MATCHED_COL_NAME = "_matched";
  private final HttpdLogFormatConfig formatConfig;
  private final HttpdParser parser;
  private final FileDescrip file;
  private InputStream fsStream;
  private final RowSetLoader rowWriter;
  private BufferedReader reader;
  private int lineNumber;
  private final CustomErrorContext errorContext;
  private final ScalarWriter rawLineWriter;
  private final ScalarWriter matchedWriter;
  private int errorCount;

  public HttpdLogBatchReader(HttpdLogFormatConfig formatConfig, EasySubScan scan, FileSchemaNegotiator negotiator) {
    this.formatConfig = formatConfig;

    // Open the input stream to the log file
    file = negotiator.file();
    openFile();
    errorContext = negotiator.parentErrorContext();
    try {
      parser = new HttpdParser(
              formatConfig.getLogFormat(),
              formatConfig.getTimestampFormat(),
              formatConfig.getFlattenWildcards(),
              formatConfig.getParseUserAgent(),
              formatConfig.getLogParserRemapping(),
              scan);
      negotiator.tableSchema(parser.setupParser(), false);
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error opening HTTPD file: " + e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }

    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    parser.addFieldsToParser(rowWriter);
    rawLineWriter = addImplicitColumn(RAW_LINE_COL_NAME, MinorType.VARCHAR);
    matchedWriter = addImplicitColumn(MATCHED_COL_NAME, MinorType.BIT);
  }

  @Override
  public boolean next() {
    while (!rowWriter.isFull()) {
      if (!nextLine(rowWriter)) {
        return false;
      }
    }
    return true;
  }

  private boolean nextLine(RowSetLoader rowWriter) {
    String line;

    try {
      line = reader.readLine();
      if (line == null) {
        return false;
      } else if (line.isEmpty()) {
        return true;
      }
    } catch (Exception e) {
      throw UserException.dataReadError(e)
        .message("Error reading HTTPD file at line number %d", lineNumber)
        .addContext(e.getMessage())
        .addContext(errorContext)
        .build(logger);
    }
    // Start the row
    rowWriter.start();

    try {
      parser.parse(line);
      matchedWriter.setBoolean(true);
    } catch (Exception e) {
      errorCount++;
      if (errorCount >= formatConfig.getMaxErrors()) {
        throw UserException.dataReadError()
          .message("Error reading HTTPD file at line number %d", lineNumber)
          .addContext(e.getMessage())
          .addContext(errorContext)
          .build(logger);
      } else {
        matchedWriter.setBoolean(false);
      }
    }

    // Write raw line
    rawLineWriter.setString(line);

    // Finish the row
    rowWriter.save();
    lineNumber++;

    return true;
  }

  @Override
  public void close() {
    if (fsStream == null) {
      return;
    }
    try {
      fsStream.close();
    } catch (IOException e) {
      logger.warn("Error when closing HTTPD file: {} {}", file.split().getPath().toString(), e.getMessage());
    }
    fsStream = null;
  }

  private void openFile() {
    Path path = file.split().getPath();
    try {
      fsStream = file.fileSystem().openPossiblyCompressedStream(path);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", path.toString())
        .addContext(e.getMessage())
        .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(fsStream, Charsets.UTF_8));
  }

  private ScalarWriter addImplicitColumn(String colName, MinorType type) {
    ColumnMetadata colSchema = MetadataUtils.newScalar(colName, type, TypeProtos.DataMode.OPTIONAL);
    colSchema.setBooleanProperty(ColumnMetadata.EXCLUDE_FROM_WILDCARD, true);
    int index = rowWriter.addColumn(colSchema);

    return rowWriter.scalar(index);
  }
}
