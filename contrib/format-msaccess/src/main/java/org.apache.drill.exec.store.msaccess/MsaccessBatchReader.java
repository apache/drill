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

package org.apache.drill.exec.store.msaccess;


import com.google.common.base.Charsets;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Database;
import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.file.FileScanFramework.FileSchemaNegotiator;
import org.apache.drill.exec.physical.impl.scan.framework.ManagedReader;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class MsaccessBatchReader implements ManagedReader<FileSchemaNegotiator> {

  private static final Logger logger = LoggerFactory.getLogger(MsaccessBatchReader.class);

  private static final String VALUE_LABEL = "_value";

  private final int maxRecords;

  private FileSplit split;

  private InputStream fsStream;

  private RowSetLoader rowWriter;

  private CustomErrorContext errorContext;


  public static class MsaccessReaderConfig {

    protected final MsaccessFormatPlugin plugin;

    public MsaccessReaderConfig(MsaccessFormatPlugin plugin) {
      this.plugin = plugin;
    }
  }

  public MsaccessBatchReader(int maxRecords) {
    this.maxRecords = maxRecords;
  }

  @Override
  public boolean open(FileSchemaNegotiator negotiator) {
    split = negotiator.split();
    //negotiator.tableSchema(buildSchema(), true);
    errorContext = negotiator.parentErrorContext();
    ResultSetLoader loader = negotiator.build();
    rowWriter = loader.writer();
    //buildReaderList();
    errorContext = negotiator.parentErrorContext();
    try {
      fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
      // negotiator.tableSchema(buildSchema(), true);
      loader = negotiator.build();
      Database db = DatabaseBuilder.open(new File(split.getPath().toString()));

      System.out.println(db.getTableNames());
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open input file: {}", split.getPath().toString())
        .addContext(errorContext)
        .addContext(e.getMessage())
        .build(logger);
    }
    InputStreamReader reader = new InputStreamReader(fsStream, Charsets.UTF_8);
    //stream(reader);//change method to accept stream
    return true;
  }
  /*

  public boolean open(FileSchemaNegotiator negotiator) {
  split = negotiator.split();
  errorContext = negotiator.parentErrorContext();
  try {
    fsStream = negotiator.fileSystem().openPossiblyCompressedStream(split.getPath());
    negotiator.tableSchema(buildSchema(), true);
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
   */

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
    if (fsStream != null) {
      AutoCloseables.closeSilently(fsStream);
      fsStream = null;
    }
  }

  private boolean processNextRow() {
    // Check to see if the limit has been reached
    if (rowWriter.limitReached(maxRecords)) {
      return false;
    }

    try {
      // Stop reading when you run out of data
      /*
      if (!msaccessReader.readNextCase()) {
        return false;
      }
*/
      rowWriter.start();
//      for (MsaccessColumnWriter msaccessColumnWriter : writerList) {
//        msaccessColumnWriter.load(msaccessReader);
//      }
      rowWriter.save();

    } catch (Exception e) {
      throw UserException.dataReadError(e).message("Error reading MSAccess File.").addContext(errorContext).build(logger);
    }
    return true;
  }
}



