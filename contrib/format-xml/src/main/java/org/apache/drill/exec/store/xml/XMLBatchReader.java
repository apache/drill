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

package org.apache.drill.exec.store.xml;

import java.io.InputStream;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.impl.scan.v3.ManagedReader;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileDescrip;
import org.apache.drill.exec.physical.impl.scan.v3.file.FileSchemaNegotiator;
import org.apache.drill.exec.physical.resultSet.ResultSetLoader;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.store.dfs.easy.EasySubScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class XMLBatchReader implements ManagedReader {

  private static final Logger logger = LoggerFactory.getLogger(XMLBatchReader.class);

  private final FileDescrip file;
  private final RowSetLoader rootRowWriter;
  private final CustomErrorContext errorContext;

  private XMLReader reader;
  private final int dataLevel;

  static class XMLReaderConfig {
    final XMLFormatPlugin plugin;
    final int dataLevel;

    XMLReaderConfig(XMLFormatPlugin plugin) {
      this.plugin = plugin;
      dataLevel = plugin.getConfig().dataLevel;
    }
  }

  public XMLBatchReader(XMLReaderConfig readerConfig, EasySubScan scan, FileSchemaNegotiator negotiator) {
    errorContext = negotiator.parentErrorContext();
    dataLevel = readerConfig.dataLevel;
    file = negotiator.file();

    ResultSetLoader loader = negotiator.build();
    rootRowWriter = loader.writer();

    openFile();
  }

  @Override
  public boolean next() {
    return reader.next();
  }

  @Override
  public void close() {
    AutoCloseables.closeSilently(reader);
  }

  private void openFile() {
    try {
      InputStream fsStream = file.fileSystem().openPossiblyCompressedStream(file.split().getPath());
      reader = new XMLReader(fsStream, dataLevel);
      reader.open(rootRowWriter, errorContext);
    } catch (Exception e) {
      throw UserException
        .dataReadError(e)
        .message(String.format("Failed to open input file: %s", file.split().getPath().toString()))
        .addContext(errorContext)
        .addContext(e.getMessage())
        .build(logger);
    }
  }
}
