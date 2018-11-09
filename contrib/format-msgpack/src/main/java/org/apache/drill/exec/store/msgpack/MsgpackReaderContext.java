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
package org.apache.drill.exec.store.msgpack;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.store.msgpack.MsgpackFormatPlugin.MsgpackFormatConfig;
import org.apache.hadoop.fs.Path;
import org.slf4j.helpers.MessageFormatter;

import jline.internal.Log;

public class MsgpackReaderContext {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReaderContext.class);

  /**
   * Keep track of the navigation inside the msgpack message. We use this to help
   * track down issues with the data files.
   */
  private final FieldPathTracker fieldPathTracker = new FieldPathTracker();

  private final boolean hasSchema;
  private final Path filePath;
  private final MsgpackFormatConfig config;
  private long runningRecordCount = 0;
  private long parseErrorCount = 0;
  private int recordCount = 0;

  public MsgpackReaderContext(Path filePath, MsgpackFormatConfig config, boolean hasSchema) {
    this.filePath = filePath;
    this.config = config;
    this.hasSchema = hasSchema;
  }

  public FieldPathTracker getFieldPathTracker() {
    return fieldPathTracker;
  }

  public boolean hasSchema() {
    return hasSchema;
  }

  public boolean isLenient() {
    return config.isLenient();
  }

  public void incrementParseErrorCount() {
    parseErrorCount++;
  }

  public int getRecordCount() {
    return this.recordCount;
  }

  public void resetRecordCount() {
    this.recordCount = 0;
  }

  public void incrementRecordCount() {
    recordCount++;
  }

  public Path getFilePath() {
    return this.filePath;
  }

  public long currentRecordNumberInFile() {
    return parseErrorCount + runningRecordCount + recordCount + 1;
  }

  public void updateRunningCount() {
    this.runningRecordCount += this.recordCount;
  }

  public void handleAndRaise(String suffix, Exception e) throws UserException {
    // TODO: use UserException here
    // throw UserException.unsupportedError().message("FIXED type not supported
    // yet").build(logger);

    String message = e.getMessage();
    int columnNr = -1;

    if (e instanceof JsonParseException) {
      final JsonParseException ex = (JsonParseException) e;
      message = ex.getOriginalMessage();
      columnNr = ex.getLocation().getColumnNr();
    }

    UserException.Builder exceptionBuilder = UserException.dataReadError(e).message("%s - %s", suffix, message);
    if (columnNr > 0) {
      exceptionBuilder.pushContext("Column ", columnNr);
    }

    if (filePath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile()).pushContext("File ",
          filePath.toUri().getPath());
    }

    throw exceptionBuilder.build(logger);
  }

  public void warn(String message) {
    if (config.isPrintToConsole()) {
      Log.warn(message);
    } else {
      logger.warn(message);
    }
  }

  public void warn(String message, Exception e) {
    if (config.isPrintToConsole()) {
      Log.warn(message, e.getMessage());
    } else {
      logger.warn(message, e);
    }
  }

  public void parseWarn() {
    warn("Parsing msgpack in " + filePath.getName() + " : line nos :" + currentRecordNumberInFile());
  }

  public void parseWarn(Exception e) {
    String message = MessageFormatter.arrayFormat("Parsing msgpack file '{}' at line number '{}' error is '{}'\n",
        new Object[] { this.filePath.getName(), this.currentRecordNumberInFile(), e.getMessage() }).getMessage();
    if (config.isPrintToConsole()) {
      Log.warn(message);
    } else {
      logger.warn(message);
    }
  }

}
