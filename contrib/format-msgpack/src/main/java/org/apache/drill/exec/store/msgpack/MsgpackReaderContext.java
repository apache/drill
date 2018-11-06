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

import org.apache.drill.common.exceptions.UserException;
import org.apache.hadoop.fs.Path;
import org.slf4j.helpers.MessageFormatter;

import com.fasterxml.jackson.core.JsonParseException;

import io.netty.buffer.DrillBuf;
import jline.internal.Log;

public class MsgpackReaderContext {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MsgpackReaderContext.class);

  // Data we're consuming
  public Path hadoopPath;
  public long runningRecordCount = 0;
  public long parseErrorCount;
  public int recordCount;
  public DrillBuf workBuf;

  public boolean printToConsole;

  public boolean lenient;

  public boolean readBinaryAsString;

  public boolean useSchema;

  public long currentRecordNumberInFile() {
    return parseErrorCount + runningRecordCount + recordCount + 1;
  }

  public void updateRunningCount() {
    this.runningRecordCount += this.recordCount;
  }


  public void handleAndRaise(String suffix, Exception e) throws UserException {

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

    if (hadoopPath != null) {
      exceptionBuilder.pushContext("Record ", currentRecordNumberInFile()).pushContext("File ",
          hadoopPath.toUri().getPath());
    }

    throw exceptionBuilder.build(logger);
  }

  @Override
  public String toString() {
    return super.toString() + "[hadoopPath = " + hadoopPath + ", recordCount = " + recordCount + ", parseErrorCount = "
        + parseErrorCount + ", runningRecordCount = " + runningRecordCount + ", ...]";
  }

  public void warn(String message) {
    if (printToConsole) {
      Log.warn(message);
    } else {
      logger.warn(message);
    }
  }

  public void warn(String message, Exception e) {
    if (printToConsole) {
      Log.warn(message, e.getMessage());
    } else {
      logger.warn(message, e);
    }
  }

  public void parseWarn() {
    warn("Parsing msgpack in " + hadoopPath.getName() + " : line nos :" + currentRecordNumberInFile());
  }

  public void parseWarn(Exception e) {
    String message = MessageFormatter.arrayFormat("Parsing msgpack file '{}' at line number '{}' error is '{}'\n",
        new Object[] { this.hadoopPath.getName(), this.currentRecordNumberInFile(), e.getMessage() }).getMessage();
    if (printToConsole) {
      Log.warn(message);
    } else {
      logger.warn(message);
    }
  }
}
