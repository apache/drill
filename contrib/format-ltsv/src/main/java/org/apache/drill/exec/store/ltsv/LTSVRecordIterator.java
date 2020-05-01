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

package org.apache.drill.exec.store.ltsv;

import org.apache.drill.common.exceptions.CustomErrorContext;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.apache.drill.exec.store.easy.EasyEVFIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;

public class LTSVRecordIterator implements EasyEVFIterator {

  private static final Logger logger = LoggerFactory.getLogger(LTSVRecordIterator.class);

  private final RowSetLoader rowWriter;

  private final BufferedReader reader;

  private final CustomErrorContext errorContext;

  private String line;

  private int lineNumber;


  public LTSVRecordIterator(RowSetLoader rowWriter, BufferedReader reader, CustomErrorContext errorContext) {
    this.rowWriter = rowWriter;
    this.reader = reader;
    this.errorContext = errorContext;
  }

  public boolean nextRow() {
    // Get the line
    try {
      line = reader.readLine();

      // Increment line number
      lineNumber++;

      if (line == null) {
        return false;
      } else if (line.trim().length() == 0) {
        // Skip empty lines
        return true;
      }
    } catch (IOException e) {
      throw UserException
        .dataReadError(e)
        .message("Error reading LTSV Data: %s", e.getMessage())
        .addContext("Line %d: %s", lineNumber, line)
        .addContext(errorContext)
        .build(logger);
    }

    processRow();

    return true;
  }

  /**
   * Function processes one row of data, splitting it up first by tabs then splitting the key/value pairs
   * finally recording it in the current Drill row.
   */
  private void processRow() {

    for (String field : line.split("\t")) {
      int index = field.indexOf(":");
      if (index <= 0) {
        throw UserException
          .dataReadError()
          .message("Invalid LTSV format at line %d: %s", lineNumber, line)
          .addContext(errorContext)
          .build(logger);
      }

      String fieldName = field.substring(0, index);
      String fieldValue = field.substring(index + 1);

      LTSVBatchReader.writeStringColumn(rowWriter, fieldName, fieldValue);
    }

    // End the row
    rowWriter.save();
  }
}
