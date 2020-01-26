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

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.resultSet.RowSetLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Iterator;

public class LTSVRecordIterator implements Iterator {

  private static final Logger logger = LoggerFactory.getLogger(LTSVRecordIterator.class);

  private RowSetLoader rowWriter;

  private BufferedReader reader;

  private String line;

  private int recordCount;

  public LTSVRecordIterator(RowSetLoader rowWriter, BufferedReader reader) {
    this.rowWriter = rowWriter;
    this.reader = reader;

    // Get the first line
    try {
      line = reader.readLine();
    } catch (IOException e) {
      throw UserException
        .dataReadError()
        .message("Error reading LTSV Data: {}", e.getMessage())
        .build(logger);
    }
  }

  @Override
  public boolean hasNext() {
    return line != null;
  }

  @Override
  public Boolean next() {
    // Skip empty lines
    if (line.trim().length() == 0) {
      try {
        // Advance the line to the next line
        line = reader.readLine();
      } catch (IOException e) {
        throw UserException
          .dataReadError()
          .message("Error reading LTSV Data: {}", e.getMessage())
          .build(logger);
      }
      return Boolean.TRUE;
    } else if (line == null) {
      return Boolean.FALSE;
    }

    // Process the row
    processRow();

    // Increment record counter
    recordCount++;

    // Get the next line
    try {
      line = reader.readLine();
      if(line == null) {
        return Boolean.FALSE;
      }
    } catch (IOException e) {
      throw UserException
        .dataReadError()
        .message("Error reading LTSV Data: {}", e.getMessage())
        .build(logger);
    }
    return Boolean.TRUE;
  }

  /**
   * Function processes one row of data, splitting it up first by tabs then splitting the key/value pairs
   * finally recording it in the current Drill row.
   */
  private void processRow() {
    // Start the row
    rowWriter.start();
    for (String field : line.split("\t")) {
      int index = field.indexOf(":");
      if (index <= 0) {
        throw UserException
          .dataReadError()
          .message("Invalid LTSV format at line %d: %s", recordCount + 1, line)
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
