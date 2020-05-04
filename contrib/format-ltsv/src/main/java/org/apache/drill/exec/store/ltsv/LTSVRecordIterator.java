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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class LTSVRecordIterator implements EasyEVFIterator {

  private static final Logger logger = LoggerFactory.getLogger(LTSVRecordIterator.class);

  private final RowSetLoader rowWriter;

  private final CustomErrorContext errorContext;

  private final Iterator<Map<String, String>> ltsvIterator;

  private int lineNumber;
  public LTSVRecordIterator(RowSetLoader rowWriter, Iterator<Map<String, String>> ltsvIterator, CustomErrorContext errorContext) {
    this.rowWriter = rowWriter;
    this.errorContext = errorContext;
    this.ltsvIterator = ltsvIterator;
  }

  public boolean nextRow() {
    if (ltsvIterator.hasNext()) {
      processRow(ltsvIterator.next());
      return true;
    } else {
      return false;
    }
  }

  /**
   * Function processes one row of data, splitting it up first by tabs then splitting the key/value pairs
   * finally recording it in the current Drill row.
   */
  private void processRow(Map<String, String> row) {
    for (Map.Entry<String,String> field : row.entrySet()) {
      LTSVBatchReader.writeStringColumn(rowWriter, field.getKey(), field.getValue());
      logger.debug("Mapping {} to {}", field.getKey(), field.getValue());
    }

    lineNumber++;
    // End the row
    rowWriter.save();
  }
}
