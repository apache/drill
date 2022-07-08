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

package org.apache.drill.exec.store.googlesheets.columns;

import org.apache.drill.exec.store.googlesheets.utils.GoogleSheetsUtils;

public class GoogleSheetsColumnRange {
  private Integer startColIndex;
  private Integer endColIndex;
  private final String sheetName;

  public GoogleSheetsColumnRange(String sheetName) {
    this.sheetName = sheetName;
  }

  public Integer getStartColIndex() {
    return startColIndex;
  }

  public String getStartColumnLetter() {
    return GoogleSheetsUtils.columnToLetter(startColIndex + 1);
  }

  public String getEndColumnLetter() {
    return GoogleSheetsUtils.columnToLetter(endColIndex + 1);
  }

  public Integer getEndColIndex() {
    return endColIndex;
  }

  public GoogleSheetsColumnRange setStartIndex(int startColIndex) {
    this.startColIndex = startColIndex;
    return this;
  }

  public GoogleSheetsColumnRange setEndIndex(int endColIndex) {
    this.endColIndex = endColIndex;
    return this;
  }
}
