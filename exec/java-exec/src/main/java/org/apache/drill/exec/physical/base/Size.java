/**
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
package org.apache.drill.exec.physical.base;

public class Size {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Size.class);

  private final long rowCount;
  private final int rowSize;

  public Size(long rowCount, int rowSize) {
    super();
    this.rowCount = rowCount;
    this.rowSize = rowSize;
  }

  public long getRecordCount() {
    return rowCount;
  }

  public int getRecordSize() {
    return rowSize;
  }
  
  public Size add(Size s){
    return new Size(rowCount + s.rowCount, Math.max(rowSize, s.rowSize));
  }
  
  public long getAggSize(){
    return rowCount * rowSize;
  }
  
}
