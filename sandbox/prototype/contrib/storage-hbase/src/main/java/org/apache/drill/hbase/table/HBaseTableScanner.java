/*******************************************************************************
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
 ******************************************************************************/
package org.apache.drill.hbase.table;

import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ref.rse.ReferenceStorageEngine;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * A Read entry for HBase that represents a HTable.
 */
public class HBaseTableScanner implements ReferenceStorageEngine.ReadEntry {

  public final HTable hTable;
  public final Scan scan;
  public final SchemaPath rootPath;

  public HBaseTableScanner(HTable hTable, Scan scan, SchemaPath rootPath) {
    this.hTable = hTable;
    this.scan = scan;
    this.rootPath = rootPath;
  }

  public ResultScanner newScanner() throws IOException {
    return this.hTable.getScanner(scan);
  }
}