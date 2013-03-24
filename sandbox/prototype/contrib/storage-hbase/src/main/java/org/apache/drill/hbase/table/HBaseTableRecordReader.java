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
import org.apache.drill.exec.ref.RecordIterator;
import org.apache.drill.exec.ref.RecordPointer;
import org.apache.drill.exec.ref.rops.ROP;
import org.apache.drill.exec.ref.rse.RecordReader;
import org.apache.drill.hbase.HBaseResultRecordPointer;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;

import static com.google.common.base.Throwables.propagate;

/**
 * Scanning RecordReader, retrieves Records from an HBase Table..
 */
public class HBaseTableRecordReader implements RecordReader {

  private final HBaseTableScanner table;
  private final ROP parent;
  private final SchemaPath rootPath;

  public HBaseTableRecordReader(HBaseTableScanner table, ROP parent) {
    this.table = table;
    this.parent = parent;
    this.rootPath = table.rootPath;
  }

  @Override
  public RecordIterator getIterator() {
    try {
      return new HBaseTableScannerRecordIterator(this.rootPath, table.newScanner(), parent);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public static class HBaseTableScannerRecordIterator implements RecordIterator {

    private final ResultScanner scanner;
    private final Iterator<Result> resultIterator;
    private final ROP parent;
    private HBaseResultRecordPointer record = new HBaseResultRecordPointer();
    private NextOutcome nextOutcome;
    private final SchemaPath rootPath;


    HBaseTableScannerRecordIterator(SchemaPath rootPath, ResultScanner scanner, ROP parent) {
      this.scanner = scanner;
      this.resultIterator = this.scanner.iterator();
      this.parent = parent;
      this.rootPath = rootPath;
    }

    @Override
    public RecordPointer getRecordPointer() {
      return record;
    }

    @Override
    public NextOutcome next() {
      if (resultIterator.hasNext()) {
        record.clearAndSet(rootPath, resultIterator.next());
        nextOutcome = NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      } else {
        record = null;
        nextOutcome = NextOutcome.NONE_LEFT;
      }
      return nextOutcome;
    }

    @Override
    public ROP getParent() {
      return parent;
    }
  }

  @Override
  public void setup() {
  }

  @Override
  public void cleanup() {
  }
}
