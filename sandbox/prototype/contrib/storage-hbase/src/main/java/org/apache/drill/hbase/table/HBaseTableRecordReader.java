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
 * Scanning RecordReader, retrieves Records from HBase by scanning the whole table.
 */
public class HBaseTableRecordReader implements RecordReader {

  private final HBaseTableScanner table;
  private final ROP parent;

  public HBaseTableRecordReader(HBaseTableScanner table, ROP parent) {
    this.table = table;
    this.parent = parent;
  }


  @Override
  public RecordIterator getIterator() {
    try {
      return new HBaseScannerRecordIterator(table.newScanner(), parent);
    } catch (IOException e) {
      throw propagate(e);
    }
  }

  public static class HBaseScannerRecordIterator implements RecordIterator {

    private final ResultScanner scanner;
    private final Iterator<Result> resultIterator;
    private final ROP parent;
    private RecordPointer currentRecord;
    private NextOutcome nextOutcome;


    HBaseScannerRecordIterator(ResultScanner scanner, ROP parent) {
      this.scanner = scanner;
      this.resultIterator = this.scanner.iterator();
      this.parent = parent;
    }

    @Override
    public RecordPointer getRecordPointer() {
      if (currentRecord == null) {
        next();
      }
      return currentRecord;
    }

    @Override
    public NextOutcome next() {
      if (resultIterator.hasNext()) {
        currentRecord = new HBaseResultRecordPointer(resultIterator.next());
        nextOutcome = NextOutcome.INCREMENTED_SCHEMA_CHANGED;
      } else {
        currentRecord = null;
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
