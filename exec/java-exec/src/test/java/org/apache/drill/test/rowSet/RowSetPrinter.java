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
package org.apache.drill.test.rowSet;

import java.io.PrintStream;

import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.vector.accessor.TupleAccessor.TupleSchema;
import org.apache.drill.test.rowSet.RowSet.RowSetReader;

/**
 * Print a row set in CSV-like format. Primarily for debugging.
 */

public class RowSetPrinter {
  private RowSet rowSet;

  public RowSetPrinter(RowSet rowSet) {
    this.rowSet = rowSet;
  }

  public void print() {
    print(System.out);
  }

  public void print(PrintStream out) {
    SelectionVectorMode selectionMode = rowSet.indirectionType();
    RowSetReader reader = rowSet.reader();
    int colCount = reader.schema().count();
    printSchema(out, selectionMode);
    while (reader.next()) {
      printHeader(out, reader, selectionMode);
      for (int i = 0; i < colCount; i++) {
        if (i > 0) {
          out.print(", ");
        }
        out.print(reader.getAsString(i));
      }
      out.println();
    }
  }

  private void printSchema(PrintStream out, SelectionVectorMode selectionMode) {
    out.print("#");
    switch (selectionMode) {
    case FOUR_BYTE:
      out.print(" (batch #, row #)");
      break;
    case TWO_BYTE:
      out.print(" (row #)");
      break;
    default:
      break;
    }
    out.print(": ");
    TupleSchema schema = rowSet.schema().hierarchicalAccess();
    for (int i = 0; i < schema.count(); i++) {
      if (i > 0) {
        out.print(", ");
      }
      out.print(schema.column(i).getName());
    }
    out.println();
  }

  private void printHeader(PrintStream out, RowSetReader reader, SelectionVectorMode selectionMode) {
    out.print(reader.index());
    switch (selectionMode) {
    case FOUR_BYTE:
      out.print(" (");
      out.print(reader.batchIndex());
      out.print(", ");
      out.print(reader.rowIndex());
      out.print(")");
      break;
    case TWO_BYTE:
      out.print(" (");
      out.print(reader.rowIndex());
      out.print(")");
      break;
    default:
      break;
    }
    out.print(": ");
  }
}
