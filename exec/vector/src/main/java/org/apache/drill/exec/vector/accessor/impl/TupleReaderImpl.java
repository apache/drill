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
package org.apache.drill.exec.vector.accessor.impl;

import org.apache.drill.exec.vector.accessor.ColumnReader;
import org.apache.drill.exec.vector.accessor.TupleReader;

/**
 * Reader for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public class TupleReaderImpl extends AbstractTupleAccessor implements TupleReader {

  private final AbstractColumnReader readers[];

  public TupleReaderImpl(TupleSchema schema, AbstractColumnReader readers[]) {
    super(schema);
    this.readers = readers;
  }

  @Override
  public ColumnReader column(int colIndex) {
    return readers[colIndex];
  }

  @Override
  public ColumnReader column(String colName) {
    int index = schema.columnIndex(colName);
    if (index == -1) {
      return null; }
    return readers[index];
  }

  @Override
  public Object get(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return null; }
    switch (colReader.valueType()) {
    case BYTES:
      return colReader.getBytes();
    case DOUBLE:
      return colReader.getDouble();
    case INTEGER:
      return colReader.getInt();
    case LONG:
      return colReader.getLong();
    case STRING:
      return colReader.getString();
    default:
      throw new IllegalArgumentException("Unsupported type " + colReader.valueType());
    }
  }

  @Override
  public String getAsString(int colIndex) {
    ColumnReader colReader = column(colIndex);
    if (colReader.isNull()) {
      return "null";
    }
    switch (colReader.valueType()) {
    case BYTES:
      StringBuilder buf = new StringBuilder()
          .append("[");
      byte value[] = colReader.getBytes();
      int len = Math.min(value.length, 20);
      for (int i = 0; i < len;  i++) {
        if (i > 0) {
          buf.append(", ");
        }
        buf.append((int) value[i]);
      }
      if (value.length > len) {
        buf.append("...");
      }
      buf.append("]");
      return buf.toString();
    case DOUBLE:
      return Double.toString(colReader.getDouble());
    case INTEGER:
      return Integer.toString(colReader.getInt());
    case LONG:
      return Long.toString(colReader.getLong());
    case STRING:
      return "\"" + colReader.getString() + "\"";
    case DECIMAL:
      return colReader.getDecimal().toPlainString();
    default:
      throw new IllegalArgumentException("Unsupported type " + colReader.valueType());
    }
  }
}
