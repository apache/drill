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
package org.apache.drill.exec.vector.accessor.writer;

import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.vector.VectorOverflowException;
import org.apache.drill.exec.vector.accessor.ArrayWriter;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ObjectWriter;
import org.apache.drill.exec.vector.accessor.ScalarWriter;
import org.apache.drill.exec.vector.accessor.TupleWriter;

/**
 * Implementation for a writer for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public abstract class AbstractTupleWriter implements TupleWriter, WriterEvents {

  /**
   * Generic object wrapper for the tuple writer.
   */

  public static class TupleObjectWriter extends AbstractObjectWriter {

    private AbstractTupleWriter tupleWriter;

    public TupleObjectWriter(AbstractTupleWriter tupleWriter) {
      this.tupleWriter = tupleWriter;
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {
      tupleWriter.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.TUPLE;
    }

    @Override
    public void set(Object value) throws VectorOverflowException {
      tupleWriter.setObject(value);
    }

    @Override
    public void startWrite() {
      tupleWriter.startWrite();
    }

    @Override
    public void startValue() {
      tupleWriter.startValue();
    }

    @Override
    public void endValue() {
      tupleWriter.endValue();
    }

    @Override
    public void endWrite() {
      tupleWriter.endWrite();
    }

    @Override
    public TupleWriter tuple() {
      return tupleWriter;
    }
  }

  protected ColumnWriterIndex vectorIndex;
  protected final TupleMetadata schema;
  private final AbstractObjectWriter writers[];

  protected AbstractTupleWriter(TupleMetadata schema, AbstractObjectWriter writers[]) {
    this.schema = schema;
    this.writers = writers;
  }

  public void bindIndex(ColumnWriterIndex index) {
    vectorIndex = index;
    for (int i = 0; i < writers.length; i++) {
      writers[i].bindIndex(index);
    }
  }

  @Override
  public TupleMetadata schema() { return schema; }

  @Override
  public int size() { return schema().size(); }

  @Override
  public void startWrite() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].startWrite();
    }
  }

  @Override
  public void startValue() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].startValue();
    }
  }

  @Override
  public void endValue() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].endValue();
    }
  }

  @Override
  public void endWrite() {
    for (int i = 0; i < writers.length;  i++) {
      writers[i].endWrite();
    }
  }

  @Override
  public ObjectWriter column(int colIndex) {
    return writers[colIndex];
  }

  @Override
  public ObjectWriter column(String colName) {
    int index = schema.index(colName);
    if (index == -1) {
      return null; }
    return writers[index];
  }

  @Override
  public void set(int colIndex, Object value) throws VectorOverflowException {
    ObjectWriter colWriter = column(colIndex);
    switch (colWriter.type()) {
    case ARRAY:
      colWriter.array().setArray(value);
      break;
    case SCALAR:
      colWriter.scalar().setObject(value);
      break;
    case TUPLE:
      colWriter.tuple().setTuple(value);
      break;
    default:
      throw new IllegalStateException("Unexpected object type: " + colWriter.type());
    }
  }

  @Override
  public void setTuple(Object ...values) throws VectorOverflowException {
    setObject(values);
  }

  @Override
  public void setObject(Object value) throws VectorOverflowException {
    Object values[] = (Object[]) value;
    int count = Math.min(values.length, schema().size());
    for (int i = 0; i < count; i++) {
      set(i, values[i]);
    }
  }

  @Override
  public ScalarWriter scalar(int colIndex) {
    return column(colIndex).scalar();
  }

  @Override
  public ScalarWriter scalar(String colName) {
    return column(colName).scalar();
  }

  @Override
  public TupleWriter tuple(int colIndex) {
    return column(colIndex).tuple();
  }

  @Override
  public TupleWriter tuple(String colName) {
    return column(colName).tuple();
  }

  @Override
  public ArrayWriter array(int colIndex) {
    return column(colIndex).array();
  }

  @Override
  public ArrayWriter array(String colName) {
    return column(colName).array();
  }

  @Override
  public ObjectType type(int colIndex) {
    return column(colIndex).type();
  }

  @Override
  public ObjectType type(String colName) {
    return column(colName).type();
  }
}
