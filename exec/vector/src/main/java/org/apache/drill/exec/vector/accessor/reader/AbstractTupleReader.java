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
package org.apache.drill.exec.vector.accessor.reader;

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.accessor.ArrayReader;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.ObjectReader;
import org.apache.drill.exec.vector.accessor.ObjectType;
import org.apache.drill.exec.vector.accessor.ScalarElementReader;
import org.apache.drill.exec.vector.accessor.ScalarReader;
import org.apache.drill.exec.vector.accessor.TupleReader;

/**
 * Reader for a tuple (a row or a map.) Provides access to each
 * column using either a name or a numeric index.
 */

public abstract class AbstractTupleReader implements TupleReader {

  public static class TupleObjectReader extends AbstractObjectReader {

    private AbstractTupleReader tupleReader;

    public TupleObjectReader(AbstractTupleReader tupleReader) {
      this.tupleReader = tupleReader;
    }

    @Override
    public void bindIndex(ColumnReaderIndex index) {
      tupleReader.bindIndex(index);
    }

    @Override
    public ObjectType type() {
      return ObjectType.TUPLE;
    }

    @Override
    public TupleReader tuple() {
      return tupleReader;
    }

    @Override
    public Object getObject() {
      return tupleReader.getObject();
    }

    @Override
    public String getAsString() {
      return tupleReader.getAsString();
    }

    @Override
    public void reposition() {
      tupleReader.reposition();
    }
  }

  protected final TupleMetadata schema;
  private final AbstractObjectReader readers[];

  protected AbstractTupleReader(TupleMetadata schema, AbstractObjectReader readers[]) {
    this.schema = schema;
    this.readers = readers;
  }

  public void bindIndex(ColumnReaderIndex index) {
    for (int i = 0; i < readers.length; i++) {
      readers[i].bindIndex(index);
    }
  }

  @Override
  public TupleMetadata schema() { return schema; }

  @Override
  public int columnCount() { return schema().size(); }

  @Override
  public ObjectReader column(int colIndex) {
    return readers[colIndex];
  }

  @Override
  public ObjectReader column(String colName) {
    int index = schema.index(colName);
    if (index == -1) {
      return null; }
    return readers[index];
  }

  @Override
  public ScalarReader scalar(int colIndex) {
    return column(colIndex).scalar();
  }

  @Override
  public ScalarReader scalar(String colName) {
    return column(colName).scalar();
  }

  @Override
  public TupleReader tuple(int colIndex) {
    return column(colIndex).tuple();
  }

  @Override
  public TupleReader tuple(String colName) {
    return column(colName).tuple();
  }

  @Override
  public ArrayReader array(int colIndex) {
    return column(colIndex).array();
  }

  @Override
  public ArrayReader array(String colName) {
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

  @Override
  public ScalarElementReader elements(int colIndex) {
    return column(colIndex).elements();
  }

  @Override
  public ScalarElementReader elements(String colName) {
    return column(colName).elements();
  }

  public void reposition() {
    for (int i = 0; i < columnCount(); i++) {
      readers[i].reposition();
    }
  }

  @Override
  public Object getObject() {
    List<Object> elements = new ArrayList<>();
    for (int i = 0; i < columnCount(); i++) {
      elements.add(readers[i].getObject());
    }
    return elements;
  }

  @Override
  public String getAsString() {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    for (int i = 0; i < columnCount(); i++) {
      if (i > 0) {
        buf.append( ", " );
      }
      buf.append(readers[i].getAsString());
    }
    buf.append(")");
    return buf.toString();
  }
}
