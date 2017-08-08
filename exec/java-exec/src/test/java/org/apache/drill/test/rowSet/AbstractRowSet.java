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

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;

/**
 * Basic implementation of a row set for both the single and multiple
 * (hyper) varieties, both the fixed and extendible varieties.
 */

public abstract class AbstractRowSet implements RowSet {

  /**
   * Row set index base class used when indexing rows within a row
   * set for a row set reader. Keeps track of the current position,
   * which starts before the first row, meaning that the client
   * must call <tt>next()</tt> to advance to the first row.
   */

  public static abstract class RowSetReaderIndex implements ColumnReaderIndex {

    protected int rowIndex = -1;
    protected final int rowCount;

    public RowSetReaderIndex(int rowCount) {
      this.rowCount = rowCount;
    }

    public int position() { return rowIndex; }
    public void set(int index) { rowIndex = index; }

    public boolean next() {
      if (++rowIndex < rowCount ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    public int size() { return rowCount; }

    public boolean valid() { return rowIndex < rowCount; }
  }

  /**
   * Common interface to access a tuple backed by a vector container or a
   * map vector.
   */

  public interface TupleStorage {
    TupleMetadata tupleSchema();
    int size();
    AbstractRowSet.ColumnStorage storage(int index);
    AbstractObjectReader[] readers();
    AbstractObjectWriter[] writers();
    void allocate(BufferAllocator allocator, int rowCount);
  }

  /**
   * Represents a column within a tuple, including the tuple metadata
   * and column storage. A wrapper around a vector to include metadata
   * and handle nested tuples.
   */

  public static abstract class ColumnStorage {
    protected final ColumnMetadata schema;

    public ColumnStorage(ColumnMetadata schema) {
      this.schema = schema;
    }

    public ColumnMetadata columnSchema() { return schema; }
    public abstract AbstractObjectReader reader();
    public abstract AbstractObjectWriter writer();
    public abstract void allocate(BufferAllocator allocator, int rowCount);
  }


  /**
   * Wrapper around a map vector to provide both a column and tuple view of
   * a single or repeated map.
   */

  public static abstract class BaseMapColumnStorage extends ColumnStorage implements TupleStorage {

    protected final ColumnStorage columns[];

    public BaseMapColumnStorage(ColumnMetadata schema, ColumnStorage columns[]) {
      super(schema);
      this.columns = columns;
    }

    @Override
    public int size() { return schema.mapSchema().size(); }

    @Override
    public TupleMetadata tupleSchema() { return schema.mapSchema(); }

    @Override
    public ColumnStorage storage(int index) { return columns[index]; }
  }


  /**
   * Wrapper around a vector container to map the vector container into the common
   * tuple format.
   */

  public static abstract class BaseRowStorage implements TupleStorage {
    private final TupleMetadata schema;
    private final VectorContainer container;
    private final ColumnStorage columns[];

    public BaseRowStorage(TupleMetadata schema, VectorContainer container, ColumnStorage columns[]) {
      this.schema = schema;
      this.container = container;
      this.columns = columns;
    }

    @Override
    public int size() { return schema.size(); }

    @Override
    public TupleMetadata tupleSchema() { return schema; }

    public VectorContainer container() { return container; }

    @Override
    public ColumnStorage storage(int index) { return columns[index]; }

    protected static AbstractObjectReader[] readers(AbstractRowSet.TupleStorage storage) {
      AbstractObjectReader[] readers = new AbstractObjectReader[storage.tupleSchema().size()];
      for (int i = 0; i < readers.length; i++) {
        readers[i] = storage.storage(i).reader();
      }
      return readers;
    }

    protected static AbstractObjectWriter[] writers(AbstractRowSet.TupleStorage storage) {
      AbstractObjectWriter[] writers = new AbstractObjectWriter[storage.size()];
      for (int i = 0; i < writers.length;  i++) {
        writers[i] = storage.storage(i).writer();
      }
      return writers;
    }
  }

  protected final BufferAllocator allocator;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();
  protected final BaseRowStorage rowStorage;


  public AbstractRowSet(BufferAllocator allocator, BaseRowStorage rowStorage) {
    this.allocator = allocator;
    this.rowStorage = rowStorage;
  }

  @Override
  public VectorAccessible vectorAccessible() { return container(); }

  @Override
  public VectorContainer container() { return rowStorage.container(); }

  @Override
  public int rowCount() { return container().getRecordCount(); }

  @Override
  public void clear() {
    VectorContainer container = container();
    container.zeroVectors();
    container.setRecordCount(0);
  }

  @Override
  public TupleMetadata schema() { return rowStorage.tupleSchema(); }

  @Override
  public BufferAllocator allocator() { return allocator; }

  @Override
  public void print() {
    new RowSetPrinter(this).print();
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException("getSize");
  }

  @Override
  public BatchSchema batchSchema() { return container().getSchema(); }
}
