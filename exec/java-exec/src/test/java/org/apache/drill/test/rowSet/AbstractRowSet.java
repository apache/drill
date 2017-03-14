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
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.SchemaChangeCallBack;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnAccessor.RowIndex;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.TupleReaderImpl;

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

  public static abstract class RowSetIndex implements RowIndex {
    protected int rowIndex = -1;

    public int position() { return rowIndex; }
    public abstract boolean next();
    public abstract int size();
    public abstract boolean valid();
    public void set(int index) { rowIndex = index; }
  }

  /**
   * Bounded (read-only) version of the row set index. When reading,
   * the row count is fixed, and set here.
   */

  public static abstract class BoundedRowIndex extends RowSetIndex {

    protected final int rowCount;

    public BoundedRowIndex(int rowCount) {
      this.rowCount = rowCount;
    }

    @Override
    public boolean next() {
      if (++rowIndex < rowCount ) {
        return true;
      } else {
        rowIndex--;
        return false;
      }
    }

    @Override
    public int size() { return rowCount; }

    @Override
    public boolean valid() { return rowIndex < rowCount; }
  }

  /**
   * Reader implementation for a row set.
   */

  public class RowSetReaderImpl extends TupleReaderImpl implements RowSetReader {

    protected final RowSetIndex index;

    public RowSetReaderImpl(TupleSchema schema, RowSetIndex index, AbstractColumnReader[] readers) {
      super(schema, readers);
      this.index = index;
    }

    @Override
    public boolean next() { return index.next(); }

    @Override
    public boolean valid() { return index.valid(); }

    @Override
    public int index() { return index.position(); }

    @Override
    public int size() { return index.size(); }

    @Override
    public int rowIndex() { return index.index(); }

    @Override
    public int batchIndex() { return index.batch(); }

    @Override
    public void set(int index) { this.index.set(index); }
  }

  protected final BufferAllocator allocator;
  protected final RowSetSchema schema;
  protected final VectorContainer container;
  protected SchemaChangeCallBack callBack = new SchemaChangeCallBack();

  public AbstractRowSet(BufferAllocator allocator, BatchSchema schema, VectorContainer container) {
    this.allocator = allocator;
    this.schema = new RowSetSchema(schema);
    this.container = container;
  }

  @Override
  public VectorAccessible vectorAccessible() { return container; }

  @Override
  public VectorContainer container() { return container; }

  @Override
  public int rowCount() { return container.getRecordCount(); }

  @Override
  public void clear() {
    container.zeroVectors();
    container.setRecordCount(0);
  }

  @Override
  public RowSetSchema schema() { return schema; }

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
  public BatchSchema batchSchema() {
    return container.getSchema();
  }
}
