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

import org.apache.drill.common.types.TypeProtos.DataMode;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.TupleMetadata;
import org.apache.drill.exec.record.TupleMetadata.ColumnMetadata;
import org.apache.drill.exec.record.TupleMetadata.StructureType;
import org.apache.drill.exec.record.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.ColumnReaderIndex;
import org.apache.drill.exec.vector.accessor.impl.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.accessor.reader.AbstractObjectReader;
import org.apache.drill.exec.vector.accessor.reader.MapReader;
import org.apache.drill.exec.vector.accessor.reader.ObjectArrayReader;
import org.apache.drill.exec.vector.accessor.reader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.test.rowSet.AbstractSingleRowSet.MapColumnStorage;
import org.apache.drill.test.rowSet.AbstractSingleRowSet.RowStorage;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;

/**
 * Implements a row set wrapper around a collection of "hyper vectors."
 * A hyper-vector is a logical vector formed by a series of physical vectors
 * stacked on top of one another. To make a row set, we have a hyper-vector
 * for each column. Another way to visualize this is as a "hyper row set":
 * a stacked collection of single row sets: each column is represented by a
 * vector per row set, with each vector in a row set having the same number
 * of rows. An SV4 then provides a uniform index into the rows in the
 * hyper set. A hyper row set is read-only.
 */

public class HyperRowSetImpl extends AbstractRowSet implements HyperRowSet {

  /**
   * Read-only row index into the hyper row set with batch and index
   * values mapping via an SV4.
   */

  public static class HyperRowIndex extends RowSetReaderIndex {

    private final SelectionVector4 sv4;

    public HyperRowIndex(SelectionVector4 sv4) {
      super(sv4.getCount());
      this.sv4 = sv4;
    }

    @Override
    public int vectorIndex() {
      return AccessorUtilities.sv4Index(sv4.get(rowIndex));
    }

    @Override
    public int batchIndex( ) {
      return AccessorUtilities.sv4Batch(sv4.get(rowIndex));
    }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static class HyperVectorAccessor implements VectorAccessor {

    private final ValueVector[] vectors;
    private ColumnReaderIndex rowIndex;

    public HyperVectorAccessor(VectorWrapper<?> vw) {
      vectors = vw.getValueVectors();
    }

    @Override
    public void bind(ColumnReaderIndex index) {
      rowIndex = index;
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batchIndex()];
    }
  }

  /**
   * Wrapper around a primitive (non-map, non-list) column vector.
   */

  public static class PrimitiveHyperColumnStorage extends ColumnStorage {
    protected final VectorWrapper<?> vectors;

    public PrimitiveHyperColumnStorage(ColumnMetadata schema, VectorWrapper<?> vectors) {
      super(schema);
      this.vectors = vectors;
    }

    @Override
    public AbstractObjectReader reader() {
      return ColumnAccessorFactory.buildColumnReader(schema.majorType(), new HyperVectorAccessor(vectors));
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      throw new IllegalStateException("Cannot allocate a hyper-vector.");
    }

    @Override
    public AbstractObjectWriter writer() {
      throw new IllegalStateException("Cannot write to a hyper-vector.");
    }
  }

  /**
   * Wrapper around a map vector to provide both a column and tuple view of
   * a single or repeated map.
   */

  public static class MapHyperColumnStorage extends BaseMapColumnStorage {
    private final VectorWrapper<?> vectors;

    public MapHyperColumnStorage(ColumnMetadata schema, VectorWrapper<?> vectors, ColumnStorage columns[]) {
      super(schema, columns);
      this.vectors = vectors;
    }

    public static MapHyperColumnStorage fromMap(ColumnMetadata schema, VectorWrapper<?> vectors) {
      return new MapHyperColumnStorage(schema, vectors, buildColumns(schema, vectors));
    }

    private static ColumnStorage[] buildColumns(ColumnMetadata schema, VectorWrapper<?> vectors) {
      TupleMetadata mapSchema = schema.mapSchema();
      ColumnStorage columns[] = new ColumnStorage[mapSchema.size()];
      for (int i = 0; i < mapSchema.size(); i++) {
        ColumnMetadata colSchema = mapSchema.metadata(i);
        VectorWrapper<?> child = vectors.getChildWrapper(new int[] {i});
        if (colSchema.structureType() == StructureType.TUPLE) {
          columns[i] = MapColumnStorage.fromMap(colSchema, (AbstractMapVector) child);
        } else {
          columns[i] = new PrimitiveHyperColumnStorage(colSchema, child);
        }
      }
      return columns;
    }

    @Override
    public AbstractObjectReader[] readers() {
      return HyperRowStorage.readers(this);
    }

    @Override
    public AbstractObjectWriter[] writers() {
      throw new IllegalStateException("Cannot write to a hyper-vector.");
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      throw new IllegalStateException("Cannot allocate a hyper-vector.");
    }

    @Override
    public AbstractObjectWriter writer() {
      throw new IllegalStateException("Cannot write to a hyper-vector.");
    }

    @Override
    public AbstractObjectReader reader() {
      AbstractObjectReader mapReader = MapReader.build(tupleSchema(), readers());
      if (schema.mode() != DataMode.REPEATED) {
        return mapReader;
      }
      return ObjectArrayReader.build(new HyperVectorAccessor(vectors), mapReader);
    }
  }

  /**
   * Wrapper around a vector container to map the vector container into the common
   * tuple format.
   */

  public static class HyperRowStorage extends BaseRowStorage {

    public HyperRowStorage(TupleMetadata schema, VectorContainer container, ColumnStorage columns[]) {
      super(schema, container, columns);
    }

    public static RowStorage fromContainer(TupleMetadata schema, VectorContainer container) {
      return new RowStorage(schema, container, buildChildren(schema, container));
    }

    public static RowStorage fromContainer(VectorContainer container) {
      return fromContainer(TupleSchema.fromFields(container.getSchema()), container);
    }

    private static ColumnStorage[] buildChildren(TupleMetadata schema, VectorContainer container) {
      assert schema.size() == container.getNumberOfColumns();
      ColumnStorage colStorage[] = new ColumnStorage[schema.size()];
      for (int i = 0; i < schema.size(); i++) {
        ColumnMetadata colSchema = schema.metadata(i);
        VectorWrapper<?> vectors = container.getValueVector(i);
        if (colSchema.structureType() == StructureType.TUPLE) {
          colStorage[i] = MapHyperColumnStorage.fromMap(colSchema, vectors);
        } else {
          colStorage[i] = new PrimitiveHyperColumnStorage(colSchema, vectors);
        }
      }
      return colStorage;
    }

    @Override
    public AbstractObjectReader[] readers() {
      return readers(this);
    }

    @Override
    public AbstractObjectWriter[] writers() {
      throw new IllegalStateException("Cannot write to a hyper-vector.");
    }

    @Override
    public void allocate(BufferAllocator allocator, int rowCount) {
      throw new IllegalStateException("Cannot allocate a hyper-vector.");
    }
  }

  /**
   * Selection vector that indexes into the hyper vectors.
   */

  private final SelectionVector4 sv4;

  public HyperRowSetImpl(BufferAllocator allocator, VectorContainer container, SelectionVector4 sv4) {
    super(allocator, HyperRowStorage.fromContainer(container));
    this.sv4 = sv4;
  }

  @Override
  public boolean isExtendable() { return false; }

  @Override
  public boolean isWritable() { return false; }

  @Override
  public RowSetReader reader() {
    return buildReader(new HyperRowIndex(sv4));
  }

  /**
   * Internal method to build the set of column readers needed for
   * this row set. Used when building a row set reader.
   * @param rowIndex object that points to the current row
   * @return an array of column readers: in the same order as the
   * (non-map) vectors.
   */

  protected RowSetReader buildReader(HyperRowIndex rowIndex) {
    return new RowSetReaderImpl(rowStorage.tupleSchema(), rowIndex, rowStorage.readers());
  }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.FOUR_BYTE; }

  @Override
  public SelectionVector4 getSv4() { return sv4; }

  @Override
  public int rowCount() { return sv4.getCount(); }

  @Override
  public RowSet merge(RowSet other) {
    return new HyperRowSetImpl(allocator, container().merge(other.container()), sv4);
  }
}
