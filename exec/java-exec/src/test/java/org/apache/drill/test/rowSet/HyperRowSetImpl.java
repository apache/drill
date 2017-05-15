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

import java.util.ArrayList;
import java.util.List;

import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.HyperVectorWrapper;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.AccessorUtilities;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader;
import org.apache.drill.exec.vector.accessor.impl.AbstractColumnReader.VectorAccessor;
import org.apache.drill.exec.vector.accessor.impl.ColumnAccessorFactory;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.test.rowSet.RowSet.HyperRowSet;
import org.apache.drill.test.rowSet.RowSetSchema.FlattenedSchema;
import org.apache.drill.test.rowSet.RowSetSchema.LogicalColumn;
import org.apache.drill.test.rowSet.RowSetSchema.PhysicalSchema;

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

  public static class HyperRowIndex extends BoundedRowIndex {

    private final SelectionVector4 sv4;

    public HyperRowIndex(SelectionVector4 sv4) {
      super(sv4.getCount());
      this.sv4 = sv4;
    }

    @Override
    public int index() {
      return AccessorUtilities.sv4Index(sv4.get(rowIndex));
    }

    @Override
    public int batch( ) {
      return AccessorUtilities.sv4Batch(sv4.get(rowIndex));
    }
  }

  /**
   * Vector accessor used by the column accessors to obtain the vector for
   * each column value. That is, position 0 might be batch 4, index 3,
   * while position 1 might be batch 1, index 7, and so on.
   */

  public static class HyperVectorAccessor implements VectorAccessor {

    private final HyperRowIndex rowIndex;
    private final ValueVector[] vectors;

    public HyperVectorAccessor(HyperVectorWrapper<ValueVector> hvw, HyperRowIndex rowIndex) {
      this.rowIndex = rowIndex;
      vectors = hvw.getValueVectors();
    }

    @Override
    public ValueVector vector() {
      return vectors[rowIndex.batch()];
    }
  }

  /**
   * Build a hyper row set by restructuring a hyper vector bundle into a uniform
   * shape. Consider this schema: <pre><code>
   * { a: 10, b: { c: 20, d: { e: 30 } } }</code></pre>
   * <p>
   * The hyper container, with two batches, has this structure:
   * <table border="1">
   * <tr><th>Batch</th><th>a</th><th>b</th></tr>
   * <tr><td>0</td><td>Int vector</td><td>Map Vector(Int vector, Map Vector(Int vector))</td></th>
   * <tr><td>1</td><td>Int vector</td><td>Map Vector(Int vector, Map Vector(Int vector))</td></th>
   * </table>
   * <p>
   * The above table shows that top-level scalar vectors (such as the Int Vector for column
   * a) appear "end-to-end" as a hyper-vector. Maps also appear end-to-end. But, the
   * contents of the map (column c) do not appear end-to-end. Instead, they appear as
   * contents in the map vector. To get to c, one indexes into the map vector, steps inside
   * the map to find c and indexes to the right row.
   * <p>
   * Similarly, the maps for d do not appear end-to-end, one must step to the right batch
   * in b, then step to d.
   * <p>
   * Finally, to get to e, one must step
   * into the hyper vector for b, then steps to the proper batch, steps to d, step to e
   * and finally step to the row within e. This is a very complex, costly indexing scheme
   * that differs depending on map nesting depth.
   * <p>
   * To simplify access, this class restructures the maps to flatten the scalar vectors
   * into end-to-end hyper vectors. For example, for the above:
   * <p>
   * <table border="1">
   * <tr><th>Batch</th><th>a</th><th>c</th><th>d</th></tr>
   * <tr><td>0</td><td>Int vector</td><td>Int vector</td><td>Int vector</td></th>
   * <tr><td>1</td><td>Int vector</td><td>Int vector</td><td>Int vector</td></th>
   * </table>
   *
   * The maps are still available as hyper vectors, but separated into map fields.
   * (Scalar access no longer needs to access the maps.) The result is a uniform
   * addressing scheme for both top-level and nested vectors.
   */

  public static class HyperVectorBuilder {

    protected final HyperVectorWrapper<?> valueVectors[];
    protected final HyperVectorWrapper<AbstractMapVector> mapVectors[];
    private final List<ValueVector> nestedScalars[];
    private int vectorIndex;
    private int mapIndex;
    private final PhysicalSchema physicalSchema;

    @SuppressWarnings("unchecked")
    public HyperVectorBuilder(RowSetSchema schema) {
      physicalSchema = schema.physical();
      FlattenedSchema flatSchema = schema.flatAccess();
      valueVectors = new HyperVectorWrapper<?>[schema.hierarchicalAccess().count()];
      if (flatSchema.mapCount() == 0) {
        mapVectors = null;
        nestedScalars = null;
      } else {
        mapVectors = (HyperVectorWrapper<AbstractMapVector>[])
            new HyperVectorWrapper<?>[flatSchema.mapCount()];
        nestedScalars = new ArrayList[flatSchema.count()];
      }
    }

    @SuppressWarnings("unchecked")
    public HyperVectorWrapper<ValueVector>[] mapContainer(VectorContainer container) {
      int i = 0;
      for (VectorWrapper<?> w : container) {
        HyperVectorWrapper<?> hvw = (HyperVectorWrapper<?>) w;
        if (w.getField().getType().getMinorType() == MinorType.MAP) {
          HyperVectorWrapper<AbstractMapVector> mw = (HyperVectorWrapper<AbstractMapVector>) hvw;
          mapVectors[mapIndex++] = mw;
          buildHyperMap(physicalSchema.column(i).mapSchema(), mw);
        } else {
          valueVectors[vectorIndex++] = hvw;
        }
        i++;
      }
      if (nestedScalars != null) {
        buildNestedHyperVectors();
      }
      return (HyperVectorWrapper<ValueVector>[]) valueVectors;
    }

    private void buildHyperMap(PhysicalSchema mapSchema, HyperVectorWrapper<AbstractMapVector> mapWrapper) {
      createHyperVectors(mapSchema);
      for (AbstractMapVector mapVector : mapWrapper.getValueVectors()) {
        buildMap(mapSchema, mapVector);
      }
    }

    private void buildMap(PhysicalSchema mapSchema, AbstractMapVector mapVector) {
      for (ValueVector v : mapVector) {
        LogicalColumn col = mapSchema.column(v.getField().getName());
        if (col.isMap()) {
          buildMap(col.mapSchema, (AbstractMapVector) v);
        } else {
          nestedScalars[col.accessIndex()].add(v);
        }
      }
    }

    private void createHyperVectors(PhysicalSchema mapSchema) {
      for (int i = 0; i < mapSchema.count(); i++) {
        LogicalColumn col = mapSchema.column(i);
        if (col.isMap()) {
          createHyperVectors(col.mapSchema);
        } else {
          nestedScalars[col.accessIndex()] = new ArrayList<ValueVector>();
        }
      }
    }

    private void buildNestedHyperVectors() {
      for (int i = 0;  i < nestedScalars.length; i++) {
        if (nestedScalars[i] == null) {
          continue;
        }
        ValueVector vectors[] = new ValueVector[nestedScalars[i].size()];
        nestedScalars[i].toArray(vectors);
        assert valueVectors[i] == null;
        valueVectors[i] = new HyperVectorWrapper<ValueVector>(vectors[0].getField(), vectors, false);
      }
    }
  }

  /**
   * Selection vector that indexes into the hyper vectors.
   */

  private final SelectionVector4 sv4;

  /**
   * Collection of hyper vectors in flattened order: a left-to-right,
   * depth first ordering of vectors in maps. Order here corresponds to
   * the order used for column indexes in the row set reader.
   */

  private final HyperVectorWrapper<ValueVector> hvw[];

  public HyperRowSetImpl(BufferAllocator allocator, VectorContainer container, SelectionVector4 sv4) {
    super(allocator, container.getSchema(), container);
    this.sv4 = sv4;
    hvw = new HyperVectorBuilder(schema).mapContainer(container);
  }

  @Override
  public boolean isExtendable() { return false; }

  @Override
  public boolean isWritable() { return false; }

  @Override
  public RowSetWriter writer() {
    throw new UnsupportedOperationException("Cannot write to a hyper vector");
  }

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
    FlattenedSchema accessSchema = schema().flatAccess();
    AbstractColumnReader readers[] = new AbstractColumnReader[accessSchema.count()];
    for (int i = 0; i < readers.length; i++) {
      MaterializedField field = accessSchema.column(i);
      readers[i] = ColumnAccessorFactory.newReader(field.getType());
      HyperVectorWrapper<ValueVector> hvw = getHyperVector(i);
      readers[i].bind(rowIndex, field, new HyperVectorAccessor(hvw, rowIndex));
    }
    return new RowSetReaderImpl(accessSchema, rowIndex, readers);
  }

  @Override
  public SelectionVectorMode indirectionType() { return SelectionVectorMode.FOUR_BYTE; }

  @Override
  public SelectionVector4 getSv4() { return sv4; }

  @Override
  public HyperVectorWrapper<ValueVector> getHyperVector(int i) { return hvw[i]; }

  @Override
  public int rowCount() { return sv4.getCount(); }

  @Override
  public RowSet merge(RowSet other) {
    return new HyperRowSetImpl(allocator, container().merge(other.container()), sv4);
  }
}
