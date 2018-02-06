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
package org.apache.drill.exec.physical.rowSet.impl;

import java.util.List;

import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.impl.ColumnState.BaseMapColumnState;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.record.metadata.TupleSchema;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractMapVector;
import org.apache.drill.exec.vector.complex.MapVector;
import org.apache.drill.exec.vector.complex.RepeatedMapVector;

/**
 * Builds the harvest vector container that includes only the columns that
 * are included in the harvest schema version. That is, it excludes columns
 * added while writing an overflow row.
 * <p>
 * Because a Drill row is actually a hierarchy, walks the internal hierarchy
 * and builds a corresponding output hierarchy.
 * <ul>
 * <li>The root node is the row itself (vector container),</li>
 * <li>Internal nodes are maps (structures),</li>
 * <li>Leaf notes are primitive vectors (which may be arrays).</li>
 * </ul>
 * The basic algorithm is to identify the version of the output schema,
 * then add any new columns added up to that version. This object maintains
 * the output container across batches, meaning that updates are incremental:
 * we need only add columns that are new since the last update. And, those new
 * columns will always appear directly after all existing columns in the row
 * or in a map.
 * <p>
 * As special case occurs when columns are added in the overflow row. These
 * columns <i>do not</i> appear in the output container for the main part
 * of the batch; instead they appear in the <i>next</i> output container
 * that includes the overflow row.
 * <p>
 * Since the container here may contain a subset of the internal columns, an
 * interesting case occurs for maps. The maps in the output container are
 * <b>not</b> the same as those used internally. Since a map column can contain
 * either one list of columns or another, the internal and external maps must
 * differ. The set of child vectors (except for child maps) are shared.
 */

public class VectorContainerBuilder {

  /**
   * Drill vector containers and maps are both tuples, but they irritatingly
   * have completely different APIs for working with their child vectors.
   * This class acts as a proxy to wrap the two APIs to provide a common
   * view for the use of the container builder.
   */

  public static abstract class TupleProxy {
    protected TupleMetadata schema;

    public TupleProxy(TupleMetadata schema) {
      this.schema = schema;
    }

    protected abstract int size();
    protected abstract ValueVector vector(int index);
    protected abstract void add(ValueVector vector);

    protected TupleProxy mapProxy(int index) {
      return new MapProxy(
          schema.metadata(index).mapSchema(),
          (AbstractMapVector) vector(index));
    }
  }

  /**
   * Proxy wrapper class for a vector container.
   */

  protected static class ContainerProxy extends TupleProxy {

    private VectorContainer container;

    protected ContainerProxy(TupleMetadata schema, VectorContainer container) {
      super(schema);
      this.container = container;
    }

    @Override
    protected int size() {
      return container.getNumberOfColumns();
    }

    @Override
    protected ValueVector vector(int index) {
      return container.getValueVector(index).getValueVector();
    }

    @Override
    protected void add(ValueVector vector) {
      container.add(vector);
    }
  }

  /**
   * Proxy wrapper for a map container.
   */

  protected static class MapProxy extends TupleProxy {

    private AbstractMapVector mapVector;

    protected MapProxy(TupleMetadata schema, AbstractMapVector mapVector) {
      super(schema);
      this.mapVector = mapVector;
    }

    @Override
    protected int size() {
      return mapVector.size();
    }

    @Override
    protected ValueVector vector(int index) {
      return mapVector.getChildByOrdinal(index);
    }

    @Override
    protected void add(ValueVector vector) {
      mapVector.putChild(vector.getField().getName(), vector);
    }
  }

  private final ResultSetLoaderImpl resultSetLoader;
  private int outputSchemaVersion = -1;
  private TupleMetadata schema;
  private VectorContainer container;

  public VectorContainerBuilder(ResultSetLoaderImpl rsLoader) {
    this.resultSetLoader = rsLoader;
    container = new VectorContainer(rsLoader.allocator);
    schema = new TupleSchema();
  }

  public void update(int targetVersion) {
    if (outputSchemaVersion >= targetVersion) {
      return;
    }
    outputSchemaVersion = targetVersion;
    updateTuple(resultSetLoader.rootState(), new ContainerProxy(schema, container));
    container.buildSchema(SelectionVectorMode.NONE);
  }

  public VectorContainer container() { return container; }

  public int outputSchemaVersion() { return outputSchemaVersion; }

  public BufferAllocator allocator() {
     return resultSetLoader.allocator();
  }

  private void updateTuple(TupleState sourceModel, TupleProxy destProxy) {
    int prevCount = destProxy.size();
    List<ColumnState> cols = sourceModel.columns();
    int currentCount = cols.size();

    // Scan any existing maps for column additions

    for (int i = 0; i < prevCount; i++) {
      ColumnState colState = cols.get(i);
      if (! colState.schema().isProjected()) {
        continue;
      }
      if (colState.schema().isMap()) {
        updateTuple((TupleState) ((BaseMapColumnState) colState).mapState(), destProxy.mapProxy(i));
      }
    }

    // Add new columns, which may be maps

    for (int i = prevCount; i < currentCount; i++) {
      ColumnState colState = cols.get(i);
      if (! colState.schema().isProjected()) {
        continue;
      }

      // If the column was added after the output schema version cutoff,
      // skip that column for now.

      if (colState.addVersion > outputSchemaVersion) {
        break;
      }
      if (colState.schema().isMap()) {
        buildMap(destProxy, (BaseMapColumnState) colState);
      } else {
        destProxy.add(colState.vector());
        destProxy.schema.addColumn(colState.schema());
        assert destProxy.size() == destProxy.schema.size();
      }
    }
  }

  @SuppressWarnings("resource")
  private void buildMap(TupleProxy parentTuple, BaseMapColumnState colModel) {

    // Creating the map vector will create its contained vectors if we
    // give it a materialized field with children. So, instead pass a clone
    // without children so we can add them.

    ColumnMetadata mapColSchema = colModel.schema().cloneEmpty();

    // Don't get the map vector from the vector cache. Map vectors may
    // have content that varies from batch to batch. Only the leaf
    // vectors can be cached.

    AbstractMapVector mapVector;
    if (mapColSchema.isArray()) {

      // A repeated map shares an offset vector with the internal
      // repeated map.

      UInt4Vector offsets = (UInt4Vector) colModel.vector();
      mapVector = new RepeatedMapVector(mapColSchema.schema(), offsets, null);
    } else {
      mapVector = new MapVector(mapColSchema.schema(), allocator(), null);
    }

    // Add the map vector and schema to the parent tuple

    parentTuple.add(mapVector);
    int index = parentTuple.schema.addColumn(mapColSchema);
    assert parentTuple.size() == parentTuple.size();

    // Update the tuple, which will add the new columns in the map

    updateTuple(colModel.mapState(), parentTuple.mapProxy(index));
  }

  public TupleMetadata schema() { return schema; }
}
