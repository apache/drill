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

import java.util.List;

import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.accessor.ColumnWriterIndex;
import org.apache.drill.exec.vector.complex.MapVector;

/**
 * Writer for a Drill Map type. Maps are actually tuples, just like rows.
 */

public abstract class MapWriter extends AbstractTupleWriter {

  /**
   * Wrap the outer index to avoid incrementing the array index
   * on the call to <tt>nextElement().</tt> For maps, the increment
   * is done at the map level, not the column level.
   */

  private static class MemberWriterIndex implements ColumnWriterIndex {
    private ColumnWriterIndex baseIndex;

    private MemberWriterIndex(ColumnWriterIndex baseIndex) {
      this.baseIndex = baseIndex;
    }

    @Override public int rowStartIndex() { return baseIndex.rowStartIndex(); }
    @Override public int vectorIndex() { return baseIndex.vectorIndex(); }
    @Override public void nextElement() { }
    @Override public void rollover() { }
    @Override public ColumnWriterIndex outerIndex() {
      return baseIndex.outerIndex();
    }

    @Override
    public String toString() {
      return new StringBuilder()
        .append("[")
        .append(getClass().getSimpleName())
        .append(" baseIndex = ")
        .append(baseIndex.toString())
        .append("]")
        .toString();
    }
  }

  /**
   * Writer for a single (non-array) map. Clients don't really "write" maps;
   * rather, this writer is a holder for the columns within the map, and those
   * columns are what is written.
   */

  protected static class SingleMapWriter extends MapWriter {
    private final MapVector mapVector;

    protected SingleMapWriter(ColumnMetadata schema, MapVector vector, List<AbstractObjectWriter> writers) {
      super(schema, writers);
      mapVector = vector;
    }

    @Override
    public void endWrite() {
      super.endWrite();

      // Special form of set value count: used only for
      // this class to avoid setting the value count of children.
      // Setting these counts was already done. Doing it again
      // will corrupt nullable vectors because the writers don't
      // set the "lastSet" field of nullable vector accessors,
      // and the initial value of -1 will cause all values to
      // be overwritten.
      //
      // Note that the map vector can be null if there is no actual
      // map vector represented by this writer.

      if (mapVector != null) {
        mapVector.setMapValueCount(vectorIndex.vectorIndex());
      }
    }
  }

  /**
   * Writer for a an array of maps. A single array index coordinates writes
   * to the constituent member vectors so that, say, the values for (row 10,
   * element 5) all occur to the same position in the columns within the map.
   * Since the map is an array, it has an associated offset vector, which the
   * parent array writer is responsible for maintaining.
   */

  protected static class ArrayMapWriter extends MapWriter {

    protected ArrayMapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }

    @Override
    public void bindIndex(ColumnWriterIndex index) {

      // This is a repeated map, so the provided index is an array element
      // index. Convert this to an index that will not increment the element
      // index on each write so that a map with three members, say, won't
      // increment the index for each member. Rather, the index must be
      // incremented at the array level.

      bindIndex(index, new MemberWriterIndex(index));
    }

    // In endWrite(), do not call setValueCount on the map vector.
    // Doing so will zero-fill the composite vectors because
    // the internal map state does not track the writer state.
    // Instead, the code in this structure has set the value
    // count for each composite vector individually.
  }

  protected static class DummyMapWriter extends MapWriter {

    protected DummyMapWriter(ColumnMetadata schema,
        List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }
  }

  protected static class DummyArrayMapWriter extends MapWriter {

    protected DummyArrayMapWriter(ColumnMetadata schema,
        List<AbstractObjectWriter> writers) {
      super(schema, writers);
    }
  }

  protected final ColumnMetadata mapColumnSchema;

  protected MapWriter(ColumnMetadata schema, List<AbstractObjectWriter> writers) {
    super(schema.mapSchema(), writers);
    mapColumnSchema = schema;
  }
}
