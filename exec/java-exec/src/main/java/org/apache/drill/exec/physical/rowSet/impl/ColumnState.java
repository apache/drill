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

import java.util.ArrayList;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.physical.rowSet.impl.SingleVectorState.OffsetVectorState;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.MapState;
import org.apache.drill.exec.record.metadata.ColumnMetadata;
import org.apache.drill.exec.vector.UInt4Vector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;
import org.apache.drill.exec.vector.accessor.writer.AbstractArrayWriter;
import org.apache.drill.exec.vector.accessor.writer.AbstractObjectWriter;
import org.apache.drill.exec.vector.accessor.writer.ColumnWriterFactory;
import org.apache.drill.exec.vector.complex.BaseRepeatedValueVector;

/**
 * Represents the write-time state for a column including the writer and the (optional)
 * backing vector. Implements per-column operations such as vector overflow. If a column
 * is a (possibly repeated) map, then the column state will hold a tuple state.
 * <p>
 * If a column is not projected, then the writer exists (to make life easier for the
 * reader), but there will be no vector backing the writer.
 * <p>
 * Different columns need different kinds of vectors: a data vector, possibly an offset
 * vector, or even a non-existent vector. The {@link VectorState} class abstracts out
 * these diffrences.
 */

public abstract class ColumnState {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnState.class);

  public static abstract class BaseMapColumnState extends ColumnState {
    protected final MapState mapState;

    public BaseMapColumnState(ResultSetLoaderImpl resultSetLoader,
         AbstractObjectWriter writer, VectorState vectorState,
         ProjectionSet projectionSet) {
      super(resultSetLoader, writer, vectorState);
      mapState = new MapState(resultSetLoader, this, projectionSet);
    }

    @Override
    public void rollover() {
      super.rollover();
      mapState.rollover();
    }

    @Override
    public void startBatch() {
      super.startBatch();
      mapState.startBatch();
    }

    @Override
    public void harvestWithLookAhead() {
      super.harvestWithLookAhead();
      mapState.harvestWithLookAhead();
    }

    @Override
    public void close() {
      super.close();
      mapState.close();
    }

    public MapState mapState() { return mapState; }
  }

  public static class MapColumnState extends BaseMapColumnState {

    public MapColumnState(ResultSetLoaderImpl resultSetLoader,
        ColumnMetadata columnSchema,
        ProjectionSet projectionSet) {
      super(resultSetLoader,
          ColumnWriterFactory.buildMap(columnSchema, null,
              new ArrayList<AbstractObjectWriter>()),
          new NullVectorState(),
          projectionSet);
    }

    @Override
    public void updateCardinality(int cardinality) {
      super.updateCardinality(cardinality);
      mapState.updateCardinality(cardinality);
    }
  }

  public static class MapArrayColumnState extends BaseMapColumnState {

    public MapArrayColumnState(ResultSetLoaderImpl resultSetLoader,
        AbstractObjectWriter writer,
        VectorState vectorState,
        ProjectionSet projectionSet) {
      super(resultSetLoader, writer,
          vectorState,
          projectionSet);
    }

    @SuppressWarnings("resource")
    public static MapArrayColumnState build(ResultSetLoaderImpl resultSetLoader,
        ColumnMetadata columnSchema,
        ProjectionSet projectionSet) {

      // Create the map's offset vector.

      UInt4Vector offsetVector = new UInt4Vector(
          BaseRepeatedValueVector.OFFSETS_FIELD,
          resultSetLoader.allocator());

      // Create the writer using the offset vector

      AbstractObjectWriter writer = ColumnWriterFactory.buildMapArray(
          columnSchema, offsetVector,
          new ArrayList<AbstractObjectWriter>());

      // Wrap the offset vector in a vector state

      VectorState vectorState = new OffsetVectorState(
            ((AbstractArrayWriter) writer.array()).offsetWriter(),
            offsetVector,
            (AbstractObjectWriter) writer.array().entry());

      // Assemble it all into the column state.

      return new MapArrayColumnState(resultSetLoader,
                  writer, vectorState, projectionSet);
    }

    @Override
    public void updateCardinality(int cardinality) {
      super.updateCardinality(cardinality);
      int childCardinality = cardinality * schema().expectedElementCount();
      mapState.updateCardinality(childCardinality);
    }
  }

  /**
   * Columns move through various lifecycle states as identified by this
   * enum. (Yes, sorry that the term "state" is used in two different ways
   * here: the variables for a column and the point within the column
   * lifecycle.
   */

  protected enum State {

    /**
     * Column is in the normal state of writing with no overflow
     * in effect.
     */

    NORMAL,

    /**
     * Like NORMAL, but means that the data has overflowed and the
     * column's data for the current row appears in the new,
     * overflow batch. For a client that omits some columns, written
     * columns will be in OVERFLOW state, unwritten columns in
     * NORMAL state.
     */

    OVERFLOW,

    /**
     * Indicates that the column has data saved
     * in the overflow batch.
     */

    LOOK_AHEAD,

    /**
     * Like LOOK_AHEAD, but indicates the special case that the column
     * was added after overflow, so there is no vector for the column
     * in the harvested batch.
     */

    NEW_LOOK_AHEAD
  }

  protected final ResultSetLoaderImpl resultSetLoader;
  protected final int addVersion;
  protected final VectorState vectorState;
  protected State state;
  protected AbstractObjectWriter writer;

  /**
   * Cardinality of the value itself. If this is an array,
   * then this is the number of arrays. A separate number,
   * the inner cardinality, is computed as the outer cardinality
   * times the expected array count (from metadata.) The inner
   * cardinality is the total number of array items in the
   * vector.
   */

  protected int outerCardinality;

  public ColumnState(ResultSetLoaderImpl resultSetLoader,
      AbstractObjectWriter writer, VectorState vectorState) {
    this.resultSetLoader = resultSetLoader;
    this.vectorState = vectorState;
    this.addVersion = resultSetLoader.bumpVersion();
    state = resultSetLoader.hasOverflow() ?
        State.NEW_LOOK_AHEAD : State.NORMAL;
    this.writer = writer;
  }

  public AbstractObjectWriter writer() { return writer; }
  public ColumnMetadata schema() { return writer.schema(); }

  public ValueVector vector() { return vectorState.vector(); }

  public void allocateVectors() {
    assert outerCardinality != 0;
    resultSetLoader.tallyAllocations(
        vectorState.allocate(outerCardinality));
  }

  /**
   * Prepare the column for a new row batch after overflow on the previous
   * batch. Restore the look-ahead buffer to the
   * active vector so we start writing where we left off.
   */

  public void startBatch() {
    switch (state) {
    case NORMAL:
      resultSetLoader.tallyAllocations(vectorState.allocate(outerCardinality));
      break;

    case NEW_LOOK_AHEAD:

      // Column is new, was not exchanged with backup vector

      break;

    case LOOK_AHEAD:

      // Restore the look-ahead values to the main vector.

      vectorState.startBatchWithLookAhead();
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // In all cases, we are back to normal writing.

    state = State.NORMAL;
  }

  /**
   * A column within the row batch overflowed. Prepare to absorb the rest of the
   * in-flight row by rolling values over to a new vector, saving the complete
   * vector for later. This column could have a value for the overflow row, or
   * for some previous row, depending on exactly when and where the overflow
   * occurs.
   */

  public void rollover() {
    assert state == State.NORMAL;

    // If the source index is 0, then we could not fit this one
    // value in the original vector. Nothing will be accomplished by
    // trying again with an an overflow vector. Just fail.
    //
    // Note that this is a judgment call. It is possible to allow the
    // vector to double beyond the limit, but that will require a bit
    // of thought to get right -- and, of course, completely defeats
    // the purpose of limiting vector size to avoid memory fragmentation...

    if (resultSetLoader.writerIndex().vectorIndex() == 0) {
      throw UserException
        .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
        .build(logger);
    }

    // Otherwise, do the roll-over to a look-ahead vector.

    vectorState.rollover(outerCardinality);

    // Remember that we did this overflow processing.

    state = State.OVERFLOW;
  }

  /**
   * Writing of a row batch is complete. Prepare the vector for harvesting
   * to send downstream. If this batch encountered overflow, set aside the
   * look-ahead vector and put the full vector buffer back into the active
   * vector.
   */

  public void harvestWithLookAhead() {
    switch (state) {
    case NEW_LOOK_AHEAD:

      // If added after overflow, no data to save from the complete
      // batch: the vector does not appear in the completed batch.

      break;

    case OVERFLOW:

      // Otherwise, restore the original, full buffer and
      // last write position.

      vectorState.harvestWithLookAhead();

      // Remember that we have look-ahead values stashed away in the
      // backup vector.

      state = State.LOOK_AHEAD;
      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  public void close() {
    vectorState.reset();
  }

  public void updateCardinality(int cardinality) {
    outerCardinality = cardinality;
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("addVersion", addVersion)
      .attribute("state", state)
      .attributeIdentity("writer", writer)
      .attribute("vectorState")
      ;
    vectorState.dump(format);
    format.endObject();
  }
}
