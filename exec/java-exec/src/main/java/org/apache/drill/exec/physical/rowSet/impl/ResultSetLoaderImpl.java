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

import java.util.Collection;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.physical.rowSet.ResultSetLoader;
import org.apache.drill.exec.physical.rowSet.ResultVectorCache;
import org.apache.drill.exec.physical.rowSet.RowSetLoader;
import org.apache.drill.exec.physical.rowSet.impl.TupleState.RowState;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.accessor.impl.HierarchicalFormatter;

/**
 * Implementation of the result set loader.
 * @see {@link ResultSetLoader}
 */

public class ResultSetLoaderImpl implements ResultSetLoader {

  /**
   * Read-only set of options for the result set loader.
   */

  public static class ResultSetOptions {
    public final int vectorSizeLimit;
    public final int rowCountLimit;
    public final ResultVectorCache vectorCache;
    public final Collection<SchemaPath> projection;
    public final TupleMetadata schema;
    public final long maxBatchSize;

    public ResultSetOptions() {
      vectorSizeLimit = ValueVector.MAX_BUFFER_SIZE;
      rowCountLimit = DEFAULT_ROW_COUNT;
      projection = null;
      vectorCache = null;
      schema = null;
      maxBatchSize = -1;
    }

    public ResultSetOptions(OptionBuilder builder) {
      this.vectorSizeLimit = builder.vectorSizeLimit;
      this.rowCountLimit = builder.rowCountLimit;
      this.projection = builder.projection;
      this.vectorCache = builder.vectorCache;
      this.schema = builder.schema;
      this.maxBatchSize = builder.maxBatchSize;
    }

    public void dump(HierarchicalFormatter format) {
      format
        .startObject(this)
        .attribute("vectorSizeLimit", vectorSizeLimit)
        .attribute("rowCountLimit", rowCountLimit)
        .attribute("projection", projection)
        .endObject();
    }
  }

  private enum State {
    /**
     * Before the first batch.
     */

    START,

    /**
     * Writing to a batch normally.
     */

    ACTIVE,

    /**
     * Batch overflowed a vector while writing. Can continue
     * to write to a temporary "overflow" batch until the
     * end of the current row.
     */

    OVERFLOW,

    /**
     * Temporary state to avoid batch-size related overflow while
     * an overflow is in progress.
     */

    IN_OVERFLOW,

    /**
     * Batch is full due to reaching the row count limit
     * when saving a row.
     * No more writes allowed until harvesting the current batch.
     */

    FULL_BATCH,

    /**
     * Current batch was harvested: data is gone. No lookahead
     * batch exists.
     */

    HARVESTED,

    /**
     * Current batch was harvested and its data is gone. However,
     * overflow occurred during that batch and the data exists
     * in the overflow vectors.
     * <p>
     * This state needs special consideration. The column writer
     * structure maintains its state (offsets, etc.) from the OVERFLOW
     * state, but the buffers currently in the vectors are from the
     * complete batch. <b>No writes can be done in this state!</b>
     * The writer state does not match the data in the buffers.
     * The code here does what it can to catch this state. But, if
     * some client tries to write to a column writer in this state,
     * bad things will happen. Doing so is invalid (the write is outside
     * of a batch), so this is not a terrible restriction.
     * <p>
     * Said another way, the current writer state is invalid with respect
     * to the active buffers, but only if the writers try to act on the
     * buffers. Since the writers won't do so, this temporary state is
     * fine. The correct buffers are restored once a new batch is started
     * and the state moves to ACTIVE.
     */

    LOOK_AHEAD,

    /**
     * Mutator is closed: no more operations are allowed.
     */

    CLOSED
  }

  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ResultSetLoaderImpl.class);

  /**
   * Options provided to this loader.
   */

  private final ResultSetOptions options;

  /**
   * Allocator for vectors created by this loader.
   */

  final BufferAllocator allocator;

  /**
   * Internal structure used to work with the vectors (real or dummy) used
   * by this loader.
   */

  final RowState rootState;

  /**
   * Top-level writer index that steps through the rows as they are written.
   * When an overflow batch is in effect, indexes into that batch instead.
   * Since a batch is really a tree of tuples, in which some branches of
   * the tree are arrays, the root indexes here feeds into array indexes
   * within the writer structure that points to the current position within
   * an array column.
   */

  private final WriterIndexImpl writerIndex;

  /**
   * The row-level writer for stepping through rows as they are written,
   * and for accessing top-level columns.
   */

  private final RowSetLoaderImpl rootWriter;

  /**
   * Vector cache for this loader.
   * @see {@link OptionBuilder#setVectorCache()}.
   */

  private final ResultVectorCache vectorCache;

  /**
   * Tracks the state of the row set loader. Handling vector overflow requires
   * careful stepping through a variety of states as the write proceeds.
   */

  private State state = State.START;

  /**
   * Track the current schema as seen by the writer. Each addition of a column
   * anywhere in the schema causes the active schema version to increase by one.
   * This allows very easy checks for schema changes: save the prior version number
   * and compare it against the current version number.
   */

  private int activeSchemaVersion;

  /**
   * Track the current schema as seen by the consumer of the batches that this
   * loader produces. The harvest schema version can be behind the active schema
   * version in the case in which new columns are added to the overflow row.
   * Since the overflow row won't be visible to the harvested batch, that batch
   * sees the schema as it existed at a prior version: the harvest schema
   * version.
   */

  private int harvestSchemaVersion;

  /**
   * Builds the harvest vector container that includes only the columns that
   * are included in the harvest schema version. That is, it excludes columns
   * added while writing the overflow row.
   */

  private VectorContainerBuilder containerBuilder;

  /**
   * Counts the batches harvested (sent downstream) from this loader. Does
   * not include the current, in-flight batch.
   */

  private int harvestBatchCount;

  /**
   * Counts the rows included in previously-harvested batches. Does not
   * include the number of rows in the current batch.
   */

  private int previousRowCount;

  /**
   * Number of rows in the harvest batch. If an overflow batch is in effect,
   * then this is the number of rows in the "main" batch before the overflow;
   * that is the number of rows in the batch that will be harvested. If no
   * overflow row is in effect, then this number is undefined (and should be
   * zero.)
   */

  private int pendingRowCount;

  /**
   * The number of rows per batch. Starts with the configured amount. Can be
   * adjusted between batches, perhaps based on the actual observed size of
   * input data.
   */

  private int targetRowCount;

  /**
   * Total bytes allocated to the current batch.
   */

  protected int accumulatedBatchSize;

  protected final ProjectionSet projectionSet;

  public ResultSetLoaderImpl(BufferAllocator allocator, ResultSetOptions options) {
    this.allocator = allocator;
    this.options = options;
    targetRowCount = options.rowCountLimit;
    writerIndex = new WriterIndexImpl(this);

    if (options.vectorCache == null) {
      vectorCache = new NullResultVectorCacheImpl(allocator);
    } else {
      vectorCache = options.vectorCache;
    }

    // If projection, build the projection map.

    projectionSet = ProjectionSetImpl.parse(options.projection);

    // Build the row set model depending on whether a schema is provided.

    rootState = new RowState(this);
    rootWriter = rootState.rootWriter();

    // If no schema, columns will be added incrementally as they
    // are discovered. Start with an empty model.

    if (options.schema != null) {

      // Schema provided. Populate a model (and create vectors) for the
      // provided schema. The schema can be extended later, but normally
      // won't be if known up front.

      logger.debug("Schema: " + options.schema.toString());
      rootState.buildSchema(options.schema);
    }
  }

  private void updateCardinality() {
    rootState.updateCardinality(targetRowCount());
  }

  public ResultSetLoaderImpl(BufferAllocator allocator) {
    this(allocator, new ResultSetOptions());
  }

  public BufferAllocator allocator() { return allocator; }

  protected int bumpVersion() {

    // Update the active schema version. We cannot update the published
    // schema version at this point because a column later in this same
    // row might cause overflow, and any new columns in this row will
    // be hidden until a later batch. But, if we are between batches,
    // then it is fine to add the column to the schema.

    activeSchemaVersion++;
    switch (state) {
    case HARVESTED:
    case START:
    case LOOK_AHEAD:
      harvestSchemaVersion = activeSchemaVersion;
      break;
    default:
      break;

    }
    return activeSchemaVersion;
  }

  @Override
  public int schemaVersion() { return harvestSchemaVersion; }

  @Override
  public void startBatch() {
    switch (state) {
    case HARVESTED:
    case START:
      logger.trace("Start batch");
      accumulatedBatchSize = 0;
      updateCardinality();
      rootState.startBatch();
      checkInitialAllocation();

      // The previous batch ended without overflow, so start
      // a new batch, and reset the write index to 0.

      writerIndex.reset();
      rootWriter.startWrite();
      break;

    case LOOK_AHEAD:

      // A row overflowed so keep the writer index at its current value
      // as it points to the second row in the overflow batch. However,
      // the last write position of each writer must be restored on
      // a column-by-column basis, which is done by the visitor.

      logger.trace("Start batch after overflow");
      rootState.startBatch();

      // Note: no need to do anything with the writers; they were left
      // pointing to the correct positions in the look-ahead batch.
      // The above simply puts the look-ahead vectors back "under"
      // the writers.

      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Update the visible schema with any pending overflow batch
    // updates.

    harvestSchemaVersion = activeSchemaVersion;
    pendingRowCount = 0;
    state = State.ACTIVE;
  }

  @Override
  public RowSetLoader writer() {
    if (state == State.CLOSED) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    return rootWriter;
  }

  @Override
  public ResultSetLoader setRow(Object... values) {
    startRow();
    writer().setTuple(values);
    saveRow();
    return this;
  }

  /**
   * Called before writing a new row. Implementation of
   * {@link RowSetLoader#start()}.
   */

  protected void startRow() {
    switch (state) {
    case ACTIVE:

      // Update the visible schema with any pending overflow batch
      // updates.

      harvestSchemaVersion = activeSchemaVersion;
      rootWriter.startRow();
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Finalize the current row. Implementation of
   * {@link RowSetLoader#save()}.
   */

  protected void saveRow() {
    switch (state) {
    case ACTIVE:
      rootWriter.endArrayValue();
      rootWriter.saveRow();
      if (! writerIndex.next()) {
        state = State.FULL_BATCH;
      }

      // No overflow row. Advertise the schema version to the client.

      harvestSchemaVersion = activeSchemaVersion;
      break;

    case OVERFLOW:

      // End the value of the look-ahead row in the look-ahead vectors.

      rootWriter.endArrayValue();
      rootWriter.saveRow();

      // Advance the writer index relative to the look-ahead batch.

      writerIndex.next();

      // Stay in the overflow state. Doing so will cause the writer
      // to report that it is full.
      //
      // Also, do not change the harvest schema version. We will
      // expose to the downstream operators the schema in effect
      // at the start of the row. Columns added within the row won't
      // appear until the next batch.

      break;

    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }
  }

  /**
   * Implementation of {@link RowSetLoader#isFull()}
   * @return true if the batch is full (reached vector capacity or the
   * row count limit), false if more rows can be added
   */

  protected boolean isFull() {
    switch (state) {
    case ACTIVE:
      return ! writerIndex.valid();
    case OVERFLOW:
    case FULL_BATCH:
      return true;
    default:
      return false;
    }
  }

  @Override
  public boolean writeable() {
    return state == State.ACTIVE || state == State.OVERFLOW;
  }

  private boolean isBatchActive() {
    return state == State.ACTIVE || state == State.OVERFLOW ||
           state == State.FULL_BATCH ;
  }

  /**
   * Implementation for {#link {@link RowSetLoader#rowCount()}.
   *
   * @return the number of rows to be sent downstream for this
   * batch. Does not include the overflow row.
   */

  protected int rowCount() {
    switch (state) {
    case ACTIVE:
    case FULL_BATCH:
      return writerIndex.size();
    case OVERFLOW:
      return pendingRowCount;
    default:
      return 0;
    }
  }

  protected WriterIndexImpl writerIndex() { return writerIndex; }

  @Override
  public void setTargetRowCount(int rowCount) {
    targetRowCount = Math.max(1, rowCount);
  }

  @Override
  public int targetRowCount() { return targetRowCount; }

  @Override
  public int targetVectorSize() { return options.vectorSizeLimit; }

  protected void overflowed() {
    logger.trace("Vector overflow");

    // If we see overflow when we are already handling overflow, it means
    // that a single value is too large to fit into an entire vector.
    // Fail the query.
    //
    // Note that this is a judgment call. It is possible to allow the
    // vector to double beyond the limit, but that will require a bit
    // of thought to get right -- and, of course, completely defeats
    // the purpose of limiting vector size to avoid memory fragmentation...
    //
    // Individual columns handle the case in which overflow occurs on the
    // first row of the main batch. This check handles the pathological case
    // in which we successfully overflowed, but then another column
    // overflowed during the overflow row -- that indicates that that one
    // column can't fit in an empty vector. That is, this check is for a
    // second-order overflow.

    if (state == State.OVERFLOW) {
      throw UserException
          .memoryError("A single column value is larger than the maximum allowed size of 16 MB")
          .build(logger);
    }
    if (state != State.ACTIVE) {
      throw new IllegalStateException("Unexpected state: " + state);
    }
    state = State.IN_OVERFLOW;

    // Preserve the number of rows in the now-complete batch.

    pendingRowCount = writerIndex.vectorIndex();

    // Roll-over will allocate new vectors. Update with the latest
    // array cardinality.

    updateCardinality();

//    rootWriter.dump(new HierarchicalPrinter());

    // Wrap up the completed rows into a batch. Sets
    // vector value counts. The rollover data still exists so
    // it can be moved, but it is now past the recorded
    // end of the vectors (though, obviously, not past the
    // physical end.)

    rootWriter.preRollover();

    // Roll over vector values.

    accumulatedBatchSize = 0;
    rootState.rollover();

    // Adjust writer state to match the new vector values. This is
    // surprisingly easy if we not that the current row is shifted to
    // the 0 position in the new vector, so we just shift all offsets
    // downward by the current row position at each repeat level.

    rootWriter.postRollover();

    // The writer index is reset back to 0. Because of the above roll-over
    // processing, some vectors may now already have values in the 0 slot.
    // However, the vector that triggered overflow has not yet written to
    // the current record, and so will now write to position 0. After the
    // completion of the row, all 0-position values should be written (or
    // at least those provided by the client.)
    //
    // For arrays, the writer might have written a set of values
    // (v1, v2, v3), and v4 might have triggered the overflow. In this case,
    // the array values have been moved, offset vectors adjusted, the
    // element writer adjusted, so that v4 will be written to index 3
    // to produce (v1, v2, v3, v4, v5, ...) in the look-ahead vector.

    writerIndex.rollover();
    checkInitialAllocation();

    // Remember that overflow is in effect.

    state = State.OVERFLOW;
  }

  protected boolean hasOverflow() { return state == State.OVERFLOW; }

  @Override
  public VectorContainer harvest() {
    int rowCount;
    switch (state) {
    case ACTIVE:
    case FULL_BATCH:
      rowCount = harvestNormalBatch();
      logger.trace("Harvesting {} rows", rowCount);
      break;
    case OVERFLOW:
      rowCount = harvestOverflowBatch();
      logger.trace("Harvesting {} rows after overflow", rowCount);
      break;
    default:
      throw new IllegalStateException("Unexpected state: " + state);
    }

    // Build the output container

    VectorContainer container = outputContainer();
    container.setRecordCount(rowCount);

    // Finalize: update counts, set state.

    harvestBatchCount++;
    previousRowCount += rowCount;
    return container;
  }

  private int harvestNormalBatch() {

    // Wrap up the vectors: final fill-in, set value count, etc.

    rootWriter.endBatch();
    harvestSchemaVersion = activeSchemaVersion;
    state = State.HARVESTED;
    return writerIndex.size();
  }

  private int harvestOverflowBatch() {
    rootState.harvestWithLookAhead();
    state = State.LOOK_AHEAD;
    return pendingRowCount;
  }

  @Override
  public VectorContainer outputContainer() {
    // Build the output container.

    if (containerBuilder == null) {
      containerBuilder = new VectorContainerBuilder(this);
    }
    containerBuilder.update(harvestSchemaVersion);
    return containerBuilder.container();
  }

  @Override
  public TupleMetadata harvestSchema() {
    return containerBuilder.schema();
  }

  @Override
  public void close() {
    if (state == State.CLOSED) {
      return;
    }
    rootState.close();

    // Do not close the vector cache; the caller owns that and
    // will, presumably, reuse those vectors for another writer.

    state = State.CLOSED;
  }

  @Override
  public int batchCount() {
    return harvestBatchCount + (rowCount() == 0 ? 0 : 1);
  }

  @Override
  public int totalRowCount() {
    int total = previousRowCount;
    if (isBatchActive()) {
      total += pendingRowCount + writerIndex.size();
    }
    return total;
  }

  public ResultVectorCache vectorCache() { return vectorCache; }
  public RowState rootState() { return rootState; }

  /**
   * Return whether a vector within the current batch can expand. Limits
   * are enforce only if a limit was provided in the options.
   *
   * @param delta increase in vector size
   * @return true if the vector can expand, false if an overflow
   * event should occur
   */

  public boolean canExpand(int delta) {
    accumulatedBatchSize += delta;
    return state == State.IN_OVERFLOW ||
           options.maxBatchSize <= 0 ||
           accumulatedBatchSize <= options.maxBatchSize;
  }

  /**
   * Accumulate the initial vector allocation sizes.
   *
   * @param allocationBytes number of bytes allocated to a vector
   * in the batch setup step
   */

  public void tallyAllocations(int allocationBytes) {
    accumulatedBatchSize += allocationBytes;
  }

  /**
   * Log and check the initial vector allocation. If a batch size
   * limit is set, warn if the initial allocation exceeds the limit.
   * This will occur if the target row count is incorrect for the
   * data size.
   */

  private void checkInitialAllocation() {
    if (options.maxBatchSize < 0) {
      logger.debug("Initial vector allocation: {}, no batch limit specified",
          accumulatedBatchSize);
    }
    else if (accumulatedBatchSize > options.maxBatchSize) {
      logger.warn("Initial vector allocation: {}, but batch size limit is: {}",
          accumulatedBatchSize, options.maxBatchSize);
    } else {
      logger.debug("Initial vector allocation: {}, batch size limit: {}",
          accumulatedBatchSize, options.maxBatchSize);
    }
  }

  public void dump(HierarchicalFormatter format) {
    format
      .startObject(this)
      .attribute("options");
    options.dump(format);
    format
      .attribute("index", writerIndex.vectorIndex())
      .attribute("state", state)
      .attribute("activeSchemaVersion", activeSchemaVersion)
      .attribute("harvestSchemaVersion", harvestSchemaVersion)
      .attribute("pendingRowCount", pendingRowCount)
      .attribute("targetRowCount", targetRowCount)
      ;
    format.attribute("root");
    rootState.dump(format);
    format.attribute("rootWriter");
    rootWriter.dump(format);
    format.endObject();
  }
}
