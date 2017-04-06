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
package org.apache.drill.exec.physical.impl.xsort.managed;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperExecContext;
import org.apache.drill.exec.ops.OperExecContextImpl;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

/**
 * External sort batch: a sort batch which can spill to disk in
 * order to operate within a defined memory footprint.
 * <p>
 * <h4>Basic Operation</h4>
 * The operator has three key phases:
 * <p>
 * <ul>
 * <li>The load phase in which batches are read from upstream.</li>
 * <li>The merge phase in which spilled batches are combined to
 * reduce the number of files below the configured limit. (Best
 * practice is to configure the system to avoid this phase.)
 * <li>The delivery phase in which batches are combined to produce
 * the final output.</li>
 * </ul>
 * During the load phase:
 * <p>
 * <ul>
 * <li>The incoming (upstream) operator provides a series of batches.</li>
 * <li>This operator sorts each batch, and accumulates them in an in-memory
 * buffer.</li>
 * <li>If the in-memory buffer becomes too large, this operator selects
 * a subset of the buffered batches to spill.</li>
 * <li>Each spill set is merged to create a new, sorted collection of
 * batches, and each is spilled to disk.</li>
 * <li>To allow the use of multiple disk storage, each spill group is written
 * round-robin to a set of spill directories.</li>
 * </ul>
 * <p>
 * Data is spilled to disk as a "run". A run consists of one or more (typically
 * many) batches, each of which is itself a sorted run of records.
 * <p>
 * During the sort/merge phase:
 * <p>
 * <ul>
 * <li>When the input operator is complete, this operator merges the accumulated
 * batches (which may be all in memory or partially on disk), and returns
 * them to the output (downstream) operator in chunks of no more than
 * 64K records.</li>
 * <li>The final merge must combine a collection of in-memory and spilled
 * batches. Several limits apply to the maximum "width" of this merge. For
 * example, each open spill run consumes a file handle, and we may wish
 * to limit the number of file handles. Further, memory must hold one batch
 * from each run, so we may need to reduce the number of runs so that the
 * remaining runs can fit into memory. A consolidation phase combines
 * in-memory and spilled batches prior to the final merge to control final
 * merge width.</li>
 * <li>A special case occurs if no batches were spilled. In this case, the input
 * batches are sorted in memory without merging.</li>
 * </ul>
 * <p>
 * Many complex details are involved in doing the above; the details are explained
 * in the methods of this class.
 * <p>
 * <h4>Configuration Options</h4>
 * <dl>
 * <dt>drill.exec.sort.external.spill.fs</dt>
 * <dd>The file system (file://, hdfs://, etc.) of the spill directory.</dd>
 * <dt>drill.exec.sort.external.spill.directories</dt>
 * <dd>The comma delimited list of directories, on the above file
 * system, to which to spill files in round-robin fashion. The query will
 * fail if any one of the directories becomes full.</dt>
 * <dt>drill.exec.sort.external.spill.file_size</dt>
 * <dd>Target size for first-generation spill files Set this to large
 * enough to get nice long writes, but not so large that spill directories
 * are overwhelmed.</dd>
 * <dt>drill.exec.sort.external.mem_limit</dt>
 * <dd>Maximum memory to use for the in-memory buffer. (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.batch_limit</dt>
 * <dd>Maximum number of batches to hold in memory. (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.spill.max_count</dt>
 * <dd>Maximum number of batches to add to “first generation” files.
 * Defaults to 0 (no limit). (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.spill.min_count</dt>
 * <dd>Minimum number of batches to add to “first generation” files.
 * Defaults to 0 (no limit). (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.merge_limit</dt>
 * <dd>Sets the maximum number of runs to be merged in a single pass (limits
 * the number of open files.)</dd>
 * </dl>
 * <p>
 * The memory limit observed by this operator is the lesser of:
 * <ul>
 * <li>The maximum allocation allowed the allocator assigned to this batch
 * as set by the Foreman, or</li>
 * <li>The maximum limit configured in the mem_limit parameter above. (Primarily for
 * testing.</li>
 * </ul>
 * <h4>Output</h4>
 * It is helpful to note that the sort operator will produce one of two kinds of
 * output batches.
 * <ul>
 * <li>A large output with sv4 if data is sorted in memory. The sv4 addresses
 * the entire in-memory sort set. A selection vector remover will copy results
 * into new batches of a size determined by that operator.</li>
 * <li>A series of batches, without a selection vector, if the sort spills to
 * disk. In this case, the downstream operator will still be a selection vector
 * remover, but there is nothing for that operator to remove. Each batch is
 * of the size set by {@link #MAX_MERGED_BATCH_SIZE}.</li>
 * </ul>
 * Note that, even in the in-memory sort case, this operator could do the copying
 * to eliminate the extra selection vector remover. That is left as an exercise
 * for another time.
 * <h4>Logging</h4>
 * Logging in this operator serves two purposes:
 * <li>
 * <ul>
 * <li>Normal diagnostic information.</li>
 * <li>Capturing the essence of the operator functionality for analysis in unit
 * tests.</li>
 * </ul>
 * Test logging is designed to capture key events and timings. Take care
 * when changing or removing log messages as you may need to adjust unit tests
 * accordingly.
 */

public class ExternalSortBatch extends AbstractRecordBatch<ExternalSort> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";
  public static final String INTERRUPTION_WHILE_MERGING = "merging";

  private final RecordBatch incoming;

  /**
   * Schema of batches that this operator produces.
   */

  private BatchSchema schema;

//  private SelectionVector4 sv4;

  /**
   * Iterates over the final, sorted results.
   */

  private SortResults resultsIterator;
  private enum SortState { START, LOAD, DELIVER, DONE }
  private SortState sortState = SortState.START;

  private SortImpl sortImpl;

  // WARNING: The enum here is used within this class. But, the members of
  // this enum MUST match those in the (unmanaged) ExternalSortBatch since
  // that is the enum used in the UI to display metrics for the query profile.

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    NOT_USED,               // Was: peak value for totalSizeInMemory
                            // But operator already provides this value
    PEAK_BATCHES_IN_MEMORY, // maximum number of batches kept in memory
    MERGE_COUNT,            // Number of second+ generation merges
    MIN_BUFFER,             // Minimum memory level observed in operation.
    SPILL_MB;               // Number of MB of data spilled to disk. This
                            // amount is first written, then later re-read.
                            // So, disk I/O is twice this amount.

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, true);
    this.incoming = incoming;

    SortConfig sortConfig = new SortConfig(context.getConfig());
    SpillSet spillSet = new SpillSet(context.getConfig(), context.getHandle(),
                                     popConfig);
    OperExecContext opContext = new OperExecContextImpl(context, oContext, popConfig, injector);
    PriorityQueueCopierWrapper copierHolder = new PriorityQueueCopierWrapper(opContext);
    SpilledRuns spilledRuns = new SpilledRuns(opContext, spillSet, copierHolder);
    sortImpl = new SortImpl(opContext, sortConfig, spilledRuns, container);

    // The upstream operator checks on record count before we have
    // results. Create an empty result set temporarily to handle
    // these calls.

    resultsIterator = new SortImpl.EmptyResults(container);
  }

  @Override
  public int getRecordCount() {
    return resultsIterator.getRecordCount();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return resultsIterator.getSv4();
  }

  /**
   * Called by {@link AbstractRecordBatch} as a fast-path to obtain
   * the first record batch and setup the schema of this batch in order
   * to quickly return the schema to the client. Note that this method
   * fetches the first batch from upstream which will be waiting for
   * us the first time that {@link #innerNext()} is called.
   */

  @Override
  public void buildSchema() {
    IterOutcome outcome = next(incoming);
    switch (outcome) {
      case OK:
      case OK_NEW_SCHEMA:
        for (VectorWrapper<?> w : incoming) {
          @SuppressWarnings("resource")
          ValueVector v = container.addOrGet(w.getField());
          if (v instanceof AbstractContainerVector) {
            w.getValueVector().makeTransferPair(v); // Can we remove this hack?
            v.clear();
          }
          v.allocateNew(); // Can we remove this? - SVR fails with NPE (TODO)
        }
        container.buildSchema(SelectionVectorMode.NONE);
        container.setRecordCount(0);
        break;
      case STOP:
        state = BatchState.STOP;
        break;
      case OUT_OF_MEMORY:
        state = BatchState.OUT_OF_MEMORY;
        break;
      case NONE:
        state = BatchState.DONE;
        break;
      default:
        throw new IllegalStateException("Unexpected iter outcome: " + outcome);
    }
  }

  /**
   * Process each request for a batch. The first request retrieves
   * all the incoming batches and sorts them, optionally spilling to
   * disk as needed. Subsequent calls retrieve the sorted results in
   * fixed-size batches.
   */

  @Override
  public IterOutcome innerNext() {
    switch (sortState) {
    case DONE:
      return IterOutcome.NONE;
    case START:
    case LOAD:
      return load();
    case DELIVER:
      return nextOutputBatch();
    default:
      throw new IllegalStateException("Unexpected sort state: " + sortState);
    }
  }

  private IterOutcome nextOutputBatch() {
    if (resultsIterator.next()) {
      injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_WHILE_MERGING);
      return IterOutcome.OK;
    } else {
      logger.trace("Deliver phase complete: Returned {} batches, {} records",
                    resultsIterator.getBatchCount(), resultsIterator.getRecordCount());
      sortState = SortState.DONE;

      // Close the iterator here to release any remaining resources such
      // as spill files. This is important when a query has a join: the
      // first branch sort may complete before the second branch starts;
      // it may be quite a while after returning the last row before the
      // fragment executor calls this opeator's close method.

      resultsIterator.close();
      resultsIterator = null;
      return IterOutcome.NONE;
    }
  }

  /**
   * Load the results and sort them. May bail out early if an exceptional
   * condition is passed up from the input batch.
   *
   * @return return code: OK_NEW_SCHEMA if rows were sorted,
   * NONE if no rows
   */

  private IterOutcome load() {
    logger.trace("Start of load phase");

    // Clear the temporary container created by
    // buildSchema().

    container.clear();

    // Loop over all input batches

    for (;;) {
      IterOutcome result = loadBatch();

      // None means all batches have been read.

      if (result == IterOutcome.NONE) {
        break; }

      // Any outcome other than OK means something went wrong.

      if (result != IterOutcome.OK) {
        return result; }
    }

    // Anything to actually sort?

    resultsIterator = sortImpl.startMerge();
    if (! resultsIterator.next()) {
      sortState = SortState.DONE;
      return IterOutcome.NONE;
    }

    // sort may have prematurely exited due to should continue returning false.

    if (! context.shouldContinue()) {
      sortState = SortState.DONE;
      return IterOutcome.STOP;
    }

    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  /**
   * Load and process a single batch, handling schema changes. In general, the
   * external sort accepts only one schema.
   *
   * @return return code depending on the amount of data read from upstream
   */

  private IterOutcome loadBatch() {

    // If this is the very first batch, then AbstractRecordBatch
    // already loaded it for us in buildSchema().

    IterOutcome upstream;
    if (sortState == SortState.START) {
      sortState = SortState.LOAD;
      upstream = IterOutcome.OK_NEW_SCHEMA;
    } else {
      upstream = next(incoming);
    }
    switch (upstream) {
    case NONE:
    case STOP:
      return upstream;
    case OK_NEW_SCHEMA:
      setupSchema();
      // Fall through

    case OK:

      // Add the batch to the in-memory generation, spilling if
      // needed.

      sortImpl.addBatch(incoming);
      break;
    case OUT_OF_MEMORY:

      // Note: it is highly doubtful that this code actually works. It
      // requires that the upstream batches got to a safe place to run
      // out of memory and that no work was in-flight and thus abandoned.
      // Consider removing this case once resource management is in place.

      logger.error("received OUT_OF_MEMORY, trying to spill");
      if (! sortImpl.forceSpill()) {
        throw UserException.memoryError("Received OUT_OF_MEMORY, but enough batches to spill")
          .build(logger);
      }
      break;
    default:
      throw new IllegalStateException("Unexpected iter outcome: " + upstream);
    }
    return IterOutcome.OK;
  }

  /**
   * Handle a new schema from upstream. The ESB is quite limited in its ability
   * to handle schema changes.
   *
   * @param upstream the status code from upstream: either OK or OK_NEW_SCHEMA
   */

  private void setupSchema()  {

    // First batch: we won't have a schema.

    if (schema == null) {
      schema = incoming.getSchema();
    } else if (incoming.getSchema().equals(schema)) {
      // Nothing to do.  Artificial schema changes are ignored.
    } else if (unionTypeEnabled) {
      schema = SchemaUtil.mergeSchemas(schema, incoming.getSchema());
    } else {
      throw UserException.unsupportedError()
            .message("Schema changes not supported in External Sort. Please enable Union type.")
            .addContext("Previous schema", schema.toString())
            .addContext("Incoming schema", incoming.getSchema().toString())
            .build(logger);
    }
    sortImpl.setSchema(schema);
  }

  @Override
  public WritableBatch getWritableBatch() {
    throw new UnsupportedOperationException("A sort batch is not writable.");
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  /**
   * Extreme paranoia to avoid leaving resources unclosed in the case
   * of an error. Since generally only the first error is of interest,
   * we track only the first exception, not potential cascading downstream
   * exceptions.
   * <p>
   * Some Drill code ends up calling close() two or more times. The code
   * here protects itself from these undesirable semantics.
   */

  @Override
  public void close() {
    RuntimeException ex = null;
    try {
      if (resultsIterator != null) {
        resultsIterator.close();
        resultsIterator = null;
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      if (sortImpl != null) {
        sortImpl.close();
        sortImpl = null;
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }

    // The call to super.close() clears out the output container.
    // Doing so requires the allocator here, so it must be closed
    // after the super call.

    try {
      super.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    // Note: allocator is closed by the FragmentManager
    if (ex != null) {
      throw ex;
    }
  }
}
