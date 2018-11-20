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
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.validate.IteratorValidatorBatchIterator;
import org.apache.drill.exec.physical.impl.xsort.managed.SortImpl.SortResults;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.SchemaUtil;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.complex.AbstractContainerVector;

import static org.apache.drill.exec.record.RecordBatch.IterOutcome.EMIT;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.NONE;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.OK_NEW_SCHEMA;
import static org.apache.drill.exec.record.RecordBatch.IterOutcome.STOP;

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
 * <dd>Maximum number of batches to add to "first generation" files.
 * Defaults to 0 (no limit). (Primarily for testing.)</dd>
 * <dt>drill.exec.sort.external.spill.min_count</dt>
 * <dd>Minimum number of batches to add to "first generation" files.
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
 * remover, but there is nothing for that operator to remove.
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

  // For backward compatibility, masquerade as the original
  // external sort. Else, some tests don't pass.

  protected static final ControlsInjector injector =
      ControlsInjectorFactory.getInjector(org.apache.drill.exec.physical.impl.xsort.ExternalSortBatch.class);

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";
  public static final String INTERRUPTION_WHILE_MERGING = "merging";
  private boolean retainInMemoryBatchesOnNone;

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

  private SortConfig sortConfig;

  private SortImpl sortImpl;

  private IterOutcome lastKnownOutcome;

  private boolean firstBatchOfSchema;

  private VectorContainer outputWrapperContainer;

  private SelectionVector4 outputSV4;

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
    outputWrapperContainer = new VectorContainer(context.getAllocator());
    outputSV4 = new SelectionVector4(context.getAllocator(), 0);
    sortConfig = new SortConfig(context.getConfig(), context.getOptions());
    oContext.setInjector(injector);
    sortImpl = createNewSortImpl();

    // The upstream operator checks on record count before we have
    // results. Create an empty result set temporarily to handle
    // these calls.

    resultsIterator = new SortImpl.EmptyResults(outputWrapperContainer);
  }

  @Override
  public int getRecordCount() {
    return resultsIterator.getRecordCount();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    // Return outputSV4 instead of resultsIterator sv4. For resultsIterator which has null SV4 outputSV4 will be empty.
    // But Sort with EMIT outcome will ideally fail in those cases while preparing output container as it's not
    // supported currently, like for spilling scenarios
    return outputSV4;
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    return resultsIterator.getSv2();
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
      return NONE;
    case START:
      return load();
    case LOAD:
      if (!this.retainInMemoryBatchesOnNone) {
        resetSortState();
      }
      return (sortState == SortState.DONE) ? NONE : load();
    case DELIVER:
      return nextOutputBatch();
    default:
      throw new IllegalStateException("Unexpected sort state: " + sortState);
    }
  }

  private IterOutcome nextOutputBatch() {
    // Call next on outputSV4 for it's state to progress in parallel to resultsIterator state
    outputSV4.next();

    // But if results iterator next returns true that means it has more results to pass
    if (resultsIterator.next()) {
      container.setRecordCount(getRecordCount());
      injector.injectUnchecked(context.getExecutionControls(), INTERRUPTION_WHILE_MERGING);
    }
    // getFinalOutcome will take care of returning correct IterOutcome when there is no data to pass and for
    // EMIT/NONE scenarios
    return getFinalOutcome();
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

    // Don't clear the temporary container created by buildSchema() after each load since across EMIT outcome we have
    // to maintain the ValueVector references for downstream operators

    // Loop over all input batches

    IterOutcome result = OK;
    for (;;) {
      result = loadBatch();

      // NONE/EMIT means all batches have been read at this record boundary
      if (result == NONE || result == EMIT) {
        break; }

      // if result is STOP that means something went wrong.

      if (result == STOP) {
        return result; }
    }

    // Anything to actually sort?
    resultsIterator = sortImpl.startMerge();
    if (! resultsIterator.next()) {
      // If there is no records to sort and we got NONE then just return NONE
      if (result == NONE) {
        sortState = SortState.DONE;
        return NONE;
      }
    }

    // sort may have prematurely exited due to shouldContinue() returning false.

    if (!context.getExecutorState().shouldContinue()) {
      sortState = SortState.DONE;
      return STOP;
    }

    // If we are here that means there is some data to be returned downstream. We have to prepare output container
    prepareOutputContainer(resultsIterator);
    return getFinalOutcome();
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

    if (sortState == SortState.START) {
      sortState = SortState.LOAD;
      lastKnownOutcome = OK_NEW_SCHEMA;
    } else {
      lastKnownOutcome = next(incoming);
    }
    switch (lastKnownOutcome) {
    case NONE:
    case STOP:
      return lastKnownOutcome;
    case OK_NEW_SCHEMA:
      firstBatchOfSchema = true;
      setupSchema();
      // Fall through

    case OK:
    case EMIT:

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
        throw UserException.memoryError("Received OUT_OF_MEMORY, but not enough batches to spill")
          .build(logger);
      }
      break;
    default:
      throw new IllegalStateException("Unexpected iter outcome: " + lastKnownOutcome);
    }
    return lastKnownOutcome;
  }

  /**
   * Handle a new schema from upstream. The ESB is quite limited in its ability
   * to handle schema changes.
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
   * </p>
   */

  @Override
  public void close() {

    // Sanity check: if close is called twice, just ignore
    // the second call.

    if (sortImpl == null) {
      return;
    }

    RuntimeException ex = null;

    // If we got far enough to have a results iterator, close
    // that first.

    try {
      if (resultsIterator != null) {
        resultsIterator.close();
        resultsIterator = null;
      }
    } catch (RuntimeException e) {
      ex = e;
    }

    // Then close the "guts" of the sort operation.
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
    // (when closing the operator context) after the super call.

    try {
      outputWrapperContainer.clear();
      outputSV4.clear();
      super.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }

    // Finally close the operator context (which closes the
    // child allocator.)

    try {
      oContext.close();
    } catch (RuntimeException e) {
      ex = ex == null ? e : ex;
    }
    if (ex != null) {
      throw ex;
    }
  }

  /**
   * Workaround for DRILL-5656. We wish to release the batches for an
   * in-memory sort once data is delivered. Normally, we can release them
   * just before returning NONE. But, the StreamingAggBatch requires that
   * the batches still be present on NONE. This method "sniffs" the input
   * provided, and if the external sort, sets a mode that retains the
   * batches. Yes, it is a horrible hack. But, necessary until the Drill
   * iterator protocol can be revised.
   *
   * @param incoming the incoming batch for some operator which may
   * (or may not) be an external sort (or, an external sort wrapped
   * in a batch iterator validator.)
   */

  public static void retainSv4OnNone(RecordBatch incoming) {
    if (incoming instanceof IteratorValidatorBatchIterator) {
      incoming = ((IteratorValidatorBatchIterator) incoming).getIncoming();
    }
    if (incoming instanceof ExternalSortBatch) {
      ((ExternalSortBatch) incoming).retainInMemoryBatchesOnNone = true;
    }
  }

  public static void releaseBatches(RecordBatch incoming) {
    if (incoming instanceof IteratorValidatorBatchIterator) {
      incoming = ((IteratorValidatorBatchIterator) incoming).getIncoming();
    }
    if (incoming instanceof ExternalSortBatch) {
      ExternalSortBatch esb = (ExternalSortBatch) incoming;
      esb.resetSortState();
    }
  }

  private void releaseResources() {
    if (resultsIterator != null) {
      resultsIterator.close();
    }

    // We only zero vectors for actual output container
    outputWrapperContainer.clear();
    outputSV4.clear();
    container.zeroVectors();

    // Close sortImpl for this boundary
    if (sortImpl != null) {
      sortImpl.close();
    }
  }

  /**
   * Method to reset sort state after every EMIT outcome is seen to process next batch of incoming records which
   * belongs to different record boundary.
   */
  private void resetSortState() {
    sortState = (lastKnownOutcome == EMIT) ? SortState.LOAD : SortState.DONE;
    // This means if it has received NONE/EMIT outcome and flag to retain is false which will be the case in presence of
    // StreamingAggBatch only since it will explicitly call releaseBacthes on ExternalSort when its done consuming
    // all the data buffer.

    // Close the iterator here to release any remaining resources such
    // as spill files. This is important when a query has a join: the
    // first branch sort may complete before the second branch starts;
    // it may be quite a while after returning the last batch before the
    // fragment executor calls this operator's close method.
    //
    // Note however, that the StreamingAgg operator REQUIRES that the sort
    // retain the batches behind an SV4 when doing an in-memory sort because
    // the StreamingAgg retains a reference to that data that it will use
    // after receiving a NONE result code. See DRILL-5656.
    releaseResources();

    if (lastKnownOutcome == EMIT) {
      sortImpl = createNewSortImpl();
      // Set the schema again since with reset we create new instance of SortImpl
      sortImpl.setSchema(schema);
      resultsIterator = new SortImpl.EmptyResults(outputWrapperContainer);
    }
  }

  /**
   * Based on first batch for this schema or not it either clears off the output container or just zero down the vectors
   * Then calls {@link SortResults#updateOutputContainer(VectorContainer, SelectionVector4, IterOutcome, BatchSchema)}
   * to populate the output container of sort with results data. It is done this way for the support of EMIT outcome
   * where SORT will return results multiple time in same minor fragment so there needs a way to preserve the
   * ValueVector references across output batches.
   * However it currently only supports SortResults of type EmptyResults and MergeSortWrapper. We don't expect
   * spilling to happen in EMIT outcome scenario hence it's not supported now.
   * @param sortResults - Final sorted result which contains the container with data
   */
  private void prepareOutputContainer(SortResults sortResults) {
    if (firstBatchOfSchema) {
      container.clear();
    } else {
      container.zeroVectors();
    }
    sortResults.updateOutputContainer(container, outputSV4, lastKnownOutcome, schema);
  }

  /**
   * Provides the final IterOutcome which Sort should return downstream with current output batch. It considers
   * following cases:
   * 1) If it is the first output batch of current known schema then return OK_NEW_SCHEMA to downstream and reset the
   * flag firstBatchOfSchema.
   * 2) If the current output row count is zero, then return outcome of EMIT or NONE based on the received outcome
   * from upstream and also reset the SortState.
   * 3) If EMIT is received from upstream and all output rows can fit in current output batch then send it downstream
   * with EMIT outcome and set SortState to LOAD for next EMIT boundary. Otherwise if all output rows cannot fit in
   * current output batch then send current batch with OK outcome and set SortState to DELIVER.
   * 4) In other cases send current output batch with OK outcome and set SortState to DELIVER. This is for cases when
   * all the incoming batches are received with OK outcome and EMIT is not seen.
   *
   * @return - IterOutcome - outcome to send downstream
   */
  private IterOutcome getFinalOutcome() {
    IterOutcome outcomeToReturn;

    // If this is the first output batch for current known schema then return OK_NEW_SCHEMA to downstream
    if (firstBatchOfSchema) {
      outcomeToReturn = OK_NEW_SCHEMA;
      firstBatchOfSchema = false;
      sortState = SortState.DELIVER;
    } else if (getRecordCount() == 0) { // There is no record to send downstream
      outcomeToReturn = lastKnownOutcome == EMIT ? EMIT : NONE;
      if (!this.retainInMemoryBatchesOnNone) {
        resetSortState();
      }
    } else if (lastKnownOutcome == EMIT) {
      final boolean hasMoreRecords = outputSV4.hasNext();
      sortState = hasMoreRecords ? SortState.DELIVER : SortState.LOAD;
      outcomeToReturn = hasMoreRecords ? OK : EMIT;
    } else {
      outcomeToReturn = OK;
      sortState = SortState.DELIVER;
    }
    return outcomeToReturn;
  }

  /**
   * Method to create new instances of SortImpl
   * @return SortImpl
   */
  private SortImpl createNewSortImpl() {
    SpillSet spillSet = new SpillSet(context.getConfig(), context.getHandle(), popConfig);
    PriorityQueueCopierWrapper copierHolder = new PriorityQueueCopierWrapper(oContext);
    SpilledRuns spilledRuns = new SpilledRuns(oContext, spillSet, copierHolder);
    return new SortImpl(oContext, sortConfig, spilledRuns, outputWrapperContainer);
  }

  @Override
  public void dump() {
    logger.error("ExternalSortBatch[schema={}, sortState={}, sortConfig={}, outputWrapperContainer={}, "
            + "outputSV4={}, container={}]",
        schema, sortState, sortConfig, outputWrapperContainer, outputSV4, container);
  }
}
