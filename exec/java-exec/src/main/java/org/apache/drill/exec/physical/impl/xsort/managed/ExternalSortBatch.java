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

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.drill.common.AutoCloseables;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.config.ExternalSort;
import org.apache.drill.exec.physical.impl.sort.RecordBatchData;
import org.apache.drill.exec.physical.impl.spill.RecordBatchSizer;
import org.apache.drill.exec.physical.impl.spill.SpillSet;
import org.apache.drill.exec.physical.impl.xsort.MSortTemplate;
import org.apache.drill.exec.physical.impl.xsort.SingleBatchSorter;
import org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.InputBatch;
import org.apache.drill.exec.physical.impl.xsort.managed.BatchGroup.SpilledRun;
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

import com.google.common.collect.Lists;

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
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExternalSortBatch.class);
  protected static final ControlsInjector injector = ControlsInjectorFactory.getInjector(ExternalSortBatch.class);

  /**
   * Smallest allowed output batch size. The smallest output batch
   * created even under constrained memory conditions.
   */
  private static final int MIN_MERGED_BATCH_SIZE = 256 * 1024;

  /**
   * In the bizarre case where the user gave us an unrealistically low
   * spill file size, set a floor at some bare minimum size. (Note that,
   * at this size, big queries will create a huge number of files, which
   * is why the configuration default is one the order of hundreds of MB.)
   */

  private static final long MIN_SPILL_FILE_SIZE = 1 * 1024 * 1024;

  public static final String INTERRUPTION_AFTER_SORT = "after-sort";
  public static final String INTERRUPTION_AFTER_SETUP = "after-setup";
  public static final String INTERRUPTION_WHILE_SPILLING = "spilling";
  public static final String INTERRUPTION_WHILE_MERGING = "merging";
  public static final long DEFAULT_SPILL_BATCH_SIZE = 8L * 1024 * 1024;
  public static final long MIN_SPILL_BATCH_SIZE = 256 * 1024;

  private final RecordBatch incoming;

  /**
   * Memory allocator for this operator itself. Incoming batches are
   * transferred into this allocator. Intermediate batches used during
   * merge also reside here.
   */

  private final BufferAllocator allocator;

  /**
   * Schema of batches that this operator produces.
   */

  private BatchSchema schema;

  /**
   * Incoming batches buffered in memory prior to spilling
   * or an in-memory merge.
   */

  private LinkedList<BatchGroup.InputBatch> bufferedBatches = Lists.newLinkedList();
  private LinkedList<BatchGroup.SpilledRun> spilledRuns = Lists.newLinkedList();
  private SelectionVector4 sv4;

  /**
   * The number of records to add to each output batch sent to the
   * downstream operator or spilled to disk.
   */

  private int mergeBatchRowCount;
  private int peakNumBatches = -1;

  /**
   * Maximum memory this operator may use. Usually comes from the
   * operator definition, but may be overridden by a configuration
   * parameter for unit testing.
   */

  private long memoryLimit;

  /**
   * Iterates over the final, sorted results.
   */

  private SortResults resultsIterator;

  /**
   * Manages the set of spill directories and files.
   */

  private final SpillSet spillSet;

  /**
   * Manages the copier used to merge a collection of batches into
   * a new set of batches.
   */

  private final CopierHolder copierHolder;

  private enum SortState { START, LOAD, DELIVER, DONE }
  private SortState sortState = SortState.START;
  private int inputRecordCount = 0;
  private int inputBatchCount = 0; // total number of batches received so far
  private final OperatorCodeGenerator opCodeGen;

  /**
   * Estimated size of the records for this query, updated on each
   * new batch received from upstream.
   */

  private int estimatedRowWidth;

  /**
   * Size of the merge batches that this operator produces. Generally
   * the same as the merge batch size, unless low memory forces a smaller
   * value.
   */

  private long targetMergeBatchSize;

  /**
   * Estimate of the input batch size based on the largest batch seen
   * thus far.
   */
  private long estimatedInputBatchSize;

  /**
   * Maximum number of spilled runs that can be merged in a single pass.
   */

  private int mergeLimit;

  /**
   * Target size of the first-generation spill files.
   */
  private long spillFileSize;

  /**
   * Tracks the minimum amount of remaining memory for use
   * in populating an operator metric.
   */

  private long minimumBufferSpace;

  /**
   * Maximum memory level before spilling occurs. That is, we can buffer input
   * batches in memory until we reach the level given by the buffer memory pool.
   */

  private long bufferMemoryPool;

  /**
   * Maximum memory that can hold batches during the merge
   * phase.
   */

  private long mergeMemoryPool;

  /**
   * The target size for merge batches sent downstream.
   */

  private long preferredMergeBatchSize;

  /**
   * Sum of the total number of bytes read from upstream.
   * This is the raw memory bytes, not actual data bytes.
   */

  private long totalInputBytes;

  /**
   * The configured size for each spill batch.
   */
  private Long preferredSpillBatchSize;

  /**
   * Tracks the maximum density of input batches. Density is
   * the amount of actual data / amount of memory consumed.
   * Low density batches indicate an EOF or something wrong in
   * an upstream operator because a low-density batch wastes
   * memory.
   */

  private int maxDensity;
  private int lastDensity = -1;

  /**
   * Estimated number of rows that fit into a single spill batch.
   */

  private int spillBatchRowCount;

  /**
   * The estimated actual spill batch size which depends on the
   * details of the data rows for any particular query.
   */

  private int targetSpillBatchSize;

  // WARNING: The enum here is used within this class. But, the members of
  // this enum MUST match those in the (unmanaged) ExternalSortBatch since
  // that is the enum used in the UI to display metrics for the query profile.

  public enum Metric implements MetricDef {
    SPILL_COUNT,            // number of times operator spilled to disk
    RETIRED1,               // Was: peak value for totalSizeInMemory
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

  /**
   * Iterates over the final sorted results. Implemented differently
   * depending on whether the results are in-memory or spilled to
   * disk.
   */

  public interface SortResults {
    boolean next();
    void close();
    int getBatchCount();
    int getRecordCount();
  }

  public ExternalSortBatch(ExternalSort popConfig, FragmentContext context, RecordBatch incoming) {
    super(popConfig, context, true);
    this.incoming = incoming;
    allocator = oContext.getAllocator();
    opCodeGen = new OperatorCodeGenerator(context, popConfig);

    spillSet = new SpillSet(context, popConfig, "sort", "run");
    copierHolder = new CopierHolder(context, allocator, opCodeGen);
    configure(context.getConfig());
  }

  private void configure(DrillConfig config) {

    // The maximum memory this operator can use as set by the
    // operator definition (propagated to the allocator.)

    memoryLimit = allocator.getLimit();

    // Optional configured memory limit, typically used only for testing.

    long configLimit = config.getBytes(ExecConstants.EXTERNAL_SORT_MAX_MEMORY);
    if (configLimit > 0) {
      memoryLimit = Math.min(memoryLimit, configLimit);
    }

    // Optional limit on the number of spilled runs to merge in a single
    // pass. Limits the number of open file handles. Must allow at least
    // two batches to merge to make progress.

    mergeLimit = getConfigLimit(config, ExecConstants.EXTERNAL_SORT_MERGE_LIMIT, Integer.MAX_VALUE, 2);

    // Limits the size of first-generation spill files.

    spillFileSize = config.getBytes(ExecConstants.EXTERNAL_SORT_SPILL_FILE_SIZE);

    // Ensure the size is reasonable.

    spillFileSize = Math.max(spillFileSize, MIN_SPILL_FILE_SIZE);

    // The spill batch size. This is a critical setting for performance.
    // Set too large and the ratio between memory and input data sizes becomes
    // small. Set too small and disk seek times dominate performance.

    preferredSpillBatchSize = config.getBytes(ExecConstants.EXTERNAL_SORT_SPILL_BATCH_SIZE);

    // In low memory, use no more than 1/4 of memory for each spill batch. Ensures we
    // can merge.

    preferredSpillBatchSize = Math.min(preferredSpillBatchSize, memoryLimit / 4);

    // But, the spill batch should be above some minimum size to prevent complete
    // thrashing.

    preferredSpillBatchSize = Math.max(preferredSpillBatchSize, MIN_SPILL_BATCH_SIZE);

    // Set the target output batch size. Use the maximum size, but only if
    // this represents less than 10% of available memory. Otherwise, use 10%
    // of memory, but no smaller than the minimum size. In any event, an
    // output batch can contain no fewer than a single record.

    preferredMergeBatchSize = config.getBytes(ExecConstants.EXTERNAL_SORT_MERGE_BATCH_SIZE);
    long maxAllowance = (long) (memoryLimit - 2 * preferredSpillBatchSize);
    preferredMergeBatchSize = Math.min(maxAllowance, preferredMergeBatchSize);
    preferredMergeBatchSize = Math.max(preferredMergeBatchSize, MIN_MERGED_BATCH_SIZE);

    logger.debug("Config: memory limit = {}, " +
                 "spill file size = {}, spill batch size = {}, merge limit = {}, merge batch size = {}",
                  memoryLimit, spillFileSize, preferredSpillBatchSize, mergeLimit,
                  preferredMergeBatchSize);
  }

  private int getConfigLimit(DrillConfig config, String paramName, int valueIfZero, int minValue) {
    int limit = config.getInt(paramName);
    if (limit > 0) {
      limit = Math.max(limit, minValue);
    } else {
      limit = valueIfZero;
    }
    return limit;
  }

  @Override
  public int getRecordCount() {
    if (sv4 != null) {
      return sv4.getCount();
    }
    return container.getRecordCount();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    return sv4;
  }

  private void closeBatchGroups(Collection<? extends BatchGroup> groups) {
    for (BatchGroup group: groups) {
      try {
        group.close();
      } catch (Exception e) {
        // collect all failure and make sure to cleanup all remaining batches
        // Originally we would have thrown a RuntimeException that would propagate to FragmentExecutor.closeOutResources()
        // where it would have been passed to context.fail()
        // passing the exception directly to context.fail(e) will let the cleanup process continue instead of stopping
        // right away, this will also make sure we collect any additional exception we may get while cleaning up
        context.fail(e);
      }
    }
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
    case OK:
      setupSchema(upstream);

      // Add the batch to the in-memory generation, spilling if
      // needed.

      processBatch();
      break;
    case OUT_OF_MEMORY:

      // Note: it is highly doubtful that this code actually works. It
      // requires that the upstream batches got to a safe place to run
      // out of memory and that no work as in-flight and thus abandoned.
      // Consider removing this case once resource management is in place.

      logger.error("received OUT_OF_MEMORY, trying to spill");
      if (bufferedBatches.size() > 2) {
        spillFromMemory();
      } else {
        logger.error("not enough batches to spill, sending OUT_OF_MEMORY downstream");
        return IterOutcome.OUT_OF_MEMORY;
      }
      break;
    default:
      throw new IllegalStateException("Unexpected iter outcome: " + upstream);
    }
    return IterOutcome.OK;
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

    if (inputRecordCount == 0) {
      sortState = SortState.DONE;
      return IterOutcome.NONE;
    }
    logger.debug("Completed load phase: read {} batches, spilled {} times, total input bytes: {}",
                 inputBatchCount, spilledRuns.size(), totalInputBytes);

    // Do the merge of the loaded batches. The merge can be done entirely in memory if
    // the results fit; else we have to do a disk-based merge of
    // pre-sorted spilled batches.

    if (canUseMemoryMerge()) {
      return sortInMemory();
    } else {
      return mergeSpilledRuns();
    }
  }

  /**
   * All data has been read from the upstream batch. Determine if we
   * can use a fast in-memory sort, or must use a merge (which typically,
   * but not always, involves spilled batches.)
   *
   * @return whether sufficient resources exist to do an in-memory sort
   * if all batches are still in memory
   */

  private boolean canUseMemoryMerge() {
    if (spillSet.hasSpilled()) { return false; }

    // Do we have enough memory for MSorter (the in-memory sorter)?

    long allocMem = allocator.getAllocatedMemory();
    long availableMem = memoryLimit - allocMem;
    long neededForInMemorySort = MSortTemplate.memoryNeeded(inputRecordCount);
    if (availableMem < neededForInMemorySort) { return false; }

    // Make sure we don't exceed the maximum number of batches SV4 can address.

    if (bufferedBatches.size() > Character.MAX_VALUE) { return false; }

    // We can do an in-memory merge.

    return true;
  }

  /**
   * Handle a new schema from upstream. The ESB is quite limited in its ability
   * to handle schema changes.
   *
   * @param upstream the status code from upstream: either OK or OK_NEW_SCHEMA
   */

  private void setupSchema(IterOutcome upstream)  {

    // First batch: we won't have a schema.

    if (schema == null) {
      schema = incoming.getSchema();

    // Subsequent batches, nothing to do if same schema.

    } else if (upstream == IterOutcome.OK) {
      return;

    // Only change in the case that the schema truly changes. Artificial schema changes are ignored.

    } else if (incoming.getSchema().equals(schema)) {
      return;
    } else if (unionTypeEnabled) {
        schema = SchemaUtil.mergeSchemas(schema, incoming.getSchema());

        // New schema: must generate a new sorter and copier.

        opCodeGen.setSchema(schema);
    } else {
      throw UserException.unsupportedError()
            .message("Schema changes not supported in External Sort. Please enable Union type.")
            .build(logger);
    }

    // Coerce all existing batches to the new schema.

    for (BatchGroup b : bufferedBatches) {
      b.setSchema(schema);
    }
    for (BatchGroup b : spilledRuns) {
      b.setSchema(schema);
    }
  }

  /**
   * Convert an incoming batch into the agree-upon format. (Also seems to
   * make a persistent shallow copy of the batch saved until we are ready
   * to sort or spill.)
   *
   * @return the converted batch, or null if the incoming batch is empty
   */

  @SuppressWarnings("resource")
  private VectorContainer convertBatch() {

    // Must accept the batch even if no records. Then clear
    // the vectors to release memory since we won't do any
    // further processing with the empty batch.

    VectorContainer convertedBatch = SchemaUtil.coerceContainer(incoming, schema, oContext);
    if (incoming.getRecordCount() == 0) {
      for (VectorWrapper<?> w : convertedBatch) {
        w.clear();
      }
      SelectionVector2 sv2 = incoming.getSelectionVector2();
      if (sv2 != null) {
        sv2.clear();
      }
      return null;
    }
    return convertedBatch;
  }

  private SelectionVector2 makeSelectionVector() {
    if (incoming.getSchema().getSelectionVectorMode() == BatchSchema.SelectionVectorMode.TWO_BYTE) {
      return incoming.getSelectionVector2().clone();
    } else {
      return newSV2();
    }
  }

  /**
   * Process the converted incoming batch by adding it to the in-memory store
   * of data, or spilling data to disk when necessary.
   */

  @SuppressWarnings("resource")
  private void processBatch() {

    // Skip empty batches (such as the first one.)

    if (incoming.getRecordCount() == 0) {
      return;
    }

    // Determine actual sizes of the incoming batch before taking
    // ownership. Allows us to figure out if we need to spill first,
    // to avoid overflowing memory simply due to ownership transfer.

    RecordBatchSizer sizer = analyzeIncomingBatch();

    // The heart of the external sort operator: spill to disk when
    // the in-memory generation exceeds the allowed memory limit.
    // Preemptively spill BEFORE accepting the new batch into our memory
    // pool. The allocator will throw an OOM exception if we accept the
    // batch when we are near the limit - despite the fact that the batch
    // is already in memory and no new memory is allocated during the transfer.

    if ( isSpillNeeded(sizer.actualSize())) {
      spillFromMemory();
    }

    // Sanity check. We should now be below the buffer memory maximum.

    long startMem = allocator.getAllocatedMemory();
    if (startMem > bufferMemoryPool) {
      logger.error( "ERROR: Failed to spill above buffer limit. Buffer pool = {}, memory = {}",
          bufferMemoryPool, startMem);
    }

    // Convert the incoming batch to the agreed-upon schema.
    // No converted batch means we got an empty input batch.
    // Converting the batch transfers memory ownership to our
    // allocator. This gives a round-about way to learn the batch
    // size: check the before and after memory levels, then use
    // the difference as the batch size, in bytes.

    VectorContainer convertedBatch = convertBatch();
    if (convertedBatch == null) {
      return;
    }

    SelectionVector2 sv2;
    try {
      sv2 = makeSelectionVector();
    } catch (Exception e) {
      convertedBatch.clear();
      throw e;
    }

    // Compute batch size, including allocation of an sv2.

    long endMem = allocator.getAllocatedMemory();
    long batchSize = endMem - startMem;
    int count = sv2.getCount();
    inputRecordCount += count;
    inputBatchCount++;
    totalInputBytes += sizer.actualSize();

    // Update the minimum buffer space metric.

    if (minimumBufferSpace == 0) {
      minimumBufferSpace = endMem;
    } else {
      minimumBufferSpace = Math.min(minimumBufferSpace, endMem);
    }
    stats.setLongStat(Metric.MIN_BUFFER, minimumBufferSpace);

    // Update the size based on the actual record count, not
    // the effective count as given by the selection vector
    // (which may exclude some records due to filtering.)

    updateMemoryEstimates(batchSize, sizer);

    // Sort the incoming batch using either the original selection vector,
    // or a new one created here.

    SingleBatchSorter sorter;
    sorter = opCodeGen.getSorter(convertedBatch);
    try {
      sorter.setup(context, sv2, convertedBatch);
    } catch (SchemaChangeException e) {
      convertedBatch.clear();
      throw UserException.unsupportedError(e)
            .message("Unexpected schema change.")
            .build(logger);
    }
    try {
      sorter.sort(sv2);
    } catch (SchemaChangeException e) {
      convertedBatch.clear();
      throw UserException.unsupportedError(e)
                .message("Unexpected schema change.")
                .build(logger);
    }
    RecordBatchData rbd = new RecordBatchData(convertedBatch, allocator);
    try {
      rbd.setSv2(sv2);
      bufferedBatches.add(new BatchGroup.InputBatch(rbd.getContainer(), rbd.getSv2(), oContext, sizer.netSize()));
      if (peakNumBatches < bufferedBatches.size()) {
        peakNumBatches = bufferedBatches.size();
        stats.setLongStat(Metric.PEAK_BATCHES_IN_MEMORY, peakNumBatches);
      }

    } catch (Throwable t) {
      rbd.clear();
      throw t;
    }
  }

  /**
   * Scan the vectors in the incoming batch to determine batch size and if
   * any oversize columns exist. (Oversize columns cause memory fragmentation.)
   *
   * @return an analysis of the incoming batch
   */

  private RecordBatchSizer analyzeIncomingBatch() {
    RecordBatchSizer sizer = new RecordBatchSizer(incoming);
    sizer.applySv2();
    if (inputBatchCount == 0) {
      logger.debug("{}", sizer.toString());
    }
    return sizer;
  }

  /**
   * Update the data-driven memory use numbers including:
   * <ul>
   * <li>The average size of incoming records.</li>
   * <li>The estimated spill and output batch size.</li>
   * <li>The estimated number of average-size records per
   * spill and output batch.</li>
   * <li>The amount of memory set aside to hold the incoming
   * batches before spilling starts.</li>
   * </ul>
   *
   * @param actualBatchSize the overall size of the current batch received from
   * upstream
   * @param actualRecordCount the number of actual (not filtered) records in
   * that upstream batch
   */

  private void updateMemoryEstimates(long memoryDelta, RecordBatchSizer sizer) {
    long actualBatchSize = sizer.actualSize();
    int actualRecordCount = sizer.rowCount();

    if (actualBatchSize != memoryDelta) {
      logger.debug("Memory delta: {}, actual batch size: {}, Diff: {}",
                   memoryDelta, actualBatchSize, memoryDelta - actualBatchSize);
    }

    // The record count should never be zero, but better safe than sorry...

    if (actualRecordCount == 0) {
      return; }

    // If the vector is less than 75% full, just ignore it, except in the
    // unfortunate case where it is the first batch. Low-density batches generally
    // occur only at the end of a file or at the end of a DFS block. In such a
    // case, we will continue to rely on estimates created on previous, high-
    // density batches.
    // We actually track the max density seen, and compare to 75% of that since
    // Parquet produces very low density record batches.

    if (sizer.avgDensity() < maxDensity * 3 / 4 && sizer.avgDensity() != lastDensity) {
      logger.trace("Saw low density batch. Density: {}", sizer.avgDensity());
      lastDensity = sizer.avgDensity();
      return;
    }
    maxDensity = Math.max(maxDensity, sizer.avgDensity());

    // We know the batch size and number of records. Use that to estimate
    // the average record size. Since a typical batch has many records,
    // the average size is a fairly good estimator. Note that the batch
    // size includes not just the actual vector data, but any unused space
    // resulting from power-of-two allocation. This means that we don't
    // have to do size adjustments for input batches as we will do below
    // when estimating the size of other objects.

    int batchRowWidth = sizer.netRowWidth();

    // Record sizes may vary across batches. To be conservative, use
    // the largest size observed from incoming batches.

    int origRowEstimate = estimatedRowWidth;
    estimatedRowWidth = Math.max(estimatedRowWidth, batchRowWidth);

    // Maintain an estimate of the incoming batch size: the largest
    // batch yet seen. Used to reserve memory for the next incoming
    // batch. Because we are using the actual observed batch size,
    // the size already includes overhead due to power-of-two rounding.

    long origInputBatchSize = estimatedInputBatchSize;
    estimatedInputBatchSize = Math.max(estimatedInputBatchSize, actualBatchSize);

    // The row width may end up as zero if all fields are nulls or some
    // other unusual situation. In this case, assume a width of 10 just
    // to avoid lots of special case code.

    if (estimatedRowWidth == 0) {
      estimatedRowWidth = 10;
    }

    // Go no further if nothing changed.

    if (estimatedRowWidth == origRowEstimate && estimatedInputBatchSize == origInputBatchSize) {
      return; }

    // Estimate the total size of each incoming batch plus sv2. Note that, due
    // to power-of-two rounding, the allocated sv2 size might be twice the data size.

    long estimatedInputSize = estimatedInputBatchSize + 4 * actualRecordCount;

    // Determine the number of records to spill per spill batch. The goal is to
    // spill batches of either 64K records, or as many records as fit into the
    // amount of memory dedicated to each spill batch, whichever is less.

    spillBatchRowCount = (int) Math.max(1, preferredSpillBatchSize / estimatedRowWidth / 2);
    spillBatchRowCount = Math.min(spillBatchRowCount, Character.MAX_VALUE);

    // Compute the actual spill batch size which may be larger or smaller
    // than the preferred size depending on the row width. Double the estimated
    // memory needs to allow for power-of-two rounding.

    targetSpillBatchSize = spillBatchRowCount * estimatedRowWidth * 2;

    // Determine the number of records per batch per merge step. The goal is to
    // merge batches of either 64K records, or as many records as fit into the
    // amount of memory dedicated to each merge batch, whichever is less.

    mergeBatchRowCount = (int) Math.max(1, preferredMergeBatchSize / estimatedRowWidth / 2);
    mergeBatchRowCount = Math.min(mergeBatchRowCount, Character.MAX_VALUE);
    mergeBatchRowCount = Math.max(1,  mergeBatchRowCount);
    targetMergeBatchSize = mergeBatchRowCount * estimatedRowWidth * 2;

    // Determine the minimum memory needed for spilling. Spilling is done just
    // before accepting a batch, so we must spill if we don't have room for a
    // (worst case) input batch. To spill, we need room for the output batch created
    // by merging the batches already in memory. Double this to allow for power-of-two
    // memory allocations.

    long spillPoint = estimatedInputBatchSize + 2 * targetSpillBatchSize;

    // The merge memory pool assumes we can spill all input batches. To make
    // progress, we must have at least two merge batches (same size as an output
    // batch) and one output batch. Again, double to allow for power-of-two
    // allocation and add one for a margin of error.

    long minMergeMemory = 2 * targetSpillBatchSize + targetMergeBatchSize;

    // If we are in a low-memory condition, then we might not have room for the
    // default output batch size. In that case, pick a smaller size.

    if (minMergeMemory > memoryLimit) {

      // Figure out the minimum output batch size based on memory,
      // must hold at least one complete row.

      long mergeAllowance = memoryLimit - 2 * targetSpillBatchSize;
      targetMergeBatchSize = Math.max(estimatedRowWidth, mergeAllowance / 2);
      mergeBatchRowCount = (int) (targetMergeBatchSize / estimatedRowWidth / 2);
      minMergeMemory = 2 * targetSpillBatchSize + targetMergeBatchSize;
    }

    // Determine the minimum total memory we would need to receive two input
    // batches (the minimum needed to make progress) and the allowance for the
    // output batch.

    long minLoadMemory = spillPoint + estimatedInputSize;

    // Determine how much memory can be used to hold in-memory batches of spilled
    // runs when reading from disk.

    bufferMemoryPool = memoryLimit - spillPoint;
    mergeMemoryPool = Math.max(memoryLimit - minMergeMemory,
                               (long) ((memoryLimit - 3 * targetMergeBatchSize) * 0.95));

    // Sanity check: if we've been given too little memory to make progress,
    // issue a warning but proceed anyway. Should only occur if something is
    // configured terribly wrong.

    long minMemoryNeeds = Math.max(minLoadMemory, minMergeMemory);
    if (minMemoryNeeds > memoryLimit) {
      logger.warn("Potential memory overflow! " +
                   "Minumum needed = {} bytes, actual available = {} bytes",
                   minMemoryNeeds, memoryLimit);
    }

    // Log the calculated values. Turn this on if things seem amiss.
    // Message will appear only when the values change.

    logger.debug("Input Batch Estimates: record size = {} bytes; input batch = {} bytes, {} records",
                 estimatedRowWidth, estimatedInputBatchSize, actualRecordCount);
    logger.debug("Merge batch size = {} bytes, {} records; spill file size: {} bytes",
                 targetSpillBatchSize, spillBatchRowCount, spillFileSize);
    logger.debug("Output batch size = {} bytes, {} records",
                 targetMergeBatchSize, mergeBatchRowCount);
    logger.debug("Available memory: {}, buffer memory = {}, merge memory = {}",
                 memoryLimit, bufferMemoryPool, mergeMemoryPool);
  }

  /**
   * Determine if spill is needed before receiving the new record batch.
   * Spilling is driven purely by memory availability (and an optional
   * batch limit for testing.)
   *
   * @return true if spilling is needed, false otherwise
   */

  private boolean isSpillNeeded(int incomingSize) {

    // Can't spill if less than two batches else the merge
    // can't make progress.

    if (bufferedBatches.size() < 2) {
      return false; }

    // Must spill if we are below the spill point (the amount of memory
    // needed to do the minimal spill.)

    return allocator.getAllocatedMemory() + incomingSize >= bufferMemoryPool;
  }

  /**
   * Perform an in-memory sort of the buffered batches. Obviously can
   * be used only for the non-spilling case.
   *
   * @return DONE if no rows, OK_NEW_SCHEMA if at least one row
   */

  private IterOutcome sortInMemory() {
    logger.debug("Starting in-memory sort. Batches = {}, Records = {}, Memory = {}",
                 bufferedBatches.size(), inputRecordCount, allocator.getAllocatedMemory());

    // Note the difference between how we handle batches here and in the spill/merge
    // case. In the spill/merge case, this class decides on the batch size to send
    // downstream. However, in the in-memory case, we must pass along all batches
    // in a single SV4. Attempts to do paging will result in errors. In the memory
    // merge case, the downstream Selection Vector Remover will split the one
    // big SV4 into multiple smaller batches to send further downstream.

    // If the sort fails or is empty, clean up here. Otherwise, cleanup is done
    // by closing the resultsIterator after all results are returned downstream.

    MergeSort memoryMerge = new MergeSort(context, allocator, opCodeGen);
    try {
      sv4 = memoryMerge.merge(bufferedBatches, this, container);
      if (sv4 == null) {
        sortState = SortState.DONE;
        return IterOutcome.STOP;
      } else {
        logger.debug("Completed in-memory sort. Memory = {}",
                     allocator.getAllocatedMemory());
        resultsIterator = memoryMerge;
        memoryMerge = null;
        sortState = SortState.DELIVER;
        return IterOutcome.OK_NEW_SCHEMA;
      }
    } finally {
      if (memoryMerge != null) {
        memoryMerge.close();
      }
    }
  }

  /**
   * Perform merging of (typically spilled) batches. First consolidates batches
   * as needed, then performs a final merge that is read one batch at a time
   * to deliver batches to the downstream operator.
   *
   * @return always returns OK_NEW_SCHEMA
   */

  private IterOutcome mergeSpilledRuns() {
    logger.debug("Starting consolidate phase. Batches = {}, Records = {}, Memory = {}, In-memory batches {}, spilled runs {}",
                 inputBatchCount, inputRecordCount, allocator.getAllocatedMemory(),
                 bufferedBatches.size(), spilledRuns.size());

    // Consolidate batches to a number that can be merged in
    // a single last pass.

    int mergeCount = 0;
    while (consolidateBatches()) {
      mergeCount++;
    }
    stats.addLongStat(Metric.MERGE_COUNT, mergeCount);

    // Merge in-memory batches and spilled runs for the final merge.

    List<BatchGroup> allBatches = new LinkedList<>();
    allBatches.addAll(bufferedBatches);
    bufferedBatches.clear();
    allBatches.addAll(spilledRuns);
    spilledRuns.clear();

    logger.debug("Starting merge phase. Runs = {}, Alloc. memory = {}",
                 allBatches.size(), allocator.getAllocatedMemory());

    // Do the final merge as a results iterator.

    CopierHolder.BatchMerger merger = copierHolder.startFinalMerge(schema, allBatches, container, mergeBatchRowCount);
    merger.next();
    resultsIterator = merger;
    sortState = SortState.DELIVER;
    return IterOutcome.OK_NEW_SCHEMA;
  }

  private boolean consolidateBatches() {

    // Determine additional memory needed to hold one batch from each
    // spilled run.

    int inMemCount = bufferedBatches.size();
    int spilledRunsCount = spilledRuns.size();

    // Can't merge more than will fit into memory at one time.

    int maxMergeWidth = (int) (mergeMemoryPool / targetSpillBatchSize);
    maxMergeWidth = Math.min(mergeLimit, maxMergeWidth);

    // But, must merge at least two batches.

    maxMergeWidth = Math.max(maxMergeWidth, 2);

    // If we can't fit all batches in memory, must spill any in-memory
    // batches to make room for multiple spill-merge-spill cycles.

    if (inMemCount > 0) {
      if (spilledRunsCount > maxMergeWidth) {
        spillFromMemory();
        return true;
      }

      // If we just plain have too many batches to merge, spill some
      // in-memory batches to reduce the burden.

      if (inMemCount + spilledRunsCount > mergeLimit) {
        spillFromMemory();
        return true;
      }

      // If the on-disk batches and in-memory batches need more memory than
      // is available, spill some in-memory batches.

      long allocated = allocator.getAllocatedMemory();
      long totalNeeds = spilledRunsCount * targetSpillBatchSize + allocated;
      if (totalNeeds > mergeMemoryPool) {
        spillFromMemory();
        return true;
      }
    }

    // Merge on-disk batches if we have too many.

    int mergeCount = spilledRunsCount - maxMergeWidth;
    if (mergeCount <= 0) {
      return false;
    }

    // Must merge at least 2 batches to make progress.

    mergeCount = Math.max(2, mergeCount);

    // We will merge. This will create yet another spilled
    // run. Account for that.

    mergeCount += 1;

    mergeCount = Math.min(mergeCount, maxMergeWidth);

    // If we are going to merge, and we have batches in memory,
    // spill them and try again. We need to do this to ensure we
    // have adequate memory to hold the merge batches. We are into
    // a second-generation sort/merge so there is no point in holding
    // onto batches in memory.

    if (inMemCount > 0) {
      spillFromMemory();
      return true;
    }

    // Do the merge, then loop to try again in case not
    // all the target batches spilled in one go.

    logger.trace("Merging {} on-disk runs, Alloc. memory = {}",
        mergeCount, allocator.getAllocatedMemory());
    mergeRuns(mergeCount);
    return true;
  }

  /**
   * This operator has accumulated a set of sorted incoming record batches.
   * We wish to spill some of them to disk. To do this, a "copier"
   * merges the target batches to produce a stream of new (merged) batches
   * which are then written to disk.
   * <p>
   * This method spills only half the accumulated batches
   * minimizing unnecessary disk writes. The exact count must lie between
   * the minimum and maximum spill counts.
   */

  private void spillFromMemory() {

    // Determine the number of batches to spill to create a spill file
    // of the desired size. The actual file size might be a bit larger
    // or smaller than the target, which is expected.

    int spillCount = 0;
    long spillSize = 0;
    for (InputBatch batch : bufferedBatches) {
      long batchSize = batch.getDataSize();
      spillSize += batchSize;
      spillCount++;
      if (spillSize + batchSize / 2 > spillFileSize) {
        break; }
    }

    // Must always spill at least 2, even if this creates an over-size
    // spill file. But, if this is a final consolidation, we may have only
    // a single batch.

    spillCount = Math.max(spillCount, 2);
    spillCount = Math.min(spillCount, bufferedBatches.size());

    // Do the actual spill.

    mergeAndSpill(bufferedBatches, spillCount);
  }

  private void mergeRuns(int targetCount) {

    // Determine the number of runs to merge. The count should be the
    // target count. However, to prevent possible memory overrun, we
    // double-check with actual spill batch size and only spill as much
    // as fits in the merge memory pool.

    int mergeCount = 0;
    long mergeSize = 0;
    for (SpilledRun run : spilledRuns) {
      long batchSize = run.getBatchSize();
      if (mergeSize + batchSize > mergeMemoryPool) {
        break;
      }
      mergeSize += batchSize;
      mergeCount++;
      if (mergeCount == targetCount) {
        break;
      }
    }

    // Must always spill at least 2, even if this creates an over-size
    // spill file. But, if this is a final consolidation, we may have only
    // a single batch.

    mergeCount = Math.max(mergeCount, 2);
    mergeCount = Math.min(mergeCount, spilledRuns.size());

    // Do the actual spill.

    mergeAndSpill(spilledRuns, mergeCount);
  }

  private void mergeAndSpill(LinkedList<? extends BatchGroup> source, int count) {
    spilledRuns.add(doMergeAndSpill(source, count));
    logger.trace("Completed spill: memory = {}",
                 allocator.getAllocatedMemory());
  }

  private BatchGroup.SpilledRun doMergeAndSpill(LinkedList<? extends BatchGroup> batchGroups, int spillCount) {
    List<BatchGroup> batchesToSpill = Lists.newArrayList();
    spillCount = Math.min(batchGroups.size(), spillCount);
    assert spillCount > 0 : "Spill count to mergeAndSpill must not be zero";
    for (int i = 0; i < spillCount; i++) {
      batchesToSpill.add(batchGroups.pollFirst());
    }

    // Merge the selected set of matches and write them to the
    // spill file. After each write, we release the memory associated
    // with the just-written batch.

    String outputFile = spillSet.getNextSpillFile();
    stats.setLongStat(Metric.SPILL_COUNT, spillSet.getFileCount());
    BatchGroup.SpilledRun newGroup = null;
    try (AutoCloseable ignored = AutoCloseables.all(batchesToSpill);
         CopierHolder.BatchMerger merger = copierHolder.startMerge(schema, batchesToSpill, spillBatchRowCount)) {
      logger.trace("Spilling {} of {} batches, spill batch size = {} rows, memory = {}, write to {}",
                   batchesToSpill.size(), bufferedBatches.size() + batchesToSpill.size(),
                   spillBatchRowCount,
                   allocator.getAllocatedMemory(), outputFile);
      newGroup = new BatchGroup.SpilledRun(spillSet, outputFile, oContext);

      // The copier will merge records from the buffered batches into
      // the outputContainer up to targetRecordCount number of rows.
      // The actual count may be less if fewer records are available.

      while (merger.next()) {

        // Add a new batch of records (given by merger.getOutput()) to the spill
        // file.
        //
        // note that addBatch also clears the merger's output container

        newGroup.addBatch(merger.getOutput());
      }
      injector.injectChecked(context.getExecutionControls(), INTERRUPTION_WHILE_SPILLING, IOException.class);
      newGroup.closeOutputStream();
      logger.trace("Spilled {} batches, {} records; memory = {} to {}",
                   merger.getBatchCount(), merger.getRecordCount(),
                   allocator.getAllocatedMemory(), outputFile);
      newGroup.setBatchSize(merger.getEstBatchSize());
      return newGroup;
    } catch (Throwable e) {
      // we only need to clean up newGroup if spill failed
      try {
        if (newGroup != null) {
          AutoCloseables.close(e, newGroup);
        }
      } catch (Throwable t) { /* close() may hit the same IO issue; just ignore */ }

      // Here the merger is holding onto a partially-completed batch.
      // It will release the memory in the close() call.

      try {
        // Rethrow so we can decide how to handle the error.

        throw e;
      }

      // If error is a User Exception, just use as is.

      catch (UserException ue) { throw ue; }
      catch (Throwable ex) {
        throw UserException.resourceError(ex)
              .message("External Sort encountered an error while spilling to disk")
              .build(logger);
      }
    }
  }

  /**
   * Allocate and initialize the selection vector used as the sort index.
   * Assumes that memory is available for the vector since memory management
   * ensured space is available.
   *
   * @return a new, populated selection vector 2
   */

  private SelectionVector2 newSV2() {
    SelectionVector2 sv2 = new SelectionVector2(allocator);
    if (!sv2.allocateNewSafe(incoming.getRecordCount())) {
      throw UserException.resourceError(new OutOfMemoryException("Unable to allocate sv2 buffer"))
            .build(logger);
    }
    for (int i = 0; i < incoming.getRecordCount(); i++) {
      sv2.setIndex(i, (char) i);
    }
    sv2.setRecordCount(incoming.getRecordCount());
    return sv2;
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
    if (spillSet.getWriteBytes() > 0) {
      logger.debug("End of sort. Total write bytes: {}, Total read bytes: {}",
                   spillSet.getWriteBytes(), spillSet.getWriteBytes());
    }
    stats.setLongStat(Metric.SPILL_MB,
        (int) Math.round( spillSet.getWriteBytes() / 1024.0D / 1024.0 ) );
    RuntimeException ex = null;
    try {
      if (bufferedBatches != null) {
        closeBatchGroups(bufferedBatches);
        bufferedBatches = null;
      }
    } catch (RuntimeException e) {
      ex = e;
    }
    try {
      if (spilledRuns != null) {
        closeBatchGroups(spilledRuns);
        spilledRuns = null;
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      if (sv4 != null) {
        sv4.clear();
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      if (resultsIterator != null) {
        resultsIterator.close();
        resultsIterator = null;
      }
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      copierHolder.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      spillSet.close();
    } catch (RuntimeException e) {
      ex = (ex == null) ? e : ex;
    }
    try {
      opCodeGen.close();
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
//    try {
//      allocator.close();
//    } catch (RuntimeException e) {
//      ex = (ex == null) ? e : ex;
//    }
    if (ex != null) {
      throw ex;
    }
  }
}
