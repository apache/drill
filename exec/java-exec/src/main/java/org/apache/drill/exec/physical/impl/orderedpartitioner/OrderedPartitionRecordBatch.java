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
package org.apache.drill.exec.physical.impl.orderedpartitioner;

import java.io.IOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.FieldReference;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.cache.CachedVectorContainer;
import org.apache.drill.exec.cache.Counter;
import org.apache.drill.exec.cache.DistributedCache;
import org.apache.drill.exec.cache.DistributedCache.CacheConfig;
import org.apache.drill.exec.cache.DistributedCache.SerializationMode;
import org.apache.drill.exec.cache.DistributedMap;
import org.apache.drill.exec.cache.DistributedMultiMap;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.ValueVectorWriteExpression;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.OrderedPartitionSender;
import org.apache.drill.exec.physical.impl.sort.SortBatch;
import org.apache.drill.exec.physical.impl.sort.SortRecordBatchBuilder;
import org.apache.drill.exec.physical.impl.sort.Sorter;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.TransferPair;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.calcite.rel.RelFieldCollation.Direction;

import org.apache.drill.shaded.guava.com.google.common.base.Preconditions;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;

/**
 * The purpose of this operator is to generate an ordered partition, rather than a random hash partition. This could be
 * used to do a total order sort, for example. This operator reads in a few incoming record batches, samples these
 * batches, and stores them in the distributed cache. The samples from all the parallel-running fragments are merged,
 * and a partition-table is built and stored in the distributed cache for use by all fragments. A new column is added to
 * the outgoing batch, whose value is determined by where each record falls in the partition table. This column is used
 * by PartitionSenderRootExec to determine which bucket to assign each record to.
 */
public class OrderedPartitionRecordBatch extends AbstractRecordBatch<OrderedPartitionSender> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OrderedPartitionRecordBatch.class);

//  private static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
//  private static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;

  public static final CacheConfig<String, CachedVectorContainer> SINGLE_CACHE_CONFIG = CacheConfig //
      .newBuilder(CachedVectorContainer.class) //
      .name("SINGLE-" + CachedVectorContainer.class.getSimpleName()) //
      .mode(SerializationMode.DRILL_SERIALIZIABLE) //
      .build();
  public static final CacheConfig<String, CachedVectorContainer> MULTI_CACHE_CONFIG = CacheConfig //
      .newBuilder(CachedVectorContainer.class) //
      .name("MULTI-" + CachedVectorContainer.class.getSimpleName()) //
      .mode(SerializationMode.DRILL_SERIALIZIABLE) //
      .build();

  private final MappingSet mainMapping = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_CONSTANT_MAP,
      ClassGenerator.DEFAULT_SCALAR_MAP);
  private final MappingSet incomingMapping = new MappingSet("inIndex", null, "incoming", null,
      ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  private final MappingSet partitionMapping = new MappingSet("partitionIndex", null, "partitionVectors", null,
      ClassGenerator.DEFAULT_CONSTANT_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);

  private final int recordsToSample; // How many records must be received before analyzing
  private final int samplingFactor; // Will collect samplingFactor * number of partitions to send to distributed cache
  private final float completionFactor; // What fraction of fragments must be completed before attempting to build
                                        // partition table
  protected final RecordBatch incoming;
  private boolean first = true;
  private OrderedPartitionProjector projector;
  private VectorContainer partitionVectors = new VectorContainer();
  private int partitions;
  private Queue<VectorContainer> batchQueue;
  private int recordsSampled;
  private int sendingMajorFragmentWidth;
  private boolean startedUnsampledBatches = false;
  private boolean upstreamNone = false;
  private int recordCount;

  private final IntVector partitionKeyVector;
  private final DistributedMap<String, CachedVectorContainer> tableMap;
  private final Counter minorFragmentSampleCount;
  private final DistributedMultiMap<String, CachedVectorContainer> mmap;
  private final String mapKey;
  private List<VectorContainer> sampledIncomingBatches;

  public OrderedPartitionRecordBatch(OrderedPartitionSender pop, RecordBatch incoming, FragmentContext context) throws OutOfMemoryException {
    super(pop, context);
    this.incoming = incoming;
    this.partitions = pop.getDestinations().size();
    this.sendingMajorFragmentWidth = pop.getSendingWidth();
    this.recordsToSample = pop.getRecordsToSample();
    this.samplingFactor = pop.getSamplingFactor();
    this.completionFactor = pop.getCompletionFactor();

    DistributedCache cache = null;
    // Clearly, this code is not used!
    this.mmap = cache.getMultiMap(MULTI_CACHE_CONFIG);
    this.tableMap = cache.getMap(SINGLE_CACHE_CONFIG);
    Preconditions.checkNotNull(tableMap);

    this.mapKey = String.format("%s_%d", context.getHandle().getQueryId(), context.getHandle().getMajorFragmentId());
    this.minorFragmentSampleCount = cache.getCounter(mapKey);

    SchemaPath outputPath = popConfig.getRef();
    MaterializedField outputField = MaterializedField.create(outputPath.getAsNamePart().getName(), Types.required(TypeProtos.MinorType.INT));
    this.partitionKeyVector = (IntVector) TypeHelper.getNewVector(outputField, oContext.getAllocator());
  }

  @Override
  public void close() {
    super.close();
    partitionVectors.clear();
    partitionKeyVector.clear();
  }


  @SuppressWarnings("resource")
  private boolean saveSamples() throws SchemaChangeException, ClassTransformationException, IOException {
    recordsSampled = 0;
    IterOutcome upstream;

    // Start collecting batches until recordsToSample records have been collected

    SortRecordBatchBuilder builder = new SortRecordBatchBuilder(oContext.getAllocator());
    WritableBatch batch = null;
    CachedVectorContainer sampleToSave = null;
    VectorContainer containerToCache = new VectorContainer();
    try {
      builder.add(incoming);

      recordsSampled += incoming.getRecordCount();

      outer: while (recordsSampled < recordsToSample) {
        upstream = next(incoming);
        switch (upstream) {
        case NONE:
        case NOT_YET:
        case STOP:
          upstreamNone = true;
          break outer;
        default:
          // fall through
        }
        builder.add(incoming);
        recordsSampled += incoming.getRecordCount();
        if (upstream == IterOutcome.NONE) {
          break;
        }
      }
      VectorContainer sortedSamples = new VectorContainer();
      builder.build(sortedSamples);

      // Sort the records according the orderings given in the configuration

      Sorter sorter = SortBatch.createNewSorter(context, popConfig.getOrderings(), sortedSamples);
      SelectionVector4 sv4 = builder.getSv4();
      sorter.setup(context, sv4, sortedSamples);
      sorter.sort(sv4, sortedSamples);

      // Project every Nth record to a new vector container, where N = recordsSampled/(samplingFactor * partitions).
      // Uses the
      // the expressions from the Orderings to populate each column. There is one column for each Ordering in
      // popConfig.orderings.

      List<ValueVector> localAllocationVectors = Lists.newArrayList();
      SampleCopier copier = getCopier(sv4, sortedSamples, containerToCache, popConfig.getOrderings(), localAllocationVectors);
      int allocationSize = 50;
      while (true) {
        for (ValueVector vv : localAllocationVectors) {
          AllocationHelper.allocate(vv, samplingFactor * partitions, allocationSize);
        }
        if (copier.copyRecords(recordsSampled / (samplingFactor * partitions), 0, samplingFactor * partitions)) {
          break;
        } else {
          containerToCache.zeroVectors();
          allocationSize *= 2;
        }
      }
      for (VectorWrapper<?> vw : containerToCache) {
        vw.getValueVector().getMutator().setValueCount(copier.getOutputRecords());
      }
      containerToCache.setRecordCount(copier.getOutputRecords());

      // Get a distributed multimap handle from the distributed cache, and put the vectors from the new vector container
      // into a serializable wrapper object, and then add to distributed map

      batch = WritableBatch.getBatchNoHVWrap(containerToCache.getRecordCount(), containerToCache, false);
      sampleToSave = new CachedVectorContainer(batch, context.getAllocator());

      mmap.put(mapKey, sampleToSave);
      this.sampledIncomingBatches = builder.getHeldRecordBatches();
    } finally {
      builder.clear();
      builder.close();
      if (batch != null) {
        batch.clear();
      }
      containerToCache.clear();
      if (sampleToSave != null) {
        sampleToSave.clear();
      }
    }
    return true;
  }

  /**
   * Wait until the at least the given timeout is expired or interrupted and the fragment status is not runnable.
   * @param timeout Timeout in milliseconds.
   * @return True if the given timeout is expired. False when interrupted and the fragment status is not runnable.
   */
  private boolean waitUntilTimeOut(final long timeout) {
    while(true) {
      try {
        Thread.sleep(timeout);
        return true;
      } catch (final InterruptedException e) {
        if (!context.getExecutorState().shouldContinue()) {
          return false;
        }
      }
    }
  }

  /**
   * This method is called when the first batch comes in. Incoming batches are collected until a threshold is met. At
   * that point, the records in the batches are sorted and sampled, and the sampled records are stored in the
   * distributed cache. Once a sufficient fraction of the fragments have shared their samples, each fragment grabs all
   * the samples, sorts all the records, builds a partition table, and attempts to push the partition table to the
   * distributed cache. Whichever table gets pushed first becomes the table used by all fragments for partitioning.
   *
   * @return True is successful. False if failed.
   */
  private boolean getPartitionVectors() {
    try {
      if (!saveSamples()) {
        return false;
      }

      CachedVectorContainer finalTable = null;

      long val = minorFragmentSampleCount.incrementAndGet();
      logger.debug("Incremented mfsc, got {}", val);

      final long fragmentsBeforeProceed = (long) Math.ceil(sendingMajorFragmentWidth * completionFactor);
      final String finalTableKey = mapKey + "final";

      if (val == fragmentsBeforeProceed) { // we crossed the barrier, build table and get data.
        buildTable();
        finalTable = tableMap.get(finalTableKey);
      } else {
        // Wait until sufficient number of fragments have submitted samples, or proceed after xx ms passed
        // TODO: this should be polling.

        if (val < fragmentsBeforeProceed) {
          if (!waitUntilTimeOut(10)) {
            return false;
          }
        }
        for (int i = 0; i < 100 && finalTable == null; i++) {
          finalTable = tableMap.get(finalTableKey);
          if (finalTable != null) {
            break;
          }
          if (!waitUntilTimeOut(10)) {
            return false;
          }
        }
        if (finalTable == null) {
          buildTable();
        }
        finalTable = tableMap.get(finalTableKey);
      }

      Preconditions.checkState(finalTable != null);

      // Extract vectors from the wrapper, and add to partition vectors. These vectors will be used for partitioning in
      // the rest of this operator
      for (VectorWrapper<?> w : finalTable.get()) {
        partitionVectors.add(w.getValueVector());
      }

    } catch (final ClassTransformationException | IOException | SchemaChangeException ex) {
      kill(false);
      context.getExecutorState().fail(ex);
      return false;
      // TODO InterruptedException
    }
    return true;
  }

  private void buildTable() throws SchemaChangeException, ClassTransformationException, IOException {

    // Get all samples from distributed map

    @SuppressWarnings("resource")
    SortRecordBatchBuilder containerBuilder = new SortRecordBatchBuilder(context.getAllocator());
    final VectorContainer allSamplesContainer = new VectorContainer();
    final VectorContainer candidatePartitionTable = new VectorContainer();
    CachedVectorContainer wrap = null;
    try {
      for (CachedVectorContainer w : mmap.get(mapKey)) {
        containerBuilder.add(w.get());
      }
      containerBuilder.build(allSamplesContainer);

      List<Ordering> orderDefs = Lists.newArrayList();
      int i = 0;
      for (Ordering od : popConfig.getOrderings()) {
        SchemaPath sp = SchemaPath.getSimplePath("f" + i++);
        orderDefs.add(new Ordering(od.getDirection(), new FieldReference(sp)));
      }

      // sort the data incoming samples.
      @SuppressWarnings("resource")
      SelectionVector4 newSv4 = containerBuilder.getSv4();
      Sorter sorter = SortBatch.createNewSorter(context, orderDefs, allSamplesContainer);
      sorter.setup(context, newSv4, allSamplesContainer);
      sorter.sort(newSv4, allSamplesContainer);

      // Copy every Nth record from the samples into a candidate partition table, where N = totalSampledRecords/partitions
      // Attempt to push this to the distributed map. Only the first candidate to get pushed will be used.
      SampleCopier copier = null;
      List<ValueVector> localAllocationVectors = Lists.newArrayList();
      copier = getCopier(newSv4, allSamplesContainer, candidatePartitionTable, orderDefs, localAllocationVectors);
      int allocationSize = 50;
      while (true) {
        for (ValueVector vv : localAllocationVectors) {
          AllocationHelper.allocate(vv, samplingFactor * partitions, allocationSize);
        }
        int skipRecords = containerBuilder.getSv4().getTotalCount() / partitions;
        if (copier.copyRecords(skipRecords, skipRecords, partitions - 1)) {
          assert copier.getOutputRecords() == partitions - 1 : String.format("output records: %d partitions: %d", copier.getOutputRecords(), partitions);
          for (VectorWrapper<?> vw : candidatePartitionTable) {
            vw.getValueVector().getMutator().setValueCount(copier.getOutputRecords());
          }
          break;
        } else {
          candidatePartitionTable.zeroVectors();
          allocationSize *= 2;
        }
      }
      candidatePartitionTable.setRecordCount(copier.getOutputRecords());
      @SuppressWarnings("resource")
      WritableBatch batch = WritableBatch.getBatchNoHVWrap(candidatePartitionTable.getRecordCount(), candidatePartitionTable, false);
      wrap = new CachedVectorContainer(batch, context.getAllocator());
      tableMap.putIfAbsent(mapKey + "final", wrap, 1, TimeUnit.MINUTES);
    } finally {
      candidatePartitionTable.clear();
      allSamplesContainer.clear();
      containerBuilder.clear();
      containerBuilder.close();
      if (wrap != null) {
        wrap.clear();
      }
    }

  }

  /**
   * Creates a copier that does a project for every Nth record from a VectorContainer incoming into VectorContainer
   * outgoing. Each Ordering in orderings generates a column, and evaluation of the expression associated with each
   * Ordering determines the value of each column. These records will later be sorted based on the values in each
   * column, in the same order as the orderings.
   *
   * @param sv4
   * @param incoming
   * @param outgoing
   * @param orderings
   * @return
   * @throws SchemaChangeException
   */
  private SampleCopier getCopier(SelectionVector4 sv4, VectorContainer incoming, VectorContainer outgoing,
      List<Ordering> orderings, List<ValueVector> localAllocationVectors) throws SchemaChangeException {
    final ErrorCollector collector = new ErrorCollectorImpl();
    final ClassGenerator<SampleCopier> cg = CodeGenerator.getRoot(SampleCopier.TEMPLATE_DEFINITION, context.getOptions());
    // Note: disabled for now. This may require some debugging:
    // no tests are available for this operator.
    // cg.getCodeGenerator().plainOldJavaCapable(true);
    // Uncomment out this line to debug the generated code.
    // cg.getCodeGenerator().saveCodeForDebugging(true);

    int i = 0;
    for (Ordering od : orderings) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), incoming, collector, context.getFunctionRegistry());
      TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.newBuilder().mergeFrom(expr.getMajorType())
          .clearMode().setMode(TypeProtos.DataMode.REQUIRED);
      TypeProtos.MajorType newType = builder.build();
      MaterializedField outputField = MaterializedField.create("f" + i++, newType);
      if (collector.hasErrors()) {
        throw new SchemaChangeException(String.format(
            "Failure while trying to materialize incoming schema.  Errors:\n %s.", collector.toErrorString()));
      }

      @SuppressWarnings("resource")
      ValueVector vector = TypeHelper.getNewVector(outputField, oContext.getAllocator());
      localAllocationVectors.add(vector);
      TypedFieldId fid = outgoing.add(vector);
      ValueVectorWriteExpression write = new ValueVectorWriteExpression(fid, expr, true);
      HoldingContainer hc = cg.addExpr(write);
      cg.getEvalBlock()._if(hc.getValue().eq(JExpr.lit(0)))._then()._return(JExpr.FALSE);
    }
    cg.rotateBlock();
    cg.getEvalBlock()._return(JExpr.TRUE);
    outgoing.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    try {
      SampleCopier sampleCopier = context.getImplementationClass(cg);
      sampleCopier.setupCopier(context, sv4, incoming, outgoing);
      return sampleCopier;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
    incoming.kill(sendUpstream);
  }

  @Override
  public IterOutcome innerNext() {
    recordCount = 0;
    container.zeroVectors();

    // if we got IterOutcome.NONE while getting partition vectors, and there are no batches on the queue, then we are
    // done
    if (upstreamNone && (batchQueue == null || batchQueue.size() == 0)) {
      return IterOutcome.NONE;
    }

    // if there are batches on the queue, process them first, rather than calling incoming.next()
    if (batchQueue != null && batchQueue.size() > 0) {
      VectorContainer vc = batchQueue.poll();
      recordCount = vc.getRecordCount();
      try {

        // Must set up a new schema each time, because ValueVectors are not reused between containers in queue
        setupNewSchema(vc);
      } catch (SchemaChangeException ex) {
        kill(false);
        logger.error("Failure during query", ex);
        context.getExecutorState().fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      vc.zeroVectors();
      return IterOutcome.OK_NEW_SCHEMA;
    }

    // Reaching this point, either this is the first iteration, or there are no batches left on the queue and there are
    // more incoming
    IterOutcome upstream = next(incoming);

    if (this.first && upstream == IterOutcome.OK) {
      throw new RuntimeException("Invalid state: First batch should have OK_NEW_SCHEMA");
    }

    // If this is the first iteration, we need to generate the partition vectors before we can proceed
    if (this.first && upstream == IterOutcome.OK_NEW_SCHEMA) {
      if (!getPartitionVectors()) {
        close();
        return IterOutcome.STOP;
      }

      batchQueue = new LinkedBlockingQueue<>(this.sampledIncomingBatches);
      first = false;

      // Now that we have the partition vectors, we immediately process the first batch on the queue
      VectorContainer vc = batchQueue.poll();
      try {
        setupNewSchema(vc);
      } catch (SchemaChangeException ex) {
        kill(false);
        logger.error("Failure during query", ex);
        context.getExecutorState().fail(ex);
        return IterOutcome.STOP;
      }
      doWork(vc);
      vc.zeroVectors();
      recordCount = vc.getRecordCount();
      return IterOutcome.OK_NEW_SCHEMA;
    }

    // if this now that all the batches on the queue are processed, we begin processing the incoming batches. For the
    // first one
    // we need to generate a new schema, even if the outcome is IterOutcome.OK After that we can reuse the schema.
    if (!this.startedUnsampledBatches) {
      this.startedUnsampledBatches = true;
      if (upstream == IterOutcome.OK) {
        upstream = IterOutcome.OK_NEW_SCHEMA;
      }
    }
    switch (upstream) {
    case NONE:
    case NOT_YET:
    case STOP:
      close();
      recordCount = 0;
      return upstream;
    case OK_NEW_SCHEMA:
      try {
        setupNewSchema(incoming);
      } catch (SchemaChangeException ex) {
        kill(false);
        logger.error("Failure during query", ex);
        context.getExecutorState().fail(ex);
        return IterOutcome.STOP;
      }
      // fall through.
    case OK:
      doWork(incoming);
      recordCount = incoming.getRecordCount();
      return upstream; // change if upstream changed, otherwise normal.
    default:
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public int getRecordCount() {
    return recordCount;
  }

  protected void doWork(VectorAccessible batch) {
    int recordCount = batch.getRecordCount();
    AllocationHelper.allocate(partitionKeyVector, recordCount, 50);
    projector.projectRecords(recordCount, 0);
    for (VectorWrapper<?> v : container) {
      ValueVector.Mutator m = v.getValueVector().getMutator();
      m.setValueCount(recordCount);
    }
  }

  /**
   * Sets up projection that will transfer all of the columns in batch, and also populate the partition column based on
   * which partition a record falls into in the partition table
   *
   * @param batch
   * @throws SchemaChangeException
   */
  protected void setupNewSchema(VectorAccessible batch) throws SchemaChangeException {
    container.clear();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final List<TransferPair> transfers = Lists.newArrayList();

    final ClassGenerator<OrderedPartitionProjector> cg = CodeGenerator.getRoot(
        OrderedPartitionProjector.TEMPLATE_DEFINITION, context.getOptions());
    // Note: disabled for now. This may require some debugging:
    // no tests are available for this operator.
//    cg.getCodeGenerator().plainOldJavaCapable(true);
    // Uncomment out this line to debug the generated code.
//    cg.getCodeGenerator().saveCodeForDebugging(true);

    for (VectorWrapper<?> vw : batch) {
      TransferPair tp = vw.getValueVector().getTransferPair(oContext.getAllocator());
      transfers.add(tp);
      container.add(tp.getTo());
    }

    cg.setMappingSet(mainMapping);

    int count = 0;
    for (Ordering od : popConfig.getOrderings()) {
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector, context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      cg.setMappingSet(incomingMapping);
      ClassGenerator.HoldingContainer left = cg.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      cg.setMappingSet(partitionMapping);
      ClassGenerator.HoldingContainer right = cg.addExpr(
          new ValueVectorReadExpression(new TypedFieldId(expr.getMajorType(), count++)), ClassGenerator.BlkCreateMode.FALSE);
      cg.setMappingSet(mainMapping);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      ClassGenerator.HoldingContainer out = cg.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      JConditional jc = cg.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
    }

    cg.getEvalBlock()._return(JExpr.lit(0));

    container.add(this.partitionKeyVector);
    container.buildSchema(batch.getSchema().getSelectionVectorMode());

    try {
      this.projector = context.getImplementationClass(cg);
      projector.setup(context, batch, this, transfers, partitionVectors, partitions, popConfig.getRef());
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
  }

  @Override
  public void dump() {
    logger.error("OrderedPartitionRecordBatch[container={}, popConfig={}, partitionVectors={}, partitions={}, " +
            "recordsSampled={}, recordCount={}]",
        container, popConfig, partitionVectors, partitions, recordsSampled, recordCount);
  }
}
