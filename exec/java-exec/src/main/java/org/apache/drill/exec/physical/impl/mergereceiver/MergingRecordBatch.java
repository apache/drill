/**
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
package org.apache.drill.exec.physical.impl.mergereceiver;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.physical.MinorFragmentEndpoint;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.proto.BitControl.FinishedReceiver;
import org.apache.drill.exec.proto.ExecProtos.FragmentHandle;
import org.apache.drill.exec.proto.GeneralRPCProtos.Ack;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.proto.UserBitShared.SerializedField;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ExpandableHyperContainer;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorAccessible;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.rpc.RpcOutcomeListener;
import org.apache.drill.exec.testing.ControlsInjector;
import org.apache.drill.exec.testing.ControlsInjectorFactory;
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;


import io.netty.buffer.ByteBuf;

/**
 * The MergingRecordBatch merges pre-sorted record batches from remote senders.
 */
public class MergingRecordBatch extends AbstractRecordBatch<MergingReceiverPOP> implements RecordBatch {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);
  private static final ControlsInjector injector = ControlsInjectorFactory.getInjector(MergingRecordBatch.class);

  private static final int OUTGOING_BATCH_SIZE = 32 * 1024;

  private RecordBatchLoader[] batchLoaders;
  private final RawFragmentBatchProvider[] fragProviders;
  private final FragmentContext context;
  private VectorContainer outgoingContainer;
  private MergingReceiverGeneratorBase merger;
  private final MergingReceiverPOP config;
  private boolean hasRun = false;
  private boolean prevBatchWasFull = false;
  private boolean hasMoreIncoming = true;

  private int outgoingPosition = 0;
  private int senderCount = 0;
  private RawFragmentBatch[] incomingBatches;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;
  private RawFragmentBatch emptyBatch = null;
  private RawFragmentBatch[] tempBatchHolder;
  private long[] inputCounts;
  private long[] outputCounts;

  public static enum Metric implements MetricDef{
    BYTES_RECEIVED,
    NUM_SENDERS,
    NEXT_WAIT_NANOS;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public MergingRecordBatch(final FragmentContext context,
                            final MergingReceiverPOP config,
                            final RawFragmentBatchProvider[] fragProviders) throws OutOfMemoryException {
    super(config, context, true, context.newOperatorContext(config));
    this.fragProviders = fragProviders;
    this.context = context;
    this.outgoingContainer = new VectorContainer(oContext);
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
    this.config = config;
    this.inputCounts = new long[config.getNumSenders()];
    this.outputCounts = new long[config.getNumSenders()];
  }

  private RawFragmentBatch getNext(final int providerIndex) throws IOException {
    stats.startWait();
    final RawFragmentBatchProvider provider = fragProviders[providerIndex];
    try {
      injector.injectInterruptiblePause(context.getExecutionControls(), "waiting-for-data", logger);
      final RawFragmentBatch b = provider.getNext();
      if (b != null) {
        stats.addLongStat(Metric.BYTES_RECEIVED, b.getByteCount());
        stats.batchReceived(0, b.getHeader().getDef().getRecordCount(), false);
        inputCounts[providerIndex] += b.getHeader().getDef().getRecordCount();
      }
      return b;
    } catch(final InterruptedException e) {
      // Preserve evidence that the interruption occurred so that code higher up on the call stack can learn of the
      // interruption and respond to it if it wants to.
      Thread.currentThread().interrupt();

      return null;
    } finally {
      stats.stopWait();
    }
  }

  private void clearBatches(List<RawFragmentBatch> batches) {
    for (RawFragmentBatch batch : batches) {
      if (batch != null) {
        batch.release();
      }
    }
  }

  @Override
  public IterOutcome innerNext() {
    if (fragProviders.length == 0) {
      return IterOutcome.NONE;
    }
    boolean schemaChanged = false;

    if (prevBatchWasFull) {
      logger.debug("Outgoing vectors were full on last iteration");
      allocateOutgoing();
      outgoingPosition = 0;
      prevBatchWasFull = false;
    }

    if (!hasMoreIncoming) {
      logger.debug("next() was called after all values have been processed");
      outgoingPosition = 0;
      return IterOutcome.NONE;
    }

    // lazy initialization
    if (!hasRun) {
      schemaChanged = true; // first iteration is always a schema change

      // set up each (non-empty) incoming record batch
      final List<RawFragmentBatch> rawBatches = Lists.newArrayList();
      int p = 0;
      for (final RawFragmentBatchProvider provider : fragProviders) {
        RawFragmentBatch rawBatch;
        // check if there is a batch in temp holder before calling getNext(), as it may have been used when building schema
        if (tempBatchHolder[p] != null) {
          rawBatch = tempBatchHolder[p];
          tempBatchHolder[p] = null;
        } else {
          try {
            rawBatch = getNext(p);
          } catch (final IOException e) {
            context.fail(e);
            return IterOutcome.STOP;
          }
        }
        if (rawBatch == null && !context.shouldContinue()) {
          clearBatches(rawBatches);
          return IterOutcome.STOP;
        }

        assert rawBatch != null : "rawBatch is null although context.shouldContinue() == true";
        if (rawBatch.getHeader().getDef().getRecordCount() != 0) {
          rawBatches.add(rawBatch);
        } else {
          // save an empty batch to use for schema purposes. ignore batch if it contains no fields, and thus no schema
          if (emptyBatch == null && rawBatch.getHeader().getDef().getFieldCount() != 0) {
            emptyBatch = rawBatch;
          }
          try {
            while ((rawBatch = getNext(p)) != null && rawBatch.getHeader().getDef().getRecordCount() == 0) {
              // Do nothing
            }
            if (rawBatch == null && !context.shouldContinue()) {
              clearBatches(rawBatches);
              return IterOutcome.STOP;
            }
          } catch (final IOException e) {
            context.fail(e);
            clearBatches(rawBatches);
            return IterOutcome.STOP;
          }
          if (rawBatch != null) {
            rawBatches.add(rawBatch);
          } else {
            rawBatches.add(emptyBatch);
          }
        }
        p++;
      }

      // allocate the incoming record batch loaders
      senderCount = rawBatches.size();
      incomingBatches = new RawFragmentBatch[senderCount];
      batchOffsets = new int[senderCount];
      batchLoaders = new RecordBatchLoader[senderCount];
      for (int i = 0; i < senderCount; ++i) {
        incomingBatches[i] = rawBatches.get(i);
        batchLoaders[i] = new RecordBatchLoader(oContext.getAllocator());
      }

      // after this point all batches have moved to incomingBatches
      rawBatches.clear();

      int i = 0;
      for (final RawFragmentBatch batch : incomingBatches) {
        // initialize the incoming batchLoaders
        final UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
        try {
          batchLoaders[i].load(rbd, batch.getBody());
          // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
          // SchemaChangeException, so check/clean catch clause below.
        } catch(final SchemaChangeException e) {
          logger.error("MergingReceiver failed to load record batch from remote host.  {}", e);
          context.fail(e);
          return IterOutcome.STOP;
        }
        batch.release();
        ++batchOffsets[i];
        ++i;
      }

      // after this point all batches have been released and their bytebuf are in batchLoaders

      // Canonicalize each incoming batch, so that vectors are alphabetically sorted based on SchemaPath.
      for (final RecordBatchLoader loader : batchLoaders) {
        loader.canonicalize();
      }

      // Ensure all the incoming batches have the identical schema.
      if (!isSameSchemaAmongBatches(batchLoaders)) {
        context.fail(new SchemaChangeException("Incoming batches for merging receiver have different schemas!"));
        return IterOutcome.STOP;
      }

      // create the outgoing schema and vector container, and allocate the initial batch
      final SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      for (final VectorWrapper<?> v : batchLoaders[0]) {

        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        outgoingContainer.addOrGet(v.getField());
      }
      allocateOutgoing();

      outgoingContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      // generate code for merge operations (copy and compare)
      try {
        merger = createMerger();
      } catch (final SchemaChangeException e) {
        logger.error("Failed to generate code for MergingReceiver.  {}", e);
        context.fail(e);
        return IterOutcome.STOP;
      }

      // allocate the priority queue with the generated comparator
      this.pqueue = new PriorityQueue<>(fragProviders.length, new Comparator<Node>() {
        @Override
        public int compare(final Node node1, final Node node2) {
          final int leftIndex = (node1.batchId << 16) + node1.valueIndex;
          final int rightIndex = (node2.batchId << 16) + node2.valueIndex;
          return merger.doEval(leftIndex, rightIndex);
        }
      });

      // populate the priority queue with initial values
      for (int b = 0; b < senderCount; ++b) {
        while (batchLoaders[b] != null && batchLoaders[b].getRecordCount() == 0) {
          try {
            final RawFragmentBatch batch = getNext(b);
            incomingBatches[b] = batch;
            if (batch != null) {
              batchLoaders[b].load(batch.getHeader().getDef(), batch.getBody());
            } else {
              batchLoaders[b].clear();
              batchLoaders[b] = null;
              if (!context.shouldContinue()) {
                return IterOutcome.STOP;
              }
            }
          } catch (IOException | SchemaChangeException e) {
            context.fail(e);
            return IterOutcome.STOP;
          }
        }
        if (batchLoaders[b] != null) {
          pqueue.add(new Node(b, 0));
        }
      }

      hasRun = true;
      // finished lazy initialization
    }

    while (!pqueue.isEmpty()) {
      // pop next value from pq and copy to outgoing batch
      final Node node = pqueue.peek();
      if (!copyRecordToOutgoingBatch(node)) {
        logger.debug("Outgoing vectors space is full; breaking");
        prevBatchWasFull = true;
      }
      pqueue.poll();

      if (node.valueIndex == batchLoaders[node.batchId].getRecordCount() - 1) {
        // reached the end of an incoming record batch
        RawFragmentBatch nextBatch;
        try {
          nextBatch = getNext(node.batchId);

          while (nextBatch != null && nextBatch.getHeader().getDef().getRecordCount() == 0) {
            nextBatch = getNext(node.batchId);
          }

          assert nextBatch != null || inputCounts[node.batchId] == outputCounts[node.batchId]
              : String.format("Stream %d input count: %d output count %d", node.batchId, inputCounts[node.batchId], outputCounts[node.batchId]);
          if (nextBatch == null && !context.shouldContinue()) {
            return IterOutcome.STOP;
          }
        } catch (final IOException e) {
          context.fail(e);
          return IterOutcome.STOP;
        }

        incomingBatches[node.batchId] = nextBatch;

        if (nextBatch == null) {
          // batch is empty
          boolean allBatchesEmpty = true;

          for (final RawFragmentBatch batch : incomingBatches) {
            // see if all batches are empty so we can return OK_* or NONE
            if (batch != null) {
              allBatchesEmpty = false;
              break;
            }
          }

          if (allBatchesEmpty) {
            hasMoreIncoming = false;
            break;
          }

          // this batch is empty; since the pqueue no longer references this batch, it will be
          // ignored in subsequent iterations.
          if (prevBatchWasFull) {
            break;
          } else {
            continue;
          }
        }

        final UserBitShared.RecordBatchDef rbd = incomingBatches[node.batchId].getHeader().getDef();
        try {
          batchLoaders[node.batchId].load(rbd, incomingBatches[node.batchId].getBody());
          // TODO:  Clean:  DRILL-2933:  That load(...) no longer throws
          // SchemaChangeException, so check/clean catch clause below.
        } catch(final SchemaChangeException ex) {
          context.fail(ex);
          return IterOutcome.STOP;
        }
        incomingBatches[node.batchId].release();
        batchOffsets[node.batchId] = 0;

        // add front value from batch[x] to priority queue
        if (batchLoaders[node.batchId].getRecordCount() != 0) {
          pqueue.add(new Node(node.batchId, 0));
        }

      } else {
        pqueue.add(new Node(node.batchId, node.valueIndex + 1));
      }

      if (prevBatchWasFull) {
        break;
      }
    }

    // set the value counts in the outgoing vectors
    for (final VectorWrapper vw : outgoingContainer) {
      vw.getValueVector().getMutator().setValueCount(outgoingPosition);
    }

    if (pqueue.isEmpty()) {
      state = BatchState.DONE;
    }

    if (schemaChanged) {
      return IterOutcome.OK_NEW_SCHEMA;
    }
    else {
      return IterOutcome.OK;
    }
  }

  @Override
  public FragmentContext getContext() {
    return context;
  }

  @Override
  public BatchSchema getSchema() {
    return outgoingContainer.getSchema();
  }

  @Override
  public void buildSchema() throws SchemaChangeException {
    // find frag provider that has data to use to build schema, and put in tempBatchHolder for later use
    tempBatchHolder = new RawFragmentBatch[fragProviders.length];
    int i = 0;
    try {
      while (true) {
        if (i >= fragProviders.length) {
          state = BatchState.DONE;
          return;
        }
        final RawFragmentBatch batch = getNext(i);
        if (batch == null) {
          if (!context.shouldContinue()) {
            state = BatchState.STOP;
          } else {
            state = BatchState.DONE;
          }

          break;
        }
        if (batch.getHeader().getDef().getFieldCount() == 0) {
          i++;
          continue;
        }
        tempBatchHolder[i] = batch;
        for (final SerializedField field : batch.getHeader().getDef().getFieldList()) {
          final ValueVector v = outgoingContainer.addOrGet(MaterializedField.create(field));
          v.allocateNew();
        }
        break;
      }
    } catch (final IOException e) {
      throw new DrillRuntimeException(e);
    }
    outgoingContainer = VectorContainer.canonicalize(outgoingContainer);
    outgoingContainer.buildSchema(SelectionVectorMode.NONE);
  }

  @Override
  public int getRecordCount() {
    return outgoingPosition;
  }

  @Override
  public void kill(final boolean sendUpstream) {
    if (sendUpstream) {
      informSenders();
    } else {
      close();
    }

    for (final RawFragmentBatchProvider provider : fragProviders) {
      provider.kill(context);
    }
  }

  private void informSenders() {
    logger.info("Informing senders of request to terminate sending.");
    final FragmentHandle handlePrototype = FragmentHandle.newBuilder()
            .setMajorFragmentId(config.getOppositeMajorFragmentId())
            .setQueryId(context.getHandle().getQueryId())
            .build();
    for (final MinorFragmentEndpoint providingEndpoint : config.getProvidingEndpoints()) {
      final FragmentHandle sender = FragmentHandle.newBuilder(handlePrototype)
              .setMinorFragmentId(providingEndpoint.getId())
              .build();
      final FinishedReceiver finishedReceiver = FinishedReceiver.newBuilder()
              .setReceiver(context.getHandle())
              .setSender(sender)
              .build();
      context.getControlTunnel(providingEndpoint.getEndpoint()).informReceiverFinished(new OutcomeListener(), finishedReceiver);
    }
  }

  // TODO: Code duplication. UnorderedReceiverBatch has the same implementation.
  private class OutcomeListener implements RpcOutcomeListener<Ack> {

    @Override
    public void failed(final RpcException ex) {
      logger.warn("Failed to inform upstream that receiver is finished");
    }

    @Override
    public void success(final Ack value, final ByteBuf buffer) {
      // Do nothing
    }

    @Override
    public void interrupted(final InterruptedException e) {
      if (context.shouldContinue()) {
        final String errMsg = "Received an interrupt RPC outcome while sending ReceiverFinished message";
        logger.error(errMsg, e);
        context.fail(new RpcException(errMsg, e));
      }
    }
  }

  @Override
  protected void killIncoming(final boolean sendUpstream) {
    //No op
  }

  @Override
  public Iterator<VectorWrapper<?>> iterator() {
    return outgoingContainer.iterator();
  }

  @Override
  public SelectionVector2 getSelectionVector2() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SelectionVector4 getSelectionVector4() {
    throw new UnsupportedOperationException();
  }

  @Override
  public TypedFieldId getValueVectorId(final SchemaPath path) {
    return outgoingContainer.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(final Class<?> clazz, final int... ids) {
    return outgoingContainer.getValueAccessorById(clazz, ids);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private boolean isSameSchemaAmongBatches(final RecordBatchLoader[] batchLoaders) {
    Preconditions.checkArgument(batchLoaders.length > 0, "0 batch is not allowed!");

    final BatchSchema schema = batchLoaders[0].getSchema();

    for (int i = 1; i < batchLoaders.length; i++) {
      if (!schema.equals(batchLoaders[i].getSchema())) {
        logger.error("Schemas are different. Schema 1 : " + schema + ", Schema 2: " + batchLoaders[i].getSchema() );
        return false;
      }
    }
    return true;
  }

  private void allocateOutgoing() {
    for (final VectorWrapper w : outgoingContainer) {
      final ValueVector v = w.getValueVector();
      if (v instanceof FixedWidthVector) {
        AllocationHelper.allocate(v, OUTGOING_BATCH_SIZE, 1);
      } else {
        v.allocateNewSafe();
      }
    }
  }

//  private boolean isOutgoingFull() {
//    return outgoingPosition == DEFAULT_ALLOC_RECORD_COUNT;
//  }

  /**
   * Creates a generate class which implements the copy and compare methods.
   *
   * @return instance of a new merger based on generated code
   * @throws SchemaChangeException
   */
  private MergingReceiverGeneratorBase createMerger() throws SchemaChangeException {

    try {
      final CodeGenerator<MergingReceiverGeneratorBase> cg = CodeGenerator.get(MergingReceiverGeneratorBase.TEMPLATE_DEFINITION, context.getFunctionRegistry(), context.getOptions());
      final ClassGenerator<MergingReceiverGeneratorBase> g = cg.getRoot();

      ExpandableHyperContainer batch = null;
      boolean first = true;
      for (final RecordBatchLoader loader : batchLoaders) {
        if (first) {
          batch = new ExpandableHyperContainer(loader);
          first = false;
        } else {
          batch.addBatch(loader);
        }
      }

      generateComparisons(g, batch);

      g.setMappingSet(COPIER_MAPPING_SET);
      CopyUtil.generateCopies(g, batch, true);
      g.setMappingSet(MAIN_MAPPING);
      final MergingReceiverGeneratorBase merger = context.getImplementationClass(cg);

      merger.doSetup(context, batch, outgoingContainer);
      return merger;
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException(e);
    }
  }

  public final MappingSet MAIN_MAPPING = new MappingSet( (String) null, null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet LEFT_MAPPING = new MappingSet("leftIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  public final MappingSet RIGHT_MAPPING = new MappingSet("rightIndex", null, ClassGenerator.DEFAULT_SCALAR_MAP, ClassGenerator.DEFAULT_SCALAR_MAP);
  GeneratorMapping COPIER_MAPPING = new GeneratorMapping("doSetup", "doCopy", null, null);
  public final MappingSet COPIER_MAPPING_SET = new MappingSet(COPIER_MAPPING, COPIER_MAPPING);

  private void generateComparisons(final ClassGenerator<?> g, final VectorAccessible batch) throws SchemaChangeException {
    g.setMappingSet(MAIN_MAPPING);

    for (final Ordering od : popConfig.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      final ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector,context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(LEFT_MAPPING);
      final HoldingContainer left = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(RIGHT_MAPPING);
      final HoldingContainer right = g.addExpr(expr, ClassGenerator.BlkCreateMode.FALSE);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      final LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      final HoldingContainer out = g.addExpr(fh, ClassGenerator.BlkCreateMode.FALSE);
      final JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if (od.getDirection() == Direction.ASCENDING) {
        jc._then()._return(out.getValue());
      } else {
        jc._then()._return(out.getValue().minus());
      }
    }

    g.getEvalBlock()._return(JExpr.lit(0));
  }

  /**
   * Copy the record referenced by the supplied node to the next output position.
   * Side Effect: increments outgoing position if successful
   *
   * @param node Reference to the next record to copy from the incoming batches
   */
  private boolean copyRecordToOutgoingBatch(final Node node) {
    assert ++outputCounts[node.batchId] <= inputCounts[node.batchId]
        : String.format("Stream %d input count: %d output count %d", node.batchId, inputCounts[node.batchId], outputCounts[node.batchId]);
    final int inIndex = (node.batchId << 16) + node.valueIndex;
    merger.doCopy(inIndex, outgoingPosition);
    outgoingPosition++;
    if (outgoingPosition == OUTGOING_BATCH_SIZE) {
      return false;
    }
    return true;
  }

  /**
   * A Node contains a reference to a single value in a specific incoming batch.  It is used
   * as a wrapper for the priority queue.
   */
  public class Node {
    public int batchId;      // incoming batch
    public int valueIndex;   // value within the batch
    Node(final int batchId, final int valueIndex) {
      this.batchId = batchId;
      this.valueIndex = valueIndex;
    }
  }

  @Override
  public void close() {
    outgoingContainer.clear();
    if (batchLoaders != null) {
      for (final RecordBatchLoader rbl : batchLoaders) {
        if (rbl != null) {
          rbl.clear();
        }
      }
    }
    super.close();
  }

}
