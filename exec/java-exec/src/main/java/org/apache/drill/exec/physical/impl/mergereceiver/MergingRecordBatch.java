package org.apache.drill.exec.physical.impl.mergereceiver;

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

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order.Ordering;
import org.apache.drill.exec.compile.sig.GeneratorMapping;
import org.apache.drill.exec.compile.sig.MappingSet;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.ClassGenerator.HoldingContainer;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.fn.FunctionGenerationHelper;
import org.apache.drill.exec.memory.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.MetricDef;
import org.apache.drill.exec.ops.OperatorContext;
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
import org.apache.drill.exec.vector.AllocationHelper;
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.FixedWidthVector;
import org.apache.drill.exec.vector.ValueVector;
import org.eigenbase.rel.RelFieldCollation.Direction;
import org.eigenbase.rel.RelFieldCollation.NullDirection;

import parquet.Preconditions;

import com.google.common.collect.Lists;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;


/**
 * The MergingRecordBatch merges pre-sorted record batches from remote senders.
 */
public class MergingRecordBatch extends AbstractRecordBatch<MergingReceiverPOP> implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);

  private static final long ALLOCATOR_INITIAL_RESERVATION = 1*1024*1024;
  private static final long ALLOCATOR_MAX_RESERVATION = 20L*1000*1000*1000;
  private static final int OUTGOING_BATCH_SIZE = 32 * 1024;

  private RecordBatchLoader[] batchLoaders;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;
  private VectorContainer outgoingContainer;
  private MergingReceiverGeneratorBase merger;
  private MergingReceiverPOP config;
  private boolean hasRun = false;
  private boolean prevBatchWasFull = false;
  private boolean hasMoreIncoming = true;

  private int outgoingPosition = 0;
  private int senderCount = 0;
  private RawFragmentBatch[] incomingBatches;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;
  private RawFragmentBatch emptyBatch = null;
  private RawFragmentBatch[] tempBatchHolder; //

  public static enum Metric implements MetricDef{
    BYTES_RECEIVED,
    NUM_SENDERS,
    NEXT_WAIT_NANOS;

    @Override
    public int metricId() {
      return ordinal();
    }
  }

  public MergingRecordBatch(FragmentContext context,
                            MergingReceiverPOP config,
                            RawFragmentBatchProvider[] fragProviders) throws OutOfMemoryException {
    super(config, context, true, new OperatorContext(config, context, false));
    //super(config, context);
    this.fragProviders = fragProviders;
    this.context = context;
    this.outgoingContainer = new VectorContainer(oContext);
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
    this.config = config;
  }

  private RawFragmentBatch getNext(RawFragmentBatchProvider provider) throws IOException{
    stats.startWait();
    try {
      RawFragmentBatch b = provider.getNext();
      if (b != null) {
        stats.addLongStat(Metric.BYTES_RECEIVED, b.getByteCount());
        stats.batchReceived(0, b.getHeader().getDef().getRecordCount(), false);
      }
      return b;
    } finally {
      stats.stopWait();
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

    if (hasMoreIncoming == false) {
      logger.debug("next() was called after all values have been processed");
      outgoingPosition = 0;
      return IterOutcome.NONE;
    }

    // lazy initialization
    if (!hasRun) {
      schemaChanged = true; // first iteration is always a schema change

      // set up each (non-empty) incoming record batch
      List<RawFragmentBatch> rawBatches = Lists.newArrayList();
      int p = 0;
      for (RawFragmentBatchProvider provider : fragProviders) {
        RawFragmentBatch rawBatch = null;
        try {
          // check if there is a batch in temp holder before calling getNext(), as it may have been used when building schema
          if (tempBatchHolder[p] != null) {
            rawBatch = tempBatchHolder[p];
            tempBatchHolder[p] = null;
          } else {
            rawBatch = getNext(provider);
          }
          p++;
          if (rawBatch == null && context.isCancelled()) {
            return IterOutcome.STOP;
          }
        } catch (IOException e) {
          context.fail(e);
          return IterOutcome.STOP;
        }
        if (rawBatch.getHeader().getDef().getRecordCount() != 0) {
          rawBatches.add(rawBatch);
        } else {
          // save an empty batch to use for schema purposes. ignore batch if it contains no fields, and thus no schema
          if (emptyBatch == null && rawBatch.getHeader().getDef().getFieldCount() != 0) {
            emptyBatch = rawBatch;
          }
          try {
            while ((rawBatch = getNext(provider)) != null && rawBatch.getHeader().getDef().getRecordCount() == 0) {
              ;
            }
            if (rawBatch == null && context.isCancelled()) {
              return IterOutcome.STOP;
            }
          } catch (IOException e) {
            context.fail(e);
            return IterOutcome.STOP;
          }
          if (rawBatch != null) {
            rawBatches.add(rawBatch);
          } else {
            rawBatches.add(emptyBatch);
          }
        }
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

      int i = 0;
      for (RawFragmentBatch batch : incomingBatches) {
        // initialize the incoming batchLoaders
        UserBitShared.RecordBatchDef rbd = batch.getHeader().getDef();
        try {
          batchLoaders[i].load(rbd, batch.getBody());
        } catch(SchemaChangeException e) {
          logger.error("MergingReceiver failed to load record batch from remote host.  {}", e);
          context.fail(e);
          return IterOutcome.STOP;
        }
        batch.release();
        ++batchOffsets[i];
        ++i;
      }

      // Canonicalize each incoming batch, so that vectors are alphabetically sorted based on SchemaPath.
      for (RecordBatchLoader loader : batchLoaders) {
        loader.canonicalize();
      }

      // Ensure all the incoming batches have the identical schema.
      if (!isSameSchemaAmongBatches(batchLoaders)) {
        logger.error("Incoming batches for merging receiver have diffferent schemas!");
        context.fail(new SchemaChangeException("Incoming batches for merging receiver have diffferent schemas!"));
        return IterOutcome.STOP;
      }

      // create the outgoing schema and vector container, and allocate the initial batch
      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      int vectorCount = 0;
      for (VectorWrapper<?> v : batchLoaders[0]) {

        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        ValueVector outgoingVector = outgoingContainer.addOrGet(v.getField());
        ++vectorCount;
      }
      allocateOutgoing();


      schema = bldr.build();
      if (schema != null && !schema.equals(schema)) {
        // TODO: handle case where one or more batches implicitly indicate schema change
        logger.debug("Initial state has incoming batches with different schemas");
      }
      outgoingContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      // generate code for merge operations (copy and compare)
      try {
        merger = createMerger();
      } catch (SchemaChangeException e) {
        logger.error("Failed to generate code for MergingReceiver.  {}", e);
        context.fail(e);
        return IterOutcome.STOP;
      }

      // allocate the priority queue with the generated comparator
      this.pqueue = new PriorityQueue<Node>(fragProviders.length, new Comparator<Node>() {
        public int compare(Node node1, Node node2) {
          int leftIndex = (node1.batchId << 16) + node1.valueIndex;
          int rightIndex = (node2.batchId << 16) + node2.valueIndex;
          return merger.doEval(leftIndex, rightIndex);
        }
      });

      // populate the priority queue with initial values
      for (int b = 0; b < senderCount; ++b) {
        while (batchLoaders[b] != null && batchLoaders[b].getRecordCount() == 0) {
          try {
            RawFragmentBatch batch = getNext(fragProviders[b]);
            incomingBatches[b] = batch;
            if (batch != null) {
              batchLoaders[b].load(batch.getHeader().getDef(), batch.getBody());
            } else {
              batchLoaders[b].clear();
              batchLoaders[b] = null;
              if (context.isCancelled()) {
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
      Node node = pqueue.peek();
      if (!copyRecordToOutgoingBatch(node)) {
        logger.debug("Outgoing vectors space is full; breaking");
        prevBatchWasFull = true;
        break;
      }
      pqueue.poll();

//      if (isOutgoingFull()) {
//        // set a flag so that we reallocate on the next iteration
//        logger.debug("Outgoing vectors record batch size reached; breaking");
//        prevBatchWasFull = true;
//      }

      if (node.valueIndex == batchLoaders[node.batchId].getRecordCount() - 1) {
        // reached the end of an incoming record batch
        RawFragmentBatch nextBatch = null;
        try {
          nextBatch = getNext(fragProviders[node.batchId]);

          while (nextBatch != null && nextBatch.getHeader().getDef().getRecordCount() == 0) {
            nextBatch = getNext(fragProviders[node.batchId]);
          }
          if (nextBatch == null && context.isCancelled()) {
            return IterOutcome.STOP;
          }
        } catch (IOException e) {
          context.fail(e);
          return IterOutcome.STOP;
        }

        incomingBatches[node.batchId] = nextBatch;

        if (nextBatch == null) {
          // batch is empty
          boolean allBatchesEmpty = true;

          for (RawFragmentBatch batch : incomingBatches) {
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
          continue;
        }

        UserBitShared.RecordBatchDef rbd = incomingBatches[node.batchId].getHeader().getDef();
        try {
          batchLoaders[node.batchId].load(rbd, incomingBatches[node.batchId].getBody());
        } catch(SchemaChangeException ex) {
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
    for (VectorWrapper vw : outgoingContainer) {
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

  public void buildSchema() {
    // find frag provider that has data to use to build schema, and put in tempBatchHolder for later use
    tempBatchHolder = new RawFragmentBatch[fragProviders.length];
    int i = 0;
    try {
      while (true) {
        if (i >= fragProviders.length) {
          state = BatchState.DONE;
          return;
        }
        RawFragmentBatch batch = getNext(fragProviders[i]);
        if (batch.getHeader().getDef().getFieldCount() == 0) {
          i++;
          continue;
        }
        tempBatchHolder[i] = batch;
        for (SerializedField field : batch.getHeader().getDef().getFieldList()) {
          ValueVector v = outgoingContainer.addOrGet(MaterializedField.create(field));
          v.allocateNew();
        }
        break;
      }
    } catch (IOException e) {
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
  public void kill(boolean sendUpstream) {
    if (sendUpstream) {
      informSenders();
    } else {
      cleanup();
      for (RawFragmentBatchProvider provider : fragProviders) {
        provider.kill(context);
      }
    }
  }

  private void informSenders() {
    FragmentHandle handlePrototype = FragmentHandle.newBuilder()
            .setMajorFragmentId(config.getOppositeMajorFragmentId())
            .setQueryId(context.getHandle().getQueryId())
            .build();
    for (int i = 0; i < config.getNumSenders(); i++) {
      FragmentHandle sender = FragmentHandle.newBuilder(handlePrototype)
              .setMinorFragmentId(i)
              .build();
      FinishedReceiver finishedReceiver = FinishedReceiver.newBuilder()
              .setReceiver(context.getHandle())
              .setSender(sender)
              .build();
      context.getControlTunnel(config.getProvidingEndpoints().get(i)).informReceiverFinished(new OutcomeListener(), finishedReceiver);
    }
  }

  private class OutcomeListener implements RpcOutcomeListener<Ack> {

    @Override
    public void failed(RpcException ex) {
      logger.warn("Failed to inform upstream that receiver is finished");
    }

    @Override
    public void success(Ack value, ByteBuf buffer) {
      // Do nothing
    }
  }

  @Override
  protected void killIncoming(boolean sendUpstream) {
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
  public TypedFieldId getValueVectorId(SchemaPath path) {
    return outgoingContainer.getValueVectorId(path);
  }

  @Override
  public VectorWrapper<?> getValueAccessorById(Class<?> clazz, int... ids) {
    return outgoingContainer.getValueAccessorById(clazz, ids);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private boolean isSameSchemaAmongBatches(RecordBatchLoader[] batchLoaders) {
    Preconditions.checkArgument(batchLoaders.length > 0, "0 batch is not allowed!");

    BatchSchema schema = batchLoaders[0].getSchema();

    for (int i = 1; i < batchLoaders.length; i++) {
      if (!schema.equals(batchLoaders[i].getSchema())) {
        logger.error("Schemas are different. Schema 1 : " + schema + ", Schema 2: " + batchLoaders[i].getSchema() );
        return false;
      }
    }
    return true;
  }

  private void allocateOutgoing() {
    for (VectorWrapper w : outgoingContainer) {
      ValueVector v = w.getValueVector();
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
      CodeGenerator<MergingReceiverGeneratorBase> cg = CodeGenerator.get(MergingReceiverGeneratorBase.TEMPLATE_DEFINITION, context.getFunctionRegistry());
      ClassGenerator<MergingReceiverGeneratorBase> g = cg.getRoot();

      ExpandableHyperContainer batch = null;
      boolean first = true;
      for (RecordBatchLoader loader : batchLoaders) {
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
      MergingReceiverGeneratorBase merger = context.getImplementationClass(cg);

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

  private void generateComparisons(ClassGenerator g, VectorAccessible batch) throws SchemaChangeException {
    g.setMappingSet(MAIN_MAPPING);

    for (Ordering od : popConfig.getOrderings()) {
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector,context.getFunctionRegistry());
      if (collector.hasErrors()) {
        throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      }
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh =
          FunctionGenerationHelper.getOrderingComparator(od.nullsSortHigh(), left, right,
                                                         context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

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
  private boolean copyRecordToOutgoingBatch(Node node) {
    int inIndex = (node.batchId << 16) + node.valueIndex;
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
    Node(int batchId, int valueIndex) {
      this.batchId = batchId;
      this.valueIndex = valueIndex;
    }
  }

  @Override
  public void cleanup() {
    outgoingContainer.clear();
    if (batchLoaders != null) {
      for (RecordBatchLoader rbl : batchLoaders) {
        if (rbl != null) {
          rbl.clear();
        }
      }
    }
    oContext.close();
    if (fragProviders != null) {
      for (RawFragmentBatchProvider f : fragProviders) {
        f.cleanup();
      }
    }
  }

}
