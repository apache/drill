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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

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
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.AbstractRecordBatch;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.BatchSchema.SelectionVectorMode;
import org.apache.drill.exec.record.ExpandableHyperContainer;
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
import org.apache.drill.exec.vector.CopyUtil;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;
import org.eigenbase.rel.RelFieldCollation.Direction;

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

  private RecordBatchLoader[] batchLoaders;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;
  private VectorContainer outgoingContainer;
  private MergingReceiverGeneratorBase merger;
  private boolean hasRun = false;
  private boolean prevBatchWasFull = false;
  private boolean hasMoreIncoming = true;

  private int outgoingPosition = 0;
  private int senderCount = 0;
  private RawFragmentBatch[] incomingBatches;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;
  private RawFragmentBatch emptyBatch = null;
  private boolean done = false;

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

    super(config, context);
    this.fragProviders = fragProviders;
    this.context = context;
    this.outgoingContainer = new VectorContainer();
    this.stats.setLongStat(Metric.NUM_SENDERS, config.getNumSenders());
  }

  private RawFragmentBatch getNext(RawFragmentBatchProvider provider) throws IOException{
    stats.startWait();
    try {
      RawFragmentBatch b = provider.getNext();
      if(b != null){
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
    if (done) {
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
      boolean firstBatch = true;
      for (RawFragmentBatchProvider provider : fragProviders) {
        RawFragmentBatch rawBatch = null;
        try {
          rawBatch = getNext(provider);
        } catch (IOException e) {
          context.fail(e);
          return IterOutcome.STOP;
        }
        if (rawBatch.getHeader().getDef().getRecordCount() != 0) {
          rawBatches.add(rawBatch);
        } else {
          if (emptyBatch == null) {
            emptyBatch = rawBatch;
          }
          try {
            while ((rawBatch = getNext(provider)) != null && rawBatch.getHeader().getDef().getRecordCount() == 0);
          } catch (IOException e) {
            context.fail(e);
            return IterOutcome.STOP;
          }
          if (rawBatch != null) {
            rawBatches.add(rawBatch);
          }
        }
      }

      // allocate the incoming record batch loaders
      senderCount = rawBatches.size();
      if (senderCount == 0) {
        if (firstBatch) {
          RecordBatchLoader loader = new RecordBatchLoader(oContext.getAllocator());
          try {
            loader.load(emptyBatch.getHeader().getDef(), emptyBatch.getBody());
          } catch (SchemaChangeException e) {
            throw new RuntimeException(e);
          }
          for (VectorWrapper w : loader) {
            outgoingContainer.add(w.getValueVector());
          }
          outgoingContainer.buildSchema(SelectionVectorMode.NONE);
          done = true;
          return IterOutcome.OK_NEW_SCHEMA;
        }
        return IterOutcome.NONE;
      }
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
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), oContext.getAllocator());
        outgoingVector.allocateNew();
        outgoingContainer.add(outgoingVector);
        ++vectorCount;
      }


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
      for (int b = 0; b < senderCount; ++b)
        pqueue.add(new Node(b, 0));

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
        if (batchLoaders[node.batchId].getRecordCount() != 0)
          pqueue.add(new Node(node.batchId, 0));

      } else {
        pqueue.add(new Node(node.batchId, node.valueIndex + 1));
      }

      if (prevBatchWasFull) break;
    }

    // set the value counts in the outgoing vectors
    for (VectorWrapper vw : outgoingContainer)
      vw.getValueVector().getMutator().setValueCount(outgoingPosition);

    if (schemaChanged)
      return IterOutcome.OK_NEW_SCHEMA;
    else
      return IterOutcome.OK;
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
  public int getRecordCount() {
    return outgoingPosition;
  }

  @Override
  public void kill() {
    cleanup();
    for (RawFragmentBatchProvider provider : fragProviders) {
      provider.kill(context);
    }
  }

  @Override
  protected void killIncoming() {
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
    outgoingContainer.allocateNew();
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

    for(Ordering od : popConfig.getOrderings()){
      // first, we rewrite the evaluation stack for each side of the comparison.
      ErrorCollector collector = new ErrorCollectorImpl();
      final LogicalExpression expr = ExpressionTreeMaterializer.materialize(od.getExpr(), batch, collector,context.getFunctionRegistry());
      if(collector.hasErrors()) throw new SchemaChangeException("Failure while materializing expression. " + collector.toErrorString());
      g.setMappingSet(LEFT_MAPPING);
      HoldingContainer left = g.addExpr(expr, false);
      g.setMappingSet(RIGHT_MAPPING);
      HoldingContainer right = g.addExpr(expr, false);
      g.setMappingSet(MAIN_MAPPING);

      // next we wrap the two comparison sides and add the expression block for the comparison.
      LogicalExpression fh = FunctionGenerationHelper.getComparator(left, right, context.getFunctionRegistry());
      HoldingContainer out = g.addExpr(fh, false);
      JConditional jc = g.getEvalBlock()._if(out.getValue().ne(JExpr.lit(0)));

      if(od.getDirection() == Direction.ASCENDING){
        jc._then()._return(out.getValue());
      }else{
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
    if (!merger.doCopy(inIndex, outgoingPosition)) {
      return false;
    } else {
      outgoingPosition++;
      return true;
    }
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
      for(RecordBatchLoader rbl : batchLoaders){
        rbl.clear();
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