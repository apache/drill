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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sun.codemodel.JArray;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.defs.OrderDef;
import org.apache.drill.common.expression.ErrorCollector;
import org.apache.drill.common.expression.ErrorCollectorImpl;
import org.apache.drill.common.expression.ExpressionPosition;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.data.Order;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.exception.ClassTransformationException;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.CodeGenerator;
import org.apache.drill.exec.expr.ExpressionTreeMaterializer;
import org.apache.drill.exec.expr.HoldingContainerExpression;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.expr.ValueVectorReadExpression;
import org.apache.drill.exec.expr.fn.impl.ComparatorFunctions;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.config.MergingReceiverPOP;
import org.apache.drill.exec.proto.UserBitShared;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.RawFragmentBatch;
import org.apache.drill.exec.record.RawFragmentBatchProvider;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.record.SchemaBuilder;
import org.apache.drill.exec.record.TypedFieldId;
import org.apache.drill.exec.record.VectorContainer;
import org.apache.drill.exec.record.VectorWrapper;
import org.apache.drill.exec.record.WritableBatch;
import org.apache.drill.exec.record.selection.SelectionVector2;
import org.apache.drill.exec.record.selection.SelectionVector4;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.allocator.VectorAllocator;

import java.io.IOException;
import java.lang.Class;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;


/**
 * The MergingRecordBatch merges pre-sorted record batches from remote senders.
 */
public class MergingRecordBatch implements RecordBatch {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MergingRecordBatch.class);

  private RecordBatchLoader[] batchLoaders;
  private RawFragmentBatchProvider[] fragProviders;
  private FragmentContext context;
  private BatchSchema schema;
  private VectorContainer outgoingContainer;
  private MergingReceiverGeneratorBase merger;
  private boolean hasRun = false;
  private boolean prevBatchWasFull = false;
  private boolean hasMoreIncoming = true;
  private final int DEFAULT_ALLOC_RECORD_COUNT = 20000;

  private int outgoingPosition = 0;
  private int senderCount = 0;
  private RawFragmentBatch[] incomingBatches;
  private int[] batchOffsets;
  private PriorityQueue <Node> pqueue;
  private List<VectorAllocator> allocators;
  private MergingReceiverPOP config;

  public MergingRecordBatch(FragmentContext context,
                            MergingReceiverPOP config,
                            RawFragmentBatchProvider[] fragProviders) {

    this.fragProviders = fragProviders;
    this.context = context;
    this.config = config;
    this.allocators = Lists.newArrayList();
    this.outgoingContainer = new VectorContainer();
  }

  @Override
  public IterOutcome next() {
    if (fragProviders.length == 0) return IterOutcome.NONE;
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
      for (RawFragmentBatchProvider provider : fragProviders) {
        RawFragmentBatch rawBatch = provider.getNext();
        if (rawBatch.getHeader().getDef().getRecordCount() != 0)
          rawBatches.add(rawBatch);
      }

      // allocate the incoming record batch loaders
      senderCount = rawBatches.size();
      incomingBatches = new RawFragmentBatch[senderCount];
      batchOffsets = new int[senderCount];
      batchLoaders = new RecordBatchLoader[senderCount];
      for (int i = 0; i < senderCount; ++i) {
        incomingBatches[i] = rawBatches.get(i);
        batchLoaders[i] = new RecordBatchLoader(context.getAllocator());
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

      // create the outgoing schema and vector container, and allocate the initial batch
      SchemaBuilder bldr = BatchSchema.newBuilder().setSelectionVectorMode(BatchSchema.SelectionVectorMode.NONE);
      int vectorCount = 0;
      for (VectorWrapper<?> v : batchLoaders[0]) {

        // add field to the output schema
        bldr.addField(v.getField());

        // allocate a new value vector
        ValueVector outgoingVector = TypeHelper.getNewVector(v.getField(), context.getAllocator());
        VectorAllocator allocator = VectorAllocator.getAllocator(v.getValueVector(), outgoingVector);
        allocator.alloc(DEFAULT_ALLOC_RECORD_COUNT);
        allocators.add(allocator);
        outgoingContainer.add(outgoingVector);
        ++vectorCount;
      }

      logger.debug("Allocating {} outgoing vectors with {} values", vectorCount, DEFAULT_ALLOC_RECORD_COUNT);

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
          return merger.doCompare(node1, node2);
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
      copyRecordToOutgoingBatch(pqueue.poll());

      if (isOutgoingFull()) {
        // set a flag so that we reallocate on the next iteration
        logger.debug("Outgoing vectors are full; breaking");
        prevBatchWasFull = true;
      }

      if (node.valueIndex == batchLoaders[node.batchId].getRecordCount() - 1) {
        // reached the end of an incoming record batch
        incomingBatches[node.batchId] = fragProviders[node.batchId].getNext();

        if (incomingBatches[node.batchId].getHeader().getIsLastBatch() ||
            incomingBatches[node.batchId].getHeader().getDef().getRecordCount() == 0) {
          // batch is empty
          incomingBatches[node.batchId].release();
          boolean allBatchesEmpty = true;

          for (RawFragmentBatch batch : incomingBatches) {
            // see if all batches are empty so we can return OK_* or NONE
            if (!batch.getHeader().getIsLastBatch()) {
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
    return schema;
  }

  @Override
  public int getRecordCount() {
    return outgoingPosition;
  }

  @Override
  public void kill() {
    for (RawFragmentBatchProvider provider : fragProviders) {
      provider.kill(context);
    }
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
  public VectorWrapper<?> getValueAccessorById(int fieldId, Class<?> clazz) {
    return outgoingContainer.getValueAccessorById(fieldId, clazz);
  }

  @Override
  public WritableBatch getWritableBatch() {
    return WritableBatch.get(this);
  }

  private void allocateOutgoing() {
    for (VectorAllocator allocator : allocators) {
      allocator.alloc(DEFAULT_ALLOC_RECORD_COUNT);
    }
  }

  private boolean isOutgoingFull() {
    return outgoingPosition == DEFAULT_ALLOC_RECORD_COUNT;
  }

  /**
   * Creates a generate class which implements the copy and compare methods.
   * 
   * @return instance of a new merger based on generated code
   * @throws SchemaChangeException
   */
  private MergingReceiverGeneratorBase createMerger() throws SchemaChangeException {

    // set up the expression evaluator and code generation
    final List<OrderDef> orderings = config.getOrderings();
    final ErrorCollector collector = new ErrorCollectorImpl();
    final CodeGenerator<MergingReceiverGeneratorBase> cg =
        new CodeGenerator<>(MergingReceiverGeneratorBase.TEMPLATE_DEFINITION, context.getFunctionRegistry());
    JExpression inIndex = JExpr.direct("inIndex");

    JType valueVector2DArray = cg.getModel().ref(ValueVector.class).array().array();
    JType valueVectorArray = cg.getModel().ref(ValueVector.class).array();
    JType incomingBatchesType = cg.getModel().ref(RecordBatchLoader.class).array();
    JType incomingBatchType = cg.getModel().ref(RecordBatchLoader.class);
    JType recordBatchType = cg.getModel().ref(RecordBatch.class);

    // declare a two-dimensional array of value vectors; batch is first dimension, ValueVectorId is the second
    JVar incomingVectors = cg.clazz.field(JMod.NONE,
      valueVector2DArray,
      "incomingVectors");

    // declare a two-dimensional array of vectors used to store a reference to all ValueVectors
    // used in a comparison operation.  first dimension is the batch id.  each batch has one or more
    // comparison vectors, maintaining the order defined by the OrderDef.
    JVar comparisonVectors = cg.clazz.field(JMod.NONE,
      valueVector2DArray,
      "comparisonVectors");

    // declare an array of incoming batches
    JVar incomingBatchesVar = cg.clazz.field(JMod.NONE,
      incomingBatchesType,
      "incomingBatches");

    // declare an array of outgoing vectors
    JVar outgoingVectors = cg.clazz.field(JMod.NONE,
      valueVectorArray,
      "outgoingVectors");

    // declare a reference to this MergingRecordBatch
    JVar outgoingBatch = cg.clazz.field(JMod.NONE,
      recordBatchType,
      "outgoingBatch");

    // create aliases for materializer
    JVar incomingVar = cg.clazz.field(JMod.NONE, incomingBatchType, "incoming");
    cg.getSetupBlock().assign(incomingBatchesVar, JExpr.direct("incomingBatchLoaders"));
    cg.getSetupBlock().assign(outgoingBatch, JExpr.direct("outgoing"));

    cg.setMappingSet(MergingReceiverGeneratorBase.COMPARE_MAPPING);

    // evaluate expression on each incoming batch and create/initialize 2d array of incoming vectors.  For example:
    //     incomingVectors = new ValueVector[][] {
    //                         new ValueVector[] {vv1, vv2},
    //                         new ValueVector[] {vv3, vv4}
    //                       });
    int fieldsPerBatch = 0; // number of fields per batch
    int batchIdx = 0;
    JArray incomingVectorInit = JExpr.newArray(cg.getModel().ref(ValueVector.class).array());
    List <List<ValueVectorReadExpression>> cmpExpressions = Lists.newArrayList();
    for (RecordBatchLoader batch : batchLoaders) {
      JArray incomingVectorInitBatch = JExpr.newArray(cg.getModel().ref(ValueVector.class));
      int fieldIdx = 0;
      for (VectorWrapper<?> vv : batch) {
        // declare incoming value vector and assign it to the array
        JVar inVV = cg.declareVectorValueSetupAndMember("incomingBatches[" + batchIdx + "]",
          new TypedFieldId(vv.getField().getType(),
            fieldIdx,
            false));

        // add vv to initialization list (e.g. { vv1, vv2, vv3 } )
        incomingVectorInitBatch.add(inVV);
        ++fieldIdx;
      }

      // add VV array to initialization list (e.g. new ValueVector[] { ... })
      incomingVectorInit.add(incomingVectorInitBatch);

      // materialize each expression for this incoming batch
      for (int i = 0; i < orderings.size(); ++i) {
        cmpExpressions.add(new ArrayList<ValueVectorReadExpression>());
        cg.getSetupBlock().assign(incomingVar, JExpr.direct("incomingBatches[" + batchIdx + "]"));
        LogicalExpression exprForCurrentBatch = ExpressionTreeMaterializer.materialize(orderings.get(i).getExpr(),
                                                                                       batch,
                                                                                       collector);
        if (collector.hasErrors()) {
          throw new SchemaChangeException(
              String.format("Failure while trying to materialize incoming schema.  Errors:\n %s.",
                            collector.toErrorString()));
        }

        // add materialized field expression to comparison list
        if (exprForCurrentBatch instanceof ValueVectorReadExpression) {
          cmpExpressions.get(batchIdx).add((ValueVectorReadExpression) exprForCurrentBatch);
        }
        else {
          throw new SchemaChangeException("Invalid expression supplied to MergingReceiver operator");
        }
      }

      ++batchIdx;
      fieldsPerBatch = fieldIdx;
    }

    // write out the incoming vector initialization block
    cg.getSetupBlock().assign(incomingVectors, incomingVectorInit);

    // Generate the comparison function:
    // The generated code compares the fields defined in each logical expression.  The batch index
    // is supplied by the function caller (at runtime).  The comparison statements for each
    // expression are generated for each schema change.  Inequality checks (< and >) for each batch
    // are executed first to accommodate multiple expressions.  Equality is checked only for the last
    // expression, and only if all previous expressions are equal.  Expression order is applied
    // to the result of the FunctionCall.

    JArray comparisonVectorInit = JExpr.newArray(cg.getModel().ref(ValueVector.class).array());
    for (int b = 0; b < cmpExpressions.size(); ++b) {
      JArray comparisonVectorInitBatch = JExpr.newArray(cg.getModel().ref(ValueVector.class));

      for (ValueVectorReadExpression vvRead : cmpExpressions.get(b)) {
        TypeProtos.DataMode mode = vvRead.getMajorType().getMode();
        TypeProtos.MinorType minor = vvRead.getMajorType().getMinorType();
        Class cmpVectorClass = TypeHelper.getValueVectorClass(minor, mode);

        comparisonVectorInitBatch.add(
            ((JExpression) incomingBatchesVar.component(JExpr.lit(b)))
               .invoke("getValueAccessorById")
                 .arg(JExpr.lit(vvRead.getFieldId().getFieldId()))
                 .arg(cg.getModel()._ref(cmpVectorClass).boxify().dotclass())
                   .invoke("getValueVector"));

      }
      comparisonVectorInit.add(comparisonVectorInitBatch);
    }

    cg.getSetupBlock().assign(comparisonVectors, comparisonVectorInit);

    int comparisonVectorIndex = 0;
    for (ValueVectorReadExpression vvRead : cmpExpressions.get(0)) {
      // generate the comparison statements based on the first batch (assumes comparison fields are homogeneous)
      TypeProtos.DataMode mode = vvRead.getMajorType().getMode();
      TypeProtos.MinorType minor = vvRead.getMajorType().getMinorType();
      JType vectorType = cg.getModel()._ref(TypeHelper.getValueVectorClass(minor, mode));
      JType valueType = TypeHelper.getHolderType(cg.getModel(), minor, mode);

      // set up a holding container expression for left-hand side of function call
      JVar leftVar = cg.getEvalBlock().decl(valueType, "leftValue" + comparisonVectorIndex, JExpr._new(valueType));
      cg.getEvalBlock().add(((JExpression) JExpr.cast(vectorType,
                                ((JExpression) comparisonVectors
                                  .component(JExpr.direct("leftNode.batchId")))
                                  .component(JExpr.lit(comparisonVectorIndex))))
        .invoke("getAccessor")
        .invoke("get")
        .arg(JExpr.direct("leftNode.valueIndex"))
        .arg(leftVar));

      CodeGenerator.HoldingContainer left = new CodeGenerator.HoldingContainer(vvRead.getMajorType(),
                                                                               leftVar,
                                                                               leftVar.ref("value"),
                                                                               leftVar.ref("isSet"));

      // set up a holding container expression for right-hand side of function call
      JVar rightVar = cg.getEvalBlock().decl(valueType, "rightValue" + comparisonVectorIndex, JExpr._new(valueType));
      cg.getEvalBlock().add(((JExpression) JExpr.cast(vectorType,
                                ((JExpression) comparisonVectors
                                    .component(JExpr.direct("rightNode.batchId")))
                                    .component(JExpr.lit(comparisonVectorIndex))))
        .invoke("getAccessor")
        .invoke("get")
        .arg(JExpr.direct("rightNode.valueIndex"))
        .arg(rightVar));

      CodeGenerator.HoldingContainer right = new CodeGenerator.HoldingContainer(vvRead.getMajorType(),
                                                                                rightVar,
                                                                                rightVar.ref("value"),
                                                                                rightVar.ref("isSet"));

      // generate the comparison function
      FunctionCall f = new FunctionCall(ComparatorFunctions.COMPARE_TO,
                                        ImmutableList.of((LogicalExpression) new HoldingContainerExpression(left),
                                                                             new HoldingContainerExpression(right)),
                                        ExpressionPosition.UNKNOWN);
      CodeGenerator.HoldingContainer out = cg.addExpr(f, false);

      // generate less than/greater than checks (fixing results for ASCending vs. DESCending)
      cg.getEvalBlock()._if(out.getValue().eq(JExpr.lit(1)))
                       ._then()
                       ._return(JExpr.lit(config.getOrderings().get(comparisonVectorIndex).getDirection() == Order.Direction.ASC ? 1 : -1));

      cg.getEvalBlock()._if(out.getValue().eq(JExpr.lit(-1)))
                       ._then()
                       ._return(JExpr.lit(config.getOrderings().get(comparisonVectorIndex).getDirection() == Order.Direction.ASC ? -1 : 1));

      ++comparisonVectorIndex;
    }

    // if all expressions are equal, return 0
    cg.getEvalBlock()._return(JExpr.lit(0));

    // allocate a new array for outgoing vectors
    cg.getSetupBlock().assign(outgoingVectors, JExpr.newArray(cg.getModel().ref(ValueVector.class), fieldsPerBatch));

    // generate copy function and setup outgoing batches
    cg.setMappingSet(MergingReceiverGeneratorBase.COPY_MAPPING);
    int fieldIdx = 0;
    for (VectorWrapper<?> vvOut : outgoingContainer) {
      // declare outgoing value vectors
      JVar outgoingVV = cg.declareVectorValueSetupAndMember("outgoingBatch",
                                                            new TypedFieldId(vvOut.getField().getType(),
                                                                             fieldIdx,
                                                                             vvOut.isHyper()));

      // assign to the appropriate slot in the outgoingVector array (in order of iteration)
      cg.getSetupBlock().assign(outgoingVectors.component(JExpr.lit(fieldIdx)), outgoingVV);

      // get the vector's type info
      Class<?> vvType = TypeHelper.getValueVectorClass(vvOut.getField().getType().getMinorType(),
                                                       vvOut.getField().getType().getMode());
      JClass vvClass = cg.getModel().ref(vvType);

      // generate the copyFrom() invocation with explicit cast to the appropriate type; for example:
      // ((IntVector) outgoingVectors[i]).copyFrom(inIndex,
      //                                           outgoingBatch.getRecordCount(),
      //                                           (IntVector) vv1);
      cg.getEvalBlock().add(
        ((JExpression) JExpr.cast(vvClass, outgoingVectors.component(JExpr.lit(fieldIdx))))
          .invoke("copyFrom")
          .arg(inIndex)
          .arg(outgoingBatch.invoke("getRecordCount"))
          .arg(JExpr.cast(vvClass,
                          ((JExpression) incomingVectors.component(JExpr.direct("inBatch")))
                            .component(JExpr.lit(fieldIdx)))));

      ++fieldIdx;
    }

    // compile generated code and call the generated setup method
    MergingReceiverGeneratorBase newMerger;
    try {
      newMerger = context.getImplementationClass(cg);
      newMerger.doSetup(context, batchLoaders, this);
    } catch (ClassTransformationException | IOException e) {
      throw new SchemaChangeException("Failure while attempting to load generated class", e);
    }
    return newMerger;
  }

  /**
   * Copy the record referenced by the supplied node to the next output position.
   * Side Effect: increments outgoing position
   *
   * @param node Reference to the next record to copy from the incoming batches
   */
  private void copyRecordToOutgoingBatch(Node node) {
    merger.doCopy(node.batchId, node.valueIndex, outgoingPosition++);
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

}